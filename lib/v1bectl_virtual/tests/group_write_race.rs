//! #55: a group write goes through the sync engine, member by member, with
//! no gap a pull can revert it in.
//!
//! A group used to write its members into the store itself, and the
//! virtual device manager committed them through the engine after that. A
//! pull in between found the store ahead of the hub with nothing pending,
//! took it for an outside change and reverted the member (`GatewayWins`).
//! One that landed before the manager read the members back left them
//! uncommitted for good: reverted, and never pushed.
//!
//! Now a group only plans a write, and the manager commits each member
//! through the engine, which arms its protection window before it writes
//! the store. These tests hold a group write right there, where the group
//! has planned it and the manager hasn't committed any of it (where the
//! members used to be in the store already), and pull every member while
//! it's held.
//!
//! The rig is the sync engine's (`lib/v1bectl_sync/tests/common`): a fake
//! hub whose traffic is logged, and pulls on demand. The steps are ordered
//! on the hub's log and on the held write, never on sleeps.

#[path = "../../v1bectl_sync/tests/common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::{drain_events, json, off, OnSet, Pulls, Rig, TestHub, WAIT};
use tokio::sync::{mpsc, Semaphore};
use v1bectl_sync::{
    DeviceEvent, DeviceId, DeviceState, DeviceStateValue, LightState, StateStore, SyncConfig,
};
use v1bectl_virtual::{
    ButtonAction, LightGroup, LightGroupLinear, VirtualDevice, VirtualDeviceConfig,
    VirtualDeviceError, VirtualDeviceManager, VirtualDeviceType, VirtualWrite,
};

const GROUP: &str = "g";
const MEMBERS: [&str; 2] = ["a", "b"];

/// A virtual device that holds every write, once the device it wraps has
/// planned it, until the test lets it go. That's where the manager has the
/// write in hand but has committed none of it.
struct Held {
    inner: Box<dyn VirtualDevice>,
    /// Told each time a write is held.
    on_hold: mpsc::UnboundedSender<()>,
    /// A held write goes on for each permit.
    go: Arc<Semaphore>,
}

#[async_trait]
impl VirtualDevice for Held {
    fn device_id(&self) -> &DeviceId {
        self.inner.device_id()
    }

    fn device_type(&self) -> VirtualDeviceType {
        self.inner.device_type()
    }

    fn config(&self) -> &VirtualDeviceConfig {
        self.inner.config()
    }

    async fn plan_write(
        &self,
        new_state: DeviceStateValue,
    ) -> Result<VirtualWrite, VirtualDeviceError> {
        let planned = self.inner.plan_write(new_state).await;
        self.on_hold.send(()).expect("the test is waiting");
        self.go.acquire().await.expect("the gate").forget();
        planned
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        self.inner.take_state(state);
    }

    async fn seed_from_inputs(&mut self) -> Result<(), VirtualDeviceError> {
        self.inner.seed_from_inputs().await
    }

    async fn on_input_changed(
        &mut self,
        device_id: &DeviceId,
        new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        self.inner.on_input_changed(device_id, new_state).await
    }

    fn accounts_for(&self, input: &DeviceId, state: &DeviceStateValue) -> bool {
        self.inner.accounts_for(input, state)
    }

    fn reactions(&self, event: &DeviceEvent) -> Vec<ButtonAction> {
        self.inner.reactions(event)
    }

    fn current_state(&self) -> DeviceStateValue {
        self.inner.current_state()
    }

    fn input_devices(&self) -> Vec<DeviceId> {
        self.inner.input_devices()
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        self.inner.output_devices()
    }
}

/// Both group kinds had the old store-write path.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Curves,
    Linear,
}

fn config(device_type: VirtualDeviceType, config: serde_json::Value) -> VirtualDeviceConfig {
    VirtualDeviceConfig {
        device_id: GROUP.to_string(),
        device_type,
        name: GROUP.to_string(),
        description: None,
        enabled: true,
        config,
    }
}

/// Group `g` of `kind` over [`MEMBERS`], each at the group's own level: a
/// 1:1 curve, or a linear range over all of 0..=100.
fn group(kind: Kind, store: &Arc<StateStore>) -> Box<dyn VirtualDevice> {
    match kind {
        Kind::Curves => {
            let curves: serde_json::Map<String, serde_json::Value> = MEMBERS
                .iter()
                .map(|id| {
                    let curve = serde_json::json!({ "breakpoints": [[0, 0], [100, 100]] });
                    ((*id).to_string(), curve)
                })
                .collect();
            let config = config(
                VirtualDeviceType::LightGroup,
                serde_json::json!({ "lights": MEMBERS, "brightness_curves": curves }),
            );
            Box::new(LightGroup::new(config, Arc::clone(store)).expect("group"))
        }
        Kind::Linear => {
            let members: HashMap<String, String> = MEMBERS
                .iter()
                .map(|id| ((*id).to_string(), (*id).to_string()))
                .collect();
            let ranges = MEMBERS
                .iter()
                .map(|id| ((*id).to_string(), (0, 100)))
                .collect();
            let config = config(VirtualDeviceType::LightGroupLinear, serde_json::json!({}));
            Box::new(
                LightGroupLinear::new(config, members, ranges, Arc::clone(store))
                    .expect("linear group"),
            )
        }
    }
}

/// The group, and each member, on at 60. The members take the group's
/// colour temperature: the one it starts with, as none of them has one.
fn on_at_60() -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on: true,
        brightness: Some(60),
        color_temp: Some(2700),
        rgb_color: None,
    })
}

/// A group write held where the group has planned it and the manager hasn't
/// committed it yet, and a pull of each member right there. Every member is
/// off in the store and on the hub before the write.
///
/// With the old path the members were in the store already, and not
/// pending. The pull reverted each one and the manager, reading them back,
/// found nothing to commit: the store stayed off, and nothing was pushed.
/// Now the store is still off there, as the hub is, so the pull has nothing
/// to reconcile. Then each member is committed: stored, pushed, and never
/// echoed back to off.
async fn a_pull_in_the_old_gap_leaves_the_write_alone(kind: Kind, optimistic_updates: bool) {
    let case = format!("{kind:?}, optimistic_updates: {optimistic_updates}");
    let rig = Rig::new(
        &MEMBERS,
        TestHub::new(OnSet::Apply, false),
        Pulls::OnDemand,
        SyncConfig {
            optimistic_updates,
            ..SyncConfig::default()
        },
    )
    .await;
    let manager = VirtualDeviceManager::new(Arc::clone(&rig.store), Arc::clone(&rig.bus));
    manager.attach_sync_engine(Arc::new(rig.engine.clone()));
    let (on_hold, mut held) = mpsc::unbounded_channel();
    let go = Arc::new(Semaphore::new(0));
    manager
        .add_virtual_device(Box::new(Held {
            inner: group(kind, &rig.store),
            on_hold,
            go: Arc::clone(&go),
        }))
        .await
        .expect("register");
    let mut rx = rig.bus.subscribe();

    let write = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .set_virtual_device_state(&GROUP.to_string(), on_at_60())
                .await
        }
    });
    tokio::time::timeout(WAIT, held.recv())
        .await
        .unwrap_or_else(|_| panic!("{case}: the write was never held"))
        .expect("the held device");

    // Each pull is handled completely before `pull` returns.
    for id in MEMBERS {
        rig.pull(id).await;
    }
    go.add_permits(1);
    tokio::time::timeout(WAIT, write)
        .await
        .unwrap_or_else(|_| panic!("{case}: the write never finished"))
        .expect("write task")
        .unwrap_or_else(|e| panic!("{case}: the write failed: {e}"));

    for id in MEMBERS {
        assert_eq!(
            rig.stored(id).await,
            on_at_60(),
            "{case}: {id} was reverted, or never committed"
        );
    }
    for id in MEMBERS {
        rig.hub
            .wait(&format!("{case}: {id} pushed"), |log| {
                log.sets_for(id) == [on_at_60()]
            })
            .await;
    }
    let reverted: Vec<_> = drain_events(&mut rx)
        .into_iter()
        .filter(|(id, _, new_value)| MEMBERS.contains(&id.as_str()) && *new_value == json(&off()))
        .collect();
    assert!(
        reverted.is_empty(),
        "{case}: a pull reverted members: {reverted:?}"
    );

    // The hub confirms the pushes, and the members stay on.
    for id in MEMBERS {
        rig.pull(id).await;
        assert_eq!(rig.stored(id).await, on_at_60(), "{case}: {id} after");
    }
    assert_eq!(
        rig.stored(GROUP).await,
        on_at_60(),
        "{case}: the group took the state it was set to"
    );
    rig.shutdown().await;
}

#[tokio::test]
async fn a_pull_in_the_old_gap_leaves_a_group_write_alone() {
    for optimistic_updates in [true, false] {
        a_pull_in_the_old_gap_leaves_the_write_alone(Kind::Curves, optimistic_updates).await;
    }
}

#[tokio::test]
async fn a_pull_in_the_old_gap_leaves_a_linear_group_write_alone() {
    for optimistic_updates in [true, false] {
        a_pull_in_the_old_gap_leaves_the_write_alone(Kind::Linear, optimistic_updates).await;
    }
}
