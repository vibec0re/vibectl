//! 🛡️ #58: a pull that lands while the sync engine is about to arm a
//! member's protection window reverts nothing.
//!
//! A virtual write commits each physical member it changes through the
//! engine (`commit_member_write`), and the engine arms the member's window
//! before anything writes the store (#55). So until the window is armed, the
//! store still holds what the hub has, and a pull finds nothing to
//! reconcile. Were the manager to write the store first, and only then hand
//! the member to the engine (mutant M3 in the #57 review), the store would
//! run ahead of the hub with nothing pending until the engine armed the
//! window. A pull in that gap takes it for an outside change and reverts it
//! (`GatewayWins`): the pull worker snapshots the store without the sync
//! buffer's lock.
//!
//! `group_write_race.rs` holds a write between planning and its first
//! commit, which pins "planning writes nothing". This file pins the order
//! *inside* a commit. A tracing subscriber parks the member's write at the
//! engine's first line (`🔥 OPTIMISTIC UPDATE for device`), where nothing is
//! armed yet. The test reads the store and pulls the member right there,
//! lets the write go, and checks that the store still held the old value,
//! nothing was reverted, and the member ends committed and pushed. It does
//! this for both paths that commit a physical light: a group write, and a
//! button action on a light (`run_action`), each with optimistic updates on
//! and off.
//!
//! The subscriber blocks the parked write's worker thread, the technique of
//! `v1bectl_sync/tests/optimistic_update.rs`. The test body runs on the
//! runtime's `block_on` thread, not a worker, so the hook's wake-up reaches
//! it while that worker is blocked. The subscriber is process-global, so
//! this file holds this one test.
//!
//! The rig is the sync engine's (`lib/v1bectl_sync/tests/common`), as in
//! `group_write_race.rs`. The steps are ordered on the parked write and the
//! hub's log, never on sleeps.

#[path = "../../v1bectl_sync/tests/common/mod.rs"]
mod common;

use std::sync::{Arc, Condvar, Mutex};

use common::{drain_events, json, light, off, OnSet, Pulls, Rig, TestHub, WAIT};
use tokio::sync::watch;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use v1bectl_sync::{
    ButtonPressType, DeviceEvent, DeviceStateValue, EventType, LightState, SyncConfig,
};
use v1bectl_virtual::{
    ButtonController, LightGroup, VirtualDevice, VirtualDeviceConfig, VirtualDeviceManager,
    VirtualDeviceType,
};

const GROUP: &str = "g";
const CONTROLLER: &str = "ctrl";
const BUTTON: &str = "btn";
const MEMBERS: [&str; 2] = ["a", "b"];
/// The member whose engine write is parked: the first one each path
/// commits.
const PARKED: &str = "a";

/// Parks one engine write, of the device it's armed for, at the engine's
/// first line until the test lets go.
struct Hook {
    /// The device whose next engine write to park. Taken when it parks.
    armed: Mutex<Option<String>>,
    /// `true` once the armed write is parked.
    parked: watch::Sender<bool>,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Hook {
    fn new() -> Self {
        Self {
            armed: Mutex::new(None),
            parked: watch::channel(false).0,
            released: Mutex::new(false),
            wake: Condvar::new(),
        }
    }

    /// Park the next engine write of `device_id`.
    fn arm(&self, device_id: &str) {
        *self.released.lock().unwrap() = false;
        self.parked.send_replace(false);
        *self.armed.lock().unwrap() = Some(device_id.to_string());
    }

    /// Let the parked write go on (or the next one through, if none is
    /// parked yet), and park nothing more.
    fn release(&self) {
        self.armed.lock().unwrap().take();
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

/// Collects an event's `message` field.
struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

struct HookSubscriber(Arc<Hook>);

impl Subscriber for HookSubscriber {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let hook = &self.0;
        let Some(device_id) = hook.armed.lock().unwrap().clone() else {
            return;
        };
        let mut message = Message(String::new());
        event.record(&mut message);
        // `apply_optimistic_update`'s first line, before it marks, locks or
        // arms anything.
        let first_line = format!("OPTIMISTIC UPDATE for device {device_id} ");
        if !message.0.contains(&first_line) || hook.armed.lock().unwrap().take().is_none() {
            return;
        }
        hook.parked.send_replace(true);
        // Block this worker thread (and so the write) until the test lets
        // go.
        let mut released = hook.released.lock().unwrap();
        while !*released {
            released = hook.wake.wait(released).unwrap();
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// The two ways a virtual device's write reaches a physical light.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// A write to group `g` over [`MEMBERS`] (`set_virtual_device_state`).
    GroupWrite,
    /// A press of `btn`, whose controller turns light `a` on (`run_action`).
    ButtonAction,
}

impl Path {
    /// The virtual device that makes the write.
    fn device(self, rig: &Rig) -> Box<dyn VirtualDevice> {
        match self {
            Self::GroupWrite => {
                let curves: serde_json::Map<String, serde_json::Value> = MEMBERS
                    .iter()
                    .map(|id| {
                        let curve = serde_json::json!({ "breakpoints": [[0, 0], [100, 100]] });
                        ((*id).to_string(), curve)
                    })
                    .collect();
                let config = config(
                    GROUP,
                    VirtualDeviceType::LightGroup,
                    serde_json::json!({ "lights": MEMBERS, "brightness_curves": curves }),
                );
                Box::new(LightGroup::new(config, Arc::clone(&rig.store)).expect("group"))
            }
            Self::ButtonAction => {
                let config = config(
                    CONTROLLER,
                    VirtualDeviceType::ButtonController,
                    serde_json::json!({}),
                );
                let on = [serde_json::json!("on"), serde_json::json!(PARKED)];
                Box::new(
                    ButtonController::new(config, BUTTON.to_string(), &on, &[], None, None, None)
                        .expect("controller"),
                )
            }
        }
    }

    /// The lights the write commits, each with the state it commits.
    fn writes(self) -> Vec<(&'static str, DeviceStateValue)> {
        match self {
            // 1:1 curves: each member at the group's state. They take the
            // group's colour temperature, the one it starts with, as none of
            // them has one.
            Self::GroupWrite => MEMBERS
                .iter()
                .map(|id| {
                    let on = DeviceStateValue::Light(LightState {
                        is_on: true,
                        brightness: Some(60),
                        color_temp: Some(2700),
                        rgb_color: None,
                    });
                    (*id, on)
                })
                .collect(),
            // `on` keeps the light's level: `off()` is at 10.
            Self::ButtonAction => vec![(PARKED, light(true, 10))],
        }
    }

    /// Make the write.
    async fn run(self, manager: &VirtualDeviceManager) {
        match self {
            Self::GroupWrite => {
                let on = self.writes()[0].1.clone();
                manager
                    .set_virtual_device_state(&GROUP.to_string(), on)
                    .await
                    .expect("group write");
            }
            Self::ButtonAction => {
                // A click, as a hub reports it: the press runs `on`. (The
                // action's own result is only logged; the checks below look
                // at the light.)
                let click = DeviceEvent {
                    timestamp: std::time::SystemTime::now(),
                    device_id: BUTTON.to_string(),
                    event_type: EventType::ButtonPressed {
                        button_id: "main".to_string(),
                        press_type: ButtonPressType::SinglePress,
                    },
                };
                manager.handle_event(&click).await.expect("input tracking");
            }
        }
    }
}

fn config(
    device_id: &str,
    device_type: VirtualDeviceType,
    config: serde_json::Value,
) -> VirtualDeviceConfig {
    VirtualDeviceConfig {
        device_id: device_id.to_string(),
        device_type,
        name: device_id.to_string(),
        description: None,
        enabled: true,
        config,
    }
}

/// `path`'s write, parked at the engine's first line for [`PARKED`], with
/// a pull of every light it writes right there. Every light is off in the
/// store and on the hub before.
///
/// Right there, the store must still hold off, as the hub does, so the pull
/// has nothing to reconcile. Had the manager written the store before it
/// called the engine, the store would be ahead with nothing pending, and
/// the pull would revert the light and echo it back to off.
async fn a_pull_at_the_engines_first_line_reverts_nothing(
    hook: &Hook,
    path: Path,
    optimistic_updates: bool,
) {
    let case = format!("{path:?}, optimistic_updates: {optimistic_updates}");
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
    manager
        .add_virtual_device(path.device(&rig))
        .await
        .expect("register");
    let writes = path.writes();
    let mut rx = rig.bus.subscribe();

    hook.arm(PARKED);
    let mut parked = hook.parked.subscribe();
    let write = tokio::spawn({
        let manager = manager.clone();
        async move { path.run(&manager).await }
    });
    let was_parked = tokio::time::timeout(WAIT, parked.wait_for(|parked| *parked))
        .await
        .is_ok();
    let mut stored_while_parked = None;
    if was_parked {
        stored_while_parked = Some(rig.stored(PARKED).await);
        // Each pull is handled completely before `pull` returns.
        for (id, _) in &writes {
            rig.pull(id).await;
        }
    }
    // Let go before asserting anything: a panic with the write's worker
    // thread still blocked would hang the runtime's shutdown.
    hook.release();
    assert!(was_parked, "{case}: the write never reached the engine");
    tokio::time::timeout(WAIT, write)
        .await
        .unwrap_or_else(|_| panic!("{case}: the write never finished"))
        .expect("write task");

    assert_eq!(
        stored_while_parked,
        Some(off()),
        "{case}: {PARKED} was in the store before the engine armed its protection"
    );
    let reverted: Vec<_> = drain_events(&mut rx)
        .into_iter()
        .filter(|(id, _, new_value)| {
            writes.iter().any(|(written, _)| id == written) && *new_value == json(&off())
        })
        .collect();
    assert!(
        reverted.is_empty(),
        "{case}: a pull reverted lights: {reverted:?}"
    );
    for (id, state) in &writes {
        assert_eq!(
            &rig.stored(id).await,
            state,
            "{case}: {id} was reverted, or never committed"
        );
        rig.hub
            .wait(&format!("{case}: {id} pushed"), |log| {
                log.sets_for(id) == [state.clone()]
            })
            .await;
    }

    // The hub confirms the pushes, and the lights stay where the write put
    // them.
    for (id, state) in &writes {
        rig.pull(id).await;
        assert_eq!(&rig.stored(id).await, state, "{case}: {id} after");
    }
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_while_the_engine_arms_protection_reverts_nothing() {
    let hook = Arc::new(Hook::new());
    tracing::subscriber::set_global_default(HookSubscriber(Arc::clone(&hook)))
        .expect("the only subscriber in this test binary");

    for optimistic_updates in [true, false] {
        for path in [Path::GroupWrite, Path::ButtonAction] {
            a_pull_at_the_engines_first_line_reverts_nothing(&hook, path, optimistic_updates).await;
        }
    }
}
