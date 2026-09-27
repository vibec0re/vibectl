//! 🧪 The shared rig for the sync engine's integration tests: a fake hub
//! whose `PATCHes` can be held at a gate and whose traffic is logged, and a
//! running engine around it.
//!
//! The tests order their steps on the hub's log, never on sleeps. Pulls come
//! either from the periodic pull worker ([`Rig::full_pull_cycle`]) or, where a
//! test needs one at an exact point, from a queued `PullFromGateway` task
//! ([`Rig::pull`]).

// Each test binary compiles this module on its own and uses a different
// part of it.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{broadcast, watch, Semaphore};
use tokio::task::JoinHandle;
use v1bectl_sync::{
    Capability, DeviceEvent, DeviceId, DeviceInfo, DeviceStateValue, DeviceType, EventBus,
    EventType, Gateway, GatewayError, GatewayHealth, LightState, StateStore, SyncConfig,
    SyncEngine, SyncPriority, SyncTask, SyncTaskType,
};

/// Bounds a wait that should resolve. Nothing sleeps through it when things
/// work; it only turns a hang into a failure.
pub const WAIT: Duration = Duration::from_secs(20);

pub fn light(is_on: bool, brightness: u8) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness: Some(brightness),
        color_temp: None,
        rgb_color: None,
    })
}

pub fn off() -> DeviceStateValue {
    light(false, 10)
}

pub fn on() -> DeviceStateValue {
    light(true, 80)
}

pub fn light_info(id: &str) -> DeviceInfo {
    DeviceInfo {
        device_id: id.to_string(),
        name: format!("Light {id}"),
        device_type: DeviceType::Light,
        capabilities: vec![Capability::OnOff, Capability::Brightness],
        device_groups: vec![],
        manufacturer: Some("IKEA".to_string()),
        model: Some("TRADFRI".to_string()),
        firmware_version: None,
        battery_powered: false,
        reachable: true,
        last_seen: 0,
        custom_attributes: HashMap::new(),
    }
}

// ---------------------------------------------------------------------
// The fake hub
// ---------------------------------------------------------------------

/// What the hub does with a PATCH once the gate lets it through.
#[derive(Clone, Copy, Debug)]
pub enum OnSet {
    /// Accept it and report the new value from then on, like a real hub.
    Apply,
    /// Accept it, but the device never changes: the hub keeps reporting the
    /// old value (a device that dropped off the mesh, say).
    Ignore,
    /// Fail it.
    Fail,
}

/// Everything the engine asked the hub, in order.
#[derive(Clone, Debug, Default)]
pub struct HubLog {
    /// Every `get_device_state`, by device, logged as it's answered.
    pub reads: Vec<DeviceId>,
    /// Every `set_device_state`, logged as it arrives (before the gate).
    pub sets_started: Vec<(DeviceId, DeviceStateValue)>,
    /// `set_device_state` calls answered (applied, ignored or failed).
    pub sets_done: usize,
    /// The `PATCHes` waiting at the gate now, by their index in
    /// `sets_started`.
    pub held: Vec<usize>,
}

impl HubLog {
    pub fn sets_for(&self, id: &str) -> Vec<DeviceStateValue> {
        self.sets_started
            .iter()
            .filter(|(device, _)| device == id)
            .map(|(_, state)| state.clone())
            .collect()
    }

    pub fn reads_of(&self, id: &str) -> usize {
        self.reads.iter().filter(|device| *device == id).count()
    }
}

pub struct TestHub {
    /// What a pull reads, per device. A device not in here (a sentinel)
    /// reads as `Empty`.
    reported: Mutex<HashMap<DeviceId, DeviceStateValue>>,
    /// Read when a PATCH passes the gate, so a test can change it while
    /// `PATCHes` are held.
    on_set: Mutex<OnSet>,
    /// `PATCHes` wait here for a permit. Closed means open: nothing waits.
    gate: Semaphore,
    /// Held `PATCHes` let through out of order ([`TestHub::release_newest`]),
    /// by their index in `sets_started`.
    picked: watch::Sender<Vec<usize>>,
    log: watch::Sender<HubLog>,
}

impl TestHub {
    pub fn new(on_set: OnSet, gated: bool) -> Arc<Self> {
        let gate = Semaphore::new(0);
        if !gated {
            gate.close();
        }
        Arc::new(Self {
            reported: Mutex::new(HashMap::new()),
            on_set: Mutex::new(on_set),
            gate,
            picked: watch::channel(vec![]).0,
            log: watch::channel(HubLog::default()).0,
        })
    }

    /// What the hub reports for `id` from now on (a physical switch, or
    /// another client).
    pub fn report(&self, id: &str, state: DeviceStateValue) {
        self.reported.lock().unwrap().insert(id.to_string(), state);
    }

    pub fn reported(&self, id: &str) -> Option<DeviceStateValue> {
        self.reported.lock().unwrap().get(id).cloned()
    }

    /// What the hub does with the `PATCHes` that pass the gate from now on,
    /// including the ones held at it now.
    pub fn set_on_set(&self, on_set: OnSet) {
        *self.on_set.lock().unwrap() = on_set;
    }

    /// Let `n` held `PATCHes` through, in arrival order.
    pub fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }

    /// Let the most recent of the held `PATCHes` through, ahead of the older
    /// ones: a hub may answer requests in any order. (Wait for it to be
    /// answered before letting the others through: `PATCHes` let through
    /// together are answered in whatever order the runtime runs them.)
    pub fn release_newest(&self) {
        let newest = *self.log().held.iter().max().expect("a PATCH at the gate");
        self.picked.send_modify(|picked| picked.push(newest));
    }

    /// Stop holding `PATCHes`, including the ones waiting now.
    pub fn open_gate(&self) {
        self.gate.close();
    }

    pub fn log(&self) -> HubLog {
        self.log.borrow().clone()
    }

    /// Resolves once the log satisfies `done`.
    pub async fn wait(&self, what: &str, done: impl FnMut(&HubLog) -> bool) {
        let mut rx = self.log.subscribe();
        tokio::time::timeout(WAIT, rx.wait_for(done))
            .await
            .unwrap_or_else(|_| panic!("{what}: never happened, hub log {:?}", self.log()))
            .expect("hub log");
    }

    /// Whether the log satisfies `done` within `bound`. For a check that
    /// something does *not* happen: bounded, so a slow machine can only let
    /// a broken build pass, never fail a correct one.
    pub async fn within(&self, bound: Duration, done: impl FnMut(&HubLog) -> bool) -> bool {
        let mut rx = self.log.subscribe();
        let happened = tokio::time::timeout(bound, rx.wait_for(done)).await.is_ok();
        happened
    }
}

#[async_trait]
impl Gateway for TestHub {
    async fn discover_devices(&self) -> Result<Vec<DeviceInfo>, GatewayError> {
        Ok(vec![])
    }

    async fn get_device_state(
        &self,
        device_id: &DeviceId,
    ) -> Result<DeviceStateValue, GatewayError> {
        let state = self
            .reported
            .lock()
            .unwrap()
            .get(device_id)
            .cloned()
            .unwrap_or(DeviceStateValue::Empty);
        self.log
            .send_modify(|log| log.reads.push(device_id.clone()));
        Ok(state)
    }

    async fn set_device_state(
        &self,
        device_id: &DeviceId,
        state: DeviceStateValue,
    ) -> Result<(), GatewayError> {
        let mut index = 0;
        self.log.send_modify(|log| {
            index = log.sets_started.len();
            log.sets_started.push((device_id.clone(), state.clone()));
            log.held.push(index);
        });
        let mut picked = self.picked.subscribe();
        tokio::select! {
            permit = self.gate.acquire() => {
                if let Ok(permit) = permit {
                    permit.forget();
                }
            }
            _ = picked.wait_for(|picked| picked.contains(&index)) => {}
        }
        self.log
            .send_modify(|log| log.held.retain(|held| *held != index));
        let on_set = *self.on_set.lock().unwrap();
        let result = match on_set {
            OnSet::Apply => {
                self.report(device_id, state);
                Ok(())
            }
            OnSet::Ignore => Ok(()),
            OnSet::Fail => Err(GatewayError::NetworkError("hub unreachable".to_string())),
        };
        self.log.send_modify(|log| log.sets_done += 1);
        result
    }

    async fn health_check(&self) -> Result<GatewayHealth, GatewayError> {
        Ok(GatewayHealth {
            reachable: true,
            response_time_ms: 0,
            connected_devices: 0,
            last_error: None,
        })
    }
}

// ---------------------------------------------------------------------
// The rig: store + bus + running engine around a TestHub
// ---------------------------------------------------------------------

/// How the test gets its pulls.
#[derive(Clone, Copy, Debug)]
pub enum Pulls {
    /// The pull worker runs back to back. Wait on [`Rig::full_pull_cycle`].
    Periodic,
    /// The pull worker only runs its first cycle, at start. Pull with
    /// [`Rig::pull`].
    OnDemand,
}

pub struct Rig {
    pub hub: Arc<TestHub>,
    pub store: Arc<StateStore>,
    pub bus: Arc<EventBus>,
    pub engine: SyncEngine,
    runner: JoinHandle<anyhow::Result<()>>,
    sentinels: AtomicUsize,
}

impl Rig {
    /// Lights `ids`, all `off()` in the store and on the hub, with the
    /// engine started. `config` fills in everything but the intervals.
    pub async fn new(ids: &[&str], hub: Arc<TestHub>, pulls: Pulls, config: SyncConfig) -> Self {
        let store = StateStore::new();
        for id in ids {
            store.add_device(light_info(id), off()).await;
            hub.report(id, off());
        }
        let bus = Arc::new(EventBus::new(1000));
        let config = match pulls {
            Pulls::Periodic => SyncConfig {
                pull_interval: Duration::from_millis(2),
                ..config
            },
            Pulls::OnDemand => SyncConfig {
                pull_interval: Duration::from_hours(1),
                push_interval: Duration::from_millis(5),
                ..config
            },
        };
        let engine = SyncEngine::new(store.clone(), bus.clone(), hub.clone(), Some(config));
        let runner = tokio::spawn({
            let engine = engine.clone();
            async move { engine.start().await }
        });
        // The first pull cycle starts at once. Let it read everything, so it
        // took its snapshot of the store before the test writes anything.
        hub.wait("first pull cycle", |log| log.reads.len() >= ids.len())
            .await;
        Self {
            hub,
            store,
            bus,
            engine,
            runner,
            sentinels: AtomicUsize::new(0),
        }
    }

    /// A user write, the way the API server and the virtual manager make it.
    pub async fn write(&self, id: &str, state: DeviceStateValue) {
        self.engine
            .apply_optimistic_update(&id.to_string(), state)
            .await
            .expect("write");
    }

    /// Makes `writes` so that one drain of the sync buffer takes them all,
    /// and resolves once that batch's first PATCH is held at the gate.
    /// Returns its device; the batch's other `PATCHes` wait behind it, in the
    /// drain's (random) order.
    ///
    /// Two writes in a row can otherwise land in different drains, if a
    /// buffer tick falls between them. So this first writes `holder` and
    /// holds its PATCH at the gate while `writes` are made, then lets it
    /// through (with whatever `OnSet` the hub has then). The hub must be
    /// gated, with nothing held or queued.
    pub async fn write_one_batch(
        &self,
        holder: &str,
        writes: &[(&str, DeviceStateValue)],
    ) -> DeviceId {
        let start = self.hub.log().sets_started.len();
        self.write(holder, on()).await;
        self.hub
            .wait("the holder's PATCH at the gate", |log| {
                log.sets_started.len() == start + 1
            })
            .await;
        for (id, state) in writes {
            self.write(id, state.clone()).await;
        }
        self.hub.release(1);
        self.hub
            .wait("the batch's first PATCH at the gate", |log| {
                log.sets_started.len() == start + 2
            })
            .await;
        self.hub.log().sets_started[start + 1].0.clone()
    }

    pub async fn stored(&self, id: &str) -> DeviceStateValue {
        self.store
            .get_device(&id.to_string())
            .await
            .expect("device in store")
            .state
    }

    /// One pull of `id` (`Pulls::OnDemand`), handled completely on return.
    ///
    /// The push worker runs queued tasks one at a time, in order, so a
    /// sentinel pull queued right behind it is read only once `id`'s pull
    /// is done. The sentinel isn't in the store, so it changes nothing.
    pub async fn pull(&self, id: &str) {
        let sentinel = format!(
            "sentinel-{}",
            self.sentinels.fetch_add(1, Ordering::Relaxed)
        );
        for device_id in [id.to_string(), sentinel.clone()] {
            self.engine
                .queue_sync(SyncTask {
                    device_id,
                    task_type: SyncTaskType::PullFromGateway,
                    created_at: Instant::now(),
                    priority: SyncPriority::Normal,
                })
                .await;
        }
        self.hub
            .wait(&format!("pull of {id}"), |log| {
                log.reads.contains(&sentinel)
            })
            .await;
    }

    /// Resolves once the engine's retry queue holds `n` retries (waiting out
    /// their backoff: a due push retry leaves it for the sync buffer).
    ///
    /// The engine doesn't tell the hub when it takes a retry out, so this
    /// polls `get_sync_stats`. The polls only order the test on the engine's
    /// state; `WAIT` bounds them.
    pub async fn wait_for_retry_queue(&self, what: &str, n: u32) {
        tokio::time::timeout(WAIT, async {
            while self.engine.get_sync_stats().await.retry_queue_size != n {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}: never happened, hub log {:?}", self.hub.log()));
    }

    /// Resolves once the pull worker (`Pulls::Periodic`) has run a whole
    /// cycle that started after this call.
    ///
    /// A cycle snapshots the store, then reads each of its `n` devices once,
    /// so cycles end on read counts that are multiples of `n`. The first
    /// boundary after `start` reads is at most `start + n`. The cycle after
    /// it snapshots the store after this call, and has been handled
    /// completely once the next cycle's first read (at most
    /// `start + 2n + 1`) comes in.
    pub async fn full_pull_cycle(&self) {
        let n = self.store.device_count().await;
        let start = self.hub.log().reads.len();
        self.hub
            .wait("a full pull cycle", |log| log.reads.len() > start + 2 * n)
            .await;
    }

    pub async fn shutdown(self) {
        self.hub.open_gate();
        self.engine.stop().await;
        self.runner
            .await
            .expect("sync engine task")
            .expect("sync engine");
    }
}

pub type Value = serde_json::Value;

pub fn json(state: &DeviceStateValue) -> Value {
    serde_json::to_value(state).expect("state to json")
}

/// `(device, old_value, new_value)` of every state event received so far.
pub fn drain_events(rx: &mut broadcast::Receiver<DeviceEvent>) -> Vec<(DeviceId, Value, Value)> {
    let mut events = vec![];
    while let Ok(event) = rx.try_recv() {
        if let EventType::AttributeChanged {
            old_value,
            new_value,
            ..
        } = event.event_type
        {
            events.push((event.device_id, old_value, new_value));
        }
    }
    events
}

/// Waits for a state event on `id` whose new value is `state`.
pub async fn wait_for_echo(
    rx: &mut broadcast::Receiver<DeviceEvent>,
    id: &str,
    state: &DeviceStateValue,
) {
    let want = json(state);
    tokio::time::timeout(WAIT, async {
        loop {
            let event = rx.recv().await.expect("event bus");
            if let EventType::AttributeChanged { new_value, .. } = event.event_type {
                if event.device_id == id && new_value == want {
                    return;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{id} was never set to {state:?}"));
}
