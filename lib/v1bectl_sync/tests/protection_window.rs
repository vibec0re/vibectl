//! 🛡️ Integration tests for the protection window of user writes (#23).
//!
//! A user (`SyncPriority::Critical`) write goes into the store at once and
//! is pushed to the gateway later, from the sync buffer. Until the hub
//! confirms it, a pull that still reports the old value must leave the store
//! alone. The pending confirmation used to be armed only when the push went
//! out, so a pull that ran while a write was still queued (behind the other
//! members of a group write, say) reverted it.
//!
//! The engine runs for real, against [`TestHub`]: a fake gateway whose
//! PATCHes can be held at a gate and whose traffic is logged. The tests
//! order their steps on that log, never on sleeps. Pulls come either from
//! the periodic pull worker ([`Rig::full_pull_cycle`]) or, where a test
//! needs one at an exact point, from a queued `PullFromGateway` task
//! ([`Rig::pull`]).

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
const WAIT: Duration = Duration::from_secs(20);

fn light(is_on: bool, brightness: u8) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness: Some(brightness),
        color_temp: None,
        rgb_color: None,
    })
}

fn off() -> DeviceStateValue {
    light(false, 10)
}

fn on() -> DeviceStateValue {
    light(true, 80)
}

fn light_info(id: &str) -> DeviceInfo {
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
enum OnSet {
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
struct HubLog {
    /// Every `get_device_state`, by device, logged as it's answered.
    reads: Vec<DeviceId>,
    /// Every `set_device_state`, logged as it arrives (before the gate).
    sets_started: Vec<(DeviceId, DeviceStateValue)>,
    /// `set_device_state` calls answered (applied, ignored or failed).
    sets_done: usize,
}

impl HubLog {
    fn sets_for(&self, id: &str) -> Vec<DeviceStateValue> {
        self.sets_started
            .iter()
            .filter(|(device, _)| device == id)
            .map(|(_, state)| state.clone())
            .collect()
    }
}

struct TestHub {
    /// What a pull reads, per device. A device not in here (a sentinel)
    /// reads as `Empty`.
    reported: Mutex<HashMap<DeviceId, DeviceStateValue>>,
    on_set: OnSet,
    /// PATCHes wait here for a permit. Closed means open: nothing waits.
    gate: Semaphore,
    log: watch::Sender<HubLog>,
}

impl TestHub {
    fn new(on_set: OnSet, gated: bool) -> Arc<Self> {
        let gate = Semaphore::new(0);
        if !gated {
            gate.close();
        }
        Arc::new(Self {
            reported: Mutex::new(HashMap::new()),
            on_set,
            gate,
            log: watch::channel(HubLog::default()).0,
        })
    }

    /// What the hub reports for `id` from now on (a physical switch, or
    /// another client).
    fn report(&self, id: &str, state: DeviceStateValue) {
        self.reported.lock().unwrap().insert(id.to_string(), state);
    }

    fn reported(&self, id: &str) -> Option<DeviceStateValue> {
        self.reported.lock().unwrap().get(id).cloned()
    }

    /// Let `n` held PATCHes through, in arrival order.
    fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }

    /// Stop holding PATCHes, including the ones waiting now.
    fn open_gate(&self) {
        self.gate.close();
    }

    fn log(&self) -> HubLog {
        self.log.borrow().clone()
    }

    /// Resolves once the log satisfies `done`.
    async fn wait(&self, what: &str, done: impl FnMut(&HubLog) -> bool) {
        let mut rx = self.log.subscribe();
        tokio::time::timeout(WAIT, rx.wait_for(done))
            .await
            .unwrap_or_else(|_| panic!("{what}: never happened, hub log {:?}", self.log()))
            .expect("hub log");
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
        self.log.send_modify(|log| {
            log.sets_started.push((device_id.clone(), state.clone()));
        });
        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
        let result = match self.on_set {
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
#[derive(Clone, Copy)]
enum Pulls {
    /// The pull worker runs back to back. Wait on [`Rig::full_pull_cycle`].
    Periodic,
    /// The pull worker only runs its first cycle, at start. Pull with
    /// [`Rig::pull`].
    OnDemand,
}

struct Rig {
    hub: Arc<TestHub>,
    store: Arc<StateStore>,
    bus: Arc<EventBus>,
    engine: SyncEngine,
    runner: JoinHandle<anyhow::Result<()>>,
    sentinels: AtomicUsize,
}

impl Rig {
    /// Lights `ids`, all `off()` in the store and on the hub, with the
    /// engine started. `config` fills in everything but the intervals.
    async fn new(ids: &[&str], hub: Arc<TestHub>, pulls: Pulls, config: SyncConfig) -> Self {
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
                pull_interval: Duration::from_secs(3600),
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
    async fn write(&self, id: &str, state: DeviceStateValue) {
        self.engine
            .apply_optimistic_update(&id.to_string(), state)
            .await
            .expect("write");
    }

    async fn stored(&self, id: &str) -> DeviceStateValue {
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
    async fn pull(&self, id: &str) {
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

    /// Resolves once the pull worker (`Pulls::Periodic`) has run a whole
    /// cycle that started after this call.
    ///
    /// A cycle snapshots the store, then reads each of its `n` devices once,
    /// so cycles end on read counts that are multiples of `n`. The first
    /// boundary after `start` reads is at most `start + n`. The cycle after
    /// it snapshots the store after this call, and has been handled
    /// completely once the next cycle's first read (at most
    /// `start + 2n + 1`) comes in.
    async fn full_pull_cycle(&self) {
        let n = self.store.device_count().await;
        let start = self.hub.log().reads.len();
        self.hub
            .wait("a full pull cycle", |log| log.reads.len() > start + 2 * n)
            .await;
    }

    async fn shutdown(self) {
        self.hub.open_gate();
        self.engine.stop().await;
        self.runner
            .await
            .expect("sync engine task")
            .expect("sync engine");
    }
}

/// `(device, old_value, new_value)` of every state event received so far.
fn drain_events(rx: &mut broadcast::Receiver<DeviceEvent>) -> Vec<(DeviceId, Value, Value)> {
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
async fn wait_for_echo(
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

type Value = serde_json::Value;

fn json(state: &DeviceStateValue) -> Value {
    serde_json::to_value(state).expect("state to json")
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The #23 regression, as a group write makes it: three member writes in
/// one burst, and the first PATCH takes its time. The other two wait in the
/// buffer behind it, and the pull that runs meanwhile still reads the old
/// value for all three. None of them may be reverted.
#[tokio::test]
async fn queued_writes_survive_a_pull_while_the_first_push_is_held() {
    const IDS: [&str; 3] = ["a", "b", "c"];
    let rig = Rig::new(
        &IDS,
        TestHub::new(OnSet::Apply, true),
        Pulls::Periodic,
        SyncConfig::default(),
    )
    .await;

    for id in IDS {
        rig.write(id, on()).await;
    }
    rig.hub
        .wait("first PATCH at the gate", |log| {
            !log.sets_started.is_empty()
        })
        .await;

    // A whole pull cycle while that PATCH is held: the hub still reports
    // `off` for every device, and at most one push is out.
    rig.full_pull_cycle().await;
    let log = rig.hub.log();
    assert_eq!(log.sets_started.len(), 1, "one PATCH out: {log:?}");
    assert_eq!(log.sets_done, 0, "the PATCH is still held: {log:?}");
    for id in IDS {
        assert_eq!(rig.hub.reported(id), Some(off()), "{id} on the hub");
    }
    let held = &log.sets_started[0].0;
    for id in IDS {
        assert_eq!(
            rig.stored(id).await,
            on(),
            "{id} was reverted by a pull while its write was {} (#23)",
            if id == held {
                "in flight"
            } else {
                "still queued"
            }
        );
    }

    // Let the pushes land. The hub reports the new value, and the store
    // keeps it.
    rig.hub.open_gate();
    rig.hub
        .wait("all three pushes", |log| log.sets_done >= IDS.len())
        .await;
    rig.full_pull_cycle().await;
    for id in IDS {
        assert_eq!(rig.hub.reported(id), Some(on()), "{id} on the hub");
        assert_eq!(rig.stored(id).await, on(), "{id} in the store");
    }

    rig.shutdown().await;
}

/// The whole lifecycle of a pending confirmation: armed at the write (a
/// pull before the push is ignored), cleared when the hub confirms the
/// value (the store keeps it and echoes the confirmation), and gone after
/// that (the next external change is taken at once, not ignored for the
/// rest of the window).
#[tokio::test]
async fn hub_confirmation_clears_the_pending_entry() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();

    rig.write("a", on()).await;
    rig.hub
        .wait("PATCH at the gate", |log| !log.sets_started.is_empty())
        .await;
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "pull before the push landed");

    rig.hub.release(1);
    rig.hub.wait("push", |log| log.sets_done >= 1).await;
    drain_events(&mut rx);
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "after the hub confirmed");
    assert_eq!(
        drain_events(&mut rx),
        vec![("a".to_string(), json(&on()), json(&on()))],
        "the confirmation (old: the expected value, new: the hub's)"
    );

    // Someone flips the physical switch right after. With the pending
    // entry still there, this pull would be ignored as "not our change".
    let flipped = light(false, 40);
    rig.hub.report("a", flipped.clone());
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, flipped, "external change after it");

    rig.shutdown().await;
}

/// The window is bounded. With the hub still reporting the old value once
/// it's up, GatewayWins reverts the write, whether the push went through
/// (and the device never changed) or failed. The store isn't left
/// "protected" with a value the hub never got.
#[tokio::test]
async fn expired_window_lets_the_gateway_win() {
    let window = Duration::from_millis(150);
    for on_set in [OnSet::Ignore, OnSet::Fail] {
        let rig = Rig::new(
            &["a"],
            TestHub::new(on_set, false),
            Pulls::Periodic,
            SyncConfig {
                protection_window: window,
                ..SyncConfig::default()
            },
        )
        .await;
        let mut rx = rig.bus.subscribe();

        let written = Instant::now();
        rig.write("a", on()).await;
        wait_for_echo(&mut rx, "a", &off()).await;
        let reverted = written.elapsed();
        assert!(
            reverted >= window,
            "{on_set:?}: reverted after {reverted:?}, inside the {window:?} window"
        );
        assert_eq!(rig.stored("a").await, off(), "{on_set:?}: store");
        rig.hub.wait("push", |log| log.sets_done >= 1).await;

        rig.shutdown().await;
    }
}

/// Two writes to one device while it's still queued. The buffer sends one
/// PATCH, with the second value, and the pending entry expects the second
/// value too. The hub reporting the first one isn't a confirmation; if it
/// were taken as one, the store would drop the user's latest write.
#[tokio::test]
async fn coalesced_writes_expect_the_latest_value() {
    let rig = Rig::new(
        &["x", "a"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();

    // Keep the buffer worker busy on another device, so both writes to `a`
    // stay queued.
    rig.write("x", on()).await;
    rig.hub
        .wait("x at the gate", |log| !log.sets_started.is_empty())
        .await;
    let (first, second) = (light(true, 30), light(true, 70));
    rig.write("a", first.clone()).await;
    rig.write("a", second.clone()).await;

    rig.hub.report("a", first.clone());
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, second, "hub reports the first write");

    rig.hub.open_gate();
    rig.hub.wait("both pushes", |log| log.sets_done >= 2).await;
    assert_eq!(rig.hub.log().sets_for("a"), vec![second.clone()], "PATCHes");
    drain_events(&mut rx);
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, second, "after the hub confirmed");
    assert_eq!(
        drain_events(&mut rx),
        vec![("a".to_string(), json(&second), json(&second))],
        "the confirmation, expecting the second write"
    );

    rig.shutdown().await;
}

/// A newer write that comes in after the buffer drained the older one. The
/// older push still goes out, and restarting the window for it must not
/// move the expectation back to the older value: the hub confirming that
/// older value would then revert the newer write.
#[tokio::test]
async fn stale_push_keeps_the_newer_expectation() {
    let rig = Rig::new(
        &["x", "a", "b"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();
    let (older, newer) = (light(true, 30), light(true, 70));

    // Hold the buffer worker on `x` while `a` and `b` are written, so the
    // next drain takes both. Its first PATCH is held too, and the other
    // device's value waits in the drained batch.
    rig.write("x", on()).await;
    rig.hub
        .wait("x at the gate", |log| log.sets_started.len() == 1)
        .await;
    rig.write("a", older.clone()).await;
    rig.write("b", older.clone()).await;
    rig.hub.release(1);
    rig.hub
        .wait("a or b at the gate", |log| log.sets_started.len() == 2)
        .await;
    let held = rig.hub.log().sets_started[1].0.clone();
    let stale = if held == "a" { "b" } else { "a" };

    // `stale`'s older value is already out of the buffer. A newer write
    // re-arms its pending entry and goes into the buffer.
    rig.write(stale, newer.clone()).await;

    // The older push goes out (its window restarts) and is held at the gate.
    // The hub already shows it while it's still answering the PATCH. This is
    // the pull that would take the older value for a confirmation if the
    // restart had moved the expectation back to it.
    rig.hub.release(1);
    rig.hub
        .wait("the older push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for(stale),
        vec![older.clone()],
        "PATCHes"
    );
    rig.hub.report(stale, older.clone());
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale}: the hub showing the older push reverted the newer write"
    );

    // The older push lands. The newer one goes out and is held: the hub
    // still reports the older value.
    rig.hub.release(1);
    rig.hub
        .wait("the newer push at the gate", |log| {
            log.sets_started.len() == 4
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for(stale),
        vec![older.clone(), newer.clone()],
        "{stale}: PATCHes"
    );
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale}: the hub confirming the older push reverted the newer write"
    );

    rig.hub.open_gate();
    rig.hub.wait("all pushes", |log| log.sets_done >= 4).await;
    drain_events(&mut rx);
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale} after the hub confirmed"
    );
    assert_eq!(
        drain_events(&mut rx),
        vec![(stale.to_string(), json(&newer), json(&newer))],
        "the confirmation, expecting the newer write"
    );

    rig.shutdown().await;
}

/// A failed push leaves the pending entry in place. The retry comes well
/// inside the window, so a pull in between must not revert the write
/// (a transient failure would flicker the UI). `expired_window_lets_the_gateway_win`
/// shows the entry still runs out.
#[tokio::test]
async fn failed_push_stays_protected_within_the_window() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;

    rig.write("a", on()).await;
    rig.hub.wait("failed push", |log| log.sets_done >= 1).await;
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "pull right after the failure");

    rig.shutdown().await;
}
