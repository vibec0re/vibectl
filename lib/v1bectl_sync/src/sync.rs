//! 🔥 The sync engine: keeps the [`StateStore`] and the gateway in step.
//!
//! - **Push.** A user write ([`SyncEngine::apply_optimistic_update`]) goes
//!   into the store right away (with [`SyncConfig::optimistic_updates`]) and
//!   into the sync buffer. The buffer worker drains the buffer every
//!   [`SyncConfig::push_interval`] and sends each device's value to the
//!   gateway, one device at a time.
//! - **Pull.** Every [`SyncConfig::pull_interval`] the pull worker reads
//!   every device from the gateway and reconciles the store with it
//!   ([`SyncConfig::conflict_resolution`]). A user write the hub hasn't
//!   confirmed yet is shielded from that for [`SyncConfig::protection_window`].
//! - **Retry.** A failed push is retried with exponential backoff
//!   ([`SyncConfig::base_retry_delay`], checked every
//!   [`SyncConfig::retry_interval`]), up to
//!   [`SyncConfig::max_retry_attempts`] failures per write. A due retry goes
//!   back into the sync buffer, so every push of a device goes out from the
//!   buffer worker: one at a time, in order.
//!
//! # Throttling
//!
//! There is no request-rate limiter. The throttle is per-device
//! coalescing: the sync buffer keeps only the latest value per device, so
//! however fast a client writes (a dragged slider, say), each drain sends at
//! most one PATCH per device, and a device gets at most one buffered PATCH
//! per `push_interval` tick (20 per second at the default 50 ms). Retries go
//! through the buffer too, so that holds for them as well.

use crate::events::*;
use crate::gateway::*;
use crate::store::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use v1bectl_state::*;

// 🔥 OPTIMISTIC STATE TRACKING!
#[derive(Debug, Clone)]
pub struct OptimisticState {
    pub client_state: DeviceStateValue,
    pub pending_sync: bool,
    pub updated_at: Instant,
}

// 🔥 SYNC BUFFER - OVERWRITES WITH LATEST VALUES! NO SPAM! <3
#[derive(Debug, Clone)]
pub struct SyncBufferEntry {
    pub state: DeviceStateValue,
    pub updated_at: Instant,
    pub priority: SyncPriority,
    /// A user write: it armed a pending confirmation. Its push restarts the
    /// protection window as it goes out; its retries don't.
    pub protected: bool,
    /// How many pushes of this write have failed already: 0 for a write,
    /// `n` for its `n`-th retry, which the retry worker puts back in the
    /// buffer (#32). A newer write replaces the entry and starts from 0.
    pub failed_attempts: u32,
}

// 🔥 PENDING CONFIRMATION TRACKER - UI CHANGES ARE SACRED! 💖
/// A user write that the hub hasn't confirmed yet: one made through
/// [`SyncEngine::apply_optimistic_update`] (whatever its priority), or a
/// `SyncPriority::Critical` push queued with [`SyncEngine::queue_sync`].
/// While it's inside its window, a pull that disagrees with
/// `expected_state` is ignored instead of reverting the store.
#[derive(Debug, Clone)]
pub struct PendingConfirmation {
    pub device_id: DeviceId,
    /// The latest value written. Every write re-arms the entry with its own.
    pub expected_state: DeviceStateValue,
    /// When the window last (re)started: at the write, and again when its
    /// push goes out to the gateway.
    pub sent_at: Instant,
    /// `SyncConfig::protection_window` - UI changes are PROTECTED!
    pub protection_window: Duration,
    /// `expected_state`'s own push has gone out to the gateway. Only then can
    /// the hub reporting that value be its confirmation. Before, the hub
    /// showing it is a coincidence: a write that toggles back to the hub's
    /// current value while the previous write's PATCH is still in flight,
    /// which is about to move the hub away. A newer write resets it (#32).
    pub pushed: bool,
}

#[derive(Clone)]
pub struct SyncEngine {
    store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
    gateway: Arc<dyn Gateway>,
    sync_queue: Arc<RwLock<VecDeque<SyncTask>>>,
    retry_queue: Arc<RwLock<HashMap<DeviceId, RetryEntry>>>,
    sync_status: Arc<RwLock<HashMap<DeviceId, SyncStatus>>>,
    config: SyncConfig,
    shutdown_tx: watch::Sender<bool>,
    // 🔥 OPTIMISTIC UPDATE TRACKING!
    optimistic_states: Arc<RwLock<HashMap<DeviceId, OptimisticState>>>,
    // 🔥 SYNC BUFFER - CONSTANTLY OVERWRITES, ONE PUSH PER DEVICE PER TICK! <3
    // This coalescing is the gateway throttle (see the module docs).
    sync_buffer: Arc<RwLock<HashMap<DeviceId, SyncBufferEntry>>>,
    // 🔥 PENDING CONFIRMATIONS - UI CHANGES GET PROTECTION WINDOW! 💖
    pending_confirmations: Arc<RwLock<HashMap<DeviceId, PendingConfirmation>>>,
}

#[derive(Debug, Clone)]
pub struct SyncTask {
    pub device_id: DeviceId,
    pub task_type: SyncTaskType,
    pub created_at: Instant,
    pub priority: SyncPriority,
}

#[derive(Debug, Clone)]
pub enum SyncTaskType {
    PushToGateway { new_state: DeviceStateValue },
    PullFromGateway,
    ForceRefresh,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncPriority {
    Critical = 0, // User-initiated changes
    High = 1,     // State conflicts
    Normal = 2,   // Regular sync
    Low = 3,      // Background refresh
}

/// A failed task waiting out its backoff. Once it's due it leaves the retry
/// queue: a push goes back into the sync buffer, anything else runs again.
/// If that fails, it comes back with one more attempt.
#[derive(Debug, Clone)]
pub struct RetryEntry {
    pub task: SyncTask,
    /// Failures so far of the write it retries: per write, not per device
    /// (#32).
    pub attempts: u32,
    pub next_retry: Instant,
    pub last_error: Option<String>,
    /// It retries a user write ([`SyncBufferEntry::protected`]).
    pub protected: bool,
}

#[derive(Debug, Clone)]
pub enum SyncStatus {
    InSync {
        last_synced: Timestamp,
    },
    PendingSync {
        queued_at: Timestamp,
    },
    Syncing {
        started_at: Timestamp,
    },
    Failed {
        error: String,
        failed_at: Timestamp,
    },
    Conflict {
        server_state: DeviceStateValue,
        gateway_state: DeviceStateValue,
    },
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// How often queued work goes out. The buffer worker drains the sync
    /// buffer at this interval (at most one push per device per tick: the
    /// engine's only gateway throttle, see the module docs), and the push
    /// worker runs a batch of queued tasks.
    pub push_interval: Duration,
    pub pull_interval: Duration,
    /// How many failed pushes of one write give it up, the first failure
    /// included: at `1` a failed push isn't retried. A newer write to the
    /// device starts its own count.
    pub max_retry_attempts: u32,
    /// Wait before a failed push's first retry. Each further attempt doubles
    /// it, up to `max_retry_delay`.
    pub base_retry_delay: Duration,
    pub max_retry_delay: Duration,
    /// How often the retry worker looks for retries that are due. A retry
    /// goes out at the first check after its backoff delay, so its actual
    /// wait is rounded up to the next check.
    pub retry_interval: Duration,
    pub batch_size: usize,
    pub conflict_resolution: ConflictResolution,
    // 🔥 NEW VIBEOPTIMIZATION SETTINGS!
    pub optimistic_updates: bool,
    /// Queue user writes ([`SyncEngine::apply_optimistic_update`]) as
    /// `SyncPriority::Critical` instead of `High`.
    ///
    /// This doesn't change their protection: every user write gets a
    /// protection window either way (#32). Nothing orders buffered pushes by
    /// priority either (each drain sends every device's latest value), so
    /// for now the flag only changes the priority recorded on the push and
    /// on its retry.
    pub client_priority_boost: bool,
    /// 🛡️ How long a user write is shielded from pulls that still report the
    /// old value. It starts at the write and restarts when the push goes out,
    /// so it covers both the wait in the sync buffer and the hub's round trip.
    ///
    /// The flip side: a physical-switch change on a device with a write in
    /// flight is ignored for up to the write's queue delay plus this window.
    /// That's at most about twice the window (10 s at the defaults): a write
    /// that waits longer than one window is expired by the next pull that
    /// disagrees, and its push then starts a fresh window. The same holds
    /// with `optimistic_updates` off.
    pub protection_window: Duration,
}

#[derive(Debug, Clone)]
pub enum ConflictResolution {
    ServerWins,
    GatewayWins,
    TimestampWins,
    Manual,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            push_interval: Duration::from_millis(50), // 🔥 ULTRA FAST! 50ms latency!
            pull_interval: Duration::from_secs(2),    // 🔥 FASTER PHYSICAL SWITCH DETECTION!
            max_retry_attempts: 5,
            base_retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            retry_interval: Duration::from_secs(1),
            batch_size: 10,
            conflict_resolution: ConflictResolution::GatewayWins, // 🔥 PHYSICAL SWITCHES WIN! <3
            // 🔥 VIBEOPTIMIZED DEFAULTS!
            optimistic_updates: true,    // INSTANT UI FEEDBACK!
            client_priority_boost: true, // TUI FEELS INSTANT!
            protection_window: Duration::from_secs(5), // 🛡️ UI changes are PROTECTED!
        }
    }
}

impl SyncEngine {
    pub fn new(
        store: Arc<StateStore>,
        event_bus: Arc<EventBus>,
        gateway: Arc<dyn Gateway>,
        config: Option<SyncConfig>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let config = config.unwrap_or_default();

        Self {
            store,
            event_bus,
            gateway,
            sync_queue: Arc::new(RwLock::new(VecDeque::new())),
            retry_queue: Arc::new(RwLock::new(HashMap::new())),
            sync_status: Arc::new(RwLock::new(HashMap::new())),
            optimistic_states: Arc::new(RwLock::new(HashMap::new())),
            sync_buffer: Arc::new(RwLock::new(HashMap::new())),
            pending_confirmations: Arc::new(RwLock::new(HashMap::new())), // 🔥 UI PROTECTION! 💖
            config,
            shutdown_tx,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        info!("Starting sync engine");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Start sync workers
        let push_worker = self.start_push_worker();
        let pull_worker = self.start_pull_worker();
        let retry_worker = self.start_retry_worker();
        let buffer_worker = self.start_buffer_worker(); // 🔥 NEW BUFFER WORKER!

        // Wait for shutdown signal
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Sync engine shutdown requested");
            }
            result = push_worker => {
                error!("Push worker terminated: {:?}", result);
            }
            result = pull_worker => {
                error!("Pull worker terminated: {:?}", result);
            }
            result = retry_worker => {
                error!("Retry worker terminated: {:?}", result);
            }
            result = buffer_worker => {
                error!("Buffer worker terminated: {:?}", result);
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        info!("Stopping sync engine");
        let _ = self.shutdown_tx.send(true);
    }

    /// Queue a sync task. A `PushToGateway` goes into the sync buffer, where
    /// the latest value per device wins; anything else joins the task queue
    /// by priority.
    ///
    /// A `Critical` push counts as a user write and gets a protection window
    /// ([`SyncConfig::protection_window`]). User writes normally come in
    /// through [`Self::apply_optimistic_update`], which protects them whatever
    /// their priority.
    pub async fn queue_sync(&self, task: SyncTask) {
        debug!("Queuing sync task: {:?}", task);
        self.mark_pending_sync(&task.device_id).await;

        // 🔥 USE SYNC BUFFER FOR PushToGateway - OVERWRITES CONSTANTLY! <3
        if let SyncTaskType::PushToGateway { new_state } = &task.task_type {
            let protected = task.priority == SyncPriority::Critical;
            let mut buffer = self.sync_buffer.write().await;
            // 🛡️ A user write is protected from the moment it's queued, not
            // from when its push reaches the gateway (#23). Arming under the
            // buffer lock means a drained value is never newer than what the
            // pending confirmation expects.
            if protected {
                self.arm_protection(&task.device_id, new_state).await;
            }
            Self::buffer_push(
                &mut buffer,
                &task.device_id,
                new_state,
                task.priority.clone(),
                protected,
                0,
            );
        } else {
            // Non-push tasks go to regular queue
            let mut queue = self.sync_queue.write().await;

            // Insert based on priority
            let insert_pos = queue
                .iter()
                .position(|t| t.priority > task.priority)
                .unwrap_or(queue.len());
            queue.insert(insert_pos, task);
        }
    }

    async fn mark_pending_sync(&self, device_id: &DeviceId) {
        let mut status = self.sync_status.write().await;
        status.insert(
            device_id.clone(),
            SyncStatus::PendingSync {
                queued_at: chrono::Utc::now().timestamp_millis() as u64,
            },
        );
    }

    /// 💖 Put `state` in the sync buffer (`buffer`, under its held lock) for
    /// `device_id`, replacing whatever was waiting there. A `protected`
    /// entry is a user write: the caller has armed its window, and the drain
    /// restarts it, unless the entry is a retry (`failed_attempts` > 0).
    fn buffer_push(
        buffer: &mut HashMap<DeviceId, SyncBufferEntry>,
        device_id: &DeviceId,
        state: &DeviceStateValue,
        priority: SyncPriority,
        protected: bool,
        failed_attempts: u32,
    ) {
        buffer.insert(
            device_id.clone(),
            SyncBufferEntry {
                state: state.clone(),
                updated_at: Instant::now(),
                priority,
                protected,
                failed_attempts,
            },
        );
        debug!(
            "💖 BUFFER UPDATED for {} - the next drain sends the latest value!",
            device_id
        );
    }

    /// The configuration this engine runs with.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    // 🔥 OPTIMISTIC UPDATE - INSTANT UI FEEDBACK!
    /// A user write: queue `new_state` for the gateway. With
    /// `optimistic_updates` on, it also goes into the store right away and is
    /// echoed on the event bus. With it off, only the push is queued: the
    /// store and the echo follow once a pull confirms the push.
    ///
    /// 🛡️ Every write through here gets a protection window
    /// ([`SyncConfig::protection_window`]), whatever priority
    /// `client_priority_boost` gives it (#32). This is the entry point for
    /// user writes (the API server's handlers and the virtual manager's
    /// member fan-out), and the path where the store runs ahead of the hub
    /// until the push lands, which is what the window covers. Protection
    /// used to follow the priority instead, so with the boost off (`High`)
    /// a pull before the push landed reverted the write.
    ///
    /// 🔒 A write is one critical section, under the sync buffer's lock
    /// (#32): arm the window, write the store, echo, queue the push. Two
    /// writes to one device can't interleave, so the store, the order of the
    /// echoes, the pending confirmation and the buffered push all end on the
    /// same (the later) write. They used to be separate steps, and a write
    /// that overtook another between its store write and its push left the
    /// store on one value and the hub on the other.
    ///
    /// Lock order: sync buffer, then pending confirmations (#23's order,
    /// also `queue_sync`'s). The store, the event bus and the optimistic
    /// states are taken and released inside; none of them is ever held
    /// while one of the first two is taken.
    pub async fn apply_optimistic_update(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> anyhow::Result<()> {
        debug!(
            "🔥 OPTIMISTIC UPDATE for device {} - UI FEELS INSTANT!",
            device_id
        );

        let priority = if self.config.client_priority_boost || !self.config.optimistic_updates {
            SyncPriority::Critical // 🔥 CLIENT CHANGES GET PRIORITY!
        } else {
            SyncPriority::High
        };
        self.mark_pending_sync(device_id).await;

        // 🔒 tests/optimistic_update.rs waits for this line: it's how it
        // knows a second write is about to queue behind the first.
        debug!("🔒 {} waiting for the write lock", device_id);
        let mut buffer = self.sync_buffer.write().await;

        // 0. 🛡️ ARM THE PROTECTION WINDOW FIRST (#23)! The store is about to
        // run ahead of the hub, and a pull that lands before the push goes
        // out must not take that for an external change and revert it.
        self.arm_protection(device_id, &new_state).await;

        if self.config.optimistic_updates {
            // 1. IMMEDIATELY update local state - NO WAITING!
            if let Err(e) = self
                .store
                .update_device_state(device_id, new_state.clone())
                .await
            {
                // Nothing was written, so there's nothing to protect.
                self.pending_confirmations.write().await.remove(device_id);
                return Err(e.into());
            }

            // 2. Broadcast event IMMEDIATELY - UI updates NOW! Still under
            // the lock, so the last echo is the store's value.
            self.event_bus
                .publish(DeviceEvent {
                    timestamp: std::time::SystemTime::now(),
                    device_id: device_id.clone(),
                    event_type: EventType::AttributeChanged {
                        attribute: "state".to_string(),
                        old_value: serde_json::Value::Null,
                        new_value: serde_json::to_value(&new_state)
                            .unwrap_or(serde_json::Value::Null),
                    },
                })
                .await;

            // 3. Track optimistic state
            self.optimistic_states.write().await.insert(
                device_id.clone(),
                OptimisticState {
                    client_state: new_state.clone(),
                    pending_sync: true,
                    updated_at: Instant::now(),
                },
            );
        }

        // 4. Queue gateway sync in background (with boost if enabled).
        Self::buffer_push(&mut buffer, device_id, &new_state, priority, true, 0);
        drop(buffer);

        debug!("✅ Optimistic update applied - user sees change INSTANTLY!");
        Ok(())
    }

    // Push worker: Send server state changes to gateway
    async fn start_push_worker(&self) -> anyhow::Result<()> {
        let mut interval = interval(self.config.push_interval);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.process_push_queue().await {
                        warn!("Push worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Push worker shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    // Pull worker: Periodically check gateway for state changes
    async fn start_pull_worker(&self) -> anyhow::Result<()> {
        let mut interval = interval(self.config.pull_interval);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.pull_gateway_states().await {
                        warn!("Pull worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Pull worker shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    // Retry worker: Handle failed sync operations
    async fn start_retry_worker(&self) -> anyhow::Result<()> {
        let mut interval = interval(self.config.retry_interval);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.process_retries().await {
                        warn!("Retry worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Retry worker shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    // 🔥 BUFFER WORKER - ULTRA LOW LATENCY MODE! FASTER RESPONSE! 💖
    async fn start_buffer_worker(&self) -> anyhow::Result<()> {
        // 🚀 Drain the buffer every push_interval (50 ms by default: 20x/sec
        // for MIN latency!). This tick is the per-device push throttle.
        let mut interval = interval(self.config.push_interval);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        debug!(
            "🔥 BUFFER WORKER STARTED - ULTRA LOW LATENCY MODE! {:?} intervals!",
            self.config.push_interval
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.process_sync_buffer().await {
                        warn!("Buffer worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Buffer worker shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    // 🔥 PROCESS SYNC BUFFER - TAKES LATEST VALUES AND SYNCS THEM!
    async fn process_sync_buffer(&self) -> anyhow::Result<()> {
        let entries = {
            let mut buffer = self.sync_buffer.write().await;
            if buffer.is_empty() {
                return Ok(());
            }

            // Take ALL entries and clear buffer - we'll sync the latest state!
            let entries: Vec<(DeviceId, SyncBufferEntry)> = buffer.drain().collect();
            entries
        };

        if entries.is_empty() {
            return Ok(());
        }

        debug!("💖 Processing {} buffered sync entries", entries.len());

        // Process each buffered entry
        for (device_id, entry) in entries {
            // Create a sync task for this buffered entry
            let task = SyncTask {
                device_id: device_id.clone(),
                task_type: SyncTaskType::PushToGateway {
                    new_state: entry.state.clone(),
                },
                created_at: entry.updated_at,
                priority: entry.priority.clone(), // Clone to fix move error
            };

            // 🔥 RESTART THE PROTECTION WINDOW - THE PUSH GOES OUT NOW! 💖
            // It was armed when the write came in (#23). However long this
            // entry waited behind other devices' pushes, the hub still gets
            // the full window to confirm it. Only the write's own push does
            // this, not its retries: a hub that keeps failing mustn't keep
            // the store protected.
            if entry.protected && entry.failed_attempts == 0 {
                self.refresh_protection(&device_id, &entry.state).await;
            }

            if let Err(e) = self.execute_sync_task(&task).await {
                // 🛡️ The pending confirmation stays and runs out on its own,
                // `protection_window` after the write's push went out. With
                // the defaults the first retry comes well inside it, so a
                // transient failure doesn't flicker the UI. A hub that stays
                // down doesn't keep the store protected: once the window is
                // up, the next pull reverts it to what the hub reports.
                // Removing the entry here could also unprotect a newer write
                // that already re-armed it.
                warn!("Buffered sync failed for {}: {}", device_id, e);
                self.queue_retry(&task, e.to_string(), entry.failed_attempts, entry.protected)
                    .await;
            } else {
                debug!("✅ BUFFERED SYNC SUCCESS for {} - no spam! <3", device_id);
            }
        }

        Ok(())
    }

    /// 🛡️ Start the protection window for a user write to `device_id`.
    ///
    /// This runs when the write is accepted, before the store shows it
    /// (#23). If the window started only when the push went out, a pull in
    /// between would find no pending confirmation, see the store ahead of
    /// the hub, and revert the write. A newer write re-arms the entry with
    /// its own value, so the entry always expects the latest one.
    async fn arm_protection(&self, device_id: &DeviceId, expected_state: &DeviceStateValue) {
        let mut pending = self.pending_confirmations.write().await;
        pending.insert(
            device_id.clone(),
            PendingConfirmation {
                device_id: device_id.clone(),
                expected_state: expected_state.clone(),
                sent_at: Instant::now(),
                protection_window: self.config.protection_window,
                pushed: false,
            },
        );
        debug!(
            "🛡️ SYNC_DEBUG: Armed pending confirmation for {} - {:?} protection window!",
            device_id, self.config.protection_window
        );
    }

    /// 🛡️ Restart the protection window as a buffered push goes out, so it
    /// still covers the hub's confirmation, and mark the entry
    /// [`PendingConfirmation::pushed`] if this push carries its value.
    ///
    /// `expected_state` stays as it is. Every write sets it, so it already
    /// holds `pushed` (the buffer keeps only the latest value) or a newer
    /// write that came in after this push was drained. Writing `pushed` back
    /// would move the expectation back to a stale value, and the hub
    /// confirming that stale value would then revert the newer write. For
    /// the same reason a stale push doesn't mark the entry pushed: the newer
    /// value hasn't gone out yet. If there is no entry (a pull already
    /// confirmed it, or it expired), the push re-arms one for `pushed`.
    async fn refresh_protection(&self, device_id: &DeviceId, pushed: &DeviceStateValue) {
        let mut pending = self.pending_confirmations.write().await;
        if let Some(confirmation) = pending.get_mut(device_id) {
            confirmation.sent_at = Instant::now();
            if self.states_equal(&confirmation.expected_state, pushed) {
                confirmation.pushed = true;
            }
            debug!(
                "🛡️ SYNC_DEBUG: Push going out for {} - protection window restarted!",
                device_id
            );
        } else {
            pending.insert(
                device_id.clone(),
                PendingConfirmation {
                    device_id: device_id.clone(),
                    expected_state: pushed.clone(),
                    sent_at: Instant::now(),
                    protection_window: self.config.protection_window,
                    pushed: true,
                },
            );
            debug!(
                "🛡️ SYNC_DEBUG: Push going out for {} - protection window re-armed!",
                device_id
            );
        }
    }

    async fn process_push_queue(&self) -> anyhow::Result<()> {
        let tasks = {
            let mut queue = self.sync_queue.write().await;
            let batch_size = self.config.batch_size.min(queue.len());
            queue.drain(..batch_size).collect::<Vec<_>>()
        };

        if tasks.is_empty() {
            return Ok(());
        }

        debug!("Processing {} push tasks", tasks.len());

        for task in tasks {
            if let Err(e) = self.execute_sync_task(&task).await {
                warn!("Sync task failed: {:?}, error: {}", task, e);
                self.queue_retry(&task, e.to_string(), 0, false).await;
            }
        }

        Ok(())
    }

    async fn execute_sync_task(&self, task: &SyncTask) -> anyhow::Result<()> {
        // Update status to syncing
        {
            let mut status = self.sync_status.write().await;
            status.insert(
                task.device_id.clone(),
                SyncStatus::Syncing {
                    started_at: chrono::Utc::now().timestamp_millis() as u64,
                },
            );
        }

        let result = match &task.task_type {
            SyncTaskType::PushToGateway { new_state } => {
                debug!("Pushing state to gateway for device: {}", task.device_id);

                // Check if this was an optimistic update
                let was_optimistic = {
                    let optimistic = self.optimistic_states.read().await;
                    optimistic.contains_key(&task.device_id)
                };

                if was_optimistic {
                    debug!(
                        "🔥 Syncing optimistic update to gateway for {}",
                        task.device_id
                    );
                }

                self.gateway
                    .set_device_state(&task.device_id, new_state.clone())
                    .await
            }
            SyncTaskType::PullFromGateway => {
                debug!("Pulling state from gateway for device: {}", task.device_id);
                let gateway_state = self.gateway.get_device_state(&task.device_id).await?;
                self.handle_gateway_state_change(&task.device_id, gateway_state)
                    .await
            }
            SyncTaskType::ForceRefresh => {
                debug!("Force refreshing device: {}", task.device_id);
                let gateway_state = self.gateway.get_device_state(&task.device_id).await?;
                self.store
                    .update_device_state(&task.device_id, gateway_state)
                    .await?;
                Ok(())
            }
        };

        match result {
            Ok(()) => {
                // Success - update status
                let mut status = self.sync_status.write().await;
                status.insert(
                    task.device_id.clone(),
                    SyncStatus::InSync {
                        last_synced: chrono::Utc::now().timestamp_millis() as u64,
                    },
                );

                // Remove from retry queue if it was there
                let mut retry_queue = self.retry_queue.write().await;
                retry_queue.remove(&task.device_id);

                // 🔥 Clear optimistic state after successful sync!
                if matches!(&task.task_type, SyncTaskType::PushToGateway { .. }) {
                    let mut optimistic = self.optimistic_states.write().await;
                    if optimistic.remove(&task.device_id).is_some() {
                        debug!("✅ Optimistic update confirmed for {}", task.device_id);
                    }
                }

                debug!("Sync task completed successfully: {:?}", task);
            }
            Err(e) => {
                // Failed - update status
                let mut status = self.sync_status.write().await;
                status.insert(
                    task.device_id.clone(),
                    SyncStatus::Failed {
                        error: e.to_string(),
                        failed_at: chrono::Utc::now().timestamp_millis() as u64,
                    },
                );

                return Err(e.into());
            }
        }

        Ok(())
    }

    async fn handle_gateway_state_change(
        &self,
        device_id: &DeviceId,
        gateway_state: DeviceStateValue,
    ) -> Result<(), GatewayError> {
        // 🔥 CHECK PENDING CONFIRMATIONS - UI CHANGES ARE PROTECTED! 💖
        {
            let mut pending = self.pending_confirmations.write().await;
            if let Some(confirmation) = pending.get(device_id) {
                let elapsed = confirmation.sent_at.elapsed();
                let expected_state = confirmation.expected_state.clone(); // Clone for later use

                if elapsed < confirmation.protection_window {
                    // Still in protection window - check if this is our
                    // confirmation: our value, and its push has gone out.
                    // Before that, the hub showing it is a coincidence (#32).
                    if confirmation.pushed && self.states_equal(&gateway_state, &expected_state) {
                        debug!(
                            "✅ SYNC_DEBUG: UI change confirmed for {} after {:?}",
                            device_id, elapsed
                        );
                        pending.remove(device_id);
                        drop(pending); // Release lock before async operations

                        // Update local state with confirmed value
                        if let Err(e) = self
                            .store
                            .update_device_state(device_id, gateway_state.clone())
                            .await
                        {
                            warn!("Failed to update confirmed state: {}", e);
                        }

                        // 🔥 PUBLISH EVENT SO WEBUI KNOWS! 💖
                        self.event_bus
                            .publish(DeviceEvent {
                                timestamp: std::time::SystemTime::now(),
                                device_id: device_id.clone(),
                                event_type: EventType::AttributeChanged {
                                    attribute: "state".to_string(),
                                    old_value: serde_json::to_value(&expected_state)
                                        .unwrap_or(serde_json::Value::Null),
                                    new_value: serde_json::to_value(&gateway_state)
                                        .unwrap_or(serde_json::Value::Null),
                                },
                            })
                            .await;
                        debug!(
                            "📢 SYNC_DEBUG: Event published for confirmed change on {}",
                            device_id
                        );

                        return Ok(());
                    } else {
                        // Not our change (or our value before its push went
                        // out) - IGNORE during protection window!
                        let remaining = confirmation.protection_window - elapsed;
                        debug!("🛡️ SYNC_DEBUG: Ignoring pull for {} - protection active ({:?} remaining)", 
                               device_id, remaining);
                        return Ok(()); // IGNORE THIS UPDATE!
                    }
                } else {
                    // Protection window expired
                    debug!(
                        "⏰ SYNC_DEBUG: Protection timeout for {} - accepting external state",
                        device_id
                    );
                    pending.remove(device_id);
                }
            }
        }

        let server_device = self.store.get_device(device_id).await;

        match server_device {
            Some(server_device) => {
                // Check for conflicts
                if !self.states_equal(&server_device.state, &gateway_state) {
                    debug!(
                        "🔍 SYNC_DEBUG: State mismatch for {} - resolving conflict",
                        device_id
                    );
                    self.resolve_conflict(device_id, &server_device.state, &gateway_state)
                        .await?;
                } else {
                    debug!("🔍 SYNC_DEBUG: States already match for {}", device_id);
                }
            }
            None => {
                warn!("Gateway reported state for unknown device: {}", device_id);
            }
        }

        Ok(())
    }

    async fn resolve_conflict(
        &self,
        device_id: &DeviceId,
        server_state: &DeviceStateValue,
        gateway_state: &DeviceStateValue,
    ) -> Result<(), GatewayError> {
        debug!("Resolving conflict for device: {}", device_id);

        let resolution = match self.config.conflict_resolution {
            ConflictResolution::ServerWins => {
                debug!("Conflict resolution: Server wins for {}", device_id);
                // Only push server state to gateway for writable device types
                if self.is_device_writable(server_state)
                    && !self.is_problematic_outlet(device_id).await
                {
                    self.gateway
                        .set_device_state(device_id, server_state.clone())
                        .await?;
                } else {
                    // For read-only devices (sensors) or problematic outlets, always use gateway state
                    debug!(
                        "Device {} is read-only or problematic outlet, using gateway state instead",
                        device_id
                    );
                    self.store
                        .update_device_state(device_id, gateway_state.clone())
                        .await
                        .map_err(|e| GatewayError::InternalError(e.to_string()))?;
                }
                Ok(())
            }
            ConflictResolution::GatewayWins => {
                debug!("Conflict resolution: Gateway wins for {}", device_id);
                // Update server with gateway state
                self.store
                    .update_device_state(device_id, gateway_state.clone())
                    .await
                    .map_err(|e| GatewayError::InternalError(e.to_string()))?;

                // Broadcast event
                self.event_bus
                    .publish(DeviceEvent {
                        timestamp: std::time::SystemTime::now(),
                        device_id: device_id.clone(),
                        event_type: EventType::AttributeChanged {
                            attribute: "state".to_string(),
                            old_value: serde_json::to_value(server_state)
                                .unwrap_or(serde_json::Value::Null),
                            new_value: serde_json::to_value(gateway_state)
                                .unwrap_or(serde_json::Value::Null),
                        },
                    })
                    .await;
                Ok(())
            }
            ConflictResolution::TimestampWins => {
                // Get timestamps and decide
                let server_timestamp = self.get_state_timestamp(server_state);
                let gateway_timestamp = self.get_state_timestamp(gateway_state);

                if server_timestamp >= gateway_timestamp {
                    debug!(
                        "Conflict resolution: Server timestamp wins for {}",
                        device_id
                    );
                    // Only push server state to gateway for writable device types
                    if self.is_device_writable(server_state)
                        && !self.is_problematic_outlet(device_id).await
                    {
                        self.gateway
                            .set_device_state(device_id, server_state.clone())
                            .await?;
                    } else {
                        // For read-only devices (sensors) or problematic outlets, always use gateway state
                        debug!("Device {} is read-only or problematic outlet, using gateway state instead", device_id);
                        self.store
                            .update_device_state(device_id, gateway_state.clone())
                            .await
                            .map_err(|e| GatewayError::InternalError(e.to_string()))?;
                    }
                } else {
                    debug!(
                        "Conflict resolution: Gateway timestamp wins for {}",
                        device_id
                    );
                    self.store
                        .update_device_state(device_id, gateway_state.clone())
                        .await
                        .map_err(|e| GatewayError::InternalError(e.to_string()))?;

                    self.event_bus
                        .publish(DeviceEvent {
                            timestamp: std::time::SystemTime::now(),
                            device_id: device_id.clone(),
                            event_type: EventType::AttributeChanged {
                                attribute: "state".to_string(),
                                old_value: serde_json::to_value(server_state)
                                    .unwrap_or(serde_json::Value::Null),
                                new_value: serde_json::to_value(gateway_state)
                                    .unwrap_or(serde_json::Value::Null),
                            },
                        })
                        .await;
                }
                Ok(())
            }
            ConflictResolution::Manual => {
                warn!(
                    "Manual conflict resolution required for device: {}",
                    device_id
                );

                // Store conflict for manual resolution
                let mut status = self.sync_status.write().await;
                status.insert(
                    device_id.clone(),
                    SyncStatus::Conflict {
                        server_state: server_state.clone(),
                        gateway_state: gateway_state.clone(),
                    },
                );
                Ok(())
            }
        };

        resolution
    }

    /// Schedule a retry of `task`, which just failed after `failed_before`
    /// earlier failures of the same write, with exponential backoff. After
    /// `max_retry_attempts` failures, the first one included, it's given up.
    /// `protected`: it's a user write ([`SyncBufferEntry::protected`]).
    ///
    /// The count is per write (#32). It travels with the retry through the
    /// sync buffer, a newer write starts from 0, and its retry replaces the
    /// waiting retry of an older write (whose value the newer one
    /// supersedes). It used to be per device, so a new write to a device
    /// with failures behind it was given up early. The first failure used to
    /// skip the cap, so `max_retry_attempts: 1` still retried once.
    async fn queue_retry(
        &self,
        task: &SyncTask,
        error: String,
        failed_before: u32,
        protected: bool,
    ) {
        let attempts = failed_before.saturating_add(1);
        let mut retry_queue = self.retry_queue.write().await;

        if attempts >= self.config.max_retry_attempts {
            // ❌ Drop it. Leaving the entry in place kept it due at every
            // retry tick, so it was retried forever (#32).
            warn!(
                "❌ Max retry attempts ({}) reached for device {}, giving up",
                attempts, task.device_id
            );
            retry_queue.remove(&task.device_id);
            return;
        }

        // Exponential backoff, capped. Saturating, so a large
        // `max_retry_attempts` can't overflow it.
        let factor = 2_u32.saturating_pow(attempts - 1);
        let delay = self
            .config
            .base_retry_delay
            .checked_mul(factor)
            .unwrap_or(self.config.max_retry_delay)
            .min(self.config.max_retry_delay);
        retry_queue.insert(
            task.device_id.clone(),
            RetryEntry {
                task: task.clone(),
                attempts,
                next_retry: Instant::now() + delay,
                last_error: Some(error),
                protected,
            },
        );

        debug!(
            "Queued retry for device: {} (attempt {})",
            task.device_id, attempts
        );
    }

    /// 🔁 Take the retries that are due out of the retry queue. A push goes
    /// back into the sync buffer ([`Self::requeue_retry`]); anything else
    /// runs again here.
    async fn process_retries(&self) -> anyhow::Result<()> {
        let now = Instant::now();
        let mut due = Vec::new();
        self.retry_queue.write().await.retain(|_, entry| {
            let is_due = entry.next_retry <= now;
            if is_due {
                due.push(entry.clone());
            }
            !is_due
        });

        for retry in due {
            debug!("Processing retry for device: {}", retry.task.device_id);
            if let SyncTaskType::PushToGateway { .. } = retry.task.task_type {
                self.requeue_retry(retry).await;
            } else if let Err(e) = self.execute_sync_task(&retry.task).await {
                self.queue_retry(&retry.task, e.to_string(), retry.attempts, retry.protected)
                    .await;
            }
        }

        Ok(())
    }

    /// 🔁 Put a due push retry back into the sync buffer, with the value
    /// [`Self::latest_push_state`] picks, for the buffer worker to send like
    /// any other push (#32).
    ///
    /// So every push of a device goes out from the buffer worker, one at a
    /// time and in order. The retry worker used to send it to the gateway
    /// itself, so a retry could be in flight while the buffer worker sent a
    /// newer write of the same device. A hub that answered the newer PATCH
    /// first ended on the retry's older value, which the pending entry never
    /// took for its confirmation; once the window ran out, the pull took the
    /// hub's value, and the user's write was reverted and never pushed again.
    ///
    /// The entry keeps the retry's failure count and whether it's a user
    /// write. Being a retry, its push doesn't restart the protection window:
    /// only a write's own push does.
    async fn requeue_retry(&self, retry: RetryEntry) {
        let device_id = &retry.task.device_id;
        let mut buffer = self.sync_buffer.write().await;
        match self.latest_push_state(&buffer, &retry).await {
            Some(state) => Self::buffer_push(
                &mut buffer,
                device_id,
                &state,
                retry.task.priority.clone(),
                retry.protected,
                retry.attempts,
            ),
            None => debug!(
                "🔁 Retry for {} dropped: a newer write is queued, or the device is gone",
                device_id
            ),
        }
    }

    /// 🔁 What a due push `retry` sends now, or `None` if there's nothing
    /// left to retry. `buffer` is the sync buffer, under the lock the caller
    /// holds to queue the retry, so no write can come in between.
    ///
    /// A retry sends the device's latest value, not the value that failed
    /// (#32): that one may be older than a write made since, and landing
    /// after it would overwrite it on the hub.
    ///
    /// - A write waiting in the sync buffer supersedes the retry: the next
    ///   drain pushes it, and it gets its own retries if that fails.
    /// - Otherwise a pending confirmation's `expected_state` is the latest
    ///   user write (every write re-arms it, under the buffer lock held
    ///   here). With `optimistic_updates` off, the store doesn't have it yet.
    /// - Otherwise it's the store's value: what the engine now holds for the
    ///   device. Once a write's window ran out and the hub's value won, that
    ///   is the hub's own value, so the write is abandoned rather than
    ///   pushed after the UI already showed it reverted.
    /// - A device that's no longer in the store has nothing to retry.
    async fn latest_push_state(
        &self,
        buffer: &HashMap<DeviceId, SyncBufferEntry>,
        retry: &RetryEntry,
    ) -> Option<DeviceStateValue> {
        let device_id = &retry.task.device_id;
        if buffer.contains_key(device_id) {
            return None;
        }
        if let Some(confirmation) = self.pending_confirmations.read().await.get(device_id) {
            return Some(confirmation.expected_state.clone());
        }
        self.store
            .get_device(device_id)
            .await
            .map(|device| device.state)
    }

    /// One pull cycle: read every device in the store from the gateway and
    /// reconcile it. A device that fails (its read, or handling what it
    /// read) is logged and skipped; the rest of the cycle still runs.
    async fn pull_gateway_states(&self) -> anyhow::Result<()> {
        debug!("Pulling states from gateway");

        self.forget_removed_devices().await;
        let devices = self.store.list_devices().await;
        let mut updated_count = 0;

        for device in devices {
            match self.gateway.get_device_state(&device.device_id).await {
                Ok(gateway_state) => {
                    if !self.states_equal(&device.state, &gateway_state) {
                        debug!(
                            "🔍 SYNC_DEBUG: Pull detected change for {} - handling",
                            device.device_id
                        );
                        // ❌ One device's error must not cost the others
                        // their pull: log it and go on to the next device.
                        match self
                            .handle_gateway_state_change(&device.device_id, gateway_state)
                            .await
                        {
                            Ok(()) => updated_count += 1,
                            Err(e) => warn!(
                                "❌ Pull: handling the gateway state of {} failed: {}",
                                device.device_id, e
                            ),
                        }
                    } else {
                        // ✅ Nothing to reconcile, but this may still be the
                        // hub confirming a user write (#32).
                        self.clear_confirmed(&device.device_id, &gateway_state)
                            .await;
                    }
                }
                Err(e) => {
                    debug!(
                        "Failed to get gateway state for device {}: {}",
                        device.device_id, e
                    );
                }
            }
        }

        if updated_count > 0 {
            info!("Updated {} device states from gateway", updated_count);
        }

        Ok(())
    }

    /// 🧹 Drop the pending confirmations and optimistic-state records of
    /// devices that are no longer in the store (#32).
    ///
    /// Nothing else would: a pending confirmation is only cleared by a pull
    /// of its device (or a write's failed store write), and pulls only read
    /// devices in the store. A device that came back under the same id
    /// (rediscovered, say) would start with a stale window that ignores its
    /// first real changes.
    ///
    /// This takes the buffer lock, then the pending one (the engine's lock
    /// order), so it can't run in the middle of a write: a write arms its
    /// entry and checks the store in one critical section under the buffer
    /// lock (see [`Self::apply_optimistic_update`]).
    async fn forget_removed_devices(&self) {
        let _buffer = self.sync_buffer.write().await;
        let mut pending = self.pending_confirmations.write().await;
        let mut optimistic = self.optimistic_states.write().await;
        let tracked: Vec<DeviceId> = pending.keys().chain(optimistic.keys()).cloned().collect();
        for device_id in tracked {
            if self.store.get_device(&device_id).await.is_none() {
                let had_pending = pending.remove(&device_id).is_some();
                optimistic.remove(&device_id);
                if had_pending {
                    debug!(
                        "🧹 {} is gone from the store - dropped its pending confirmation",
                        device_id
                    );
                }
            }
        }
    }

    /// ✅ A pull found the store and the hub agreeing on `gateway_state`. If
    /// that's the value a pending user write expects, and that value's push
    /// has gone out, this is the hub's confirmation: drop the entry.
    ///
    /// With optimistic updates the store shows a write before the hub does,
    /// so once the push lands the periodic pull finds the two equal and never
    /// reaches [`Self::handle_gateway_state_change`], where confirmations are
    /// otherwise taken. Without this, the entry lingered for the rest of its
    /// window, and a physical-switch change in that time was ignored as "not
    /// our change". There's nothing to echo: the store already had the value,
    /// and the write echoed it. An entry expecting something else is left
    /// alone; it still waits for its own value (a newer write, say).
    ///
    /// So is an entry whose push hasn't gone out yet
    /// ([`PendingConfirmation::pushed`]). A user who toggles a light on, and
    /// back off while the `on` PATCH is in flight, leaves the store, the hub
    /// and the entry all on `off`. The hub hasn't confirmed anything, though:
    /// the `on` PATCH is about to land. Taking that for a confirmation
    /// dropped the entry, and the pull after the PATCH landed reverted the
    /// toggle-back.
    async fn clear_confirmed(&self, device_id: &DeviceId, gateway_state: &DeviceStateValue) {
        let mut pending = self.pending_confirmations.write().await;
        let confirmed = pending.get(device_id).is_some_and(|confirmation| {
            confirmation.pushed && self.states_equal(&confirmation.expected_state, gateway_state)
        });
        if confirmed {
            pending.remove(device_id);
            debug!(
                "✅ SYNC_DEBUG: UI change confirmed for {} - store and hub agree",
                device_id
            );
        }
    }

    fn states_equal(&self, state1: &DeviceStateValue, state2: &DeviceStateValue) -> bool {
        // 🔥 PROPER STATE COMPARISON - NO MORE DEBUG FORMAT! <3
        match (state1, state2) {
            (DeviceStateValue::Light(l1), DeviceStateValue::Light(l2)) => {
                l1.is_on == l2.is_on
                    && l1.brightness == l2.brightness
                    && l1.color_temp == l2.color_temp
                    && l1.rgb_color == l2.rgb_color
            }
            (DeviceStateValue::Outlet(o1), DeviceStateValue::Outlet(o2)) => o1.is_on == o2.is_on,
            (DeviceStateValue::Sensor(s1), DeviceStateValue::Sensor(s2)) => {
                // Compare sensor values (temp/humidity)
                s1.temperature == s2.temperature && s1.humidity == s2.humidity
            }
            (DeviceStateValue::Switch(sw1), DeviceStateValue::Switch(sw2)) => {
                sw1.is_pressed == sw2.is_pressed && sw1.battery_level == sw2.battery_level
            }
            (DeviceStateValue::MotionSensor(m1), DeviceStateValue::MotionSensor(m2)) => {
                m1.motion_detected == m2.motion_detected && m1.battery_level == m2.battery_level
            }
            (DeviceStateValue::Scene(sc1), DeviceStateValue::Scene(sc2)) => {
                sc1.is_active == sc2.is_active
            }
            (DeviceStateValue::Timer(t1), DeviceStateValue::Timer(t2)) => {
                t1.timer_name == t2.timer_name && t1.action == t2.action
            }
            _ => false, // Different types are never equal
        }
    }

    fn get_state_timestamp(&self, _state: &DeviceStateValue) -> u64 {
        // TODO: Extract timestamp from state if available
        // For now, return current time
        chrono::Utc::now().timestamp_millis() as u64
    }

    /// Check if a device state is writable (can be pushed back to gateway)
    fn is_device_writable(&self, state: &DeviceStateValue) -> bool {
        match state {
            DeviceStateValue::Light(_) => true,         // Lights are writable
            DeviceStateValue::Switch(_) => true,        // Switches are writable
            DeviceStateValue::Sensor(_) => false,       // Sensors are read-only
            DeviceStateValue::Scene(_) => true,         // Scenes are writable
            DeviceStateValue::Timer(_) => true,         // Timers are writable
            DeviceStateValue::MotionSensor(_) => false, // Motion sensors are read-only
            DeviceStateValue::Outlet(_) => true,        // Outlets are writable
            DeviceStateValue::Empty => false,           // 🔥 Empty state is not writable! 💖
        }
    }

    /// Check if this is a problematic outlet that causes HTTP 500 errors
    async fn is_problematic_outlet(&self, device_id: &DeviceId) -> bool {
        // Known problematic outlet device IDs that cause HTTP 500 errors in Dirigera API
        // These should only be controlled via direct user commands, not sync engine
        matches!(
            device_id.as_str(),
            "f996b7c9-dd18-44c6-ab84-c10a64175df6_1" | // Deko outlet
            "c6438f6d-e3b9-4abe-8356-81554b9d7602_1" | // Deko Flagge outlet
            "132e021a-b407-4448-ac45-cc7cdadf7f02_1" | // Fan outlet
            "0d3698c6-7712-42f8-ab6b-f067cdb8ce82_1" | // Dose outlet
            "a316fc31-3199-4c65-91ef-c7934435e6c2_3" // Bunte Licht outlet
        )
    }

    pub async fn get_sync_status(&self, device_id: &DeviceId) -> Option<SyncStatus> {
        let status = self.sync_status.read().await;
        status.get(device_id).cloned()
    }

    pub async fn get_sync_stats(&self) -> SyncStats {
        let status = self.sync_status.read().await;
        let retry_queue = self.retry_queue.read().await;
        let sync_queue = self.sync_queue.read().await;

        let mut stats = SyncStats {
            pending_sync_tasks: sync_queue.len() as u32,
            retry_queue_size: retry_queue.len() as u32,
            ..Default::default()
        };

        for sync_status in status.values() {
            match sync_status {
                SyncStatus::InSync { .. } => stats.in_sync_devices += 1,
                SyncStatus::PendingSync { .. } => stats.pending_sync_devices += 1,
                SyncStatus::Syncing { .. } => stats.syncing_devices += 1,
                SyncStatus::Failed { .. } => stats.failed_devices += 1,
                SyncStatus::Conflict { .. } => stats.conflict_devices += 1,
            }
        }

        stats
    }
}

#[derive(Debug, Default)]
pub struct SyncStats {
    pub in_sync_devices: u32,
    pub pending_sync_devices: u32,
    pub syncing_devices: u32,
    pub failed_devices: u32,
    pub conflict_devices: u32,
    pub pending_sync_tasks: u32,
    pub retry_queue_size: u32,
}
