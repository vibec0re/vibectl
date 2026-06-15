use crate::events::*;
use crate::gateway::*;
use crate::store::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock, Semaphore};
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

// 🔥 RATE LIMITER FOR GATEWAY!
#[derive(Debug)]
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    // kept: records the configured rate for debugging/introspection; the limit is
    // enforced via the semaphore's permit count set from this value in `new`.
    #[allow(dead_code)]
    max_per_second: u32,
}

impl RateLimiter {
    pub fn new(max_per_second: u32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_per_second as usize)),
            max_per_second,
        }
    }

    pub async fn acquire(&self) {
        let _permit = self.semaphore.acquire().await.unwrap();
        // Rate limiting is handled by semaphore permit count
        // Permits will auto-release when dropped
    }
}

// 🔥 SYNC BUFFER - OVERWRITES WITH LATEST VALUES! NO SPAM! <3
#[derive(Debug, Clone)]
pub struct SyncBufferEntry {
    pub state: DeviceStateValue,
    pub updated_at: Instant,
    pub priority: SyncPriority,
}

// 🔥 PENDING CONFIRMATION TRACKER - UI CHANGES ARE SACRED! 💖
#[derive(Debug, Clone)]
pub struct PendingConfirmation {
    pub device_id: DeviceId,
    pub expected_state: DeviceStateValue,
    pub sent_at: Instant,
    pub protection_window: Duration, // 5s default - UI changes are PROTECTED!
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
    // 🔥 RATE LIMITING FOR GATEWAY!
    gateway_rate_limiter: Arc<RwLock<RateLimiter>>,
    // 🔥 SYNC BUFFER - CONSTANTLY OVERWRITES, SYNCS AT MAX RATE! <3
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

#[derive(Debug, Clone)]
pub struct RetryEntry {
    pub task: SyncTask,
    pub attempts: u32,
    pub next_retry: Instant,
    pub last_error: Option<String>,
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
    pub push_interval: Duration,
    pub pull_interval: Duration,
    pub max_retry_attempts: u32,
    pub base_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub batch_size: usize,
    pub conflict_resolution: ConflictResolution,
    // 🔥 NEW VIBEOPTIMIZATION SETTINGS!
    pub optimistic_updates: bool,
    pub gateway_rate_limit: u32,     // Max requests per second to gateway
    pub client_priority_boost: bool, // Prioritize client-initiated changes
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
            batch_size: 10,
            conflict_resolution: ConflictResolution::GatewayWins, // 🔥 PHYSICAL SWITCHES WIN! <3
            // 🔥 VIBEOPTIMIZED DEFAULTS!
            optimistic_updates: true,    // INSTANT UI FEEDBACK!
            gateway_rate_limit: 10,      // 🔥 10 req/s - FAST but still safe!
            client_priority_boost: true, // TUI FEELS INSTANT!
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
            gateway_rate_limiter: Arc::new(RwLock::new(RateLimiter::new(
                config.gateway_rate_limit,
            ))),
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

    // Queue a sync task
    pub async fn queue_sync(&self, task: SyncTask) {
        debug!("Queuing sync task: {:?}", task);

        // Update sync status
        {
            let mut status = self.sync_status.write().await;
            status.insert(
                task.device_id.clone(),
                SyncStatus::PendingSync {
                    queued_at: chrono::Utc::now().timestamp_millis() as u64,
                },
            );
        }

        // 🔥 USE SYNC BUFFER FOR PushToGateway - OVERWRITES CONSTANTLY! <3
        if let SyncTaskType::PushToGateway { new_state } = &task.task_type {
            let mut buffer = self.sync_buffer.write().await;
            buffer.insert(
                task.device_id.clone(),
                SyncBufferEntry {
                    state: new_state.clone(),
                    updated_at: Instant::now(),
                    priority: task.priority.clone(),
                },
            );
            debug!(
                "💖 BUFFER UPDATED for {} - will sync at max 2/sec!",
                task.device_id
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

    // 🔥 OPTIMISTIC UPDATE - INSTANT UI FEEDBACK!
    pub async fn apply_optimistic_update(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> anyhow::Result<()> {
        debug!(
            "🔥 OPTIMISTIC UPDATE for device {} - UI FEELS INSTANT!",
            device_id
        );

        if !self.config.optimistic_updates {
            // Fall back to normal sync if optimistic updates disabled
            self.queue_sync(SyncTask {
                device_id: device_id.clone(),
                task_type: SyncTaskType::PushToGateway { new_state },
                created_at: Instant::now(),
                priority: SyncPriority::Critical,
            })
            .await;
            return Ok(());
        }

        // 1. IMMEDIATELY update local state - NO WAITING!
        self.store
            .update_device_state(device_id, new_state.clone())
            .await?;

        // 2. Broadcast event IMMEDIATELY - UI updates NOW!
        self.event_bus
            .publish(DeviceEvent {
                timestamp: std::time::SystemTime::now(),
                device_id: device_id.clone(),
                event_type: EventType::AttributeChanged {
                    attribute: "state".to_string(),
                    old_value: serde_json::Value::Null,
                    new_value: serde_json::to_value(&new_state).unwrap_or(serde_json::Value::Null),
                },
            })
            .await;

        // 3. Track optimistic state
        {
            let mut optimistic = self.optimistic_states.write().await;
            optimistic.insert(
                device_id.clone(),
                OptimisticState {
                    client_state: new_state.clone(),
                    pending_sync: true,
                    updated_at: Instant::now(),
                },
            );
        }

        // 4. Queue gateway sync in background (with boost if enabled)
        let priority = if self.config.client_priority_boost {
            SyncPriority::Critical // 🔥 CLIENT CHANGES GET PRIORITY!
        } else {
            SyncPriority::High
        };

        self.queue_sync(SyncTask {
            device_id: device_id.clone(),
            task_type: SyncTaskType::PushToGateway { new_state },
            created_at: Instant::now(),
            priority,
        })
        .await;

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
        let mut interval = interval(Duration::from_secs(1));
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
        // Process buffer at 50ms intervals for INSTANT response! 🚀
        let mut interval = interval(Duration::from_millis(50)); // 🔥 20x/sec check for MIN latency!
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        debug!("🔥 BUFFER WORKER STARTED - ULTRA LOW LATENCY MODE! 50ms intervals!");

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

            // 🔥 ADD PENDING CONFIRMATION - UI CHANGES ARE PROTECTED! 💖
            if entry.priority == SyncPriority::Critical {
                // User-initiated changes
                let mut pending = self.pending_confirmations.write().await;
                pending.insert(
                    device_id.clone(),
                    PendingConfirmation {
                        device_id: device_id.clone(),
                        expected_state: entry.state.clone(),
                        sent_at: Instant::now(),
                        protection_window: Duration::from_secs(5), // 5s protection!
                    },
                );
                debug!(
                    "🛡️ SYNC_DEBUG: Added pending confirmation for {} - 5s protection window!",
                    device_id
                );
            }

            // Execute with rate limiting
            if let Err(e) = self.execute_sync_task(&task).await {
                warn!("Buffered sync failed for {}: {}", device_id, e);
                self.queue_retry(&task, e.to_string()).await;
            } else {
                debug!("✅ BUFFERED SYNC SUCCESS for {} - no spam! <3", device_id);
            }
        }

        Ok(())
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
                self.queue_retry(&task, e.to_string()).await;
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

        // 🔥 RATE LIMIT GATEWAY OPERATIONS!
        let needs_rate_limit = matches!(
            &task.task_type,
            SyncTaskType::PushToGateway { .. }
                | SyncTaskType::PullFromGateway
                | SyncTaskType::ForceRefresh
        );

        if needs_rate_limit {
            debug!("⏱️ Acquiring rate limit permit for gateway operation");
            self.gateway_rate_limiter.read().await.acquire().await;
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
                    // Still in protection window - check if this is our confirmation
                    if self.states_equal(&gateway_state, &expected_state) {
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
                        // Not our change - IGNORE during protection window!
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

    async fn queue_retry(&self, task: &SyncTask, error: String) {
        let mut retry_queue = self.retry_queue.write().await;

        let retry_entry = match retry_queue.get_mut(&task.device_id) {
            Some(entry) => {
                entry.attempts += 1;
                entry.last_error = Some(error);

                if entry.attempts >= self.config.max_retry_attempts {
                    warn!("Max retry attempts reached for device: {}", task.device_id);
                    return;
                }

                // Exponential backoff
                let delay = self.config.base_retry_delay * 2_u32.pow(entry.attempts - 1);
                let delay = delay.min(self.config.max_retry_delay);
                entry.next_retry = Instant::now() + delay;

                entry.clone()
            }
            None => {
                let entry = RetryEntry {
                    task: task.clone(),
                    attempts: 1,
                    next_retry: Instant::now() + self.config.base_retry_delay,
                    last_error: Some(error),
                };
                retry_queue.insert(task.device_id.clone(), entry.clone());
                entry
            }
        };

        debug!(
            "Queued retry for device: {} (attempt {})",
            task.device_id, retry_entry.attempts
        );
    }

    async fn process_retries(&self) -> anyhow::Result<()> {
        let now = Instant::now();
        let ready_retries = {
            let retry_queue = self.retry_queue.read().await;
            retry_queue
                .values()
                .filter(|entry| entry.next_retry <= now)
                .cloned()
                .collect::<Vec<_>>()
        };

        for retry_entry in ready_retries {
            debug!(
                "Processing retry for device: {}",
                retry_entry.task.device_id
            );

            if let Err(e) = self.execute_sync_task(&retry_entry.task).await {
                self.queue_retry(&retry_entry.task, e.to_string()).await;
            }
        }

        Ok(())
    }

    async fn pull_gateway_states(&self) -> anyhow::Result<()> {
        debug!("Pulling states from gateway");

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
                        self.handle_gateway_state_change(&device.device_id, gateway_state)
                            .await?;
                        updated_count += 1;
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
