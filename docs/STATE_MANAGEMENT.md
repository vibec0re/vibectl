# State Management System Design

## Core Principles

1. **Single Source of Truth**: Server maintains authoritative state
2. **Eventual Consistency**: Gateway and server state sync over time
3. **Conflict Resolution**: Last-write-wins with timestamp ordering
4. **Real-time Propagation**: Changes broadcast immediately to subscribers

## State Store Architecture

### In-Memory State Store
```rust
struct StateStore {
    devices: HashMap<DeviceId, DeviceState>,
    virtual_devices: HashMap<DeviceId, VirtualDeviceState>,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    state_history: CircularBuffer<StateChange>,
    sync_status: HashMap<DeviceId, SyncStatus>,
}

struct DeviceState {
    device_id: DeviceId,
    device_type: DeviceType,
    state: DeviceStateValue,
    last_updated: Timestamp,
    last_synced_to_gateway: Option<Timestamp>,
    last_synced_from_gateway: Option<Timestamp>,
    pending_changes: Vec<PendingChange>,
}
```

### State Synchronization Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client API    │    │   State Store    │    │ Gateway Client  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │ 1. Set Light(50%)     │                       │
         ├──────────────────────▶│                       │
         │                       │ 2. Update State       │
         │                       │ 3. Queue Sync         │
         │                       ├──────────────────────▶│
         │                       │                       │ 4. Send to Gateway
         │                       │                       ├──────────▶ Dirigera
         │ 5. Response           │                       │
         ◄──────────────────────┤                       │
         │                       │ 6. Confirm Sync       │
         │                       ◄──────────────────────┤
         │                       │                       │
         │ 7. Broadcast Event    │                       │
         ◄──────────────────────┤                       │
```

## Synchronization Strategies

### Push-Based Sync (Server → Gateway)
- Immediate sync on state changes from API
- Retry logic with exponential backoff
- Dead letter queue for failed syncs
- Batch operations for multiple rapid changes

### Pull-Based Sync (Gateway → Server)  
- Periodic polling every 5 seconds
- Dirigera webhook subscriptions when available
- Change detection via state comparison
- Conflict resolution using timestamps

### Conflict Resolution Algorithm
```rust
fn resolve_conflict(
    server_state: &DeviceState,
    gateway_state: &DeviceState,
) -> ConflictResolution {
    match (server_state.last_updated, gateway_state.last_updated) {
        // Server state is newer - push to gateway
        (server_time, gateway_time) if server_time > gateway_time => {
            ConflictResolution::PushToGateway(server_state.clone())
        },
        // Gateway state is newer - update server
        (server_time, gateway_time) if gateway_time > server_time => {
            ConflictResolution::UpdateServer(gateway_state.clone())
        },
        // Same timestamp - prefer server state (rare edge case)
        _ => ConflictResolution::PushToGateway(server_state.clone())
    }
}
```

## State Change Events

### Event Types
```rust
enum StateChangeEvent {
    DeviceStateChanged {
        device_id: DeviceId,
        old_state: DeviceStateValue,
        new_state: DeviceStateValue,
        source: ChangeSource,
        timestamp: Timestamp,
    },
    DeviceAdded {
        device_id: DeviceId,
        device_info: DeviceInfo,
        timestamp: Timestamp,
    },
    DeviceRemoved {
        device_id: DeviceId,
        timestamp: Timestamp,
    },
    SyncStatusChanged {
        device_id: DeviceId,
        sync_status: SyncStatus,
        timestamp: Timestamp,
    },
}

enum ChangeSource {
    Api,           // Change via v1bectl API
    Gateway,       // Change detected from gateway
    VirtualDevice, // Change from virtual device logic
    System,        // System-initiated change
}
```

### Event Propagation
- Events stored in circular buffer (last 1000 events)
- Immediate broadcast to WebSocket subscribers
- Filtering based on subscription preferences
- Event deduplication to prevent loops

## Persistence Strategy

### Current Session Only
- All state in memory during server lifetime
- Fast startup by querying gateway for current state
- No complex migration or corruption issues

### Future: Optional Persistence
- SQLite for state history and analytics
- Recovery from last known state on startup
- Configurable retention policies

## Implementation Details (v1bectl_state crate)

### Core Components

#### StateStore (`store.rs`)
- Thread-safe in-memory store using `Arc<RwLock<HashMap>>`
- Device state tracking with timestamps
- Device group management
- Async CRUD operations for devices and groups

#### SyncEngine (`sync.rs`)
- **Workers**:
  - Push Worker: 100ms intervals for server → gateway
  - Pull Worker: 5s intervals for gateway → server  
  - Retry Worker: 1s tick for processing failed operations
- **Task Queue**: Priority-based with VecDeque
- **Retry Queue**: HashMap with exponential backoff tracking
- **Sync Status**: Per-device tracking (InSync, Pending, Syncing, Failed, Conflict)

#### EventBus (`events.rs`)
- Tokio broadcast channel (1000 subscriber capacity)
- Circular buffer for event history
- Real-time event publishing to all subscribers
- Event types: StateChange, DeviceAdded, DeviceRemoved, etc.

### Conflict Resolution Implementation

```rust
enum ConflictResolution {
    ServerWins,      // Always push server state to gateway
    GatewayWins,     // Always update server with gateway state
    TimestampWins,   // Compare timestamps, newer wins
    Manual,          // Store conflict for user resolution
}
```

## Performance Considerations

### Memory Usage
- Device limit: ~10,000 devices max
- State history: 1000 events (configurable)
- Subscription cleanup on client disconnect
- Sync queue: VecDeque with priority ordering
- Retry queue: HashMap with retry metadata

### Latency Optimization
- Local state queries: < 1ms
- Gateway sync: < 100ms typical
- WebSocket broadcasts: < 10ms
- Push sync interval: 100ms
- Pull sync interval: 5s

### Concurrency
- Read-heavy workload optimized with RwLock
- Atomic state updates using Arc<RwLock>
- Async I/O for all gateway communication
- Multiple tokio workers running concurrently

## Error Handling

### Sync Failures
- Retry with exponential backoff (1s, 2s, 4s, 8s, 16s max)
- Mark device as "sync pending" in UI
- Log detailed error information
- Continue serving stale data with warnings

### Gateway Disconnection
- Graceful degradation to read-only mode
- Queue all changes for when connection restored
- Health check endpoint reports gateway status
- Automatic reconnection attempts