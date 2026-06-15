# v1bectl API Specification

## Protocol Overview

- **Transport**: TCP Socket + WebSocket upgrade
- **Encoding**: CBOR (Compact Binary Object Representation)
- **Port**: 31337 (VIBEC0RE port! 🔥)
- **Authentication**: None (local network trusted)

## Message Structure

All messages follow this envelope format:

```rust
#[derive(Serialize, Deserialize)]
struct Message {
    correlation_id: String,    // UUID for request/response matching
    message_type: MessageType,
    payload: Vec<u8>,         // CBOR-encoded payload
    timestamp: u64,           // Unix timestamp in milliseconds
}

#[derive(Serialize, Deserialize)]
enum MessageType {
    Request,
    Response,
    Event,        // For subscriptions
    Error,
}
```

## Core API Functions

### Device Discovery & Enumeration

#### Discover All Devices
```rust
// Request
struct DiscoverDevicesRequest {
    force_refresh: Option<bool>,  // Force re-scan from gateway
    device_types: Option<Vec<DeviceType>>, // Filter by type
}

// Response
struct DiscoverDevicesResponse {
    devices: Vec<DeviceInfo>,
    total_count: u32,
    discovery_timestamp: u64,
    gateway_scan_duration_ms: u64,
}

struct DeviceInfo {
    device_id: String,
    name: String,
    device_type: DeviceType,
    capabilities: Vec<Capability>,
    device_groups: Vec<String>, // Can belong to multiple groups
    manufacturer: Option<String>,
    model: Option<String>,
    firmware_version: Option<String>,
    battery_powered: bool,
    reachable: bool,
    last_seen: u64,
    custom_attributes: HashMap<String, serde_json::Value>,
}

enum DeviceType {
    Light,
    Switch,
    Sensor,
    Outlet,
    Blinds,
    Speaker,
    Gateway,
    VirtualLightGroup,
    VirtualScene,
    VirtualTimer,
    VirtualConditional,
    Unknown(String), // For future device types
}

enum Capability {
    OnOff,
    Brightness,
    ColorTemperature,
    RgbColor,
    Temperature,
    Humidity,
    Motion,
    ContactSensor,
    BatteryLevel,
    Volume,
    Position, // For blinds
}
```

#### Get Device Details
```rust
// Request
struct GetDeviceInfoRequest {
    device_id: String,
    include_history: Option<bool>,
}

// Response
struct GetDeviceInfoResponse {
    device_info: DeviceInfo,
    current_state: DeviceState,
    state_history: Option<Vec<StateHistoryEntry>>,
    connectivity_status: ConnectivityStatus,
}

struct ConnectivityStatus {
    reachable: bool,
    signal_strength: Option<i8>, // dBm
    last_communication: u64,
    communication_errors: u32,
}
```

#### List Devices by Type/Group
```rust
// Request
struct ListDevicesRequest {
    device_type: Option<DeviceType>,
    device_groups: Option<Vec<String>>, // Filter by one or more groups
    reachable_only: Option<bool>,
    include_virtual: Option<bool>,
}

// Response
struct ListDevicesResponse {
    devices: Vec<DeviceInfo>,
    grouped_by_group: HashMap<String, Vec<DeviceInfo>>,
    grouped_by_type: HashMap<DeviceType, Vec<DeviceInfo>>,
}
```

### Device Group Management

#### Create Device Group
```rust
// Request
struct CreateDeviceGroupRequest {
    group_name: String,
    icon_ref: String, // "kitchen", "bedroom", "outdoor", "security", etc.
    device_ids: Vec<String>,
}

// Response
struct CreateDeviceGroupResponse {
    group_name: String,
    devices_added: Vec<String>,
    devices_not_found: Vec<String>,
}
```

#### Modify Device Group
```rust
// Request
struct ModifyDeviceGroupRequest {
    group_name: String,
    add_devices: Option<Vec<String>>,
    remove_devices: Option<Vec<String>>,
    new_icon_ref: Option<String>,
}

// Response
struct ModifyDeviceGroupResponse {
    group_name: String,
    current_devices: Vec<String>,
    added: Vec<String>,
    removed: Vec<String>,
}
```

#### List Device Groups
```rust
// Request
struct ListDeviceGroupsRequest {}

// Response
struct ListDeviceGroupsResponse {
    groups: Vec<DeviceGroupInfo>,
}

struct DeviceGroupInfo {
    group_name: String,
    icon_ref: String,
    device_count: u32,
    device_ids: Vec<String>,
}
```

### Device State Management

#### Get Light State
```rust
// Request
struct GetLightRequest {
    device_id: String,
}

// Response  
struct GetLightResponse {
    device_id: String,
    is_on: bool,
    brightness: Option<u8>,    // 0-100
    color_temp: Option<u16>,   // Kelvin
    rgb_color: Option<RgbColor>,
}
```

#### Set Light State
```rust
// Request
struct SetLightRequest {
    device_id: String,
    is_on: Option<bool>,
    brightness: Option<u8>,
    color_temp: Option<u16>,
    rgb_color: Option<RgbColor>,
    transition_time: Option<u16>, // milliseconds
}

// Response
struct SetLightResponse {
    device_id: String,
    success: bool,
    new_state: LightState,
}
```

#### Get Switch State
```rust
// Request
struct GetSwitchRequest {
    device_id: String,
}

// Response
struct GetSwitchResponse {
    device_id: String,
    is_pressed: bool,
    last_pressed: Option<u64>, // timestamp
    battery_level: Option<u8>, // 0-100
}
```

#### Get Sensor Data
```rust
// Request
struct GetSensorRequest {
    device_id: String,
}

// Response
struct GetSensorResponse {
    device_id: String,
    temperature: Option<f32>,  // Celsius
    humidity: Option<f32>,     // Percentage
    last_updated: u64,         // timestamp
}
```

### Subscription Management

#### Subscribe to Changes
```rust
// Request
struct SubscribeRequest {
    device_ids: Vec<String>,   // Empty = all devices
    event_types: Vec<EventType>,
}

enum EventType {
    StateChange,
    DeviceAdded,
    DeviceRemoved,
    DeviceReachabilityChanged,
    VirtualDeviceTriggered,
    DiscoveryStarted,
    DiscoveryCompleted,
}

// Events sent over WebSocket
struct DeviceEvent {
    device_id: String,
    event_type: EventType,
    old_state: Option<DeviceState>,
    new_state: Option<DeviceState>,
    device_info: Option<DeviceInfo>, // For added/removed events
    timestamp: u64,
}

// Discovery-specific events
struct DiscoveryEvent {
    event_type: EventType, // DiscoveryStarted or DiscoveryCompleted
    devices_found: Option<Vec<DeviceInfo>>,
    devices_lost: Option<Vec<String>>, // device_ids that disappeared
    scan_duration_ms: Option<u64>,
    timestamp: u64,
}
```

### Virtual Device Management

#### Create Virtual Device
```rust
struct CreateVirtualDeviceRequest {
    name: String,
    device_type: VirtualDeviceType,
    config: VirtualDeviceConfig,
}

enum VirtualDeviceType {
    LightGroup,
    Scene,
    Timer,
    Conditional,
}

// Response includes generated device_id
```

### Service Status

#### Health Check
```rust
// Request
struct HealthCheckRequest {}

// Response
struct HealthCheckResponse {
    status: ServiceStatus,
    connected_devices: u32,
    active_subscriptions: u32,
    gateway_status: GatewayStatus,
    uptime_seconds: u64,
}
```

## Error Handling

```rust
struct ErrorResponse {
    error_code: ErrorCode,
    message: String,
    details: Option<serde_json::Value>,
}

enum ErrorCode {
    DeviceNotFound,
    InvalidRequest,
    GatewayError,
    StateConflict,
    InternalError,
}
```

## WebSocket Upgrade

1. Client connects to TCP socket
2. Client sends `UpgradeRequest` with WebSocket headers
3. Server responds with WebSocket handshake
4. All subsequent communication over WebSocket frames
5. CBOR payloads within WebSocket binary frames

## Schema Generation

- All structs defined in `schemas/` directory
- Code generation creates Rust structs in `v1bectl_state` crate
- Validation and serialization automatically derived
- Schema versioning for backward compatibility
- Shared data model used across all crates (server, CLI, gateway, etc.)