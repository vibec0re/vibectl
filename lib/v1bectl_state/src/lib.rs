// 🔥 V1BECTL STATE - SHARED TYPES WITHOUT TOKIO! 🔥

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type DeviceId = String;
pub type Timestamp = u64;

// Core message envelope for all API communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub correlation_id: String,
    pub message_type: MessageType,
    pub payload: Vec<u8>, // CBOR-encoded payload
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    Request,
    Response,
    Event,
    Error,
}

// Device info and state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeviceInfo {
    pub device_id: DeviceId,
    pub name: String,
    pub device_type: DeviceType,
    pub capabilities: Vec<Capability>,
    pub device_groups: Vec<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub firmware_version: Option<String>,
    pub battery_powered: bool,
    pub reachable: bool,
    pub last_seen: Timestamp,
    pub custom_attributes: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeviceState {
    pub device_id: DeviceId,
    pub device_info: DeviceInfo,
    pub state: DeviceStateValue,
    pub last_updated: Timestamp,
    pub last_synced_to_gateway: Option<Timestamp>,
    pub last_synced_from_gateway: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceType {
    Light,
    Switch,
    Sensor,
    Outlet,
    Blinds,
    Speaker,
    Gateway,
    MotionSensor,
    VirtualLightGroup,
    VirtualScene,
    VirtualTimer,
    VirtualConditional,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Capability {
    OnOff,
    Brightness,
    MotionDetection,
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

// Device state representations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeviceStateValue {
    Light(LightState),
    Switch(SwitchState),
    Sensor(SensorState),
    Scene(SceneState),
    Timer(TimerState),
    MotionSensor(MotionSensorState),
    Outlet(OutletState),
    Empty, // 🔥 For devices with no state like ButtonController! 💖
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LightState {
    pub is_on: bool,
    pub brightness: Option<u8>,  // 0-100
    pub color_temp: Option<u16>, // Kelvin
    pub rgb_color: Option<RgbColor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RgbColor {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwitchState {
    pub is_pressed: bool,
    pub last_pressed: Option<Timestamp>,
    pub battery_level: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SensorState {
    pub temperature: Option<f32>,
    pub humidity: Option<f32>,
    pub last_updated: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MotionSensorState {
    pub motion_detected: bool,
    pub last_motion: Option<Timestamp>,
    pub battery_level: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutletState {
    pub is_on: bool,
    pub power_consumption: Option<f32>, // In watts
    pub total_energy: Option<f32>,      // In kWh
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SceneState {
    pub scene_name: String,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimerState {
    pub timer_name: String,
    pub action: TimerAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimerAction {
    Start,
    Stop,
    Reset,
}

// Device groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDeviceGroupRequest {
    pub group_name: String,
    pub icon_ref: String,
    pub device_ids: Vec<DeviceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceGroupInfo {
    pub group_name: String,
    pub icon_ref: String,
    pub device_count: u32,
    pub device_ids: Vec<DeviceId>,
}

// Events
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceEvent {
    pub timestamp: std::time::SystemTime,
    pub device_id: String,
    pub event_type: EventType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventType {
    AttributeChanged {
        attribute: String,
        old_value: serde_json::Value,
        new_value: serde_json::Value,
    },
    StateChanged {
        old_state: Option<DeviceStateValue>,
        new_state: Option<DeviceStateValue>,
    },
    ButtonPressed {
        button_id: String,
        press_type: ButtonPressType,
    },
    DeviceAdded {
        device_type: String,
    },
    DeviceRemoved,
    DeviceReachabilityChanged {
        reachable: bool,
    },
    SceneActivated {
        scene_id: String,
    },
    BatteryLevelChanged {
        old_level: u8,
        new_level: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ButtonPressType {
    SinglePress,
    DoublePress,
    LongPress,
}

// Discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverDevicesRequest {
    pub force_refresh: Option<bool>,
    pub device_types: Option<Vec<DeviceType>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverDevicesResponse {
    pub devices: Vec<DeviceInfo>,
    pub total_count: u32,
    pub discovery_timestamp: Timestamp,
    pub gateway_scan_duration_ms: u64,
}
