//! 🔥 Serde round-trip pins for the v1bectl_state wire schema.
//!
//! `v1bectl_state` is the hand-maintained CBOR/JSON schema shared across the
//! whole workspace (see CLAUDE.md: "Manual CBOR schema (no code generation
//! yet)"). These tests exist so that an accidental field rename, dropped
//! variant, or retagging shows up as a red test here instead of as a silent
//! wire incompatibility discovered in production.
//!
//! Two round-trip helpers are used depending on whether the type derives
//! `PartialEq`:
//! - `round_trip_eq`: serialize -> deserialize -> compare the *values* with
//!   `assert_eq!`. Used for types with `PartialEq`.
//! - `round_trip_json_eq`: serialize -> deserialize -> re-serialize ->
//!   compare the two JSON *shapes*. Used for the handful of request/response
//!   types that don't derive `PartialEq`.

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use v1bectl_state::*;

fn round_trip_eq<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(&value).expect("serialize");
    let deserialized: T =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("deserialize {json:?}: {e}"));
    assert_eq!(value, deserialized, "round trip mismatch; json = {json}");
}

fn round_trip_json_eq<T>(value: T)
where
    T: Serialize + DeserializeOwned + std::fmt::Debug,
{
    let json = serde_json::to_value(&value).expect("serialize");
    let deserialized: T = serde_json::from_value(json.clone())
        .unwrap_or_else(|e| panic!("deserialize {json:?}: {e}"));
    let json_again = serde_json::to_value(&deserialized).expect("re-serialize");
    assert_eq!(
        json, json_again,
        "round trip changed JSON shape for {value:?}"
    );
}

// ---------------------------------------------------------------------
// MessageType / Message
// ---------------------------------------------------------------------

#[test]
fn message_type_round_trips_every_variant() {
    for variant in [
        MessageType::Request,
        MessageType::Response,
        MessageType::Event,
        MessageType::Error,
    ] {
        round_trip_json_eq(variant);
    }
}

#[test]
fn message_round_trips_with_binary_payload() {
    let message = Message {
        correlation_id: "corr-1".to_string(),
        message_type: MessageType::Request,
        payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
        timestamp: 1_700_000_000_123,
    };
    round_trip_json_eq(message);
}

// ---------------------------------------------------------------------
// DeviceType
// ---------------------------------------------------------------------

fn all_device_types() -> Vec<DeviceType> {
    vec![
        DeviceType::Light,
        DeviceType::Switch,
        DeviceType::Sensor,
        DeviceType::Outlet,
        DeviceType::Blinds,
        DeviceType::Speaker,
        DeviceType::Gateway,
        DeviceType::MotionSensor,
        DeviceType::VirtualLightGroup,
        DeviceType::VirtualScene,
        DeviceType::VirtualTimer,
        DeviceType::VirtualConditional,
        DeviceType::Unknown("mystery-device".to_string()),
    ]
}

#[test]
fn device_type_round_trips_every_variant() {
    for variant in all_device_types() {
        round_trip_eq(variant);
    }
}

#[test]
fn device_type_unknown_preserves_arbitrary_string() {
    let variant = DeviceType::Unknown("some.vendor.custom_type".to_string());
    let json = serde_json::to_string(&variant).unwrap();
    let back: DeviceType = serde_json::from_str(&json).unwrap();
    match back {
        DeviceType::Unknown(s) => assert_eq!(s, "some.vendor.custom_type"),
        other => panic!("expected Unknown, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Capability
// ---------------------------------------------------------------------

#[test]
fn capability_round_trips_every_variant() {
    for variant in [
        Capability::OnOff,
        Capability::Brightness,
        Capability::MotionDetection,
        Capability::ColorTemperature,
        Capability::RgbColor,
        Capability::Temperature,
        Capability::Humidity,
        Capability::Motion,
        Capability::ContactSensor,
        Capability::BatteryLevel,
        Capability::Volume,
        Capability::Position,
    ] {
        round_trip_eq(variant);
    }
}

// ---------------------------------------------------------------------
// DeviceStateValue and its nested state structs
// ---------------------------------------------------------------------

fn sample_light_state() -> LightState {
    LightState {
        is_on: true,
        brightness: Some(75),
        color_temp: Some(2700),
        rgb_color: Some(RgbColor {
            r: 255,
            g: 128,
            b: 64,
        }),
    }
}

#[test]
fn light_state_round_trips_with_all_fields_present() {
    round_trip_eq(sample_light_state());
}

#[test]
fn light_state_round_trips_with_all_optional_fields_absent() {
    round_trip_eq(LightState {
        is_on: false,
        brightness: None,
        color_temp: None,
        rgb_color: None,
    });
}

#[test]
fn rgb_color_round_trips_boundary_values() {
    round_trip_eq(RgbColor { r: 0, g: 0, b: 0 });
    round_trip_eq(RgbColor {
        r: 255,
        g: 255,
        b: 255,
    });
}

#[test]
fn switch_state_round_trips() {
    round_trip_eq(SwitchState {
        is_pressed: true,
        last_pressed: Some(1_700_000_000_000),
        battery_level: Some(87),
    });
    round_trip_eq(SwitchState {
        is_pressed: false,
        last_pressed: None,
        battery_level: None,
    });
}

#[test]
fn sensor_state_round_trips_with_and_without_readings() {
    round_trip_eq(SensorState {
        temperature: Some(21.5),
        humidity: Some(45.0),
        last_updated: 1_700_000_000_000,
    });
    round_trip_eq(SensorState {
        temperature: None,
        humidity: None,
        last_updated: 0,
    });
}

#[test]
fn motion_sensor_state_round_trips() {
    round_trip_eq(MotionSensorState {
        motion_detected: true,
        last_motion: Some(1_700_000_000_000),
        battery_level: Some(50),
    });
}

#[test]
fn outlet_state_round_trips() {
    round_trip_eq(OutletState {
        is_on: true,
        power_consumption: Some(12.34),
        total_energy: Some(567.8),
    });
    round_trip_eq(OutletState {
        is_on: false,
        power_consumption: None,
        total_energy: None,
    });
}

#[test]
fn scene_state_round_trips() {
    round_trip_eq(SceneState {
        scene_name: "movie_night".to_string(),
        is_active: true,
    });
}

#[test]
fn timer_action_round_trips_every_variant() {
    for variant in [TimerAction::Start, TimerAction::Stop, TimerAction::Reset] {
        round_trip_eq(variant);
    }
}

#[test]
fn timer_state_round_trips() {
    round_trip_eq(TimerState {
        timer_name: "bedtime".to_string(),
        action: TimerAction::Start,
    });
}

#[test]
fn device_state_value_round_trips_every_variant() {
    round_trip_eq(DeviceStateValue::Light(sample_light_state()));
    round_trip_eq(DeviceStateValue::Switch(SwitchState {
        is_pressed: true,
        last_pressed: Some(1),
        battery_level: Some(2),
    }));
    round_trip_eq(DeviceStateValue::Sensor(SensorState {
        temperature: Some(20.0),
        humidity: Some(40.0),
        last_updated: 3,
    }));
    round_trip_eq(DeviceStateValue::Scene(SceneState {
        scene_name: "evening".to_string(),
        is_active: false,
    }));
    round_trip_eq(DeviceStateValue::Timer(TimerState {
        timer_name: "wake_up".to_string(),
        action: TimerAction::Reset,
    }));
    round_trip_eq(DeviceStateValue::MotionSensor(MotionSensorState {
        motion_detected: false,
        last_motion: None,
        battery_level: None,
    }));
    round_trip_eq(DeviceStateValue::Outlet(OutletState {
        is_on: true,
        power_consumption: None,
        total_energy: None,
    }));
    round_trip_eq(DeviceStateValue::Empty);
}

// ---------------------------------------------------------------------
// DeviceInfo / DeviceState
// ---------------------------------------------------------------------

fn sample_device_info() -> DeviceInfo {
    let mut custom_attributes = HashMap::new();
    custom_attributes.insert("room".to_string(), serde_json::json!("kitchen"));
    custom_attributes.insert("zone".to_string(), serde_json::json!(3));

    DeviceInfo {
        device_id: "light-1_1".to_string(),
        name: "Kitchen Light".to_string(),
        device_type: DeviceType::Light,
        capabilities: vec![Capability::OnOff, Capability::Brightness],
        device_groups: vec!["kitchen".to_string(), "downstairs".to_string()],
        manufacturer: Some("IKEA".to_string()),
        model: Some("TRADFRI bulb".to_string()),
        firmware_version: Some("1.2.3".to_string()),
        battery_powered: false,
        reachable: true,
        last_seen: 1_700_000_000_000,
        custom_attributes,
    }
}

#[test]
fn device_info_round_trips_with_custom_attributes() {
    round_trip_eq(sample_device_info());
}

#[test]
fn device_info_round_trips_with_all_optionals_absent() {
    round_trip_eq(DeviceInfo {
        device_id: "sensor-1".to_string(),
        name: "Motion Sensor".to_string(),
        device_type: DeviceType::MotionSensor,
        capabilities: vec![],
        device_groups: vec![],
        manufacturer: None,
        model: None,
        firmware_version: None,
        battery_powered: true,
        reachable: false,
        last_seen: 0,
        custom_attributes: HashMap::new(),
    });
}

#[test]
fn device_state_round_trips() {
    round_trip_eq(v1bectl_state::DeviceState {
        device_id: "light-1_1".to_string(),
        device_info: sample_device_info(),
        state: DeviceStateValue::Light(sample_light_state()),
        last_updated: 1_700_000_000_500,
        last_synced_to_gateway: Some(1_700_000_000_400),
        last_synced_from_gateway: None,
    });
}

// ---------------------------------------------------------------------
// Device groups / discovery (no PartialEq -> JSON-shape round trip)
// ---------------------------------------------------------------------

#[test]
fn create_device_group_request_round_trips() {
    round_trip_json_eq(CreateDeviceGroupRequest {
        group_name: "kitchen".to_string(),
        icon_ref: "icon://kitchen".to_string(),
        device_ids: vec!["light-1_1".to_string(), "outlet-2_1".to_string()],
    });
}

#[test]
fn device_group_info_round_trips() {
    round_trip_json_eq(DeviceGroupInfo {
        group_name: "kitchen".to_string(),
        icon_ref: "icon://kitchen".to_string(),
        device_count: 2,
        device_ids: vec!["light-1_1".to_string(), "outlet-2_1".to_string()],
    });
}

#[test]
fn discover_devices_request_round_trips_with_and_without_filters() {
    round_trip_json_eq(DiscoverDevicesRequest {
        force_refresh: Some(true),
        device_types: Some(vec![DeviceType::Light, DeviceType::Outlet]),
    });
    round_trip_json_eq(DiscoverDevicesRequest {
        force_refresh: None,
        device_types: None,
    });
}

#[test]
fn discover_devices_response_round_trips() {
    round_trip_json_eq(DiscoverDevicesResponse {
        devices: vec![sample_device_info()],
        total_count: 1,
        discovery_timestamp: 1_700_000_000_000,
        gateway_scan_duration_ms: 250,
    });
}

// ---------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------

fn fixed_timestamp() -> SystemTime {
    // A deterministic instant so the test is hermetic (no `SystemTime::now()`).
    SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_123)
}

#[test]
fn button_press_type_round_trips_every_variant() {
    for variant in [
        ButtonPressType::SinglePress,
        ButtonPressType::DoublePress,
        ButtonPressType::LongPress,
    ] {
        round_trip_eq(variant);
    }
}

#[test]
fn device_event_round_trips_every_event_type_variant() {
    let event_types = vec![
        EventType::AttributeChanged {
            attribute: "brightness".to_string(),
            old_value: serde_json::json!(10),
            new_value: serde_json::json!(75),
        },
        EventType::StateChanged {
            old_state: Some(DeviceStateValue::Light(sample_light_state())),
            new_state: None,
        },
        EventType::ButtonPressed {
            button_id: "button-1".to_string(),
            press_type: ButtonPressType::DoublePress,
        },
        EventType::DeviceAdded {
            device_type: "light".to_string(),
        },
        EventType::DeviceRemoved,
        EventType::DeviceReachabilityChanged { reachable: false },
        EventType::SceneActivated {
            scene_id: "movie_night".to_string(),
        },
        EventType::BatteryLevelChanged {
            old_level: 90,
            new_level: 85,
        },
    ];

    for event_type in event_types {
        let event = DeviceEvent {
            timestamp: fixed_timestamp(),
            device_id: "device-1".to_string(),
            event_type,
        };
        round_trip_eq(event);
    }
}
