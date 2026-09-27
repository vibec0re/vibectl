//! 🔥 Golden-JSON pins for the two most important wire types.
//!
//! `v1bectl_state` is a hand-maintained CBOR/JSON schema (CLAUDE.md: "Manual
//! CBOR schema (no code generation yet)"). `serde_round_trip.rs` proves
//! values survive a round trip, but a round trip alone would not notice a
//! `#[serde(rename = ...)]` or a retagging change that shifts the wire shape
//! consistently for both serialize and deserialize. These tests instead pin
//! the literal JSON produced for:
//!
//! - `DeviceStateValue`: externally tagged (default serde tagging), because
//!   this is the state payload every device write/read carries.
//! - `EventType`: internally tagged via `#[serde(tag = "type", rename_all =
//!   "snake_case")]`, because this is the event payload streamed over the
//!   WebSocket API to every client.
//!
//! Any change to field names or enum tagging on these types should turn one
//! of these tests red.

use serde_json::json;
use v1bectl_state::{DeviceStateValue, EventType, LightState, OutletState, RgbColor};

#[test]
fn device_state_value_light_pins_external_tagging_and_field_names() {
    let value = DeviceStateValue::Light(LightState {
        is_on: true,
        brightness: Some(75),
        color_temp: Some(2700),
        rgb_color: Some(RgbColor {
            r: 255,
            g: 200,
            b: 150,
        }),
    });

    let expected = json!({
        "Light": {
            "is_on": true,
            "brightness": 75,
            "color_temp": 2700,
            "rgb_color": { "r": 255, "g": 200, "b": 150 }
        }
    });

    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        expected,
        "DeviceStateValue::Light wire shape changed"
    );

    let from_literal: DeviceStateValue = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}

#[test]
fn device_state_value_outlet_pins_external_tagging() {
    let value = DeviceStateValue::Outlet(OutletState {
        is_on: false,
        power_consumption: Some(3.5),
        total_energy: None,
    });

    let expected = json!({
        "Outlet": {
            "is_on": false,
            "power_consumption": 3.5,
            "total_energy": null
        }
    });

    assert_eq!(serde_json::to_value(&value).unwrap(), expected);
    let from_literal: DeviceStateValue = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}

#[test]
fn device_state_value_empty_pins_bare_unit_variant_tagging() {
    // A unit variant with no payload serializes as the bare variant name,
    // NOT as `{"Empty": null}` — worth pinning since it's easy to get wrong
    // by hand-writing the CBOR/JSON schema on the other side.
    let value = DeviceStateValue::Empty;
    let expected = json!("Empty");

    assert_eq!(serde_json::to_value(&value).unwrap(), expected);
    let from_literal: DeviceStateValue = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}

#[test]
fn event_type_attribute_changed_pins_internal_tag_and_snake_case() {
    let value = EventType::AttributeChanged {
        attribute: "brightness".to_string(),
        old_value: json!(10),
        new_value: json!(75),
    };

    let expected = json!({
        "type": "attribute_changed",
        "attribute": "brightness",
        "old_value": 10,
        "new_value": 75
    });

    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        expected,
        "EventType::AttributeChanged wire shape changed"
    );

    let from_literal: EventType = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}

#[test]
fn event_type_device_removed_pins_unit_variant_internal_tag() {
    let value = EventType::DeviceRemoved;
    let expected = json!({ "type": "device_removed" });

    assert_eq!(serde_json::to_value(&value).unwrap(), expected);
    let from_literal: EventType = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}

#[test]
fn event_type_battery_level_changed_pins_field_names() {
    let value = EventType::BatteryLevelChanged {
        old_level: 90,
        new_level: 85,
    };

    let expected = json!({
        "type": "battery_level_changed",
        "old_level": 90,
        "new_level": 85
    });

    assert_eq!(serde_json::to_value(&value).unwrap(), expected);
    let from_literal: EventType = serde_json::from_value(expected).unwrap();
    assert_eq!(from_literal, value);
}
