//! 🎚️ #66: a light group reads back the level its members were set from,
//! not the average of their own levels.
//!
//! A `LightGroup` maps the group's level to each member through its own
//! brightness curve. Here: `uplight` gets what `v1bectl_server` makes of a
//! TOML curve with `min = 10` and `max = 60` (#62), `ceiling` holds 40 from
//! 30 to 60, and `desk` follows the group 1:1. At 50 they go to 35, 40 and
//! 50. When a member changes from outside, the manager re-derives the group
//! from its members. That used to average the members' own levels, so the
//! group drifted off the level it was set to: with `desk` turned off, 35
//! and 40 read as 37. Now each member counts at the group level its curve
//! inverts its level to, and the group stays at 50, as a linear group does
//! (#10, `linear_group_levels.rs`).
//!
//! Input tracking is driven by hand here ([`pump`]), so each step is
//! handled before the next, and nothing waits on a clock.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::broadcast::{self, error::TryRecvError};
use v1bectl_sync::{
    Capability, DeviceEvent, DeviceInfo, DeviceStateValue, DeviceType, EventBus, EventType,
    LightState, StateStore,
};
use v1bectl_virtual::{LightGroup, VirtualDeviceConfig, VirtualDeviceManager, VirtualDeviceType};

const GROUP: &str = "virtual_curved_lights";
/// The member moved at the wall here: 10 at group 0, 60 at 100.
const UPLIGHT: &str = "uplight";
/// The member turned off at the wall here: 1:1.
const DESK: &str = "desk";
/// Each member, its curve's breakpoints, and its level with the group at 50.
const MEMBERS_AT_50: [(&str, &[[u8; 2]], u8); 3] = [
    (UPLIGHT, &[[0, 10], [100, 60]], 35),
    ("ceiling", &[[0, 0], [30, 40], [60, 40], [100, 100]], 40),
    (DESK, &[[0, 0], [100, 100]], 50),
];

fn light(is_on: bool, brightness: u8) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness: Some(brightness),
        color_temp: Some(2700),
        rgb_color: None,
    })
}

fn light_info(device_id: &str) -> DeviceInfo {
    DeviceInfo {
        device_id: device_id.to_string(),
        name: device_id.to_string(),
        device_type: DeviceType::Light,
        capabilities: vec![Capability::OnOff, Capability::Brightness],
        device_groups: vec![],
        manufacturer: None,
        model: None,
        firmware_version: None,
        battery_powered: false,
        reachable: true,
        last_seen: 0,
        custom_attributes: HashMap::new(),
    }
}

/// [`GROUP`], built the way `v1bectl_server` and the API's
/// `CreateVirtualDevice` build a `LightGroup`.
fn curved_lights(store: &Arc<StateStore>) -> LightGroup {
    let lights: Vec<&str> = MEMBERS_AT_50.iter().map(|(id, ..)| *id).collect();
    let curves: serde_json::Map<String, serde_json::Value> = MEMBERS_AT_50
        .iter()
        .map(|(id, breakpoints, _)| {
            let curve = serde_json::json!({ "breakpoints": breakpoints });
            ((*id).to_string(), curve)
        })
        .collect();
    let config = VirtualDeviceConfig {
        device_id: GROUP.to_string(),
        device_type: VirtualDeviceType::LightGroup,
        name: "Curved Lights".to_string(),
        description: None,
        enabled: true,
        config: serde_json::json!({ "lights": lights, "brightness_curves": curves }),
    };
    LightGroup::new(config, Arc::clone(store)).expect("Curved Lights")
}

/// Input tracking, caught up: hands the manager every event published so
/// far, in order, including the ones that handling publishes in turn.
/// Returns them all.
async fn pump(
    manager: &VirtualDeviceManager,
    rx: &mut broadcast::Receiver<DeviceEvent>,
) -> Vec<DeviceEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(event) => {
                manager.handle_event(&event).await.expect("input tracking");
                events.push(event);
            }
            Err(TryRecvError::Empty) => return events,
            Err(e) => panic!("event bus: {e}"),
        }
    }
}

/// The states echoed for `device_id` in `events`.
fn echoes(events: &[DeviceEvent], device_id: &str) -> Vec<DeviceStateValue> {
    events
        .iter()
        .filter(|e| e.device_id == device_id)
        .filter_map(|e| match &e.event_type {
            EventType::AttributeChanged {
                attribute,
                new_value,
                ..
            } if attribute == "state" => serde_json::from_value(new_value.clone()).ok(),
            _ => None,
        })
        .collect()
}

async fn stored(store: &StateStore, device_id: &str) -> DeviceStateValue {
    store
        .get_device(&device_id.to_string())
        .await
        .expect(device_id)
        .state
}

/// `device_id` moved to `state` from outside (a wall switch, the hub app),
/// as the sync engine reports it: stored, and echoed.
async fn moved(store: &StateStore, bus: &EventBus, device_id: &str, state: DeviceStateValue) {
    let id = device_id.to_string();
    let old = stored(store, device_id).await;
    store
        .update_device_state(&id, state.clone())
        .await
        .expect(device_id);
    bus.publish(DeviceEvent {
        timestamp: std::time::SystemTime::now(),
        device_id: id,
        event_type: EventType::AttributeChanged {
            attribute: "state".to_string(),
            old_value: serde_json::to_value(&old).expect("old state"),
            new_value: serde_json::to_value(&state).expect("new state"),
        },
    })
    .await;
}

/// [`GROUP`] over its members, all off, with a manager that has input
/// tracking left to [`pump`], set to 50.
async fn home_at_50() -> (
    VirtualDeviceManager,
    Arc<StateStore>,
    Arc<EventBus>,
    broadcast::Receiver<DeviceEvent>,
) {
    let store = StateStore::new();
    for (member, ..) in MEMBERS_AT_50 {
        store.add_device(light_info(member), light(false, 0)).await;
    }
    let bus = Arc::new(EventBus::new(100));
    let manager = VirtualDeviceManager::new(Arc::clone(&store), Arc::clone(&bus));
    manager
        .add_virtual_device(Box::new(curved_lights(&store)))
        .await
        .expect("register Curved Lights");
    let mut rx = bus.subscribe();

    manager
        .set_virtual_device_state(&GROUP.to_string(), light(true, 50))
        .await
        .expect("Curved Lights to 50");
    pump(&manager, &mut rx).await;
    assert_eq!(group(&manager, &store).await, light(true, 50), "set to 50");
    for (member, _, level) in MEMBERS_AT_50 {
        assert_eq!(stored(&store, member).await, light(true, level), "{member}");
    }
    (manager, store, bus, rx)
}

/// The group, as the manager and the store hold it: they must agree.
async fn group(manager: &VirtualDeviceManager, store: &StateStore) -> DeviceStateValue {
    let own = manager
        .get_virtual_device_state(&GROUP.to_string())
        .await
        .expect("Curved Lights");
    assert_eq!(own, stored(store, GROUP).await, "the group's own state");
    own
}

/// #66: the group set to 50 stays at 50 when an outside change of one
/// member re-derives it. With `desk` turned off, `uplight` (35) and
/// `ceiling` (40) are still where 50 put them, so each inverts to 50.
/// Averaging their own levels read 37 instead. The re-derive lands where
/// the group was, so it isn't echoed. With `desk` back on at 50, the group
/// accounts for it again.
#[tokio::test]
async fn a_group_set_to_50_stays_at_50_when_a_member_change_re_derives_it() {
    let (manager, store, bus, mut rx) = home_at_50().await;

    moved(&store, &bus, DESK, light(false, 0)).await;
    let events = pump(&manager, &mut rx).await;
    assert_eq!(
        group(&manager, &store).await,
        light(true, 50),
        "re-derived with desk off"
    );
    assert!(echoes(&events, GROUP).is_empty(), "the group was echoed");

    moved(&store, &bus, DESK, light(true, 50)).await;
    pump(&manager, &mut rx).await;
    assert_eq!(
        group(&manager, &store).await,
        light(true, 50),
        "desk back on"
    );
}

/// #66: `uplight` dimmed at the wall from 35 to 20 re-derives the group to
/// 40. `ceiling` and `desk` still invert to 50, and 20 is where 19 and 20
/// put `uplight` (10-60), which inverts to the one nearer 50:
/// (20 + 50 + 50) / 3 is 40. Averaging the members' own levels read
/// (20 + 40 + 50) / 3 = 36. Turned back to 35, `uplight` inverts to 50
/// again, and so does the group: it still inverts towards the level it was
/// set to.
#[tokio::test]
async fn a_member_dimmed_at_the_wall_re_derives_the_group_to_its_inverted_level() {
    let (manager, store, bus, mut rx) = home_at_50().await;

    moved(&store, &bus, UPLIGHT, light(true, 20)).await;
    let events = pump(&manager, &mut rx).await;
    let at_40 = light(true, 40);
    assert_eq!(group(&manager, &store).await, at_40, "uplight dimmed to 20");
    assert_eq!(echoes(&events, GROUP), vec![at_40], "echoed once");

    moved(&store, &bus, UPLIGHT, light(true, 35)).await;
    pump(&manager, &mut rx).await;
    assert_eq!(
        group(&manager, &store).await,
        light(true, 50),
        "uplight at 35"
    );
}
