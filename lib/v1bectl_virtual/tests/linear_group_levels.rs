//! 🎚️ #10: a linear group reads back the level its members were set from,
//! not the average of their own levels.
//!
//! The shipped Bedroom Lights (`virtual_devices/bedroom_lights.toml`) maps
//! the group's level to each member through its own range: at 50, `top`
//! (80-100) goes to 90, `main` (40-90) to 65 and `bed` (0-50) to 25. When a
//! member changes from outside, the manager re-derives the group from its
//! members. That used to average the members' own levels, so the group
//! drifted off the level it was set to: with `bed` turned off, 90 and 65
//! read as 77. Now each member counts at the group level its range inverts
//! its level to, and the group stays at 50.
//!
//! Input tracking is driven by hand here ([`pump`]), so each step is
//! handled before the next, and nothing waits on a clock.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::broadcast::{self, error::TryRecvError};
use v1bectl_sync::{
    Capability, DeviceEvent, DeviceInfo, DeviceStateValue, DeviceType, EventBus, EventType,
    LightState, StateStore,
};
use v1bectl_virtual::{
    load_virtual_devices_from_dir, LightGroupLinear, VirtualDeviceConfig, VirtualDeviceManager,
    VirtualDeviceTomlConfig, VirtualDeviceType,
};

/// The shipped Bedroom Lights.
const GROUP: &str = "virtual_bedroom_lights";
/// Its `bed` member (0-50), the one moved at the wall here.
const BED: &str = "light_kitchen";
/// Each member, and its level with Bedroom Lights at 50.
const MEMBERS_AT_50: [(&str, u8); 3] = [
    ("light_bedroom", 90),
    ("light_living_room", 65),
    ("light_kitchen", 25),
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

/// The shipped Bedroom Lights, read from its file and built the way
/// `v1bectl_server` builds a linear group.
async fn bedroom_lights(store: &Arc<StateStore>) -> LightGroupLinear {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../virtual_devices");
    let configs = load_virtual_devices_from_dir(&dir)
        .await
        .expect("load virtual_devices/");
    let Some(c) = configs.into_iter().find_map(|config| match config {
        VirtualDeviceTomlConfig::LightGroupLinear(c) if c.device_id == GROUP => Some(c),
        _ => None,
    }) else {
        panic!("virtual_devices/ no longer ships {GROUP}");
    };
    let ranges: HashMap<String, (u8, u8)> = c
        .brightness
        .iter()
        .map(|(name, [min, max])| (name.clone(), (*min, *max)))
        .collect();
    let config = VirtualDeviceConfig {
        device_id: c.device_id.clone(),
        device_type: VirtualDeviceType::LightGroupLinear,
        name: c.name.clone(),
        description: None,
        enabled: true,
        config: serde_json::json!({}),
    };
    LightGroupLinear::new(config, c.members, ranges, Arc::clone(store)).expect("Bedroom Lights")
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

/// Bedroom Lights over its members, all off, with a manager that has input
/// tracking left to [`pump`].
async fn home() -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
    let store = StateStore::new();
    for (member, _) in MEMBERS_AT_50 {
        store.add_device(light_info(member), light(false, 0)).await;
    }
    let bus = Arc::new(EventBus::new(100));
    let manager = VirtualDeviceManager::new(Arc::clone(&store), Arc::clone(&bus));
    let group = bedroom_lights(&store).await;
    manager
        .add_virtual_device(Box::new(group))
        .await
        .expect("register Bedroom Lights");
    (manager, store, bus)
}

/// The group, as the manager and the store hold it: they must agree.
async fn group(manager: &VirtualDeviceManager, store: &StateStore) -> DeviceStateValue {
    let own = manager
        .get_virtual_device_state(&GROUP.to_string())
        .await
        .expect("Bedroom Lights");
    assert_eq!(own, stored(store, GROUP).await, "the group's own state");
    own
}

/// #10: Bedroom Lights set to 50 stays at 50 when an outside change of one
/// member re-derives it. With `bed` turned off, `top` (90) and `main` (65)
/// are still where 50 put them, so each inverts to 50. Averaging their own
/// levels read 77 instead. The re-derive lands where the group was, so it
/// isn't echoed. With `bed` back on at 25, the group accounts for it again.
#[tokio::test]
async fn a_group_set_to_50_stays_at_50_when_a_member_change_re_derives_it() {
    let (manager, store, bus) = home().await;
    let mut rx = bus.subscribe();

    manager
        .set_virtual_device_state(&GROUP.to_string(), light(true, 50))
        .await
        .expect("Bedroom Lights to 50");
    pump(&manager, &mut rx).await;
    assert_eq!(group(&manager, &store).await, light(true, 50), "set to 50");
    for (member, level) in MEMBERS_AT_50 {
        assert_eq!(stored(&store, member).await, light(true, level), "{member}");
    }

    moved(&store, &bus, BED, light(false, 0)).await;
    let events = pump(&manager, &mut rx).await;
    assert_eq!(
        group(&manager, &store).await,
        light(true, 50),
        "re-derived with bed off"
    );
    assert!(echoes(&events, GROUP).is_empty(), "the group was echoed");

    moved(&store, &bus, BED, light(true, 25)).await;
    pump(&manager, &mut rx).await;
    assert_eq!(
        group(&manager, &store).await,
        light(true, 50),
        "bed back on"
    );
}

/// #10: `bed` dimmed at the wall from 25 to 10 re-derives Bedroom Lights
/// to 40. `top` and `main` still invert to 50, and 10 is where 19 and 20
/// put `bed` (0-50), which inverts to the one nearer 50: (50 + 50 + 20) / 3
/// is 40. Averaging the members' own levels read 55. Turned back to 25,
/// `bed` inverts to 50 again, and so does the group: it still inverts
/// towards the level it was set to.
#[tokio::test]
async fn a_member_dimmed_at_the_wall_re_derives_the_group_to_its_inverted_level() {
    let (manager, store, bus) = home().await;
    let mut rx = bus.subscribe();
    manager
        .set_virtual_device_state(&GROUP.to_string(), light(true, 50))
        .await
        .expect("Bedroom Lights to 50");
    pump(&manager, &mut rx).await;

    moved(&store, &bus, BED, light(true, 10)).await;
    let events = pump(&manager, &mut rx).await;
    let at_40 = light(true, 40);
    assert_eq!(group(&manager, &store).await, at_40, "bed dimmed to 10");
    assert_eq!(echoes(&events, GROUP), vec![at_40], "echoed once");

    moved(&store, &bus, BED, light(true, 25)).await;
    pump(&manager, &mut rx).await;
    assert_eq!(group(&manager, &store).await, light(true, 50), "bed at 25");
}
