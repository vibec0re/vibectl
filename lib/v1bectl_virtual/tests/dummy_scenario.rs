//! #2: the shipped `virtual_devices/*.toml` must work against the dummy
//! `basic_home` scenario that `v1bectl_server dummy` loads them next to.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use v1bectl_gateway::Gateway;
use v1bectl_sync::*;
use v1bectl_virtual::*;

fn virtual_devices_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../virtual_devices")
}

fn config_id(config: &VirtualDeviceTomlConfig) -> &str {
    match config {
        VirtualDeviceTomlConfig::LightGroup(c) => &c.device_id,
        VirtualDeviceTomlConfig::LightGroupLinear(c) => &c.device_id,
        VirtualDeviceTomlConfig::ButtonController(c) => &c.device_id,
        VirtualDeviceTomlConfig::SceneController(c) => &c.device_id,
    }
}

/// Every shipped config, failing (rather than skipping, as the loader does)
/// on one that doesn't parse.
async fn shipped_configs() -> Vec<VirtualDeviceTomlConfig> {
    let dir = virtual_devices_dir();
    let toml_files = std::fs::read_dir(&dir)
        .expect("virtual_devices/ exists")
        .filter(|entry| {
            entry.as_ref().expect("dir entry").path().extension() == Some("toml".as_ref())
        })
        .count();
    let configs = load_virtual_devices_from_dir(&dir).await.expect("load");
    assert_eq!(
        configs.len(),
        toml_files,
        "a virtual_devices/*.toml failed to parse"
    );
    configs
}

/// A store seeded from the dummy hub, as the server does at startup.
async fn dummy_home_store() -> (Arc<StateStore>, HashSet<DeviceId>) {
    let gateway = DummyGateway::new("basic_home");
    let store = StateStore::new();
    let mut ids = HashSet::new();
    for info in gateway.discover_devices().await.expect("discover") {
        let state = gateway
            .get_device_state(&info.device_id)
            .await
            .expect("initial state");
        ids.insert(info.device_id.clone());
        store.add_device(info, state).await;
    }
    (store, ids)
}

fn vd_config(device_id: &str, name: &str, device_type: VirtualDeviceType) -> VirtualDeviceConfig {
    VirtualDeviceConfig {
        device_id: device_id.to_string(),
        device_type,
        name: name.to_string(),
        description: None,
        enabled: true,
        config: serde_json::json!({}),
    }
}

/// A shipped linear group, built as the server builds it.
fn linear_group(c: LightGroupLinearConfig, store: &Arc<StateStore>) -> LightGroupLinear {
    let ranges: HashMap<String, (u8, u8)> = c
        .brightness
        .iter()
        .map(|(name, [min, max])| (name.clone(), (*min, *max)))
        .collect();
    let config = vd_config(&c.device_id, &c.name, VirtualDeviceType::LightGroupLinear);
    LightGroupLinear::new(config, c.members, ranges, store.clone())
        .unwrap_or_else(|e| panic!("{}: {e}", c.device_id))
}

/// Group members, scene devices, controller buttons and controller targets
/// must all be devices the dummy registers (or, for controller targets,
/// shipped virtual devices).
#[tokio::test]
async fn shipped_virtual_devices_only_reference_dummy_devices() {
    let configs = shipped_configs().await;
    let (_store, registered) = dummy_home_store().await;
    let virtual_ids: HashSet<&str> = configs.iter().map(config_id).collect();

    let mut dangling = Vec::new();
    for config in &configs {
        let id = config_id(config);
        match config {
            VirtualDeviceTomlConfig::LightGroup(c) => {
                for member in &c.members {
                    let found = match member.strip_suffix('*') {
                        Some(prefix) => registered.iter().any(|d| d.starts_with(prefix)),
                        None => registered.contains(member),
                    };
                    if !found {
                        dangling.push(format!("{id}: member {member}"));
                    }
                }
            }
            VirtualDeviceTomlConfig::LightGroupLinear(c) => {
                for (name, member) in &c.members {
                    if !registered.contains(member) {
                        dangling.push(format!("{id}: member {name} = {member}"));
                    }
                }
            }
            VirtualDeviceTomlConfig::ButtonController(c) => {
                if !registered.contains(&c.button) {
                    dangling.push(format!("{id}: button {}", c.button));
                }
                let actions = [Some(&c.press_on), Some(&c.press_off)]
                    .into_iter()
                    .chain([c.press_on_long.as_ref(), c.press_off_long.as_ref()])
                    .flatten();
                for action in actions {
                    let Some(target) = action.get(1).and_then(|v| v.as_str()) else {
                        dangling.push(format!("{id}: action without a target: {action:?}"));
                        continue;
                    };
                    if !registered.contains(target) && !virtual_ids.contains(target) {
                        dangling.push(format!("{id}: target {target}"));
                    }
                }
            }
            VirtualDeviceTomlConfig::SceneController(c) => {
                for device in c.scenes.iter().flat_map(|s| &s.devices) {
                    if !registered.contains(&device.device_id) {
                        dangling.push(format!("{id}: scene device {}", device.device_id));
                    }
                }
            }
        }
    }

    assert!(
        dangling.is_empty(),
        "virtual_devices/*.toml reference devices the dummy never registers:\n  {}",
        dangling.join("\n  ")
    );
}

/// #16: no two shipped light groups drive the same lights the same way. Once
/// #14 pointed `bedroom_lights.toml` at the dummy's lights,
/// `bedroom_lights_test.toml` became a copy of it under another id: the same
/// three lights, with the same ranges.
#[tokio::test]
async fn shipped_light_groups_are_not_duplicates() {
    let configs = shipped_configs().await;
    let mut seen: HashMap<String, &str> = HashMap::new();
    for config in &configs {
        // What the group drives: each light and how it maps the level.
        let mut drives: Vec<String> = match config {
            VirtualDeviceTomlConfig::LightGroupLinear(c) => c
                .members
                .iter()
                .map(|(name, light)| format!("{light} {:?}", c.brightness.get(name)))
                .collect(),
            VirtualDeviceTomlConfig::LightGroup(c) => c
                .members
                .iter()
                .map(|light| {
                    let curve = c.brightness_curves.get(light).map(|b| (b.min, b.max));
                    format!("{light} {curve:?}")
                })
                .collect(),
            _ => continue,
        };
        drives.sort();
        let id = config_id(config);
        if let Some(other) = seen.insert(drives.join(", "), id) {
            panic!("{id} duplicates {other}: both drive {drives:?}");
        }
    }
}

/// #16: every shipped controller binds a switch the dummy registers, and no
/// two bind the same one, or one press would run two actions. It used to be
/// that one controller bound a real-hub remote the dummy doesn't have, and
/// the other took the dummy's only switch.
#[tokio::test]
async fn shipped_button_controllers_bind_distinct_dummy_switches() {
    let configs = shipped_configs().await;
    let gateway = DummyGateway::new("basic_home");
    let switches: HashSet<DeviceId> = gateway
        .discover_devices()
        .await
        .expect("discover")
        .into_iter()
        .filter(|info| info.device_type == DeviceType::Switch)
        .map(|info| info.device_id)
        .collect();

    let mut bound: HashMap<&str, &str> = HashMap::new();
    for config in &configs {
        let VirtualDeviceTomlConfig::ButtonController(c) = config else {
            continue;
        };
        assert!(
            switches.contains(&c.button),
            "{}: button {} isn't a switch the dummy registers ({switches:?})",
            c.device_id,
            c.button
        );
        if let Some(other) = bound.insert(&c.button, &c.device_id) {
            panic!(
                "{} and {other} both bind {}: one press would run two actions",
                c.device_id, c.button
            );
        }
    }
    assert!(
        !bound.is_empty(),
        "no button controller is shipped any more"
    );
}

/// The shipped linear groups and button controllers, registered with a
/// manager over the dummy's devices the way the server registers them, with
/// tracking fed by hand.
struct ShippedHome {
    store: Arc<StateStore>,
    bus: Arc<EventBus>,
    manager: VirtualDeviceManager,
    /// Each shipped linear group's member lights.
    group_members: HashMap<String, Vec<String>>,
    controllers: Vec<ButtonControllerConfig>,
}

async fn shipped_home() -> ShippedHome {
    let configs = shipped_configs().await;
    let (store, _) = dummy_home_store().await;
    let bus = Arc::new(EventBus::new(1000));
    let manager = VirtualDeviceManager::new(store.clone(), bus.clone());

    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
    let mut controllers = Vec::new();
    for config in configs {
        match config {
            VirtualDeviceTomlConfig::LightGroupLinear(c) => {
                group_members.insert(c.device_id.clone(), c.members.values().cloned().collect());
                let group = linear_group(c, &store);
                manager
                    .add_virtual_device(Box::new(group))
                    .await
                    .expect("register group");
            }
            VirtualDeviceTomlConfig::ButtonController(c) => controllers.push(c),
            _ => {}
        }
    }
    assert!(
        !controllers.is_empty(),
        "no button controller is shipped any more"
    );
    for c in &controllers {
        let config = vd_config(&c.device_id, &c.name, VirtualDeviceType::ButtonController);
        let controller = ButtonController::new(
            config,
            c.button.clone(),
            c.press_on.clone(),
            c.press_off.clone(),
            c.press_on_long.clone(),
            c.press_off_long.clone(),
            store.clone(),
            bus.clone(),
        )
        .unwrap_or_else(|e| panic!("{}: {e}", c.device_id));
        manager
            .add_virtual_device(Box::new(controller))
            .await
            .expect("register controller");
    }
    ShippedHome {
        store,
        bus,
        manager,
        group_members,
        controllers,
    }
}

impl ShippedHome {
    /// Input tracking, caught up: every event published since `rx`
    /// subscribed, and the ones handling them publishes in turn.
    async fn pump(
        &self,
        rx: &mut tokio::sync::broadcast::Receiver<DeviceEvent>,
    ) -> Vec<DeviceEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            self.manager
                .handle_event(&event)
                .await
                .expect("input tracking");
            events.push(event);
        }
        events
    }

    /// `device_id`'s light state in the store, as `(is_on, brightness)`.
    async fn light(&self, device_id: &str) -> (bool, Option<u8>) {
        match self.store.get_device(&device_id.to_string()).await {
            Some(DeviceState {
                state: DeviceStateValue::Light(light),
                ..
            }) => (light.is_on, light.brightness),
            other => panic!("{device_id} isn't a light in the store: {other:?}"),
        }
    }
}

/// `(is_on, brightness)` of each light state echoed for `device_id`.
fn light_echoes(events: &[DeviceEvent], device_id: &str) -> Vec<(bool, Option<u8>)> {
    events
        .iter()
        .filter(|e| e.device_id == device_id)
        .filter_map(|e| match &e.event_type {
            EventType::AttributeChanged {
                attribute,
                new_value,
                ..
            } if attribute == "state" => {
                match serde_json::from_value::<DeviceStateValue>(new_value.clone()) {
                    Ok(DeviceStateValue::Light(light)) => Some((light.is_on, light.brightness)),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect()
}

/// #16: a press of a shipped controller's button, against the dummy, must
/// light the members of the group it targets. Driven through the manager
/// as the server wires it, with tracking fed by hand.
#[tokio::test]
async fn shipped_button_controller_lights_its_group_against_dummy() {
    let home = shipped_home().await;
    let (store, bus) = (&home.store, &home.bus);
    for c in &home.controllers {
        let target = c.press_on[1].as_str().expect("target");
        let members = home
            .group_members
            .get(target)
            .unwrap_or_else(|| panic!("{}: {target} isn't a shipped group", c.device_id));
        // Every member off first, so lighting them is the press's doing.
        for member in members {
            store
                .update_device_state(
                    member,
                    DeviceStateValue::Light(LightState {
                        is_on: false,
                        brightness: Some(0),
                        color_temp: None,
                        rgb_color: None,
                    }),
                )
                .await
                .unwrap();
        }
        let mut rx = bus.subscribe();

        // The press, as the sync engine reports it: stored, and echoed.
        let pressed = DeviceStateValue::Switch(SwitchState {
            is_pressed: true,
            last_pressed: None,
            battery_level: Some(85),
        });
        store
            .update_device_state(&c.button, pressed.clone())
            .await
            .unwrap();
        bus.publish(DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: c.button.clone(),
            event_type: EventType::AttributeChanged {
                attribute: "state".to_string(),
                old_value: serde_json::Value::Null,
                new_value: serde_json::to_value(&pressed).unwrap(),
            },
        })
        .await;
        home.pump(&mut rx).await;

        for member in members {
            let state = store.get_device(member).await.unwrap().state;
            assert!(
                matches!(
                    state,
                    DeviceStateValue::Light(LightState { is_on: true, .. })
                ),
                "{}: pressing {} left {member} of {target} unlit: {state:?}",
                c.device_id,
                c.button
            );
        }
    }
}

/// #35: the shipped controller follows the gestures a hub reports whole
/// (`ButtonPressed`), against the dummy. Its binding is `press_on = on`,
/// `press_off = off` and `press_on_long = inc 10`, on Bedroom Lights, which
/// starts on at 75 (the dummy's kitchen light).
/// - A click is a press and a release: the group goes on, then off, and so
///   does every member. It keeps its level.
/// - A long press runs `inc 10` only: the group and every member come back
///   on, a step brighter.
#[tokio::test]
async fn shipped_button_controller_follows_reported_gestures_against_dummy() {
    const GROUP: &str = "virtual_bedroom_lights";
    let home = shipped_home().await;
    let [c] = home.controllers.as_slice() else {
        panic!("one shipped controller: {:?}", home.controllers);
    };
    assert_eq!(
        (&c.press_on, &c.press_off, &c.press_on_long),
        (
            &vec![serde_json::json!("on"), serde_json::json!(GROUP)],
            &vec![serde_json::json!("off"), serde_json::json!(GROUP)],
            &Some(vec![
                serde_json::json!("inc"),
                serde_json::json!(GROUP),
                serde_json::json!(10)
            ]),
        ),
        "the binding this test drives"
    );
    let members = &home.group_members[GROUP];
    assert_eq!(home.light(GROUP).await, (true, Some(75)), "group at start");
    let report = |press_type| DeviceEvent {
        timestamp: std::time::SystemTime::now(),
        device_id: c.button.clone(),
        event_type: EventType::ButtonPressed {
            button_id: "main".to_string(),
            press_type,
        },
    };
    let mut rx = home.bus.subscribe();

    home.bus.publish(report(ButtonPressType::SinglePress)).await;
    let events = home.pump(&mut rx).await;
    assert_eq!(
        light_echoes(&events, GROUP),
        vec![(true, Some(75)), (false, Some(75))],
        "a click: on, then off"
    );
    assert_eq!(home.light(GROUP).await, (false, Some(75)));
    for member in members {
        let echoed: Vec<bool> = light_echoes(&events, member)
            .into_iter()
            .map(|(is_on, _)| is_on)
            .collect();
        assert_eq!(echoed, vec![true, false], "{member}: on, then off");
        assert!(!home.light(member).await.0, "{member} is still on");
    }

    home.bus.publish(report(ButtonPressType::LongPress)).await;
    let events = home.pump(&mut rx).await;
    assert_eq!(
        light_echoes(&events, GROUP),
        vec![(true, Some(85))],
        "a long press: inc 10, and nothing else"
    );
    for member in members {
        assert!(home.light(member).await.0, "{member} is still off");
    }
}

/// The symptom of #2: a write to a shipped linear group must succeed (not
/// fail on a missing member) against the dummy.
#[tokio::test]
async fn shipped_light_groups_accept_writes_against_dummy() {
    let configs = shipped_configs().await;
    let (store, _) = dummy_home_store().await;
    let manager = VirtualDeviceManager::new(store.clone(), Arc::new(EventBus::new(100)));

    let mut groups = Vec::new();
    for config in configs {
        let VirtualDeviceTomlConfig::LightGroupLinear(c) = config else {
            continue;
        };
        let device_id = c.device_id.clone();
        manager
            .add_virtual_device(Box::new(linear_group(c, &store)))
            .await
            .expect("register");
        groups.push(device_id);
    }
    assert!(
        groups.iter().any(|id| id == "virtual_bedroom_lights"),
        "Bedroom Lights is no longer shipped: {groups:?}"
    );

    let on = DeviceStateValue::Light(LightState {
        is_on: true,
        brightness: Some(50),
        color_temp: Some(2700),
        rgb_color: None,
    });
    for id in &groups {
        manager
            .set_virtual_device_state(id, on.clone())
            .await
            .unwrap_or_else(|e| panic!("writing {id} failed: {e}"));
    }
}
