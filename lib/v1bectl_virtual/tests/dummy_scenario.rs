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

/// Group members, scene devices and controller targets must all be devices
/// the dummy registers (or, for controller targets, shipped virtual devices).
/// Controller *buttons* aren't checked: `button_ctrl.toml` binds a real-hub
/// remote, and the dummy's only switch is already `button_test.toml`'s.
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
        let ranges: HashMap<String, (u8, u8)> = c
            .brightness
            .iter()
            .map(|(name, [min, max])| (name.clone(), (*min, *max)))
            .collect();
        let vd_config = VirtualDeviceConfig {
            device_id: c.device_id.clone(),
            device_type: VirtualDeviceType::LightGroupLinear,
            name: c.name.clone(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let group = LightGroupLinear::new(vd_config, c.members, ranges, store.clone())
            .unwrap_or_else(|e| panic!("{}: {e}", c.device_id));
        manager
            .add_virtual_device(Box::new(group))
            .await
            .expect("register");
        groups.push(c.device_id);
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
