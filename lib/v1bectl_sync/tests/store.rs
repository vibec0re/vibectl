//! 🔥 Integration tests for `StateStore` (`lib/v1bectl_sync/src/store.rs`).
//!
//! Covers every public method: `add/get/list/list_by_type/list_by_group`,
//! update (including the `DeviceNotFound` error path and `last_updated`
//! monotonicity), remove, the device-group CRUD, the stats helpers, and a
//! concurrency smoke test.

use std::collections::HashMap;
use std::time::Duration;

use v1bectl_sync::{Capability, DeviceGroupInfo, DeviceInfo, DeviceType, StateError, StateStore};
use v1bectl_sync::{DeviceStateValue, LightState, SwitchState};

fn device_info(id: &str, device_type: DeviceType, groups: &[&str], reachable: bool) -> DeviceInfo {
    DeviceInfo {
        device_id: id.to_string(),
        name: format!("Device {id}"),
        device_type,
        capabilities: vec![Capability::OnOff],
        device_groups: groups.iter().map(ToString::to_string).collect(),
        manufacturer: Some("IKEA".to_string()),
        model: Some("TRADFRI".to_string()),
        firmware_version: Some("1.0.0".to_string()),
        battery_powered: false,
        reachable,
        last_seen: 0,
        custom_attributes: HashMap::new(),
    }
}

fn light_state(is_on: bool, brightness: u8) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness: Some(brightness),
        color_temp: None,
        rgb_color: None,
    })
}

// ---------------------------------------------------------------------
// add / get
// ---------------------------------------------------------------------

#[tokio::test]
async fn add_device_then_get_round_trips_the_stored_state() {
    let store = StateStore::new();
    let info = device_info("light-1", DeviceType::Light, &["kitchen"], true);
    store.add_device(info.clone(), light_state(true, 50)).await;

    let fetched = store
        .get_device(&"light-1".to_string())
        .await
        .expect("device should be present after add_device");

    assert_eq!(fetched.device_id, "light-1");
    assert_eq!(fetched.device_info, info);
    assert_eq!(fetched.state, light_state(true, 50));
    assert!(fetched.last_updated > 0);
    assert_eq!(fetched.last_synced_to_gateway, None);
    assert_eq!(fetched.last_synced_from_gateway, None);
}

#[tokio::test]
async fn get_device_returns_none_for_unknown_id() {
    let store = StateStore::new();
    assert!(store.get_device(&"nope".to_string()).await.is_none());
}

// ---------------------------------------------------------------------
// list / count
// ---------------------------------------------------------------------

#[tokio::test]
async fn list_devices_and_device_count_reflect_all_added_devices() {
    let store = StateStore::new();
    for i in 0..3 {
        let id = format!("device-{i}");
        store
            .add_device(
                device_info(&id, DeviceType::Light, &[], true),
                light_state(false, 0),
            )
            .await;
    }

    assert_eq!(store.device_count().await, 3);
    let listed = store.list_devices().await;
    assert_eq!(listed.len(), 3);
    let mut ids: Vec<_> = listed.into_iter().map(|d| d.device_id).collect();
    ids.sort();
    assert_eq!(ids, vec!["device-0", "device-1", "device-2"]);
}

#[tokio::test]
async fn list_devices_by_type_filters_to_matching_type_only() {
    let store = StateStore::new();
    store
        .add_device(
            device_info("light-1", DeviceType::Light, &[], true),
            light_state(true, 10),
        )
        .await;
    store
        .add_device(
            device_info("switch-1", DeviceType::Switch, &[], true),
            DeviceStateValue::Switch(SwitchState {
                is_pressed: false,
                last_pressed: None,
                battery_level: None,
            }),
        )
        .await;

    let lights = store.list_devices_by_type(&DeviceType::Light).await;
    assert_eq!(lights.len(), 1);
    assert_eq!(lights[0].device_id, "light-1");

    let switches = store.list_devices_by_type(&DeviceType::Switch).await;
    assert_eq!(switches.len(), 1);
    assert_eq!(switches[0].device_id, "switch-1");

    let sensors = store.list_devices_by_type(&DeviceType::Sensor).await;
    assert!(sensors.is_empty());
}

#[tokio::test]
async fn list_devices_by_group_filters_to_matching_group_only() {
    let store = StateStore::new();
    store
        .add_device(
            device_info("light-1", DeviceType::Light, &["kitchen"], true),
            light_state(true, 10),
        )
        .await;
    store
        .add_device(
            device_info("light-2", DeviceType::Light, &["bedroom"], true),
            light_state(false, 0),
        )
        .await;
    store
        .add_device(
            device_info(
                "light-3",
                DeviceType::Light,
                &["kitchen", "downstairs"],
                true,
            ),
            light_state(false, 0),
        )
        .await;

    let mut kitchen_ids: Vec<_> = store
        .list_devices_by_group("kitchen")
        .await
        .into_iter()
        .map(|d| d.device_id)
        .collect();
    kitchen_ids.sort();
    assert_eq!(kitchen_ids, vec!["light-1", "light-3"]);

    assert!(store.list_devices_by_group("nonexistent").await.is_empty());
}

// ---------------------------------------------------------------------
// update_device_state
// ---------------------------------------------------------------------

/// Pins two behaviours of `update_device_state` in one place: the state is
/// actually replaced, and `last_updated` moves forward on every update
/// rather than staying frozen at the `add_device` value.
///
/// The first comparison (add -> first update) uses `>=` per the task's
/// flakiness guidance, since both could in principle land in the same
/// millisecond. The second comparison brackets an explicit sleep so the two
/// update timestamps are controlled to fall in different milliseconds,
/// which lets it assert strict `>` reliably: this is the assertion that
/// catches a `last_updated` that silently stops advancing.
#[tokio::test]
async fn update_device_state_replaces_state_and_last_updated_is_monotonic() {
    let store = StateStore::new();
    let id = "light-1".to_string();
    store
        .add_device(
            device_info(&id, DeviceType::Light, &[], true),
            light_state(false, 0),
        )
        .await;

    let after_add = store.get_device(&id).await.unwrap().last_updated;

    store
        .update_device_state(&id, light_state(true, 50))
        .await
        .expect("update on existing device should succeed");
    let after_first_update = store.get_device(&id).await.unwrap();
    assert_eq!(after_first_update.state, light_state(true, 50));
    assert!(after_first_update.last_updated >= after_add);

    // Control time so the two update timestamps cannot collide in the same
    // millisecond, then require the timestamp to have strictly advanced.
    tokio::time::sleep(Duration::from_millis(15)).await;

    store
        .update_device_state(&id, light_state(false, 0))
        .await
        .expect("second update on existing device should succeed");
    let after_second_update = store.get_device(&id).await.unwrap();
    assert_eq!(after_second_update.state, light_state(false, 0));
    assert!(
        after_second_update.last_updated > after_first_update.last_updated,
        "last_updated did not advance across updates: {} -> {}",
        after_first_update.last_updated,
        after_second_update.last_updated
    );
}

#[tokio::test]
async fn update_device_state_on_missing_device_returns_device_not_found_with_id() {
    let store = StateStore::new();
    let missing_id = "does-not-exist".to_string();

    match store
        .update_device_state(&missing_id, light_state(true, 1))
        .await
    {
        Err(StateError::DeviceNotFound(id)) => assert_eq!(id, missing_id),
        other => panic!("expected Err(DeviceNotFound), got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// remove_device
// ---------------------------------------------------------------------

#[tokio::test]
async fn remove_device_returns_it_and_then_get_returns_none() {
    let store = StateStore::new();
    let id = "light-1".to_string();
    let info = device_info(&id, DeviceType::Light, &[], true);
    store.add_device(info.clone(), light_state(true, 20)).await;

    let before_removal = store.get_device(&id).await.unwrap();

    let removed = store
        .remove_device(&id)
        .await
        .expect("removing an existing device should succeed");
    assert_eq!(removed, before_removal);

    assert!(store.get_device(&id).await.is_none());
    assert_eq!(store.device_count().await, 0);
}

#[tokio::test]
async fn remove_device_on_missing_device_returns_device_not_found_with_id() {
    let store = StateStore::new();
    let missing_id = "does-not-exist".to_string();

    match store.remove_device(&missing_id).await {
        Err(StateError::DeviceNotFound(id)) => assert_eq!(id, missing_id),
        other => panic!("expected Err(DeviceNotFound), got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// device groups
// ---------------------------------------------------------------------

fn group_info(name: &str, device_ids: &[&str]) -> DeviceGroupInfo {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "device count, always far below u32::MAX"
    )]
    let device_count = device_ids.len() as u32;
    DeviceGroupInfo {
        group_name: name.to_string(),
        icon_ref: format!("icon://{name}"),
        device_count,
        device_ids: device_ids.iter().map(ToString::to_string).collect(),
    }
}

#[tokio::test]
async fn create_device_group_then_duplicate_name_fails() {
    let store = StateStore::new();
    store
        .create_device_group(group_info("kitchen", &["light-1"]))
        .await
        .expect("first creation should succeed");

    match store
        .create_device_group(group_info("kitchen", &["light-2"]))
        .await
    {
        Err(StateError::GroupAlreadyExists(name)) => assert_eq!(name, "kitchen"),
        other => panic!("expected Err(GroupAlreadyExists), got {other:?}"),
    }
}

#[tokio::test]
async fn list_and_get_device_group_reflect_created_groups() {
    let store = StateStore::new();
    store
        .create_device_group(group_info("kitchen", &["light-1"]))
        .await
        .unwrap();
    store
        .create_device_group(group_info("bedroom", &["light-2", "light-3"]))
        .await
        .unwrap();

    let mut names: Vec<_> = store
        .list_device_groups()
        .await
        .into_iter()
        .map(|g| g.group_name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["bedroom", "kitchen"]);

    let fetched = store
        .get_device_group("bedroom")
        .await
        .expect("bedroom group should exist");
    assert_eq!(fetched.device_ids, vec!["light-2", "light-3"]);
    assert_eq!(fetched.device_count, 2);

    assert!(store.get_device_group("nonexistent").await.is_none());
}

// ---------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------

#[tokio::test]
async fn reachable_device_count_counts_only_reachable_devices() {
    let store = StateStore::new();
    store
        .add_device(
            device_info("light-1", DeviceType::Light, &[], true),
            light_state(true, 10),
        )
        .await;
    store
        .add_device(
            device_info("light-2", DeviceType::Light, &[], true),
            light_state(true, 10),
        )
        .await;
    store
        .add_device(
            device_info("light-3", DeviceType::Light, &[], false),
            light_state(false, 0),
        )
        .await;

    assert_eq!(store.device_count().await, 3);
    assert_eq!(store.reachable_device_count().await, 2);
}

// ---------------------------------------------------------------------
// concurrency smoke test
// ---------------------------------------------------------------------

/// N tasks concurrently update N distinct devices. This does not stress any
/// single key (that's covered by the `RwLock`'s own guarantees) — it instead
/// pins the higher-level contract that concurrent updates to *different*
/// devices don't get lost or cross-applied to the wrong device.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_to_distinct_devices_all_land() {
    const N: usize = 20;
    let store = StateStore::new();

    for i in 0..N {
        let id = format!("device-{i}");
        store
            .add_device(
                device_info(&id, DeviceType::Light, &[], true),
                light_state(false, 0),
            )
            .await;
    }

    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let id = format!("device-{i}");
            #[expect(
                clippy::cast_possible_truncation,
                reason = "i is 0..N (N=20), so i*2 never exceeds u8::MAX"
            )]
            let brightness = (i * 2) as u8;
            store
                .update_device_state(&id, light_state(true, brightness))
                .await
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("task should not panic")
            .expect("update should succeed");
    }

    for i in 0..N {
        let id = format!("device-{i}");
        let device = store
            .get_device(&id)
            .await
            .unwrap_or_else(|| panic!("device {id} should still exist"));
        #[expect(
            clippy::cast_possible_truncation,
            reason = "i is 0..N (N=20), so i*2 never exceeds u8::MAX"
        )]
        let expected_brightness = (i * 2) as u8;
        assert_eq!(device.state, light_state(true, expected_brightness));
    }
}
