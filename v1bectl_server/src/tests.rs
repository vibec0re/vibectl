//! #10: a scene controller configured in TOML comes alive. The server loads
//! it from `virtual_devices/` with every other virtual device
//! ([`load_virtual_devices`]), and setting one of its scenes sets that
//! scene's devices the way the server wires them: through the sync engine,
//! stored, echoed, and queued for the gateway.
//!
//! The configs are `tests/fixtures/virtual_devices/*.toml`, over the dummy
//! `basic_home` scenario. The clock is paused, so a fade's delays pass at
//! once and measure exactly.

use super::load_virtual_devices;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use v1bectl_api::AxumServer;
use v1bectl_gateway::Gateway;
use v1bectl_sync::{
    DeviceEvent, DeviceStateValue, EventBus, EventType, LightState, OutletState, SceneState,
    StateStore, SyncEngine, SyncStatus,
};
use v1bectl_virtual::{DummyGateway, VirtualDeviceManager};

/// `living_room_scenes.toml`: instant.
const LIVING_ROOM: &str = "scene_living_room";
/// `bedroom_scenes.toml`: a 2 s fade.
const BEDROOM: &str = "scene_bedroom";

/// The server over the dummy `basic_home`, set up as `run_server` sets it
/// up, with the fixtures loaded where it loads `virtual_devices/`.
struct Server {
    store: Arc<StateStore>,
    engine: Arc<SyncEngine>,
    manager: Arc<VirtualDeviceManager>,
    /// Subscribed before the virtual devices loaded.
    events: broadcast::Receiver<DeviceEvent>,
}

async fn server() -> Server {
    server_over("virtual_devices").await
}

/// The server over the dummy `basic_home`, set up as `run_server` sets it
/// up, with `tests/fixtures/{fixtures}` loaded where it loads
/// `virtual_devices/`.
async fn server_over(fixtures: &str) -> Server {
    let store = StateStore::new();
    let bus = Arc::new(EventBus::new(1000));
    let gateway: Arc<dyn Gateway> = Arc::new(DummyGateway::new("basic_home"));
    for info in gateway.discover_devices().await.expect("discover") {
        let state = gateway
            .get_device_state(&info.device_id)
            .await
            .expect("initial state");
        store.add_device(info, state).await;
    }
    let engine = Arc::new(SyncEngine::new(
        store.clone(),
        bus.clone(),
        gateway.clone(),
        None,
    ));
    let axum_server =
        AxumServer::new(0, store.clone(), bus.clone(), gateway).with_sync_engine(engine.clone());
    let manager = axum_server.virtual_device_manager();
    let events = bus.subscribe();

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixtures);
    load_virtual_devices(&fixtures, &store, &manager).await;
    Server {
        store,
        engine,
        manager,
        events,
    }
}

impl Server {
    /// The events published since the last call.
    fn drain(&mut self) -> Vec<DeviceEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            events.push(event);
        }
        events
    }

    async fn stored(&self, device_id: &str) -> DeviceStateValue {
        self.store
            .get_device(&device_id.to_string())
            .await
            .unwrap_or_else(|| panic!("{device_id} isn't in the store"))
            .state
    }

    /// Set scene `scene` of `controller`, as the API's `ActivateScene`
    /// does, and return how long it took.
    async fn activate(&self, controller: &str, scene: &str) -> Duration {
        let started = tokio::time::Instant::now();
        self.manager
            .set_virtual_device_state(&controller.to_string(), active(scene))
            .await
            .unwrap_or_else(|e| panic!("activating {scene} of {controller}: {e}"));
        started.elapsed()
    }
}

fn active(scene: &str) -> DeviceStateValue {
    DeviceStateValue::Scene(SceneState {
        scene_name: scene.to_string(),
        is_active: true,
    })
}

/// #62: `virtual_devices_light_group/lg_curves.toml`, loaded the way the
/// server loads `virtual_devices/`. A separate fixture dir from the scene
/// controllers' above, so its light group's members don't collide with
/// theirs.
async fn light_group_server() -> Server {
    server_over("virtual_devices_light_group").await
}

/// A scene controller's state before any of its scenes is set.
fn inactive() -> DeviceStateValue {
    DeviceStateValue::Scene(SceneState {
        scene_name: "none".to_string(),
        is_active: false,
    })
}

fn light(is_on: bool, brightness: Option<u8>, color_temp: Option<u16>) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness,
        color_temp,
        rgb_color: None,
    })
}

/// The dummy's `outlet_tv`, on or off. A scene changes only the fields it
/// names (#64), so it keeps the dummy's readings.
fn outlet(is_on: bool) -> DeviceStateValue {
    DeviceStateValue::Outlet(OutletState {
        is_on,
        power_consumption: Some(45.5),
        total_energy: Some(123.4),
    })
}

/// Each state echoed for `device_id`, in order.
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

/// Both fixture controllers are registered: in the manager and the store,
/// announced, with their scenes and display names, and nothing they set
/// is missing from the dummy.
#[tokio::test(start_paused = true)]
async fn the_server_registers_toml_scene_controllers() {
    let mut server = server().await;
    let events = server.drain();

    for id in [LIVING_ROOM, BEDROOM] {
        let state = server
            .manager
            .get_virtual_device_state(&id.to_string())
            .await
            .unwrap_or_else(|e| panic!("{id} isn't registered: {e}"));
        assert_eq!(state, inactive(), "{id}");
        assert_eq!(server.stored(id).await, inactive(), "{id} in the store");
        assert!(
            events.iter().any(|e| e.device_id == id
                && matches!(&e.event_type, EventType::DeviceAdded { device_type } if device_type == "VirtualScene")),
            "{id} wasn't announced"
        );
    }

    let config = server
        .manager
        .get_virtual_device_config(&LIVING_ROOM.to_string())
        .await
        .expect("config");
    assert_eq!(config.name, "Living Room Scenes");
    let scenes = &config.config["scenes"];
    assert_eq!(scenes["movie"]["display_name"], "Movie Night");
    assert_eq!(scenes["lights_out"]["display_name"], "Lights Out");
    assert_eq!(
        scenes.as_object().map(serde_json::Map::len),
        Some(2),
        "{scenes}"
    );

    assert_eq!(
        server.manager.dangling_references().await,
        Vec::new(),
        "the fixtures set only the dummy's devices"
    );
}

/// `transition_duration = 0`: a scene sets its devices at once, each
/// stored at its target, echoed once, and queued for the gateway. The
/// outlet is set as an outlet (the dummy's is one), and the other
/// controller's light is left alone.
#[tokio::test(start_paused = true)]
async fn an_instant_toml_scene_sets_its_devices_at_once() {
    let mut server = server().await;
    let bedroom_light = server.stored("light_bedroom").await;
    server.drain();

    for (scene, targets) in [
        (
            "movie",
            [
                ("light_living_room", light(true, Some(20), Some(2200))),
                // The scene names only `is_on`: it keeps its level (#64).
                ("light_kitchen", light(false, Some(75), None)),
                // Already on: the scene names only `is_on`, so it's left
                // as it is (#64).
                ("outlet_tv", outlet(true)),
            ],
        ),
        (
            "lights_out",
            [
                // The scene names only `is_on`: it keeps the level and
                // colour `movie` set (#64).
                ("light_living_room", light(false, Some(20), Some(2200))),
                // The scene names only `is_on`: it keeps its level (#64).
                ("light_kitchen", light(false, Some(75), None)),
                ("outlet_tv", outlet(false)),
            ],
        ),
    ] {
        let elapsed = server.activate(LIVING_ROOM, scene).await;
        assert_eq!(elapsed, Duration::ZERO, "{scene} waited");
        let events = server.drain();

        for (id, target) in &targets {
            let echoed = echoes(&events, id);
            assert_eq!(&server.stored(id).await, target, "{scene}: {id}");
            // A scene changes only the fields it names (#64), so what it
            // leaves as it is has nothing to echo: the kitchen light, off
            // at its level after `movie`, in `lights_out`, and the TV,
            // already on, in `movie`.
            let unchanged = (scene == "lights_out" && *id == "light_kitchen")
                || (scene == "movie" && *id == "outlet_tv");
            let want = if unchanged {
                Vec::new()
            } else {
                vec![target.clone()]
            };
            assert_eq!(echoed, want, "{scene}: {id}'s echoes");
            // The TV isn't written by `movie` (#64), so nothing is queued
            // for it yet.
            if scene == "movie" && *id == "outlet_tv" {
                continue;
            }
            let status = server.engine.get_sync_status(&id.to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "{scene}: {id} never queued for the gateway: {status:?}"
            );
        }
        assert_eq!(server.stored(LIVING_ROOM).await, active(scene), "{scene}");
        assert_eq!(
            echoes(&events, LIVING_ROOM),
            vec![active(scene)],
            "{scene}: the controller's echo"
        );
        assert_eq!(
            server.stored("light_bedroom").await,
            bedroom_light,
            "{scene} touched the bedroom"
        );
        assert!(echoes(&events, "light_bedroom").is_empty(), "{scene}");
    }
}

/// `transition_duration = 2000`: a scene fades for 2 s, then commits its
/// light at its target. Its steps are the scene's own (#55), so the light
/// is echoed once, at the end.
#[tokio::test(start_paused = true)]
async fn a_fading_toml_scene_ends_at_its_targets_after_its_transition() {
    let mut server = server().await;
    server.drain();

    for (scene, target) in [
        ("wake_up", light(true, Some(80), Some(4000))),
        // The scene names no colour: it keeps `wake_up`'s (#64).
        ("sleep", light(false, Some(0), Some(4000))),
    ] {
        let elapsed = server.activate(BEDROOM, scene).await;
        assert_eq!(elapsed, Duration::from_secs(2), "{scene}: the fade's time");
        let events = server.drain();

        assert_eq!(server.stored("light_bedroom").await, target, "{scene}");
        assert_eq!(
            echoes(&events, "light_bedroom"),
            vec![target.clone()],
            "{scene}: echoed once, at its target"
        );
        assert_eq!(server.stored(BEDROOM).await, active(scene), "{scene}");
        assert_eq!(
            echoes(&events, BEDROOM),
            vec![active(scene)],
            "{scene}: the controller's echo"
        );
    }
}

/// #62: a `light_group`'s `brightness_curves` are mapped into the
/// `LightGroup` it builds, instead of every member getting a forced 1:1
/// curve. `lg_curves.toml` gives `light_living_room` a curve (group 100 ->
/// member 60) and leaves `light_kitchen` without one, so it falls back to
/// 1:1.
///
/// Mutant: force every member back to a 1:1 curve (the pre-#62 behaviour)
/// and `light_living_room` comes out at 100, not 60 — red.
#[tokio::test(start_paused = true)]
async fn a_toml_light_groups_brightness_curve_is_used() {
    let server = light_group_server().await;

    server
        .manager
        .set_virtual_device_state(
            &"lg_test_curves".to_string(),
            light(true, Some(100), Some(2700)),
        )
        .await
        .expect("setting the light group to 100");

    assert_eq!(
        server.stored("light_living_room").await,
        light(true, Some(60), Some(2700)),
        "light_living_room has a curve capping it at 60"
    );
    assert_eq!(
        server.stored("light_kitchen").await,
        light(true, Some(100), Some(2700)),
        "light_kitchen has no curve, so it falls back to 1:1"
    );
}

/// #58: `virtual_devices_nested/`, where the scene controller's file
/// (`a_movie_scenes.toml`) sorts before the file of the light group its
/// scene sets (`b_living_group.toml`). The scene loads first, and isn't
/// rejected or reshaped for a target that isn't there yet: both are
/// registered, and nothing is dangling. Setting `movie` then reaches the
/// group's lights, through the group: each at the level its range maps 30
/// to, echoed once and queued for the gateway. The group reports 30. It
/// used to take the scene's state in the store while no light moved.
#[tokio::test(start_paused = true)]
async fn a_toml_scene_that_loads_before_the_group_it_sets_reaches_its_lights() {
    const SCENES: &str = "scene_nested";
    const GROUP: &str = "group_nested";
    let mut server = server_over("virtual_devices_nested").await;
    let announced: Vec<String> = server
        .drain()
        .into_iter()
        .filter(|e| matches!(e.event_type, EventType::DeviceAdded { .. }))
        .map(|e| e.device_id)
        .filter(|id| [SCENES, GROUP].contains(&id.as_str()))
        .collect();
    assert_eq!(announced, [SCENES, GROUP], "the scene loads first");
    assert_eq!(server.manager.dangling_references().await, Vec::new());
    let bedroom_light = server.stored("light_bedroom").await;

    let elapsed = server.activate(SCENES, "movie").await;
    assert_eq!(elapsed, Duration::ZERO, "movie waited");
    let events = server.drain();

    for (id, level) in [("light_living_room", 86), ("light_kitchen", 15)] {
        let target = light(true, Some(level), Some(2700));
        assert_eq!(server.stored(id).await, target, "{id}");
        assert_eq!(echoes(&events, id), vec![target], "{id}'s echo");
        let status = server.engine.get_sync_status(&id.to_string()).await;
        assert!(
            matches!(status, Some(SyncStatus::PendingSync { .. })),
            "{id} never queued for the gateway: {status:?}"
        );
    }
    let group = light(true, Some(30), Some(2700));
    assert_eq!(server.stored(GROUP).await, group, "the group");
    assert_eq!(echoes(&events, GROUP), vec![group], "the group's echo");
    assert_eq!(echoes(&events, SCENES), vec![active("movie")]);
    let status = server.engine.get_sync_status(&GROUP.to_string()).await;
    assert!(status.is_none(), "the group was queued for the gateway");
    assert_eq!(server.stored("light_bedroom").await, bedroom_light);
}

/// #58: a `light_group`'s `*` wildcard matches only physical devices. In
/// `virtual_devices_wildcard/`, the linear group `light_virtual_pair` loads
/// (and is in the store) before `all_lights`, whose `light_*` would match
/// its id. It used to join the group, as a member the write would now fan
/// out through, and only if its file happened to load first. A virtual
/// member is named, never matched.
#[tokio::test(start_paused = true)]
async fn a_toml_light_groups_wildcard_matches_no_virtual_device() {
    const VIRTUAL: &str = "light_virtual_pair";
    let server = server_over("virtual_devices_wildcard").await;
    assert!(
        server
            .store
            .get_device(&VIRTUAL.to_string())
            .await
            .is_some(),
        "{VIRTUAL} loaded"
    );

    let config = server
        .manager
        .get_virtual_device_config(&"all_lights".to_string())
        .await
        .expect("all_lights loaded");
    let mut members: Vec<String> =
        serde_json::from_value(config.config["lights"].clone()).expect("its lights");
    members.sort();
    let mut lights: Vec<String> = server
        .store
        .list_devices()
        .await
        .into_iter()
        .map(|device| device.device_info.device_id)
        .filter(|id| id.starts_with("light_") && id != VIRTUAL)
        .collect();
    lights.sort();
    assert!(!lights.is_empty(), "the dummy has lights");
    assert_eq!(members, lights, "light_* matched a virtual device");
}

/// #72 review, finding 2: `virtual_devices_group_of_groups/`, the nesting
/// example of `docs/VIRTUAL_DEVICES.md` over the dummy's lights, with each
/// file named after its id. So the outer group, `downstairs_lights`, loads
/// before `living_room_lights`, the group it nests.
///
/// It catches up with the inner group once that's loaded: it shows its lit
/// members (the dummy's kitchen light, on at 75, puts `living_room_lights`
/// at 75, which `downstairs_lights`' range of 20-100 for it inverts to 69).
/// So a click of the switch that toggles it switches the lit room off. It
/// used to seed without the inner group, as off at 100, and stay so: the
/// toggle then lit every light at 100.
#[tokio::test(start_paused = true)]
async fn a_toml_group_of_groups_that_loads_first_starts_with_its_lit_members() {
    const OUTER: &str = "downstairs_lights";
    const INNER: &str = "living_room_lights";
    let mut server = server_over("virtual_devices_group_of_groups").await;
    let loaded: Vec<String> = server
        .drain()
        .into_iter()
        .filter(|e| matches!(e.event_type, EventType::DeviceAdded { .. }))
        .map(|e| e.device_id)
        .filter(|id| [OUTER, INNER].contains(&id.as_str()))
        .collect();
    assert_eq!(loaded, [OUTER, INNER], "the outer group loads first");

    assert_eq!(
        server.stored(INNER).await,
        light(true, Some(75), Some(2700))
    );
    let outer = light(true, Some(69), Some(2700));
    assert_eq!(server.stored(OUTER).await, outer, "the outer group");
    let own = server
        .manager
        .get_virtual_device_state(&OUTER.to_string())
        .await
        .expect(OUTER);
    assert_eq!(own, outer, "its own state");

    let click = DeviceEvent {
        timestamp: std::time::SystemTime::now(),
        device_id: "switch_hallway".to_string(),
        event_type: EventType::ButtonPressed {
            button_id: "main".to_string(),
            press_type: v1bectl_sync::ButtonPressType::SinglePress,
        },
    };
    server
        .manager
        .handle_event(&click)
        .await
        .expect("the click");
    for id in [
        "light_living_room",
        "light_kitchen",
        "light_bedroom",
        INNER,
        OUTER,
    ] {
        let state = server.stored(id).await;
        assert!(
            matches!(&state, DeviceStateValue::Light(light) if !light.is_on),
            "{id} is on after the toggle: {state:?}"
        );
    }
    assert_eq!(
        server.stored(OUTER).await,
        light(false, Some(69), Some(2700))
    );
}
