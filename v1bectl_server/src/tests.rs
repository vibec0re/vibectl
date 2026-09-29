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

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/virtual_devices");
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

fn outlet(is_on: bool) -> DeviceStateValue {
    DeviceStateValue::Outlet(OutletState {
        is_on,
        power_consumption: None,
        total_energy: None,
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
                ("light_kitchen", light(false, None, None)),
                ("outlet_tv", outlet(true)),
            ],
        ),
        (
            "lights_out",
            [
                ("light_living_room", light(false, None, None)),
                ("light_kitchen", light(false, None, None)),
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
            // The kitchen light is off with no level after `movie`, so
            // `lights_out` leaves it as it is, and has nothing to echo.
            let want = if scene == "lights_out" && *id == "light_kitchen" {
                Vec::new()
            } else {
                vec![target.clone()]
            };
            assert_eq!(echoed, want, "{scene}: {id}'s echoes");
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
        ("sleep", light(false, Some(0), None)),
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
