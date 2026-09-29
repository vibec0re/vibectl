//! #58: a write fans out through the virtual devices among its members. A
//! scene that sets a light group reaches the group's lights, through the
//! group's own plan, and so does a group that has a group among its
//! members. It used to write the inner group's state into the store and
//! stop there: no light moved, and the hub saw nothing. A light reached
//! through two paths of one write is sent one PATCH, by the most direct
//! path (#72 review), and a write that can't be made sends none.
//!
//! The rig is the sync engine's (`lib/v1bectl_sync/tests/common`): a fake
//! hub whose traffic is logged, and pulls on demand. The steps are ordered on
//! the hub's log, never on sleeps. Input tracking is fed the bus by hand
//! ([`track`]), so each test decides exactly when it catches up.

#[path = "../../v1bectl_sync/tests/common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{off, OnSet, Pulls, Rig, TestHub};
use tokio::sync::broadcast;
use v1bectl_sync::{DeviceEvent, DeviceStateValue, EventType, LightState, SceneState, SyncConfig};
use v1bectl_virtual::{
    LightGroup, LightGroupLinear, Scene, SceneController, TransitionType, VirtualDevice,
    VirtualDeviceConfig, VirtualDeviceManager, VirtualDeviceType,
};

/// A light on at `level`, in the colour a group starts with (2700 K), which
/// is the colour it hands its members when the write names none.
fn lit(level: u8) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on: true,
        brightness: Some(level),
        color_temp: Some(2700),
        rgb_color: None,
    })
}

fn config(
    device_id: &str,
    device_type: VirtualDeviceType,
    config: serde_json::Value,
) -> VirtualDeviceConfig {
    VirtualDeviceConfig {
        device_id: device_id.to_string(),
        device_type,
        name: device_id.to_string(),
        description: None,
        enabled: true,
        config,
    }
}

/// A `LightGroupLinear` named `device_id` over `ranges`, each a device and
/// its `[min, max]`.
fn linear_group(device_id: &str, ranges: &[(&str, (u8, u8))], rig: &Rig) -> Box<dyn VirtualDevice> {
    let members = ranges
        .iter()
        .map(|(id, _)| ((*id).to_string(), (*id).to_string()))
        .collect();
    let ranges = ranges
        .iter()
        .map(|(id, range)| ((*id).to_string(), *range))
        .collect();
    let config = config(
        device_id,
        VirtualDeviceType::LightGroupLinear,
        serde_json::json!({}),
    );
    Box::new(
        LightGroupLinear::new(config, members, ranges, Arc::clone(&rig.store))
            .expect("linear group"),
    )
}

/// A manager over the rig's store and bus, committing through its engine.
fn manager(rig: &Rig) -> VirtualDeviceManager {
    let manager = VirtualDeviceManager::new(Arc::clone(&rig.store), Arc::clone(&rig.bus));
    manager.attach_sync_engine(Arc::new(rig.engine.clone()));
    manager
}

async fn register(manager: &VirtualDeviceManager, device: Box<dyn VirtualDevice>) {
    let id = device.device_id().clone();
    manager
        .add_virtual_device(device)
        .await
        .unwrap_or_else(|e| panic!("register {id}: {e}"));
}

/// Every event published since the last call.
fn drain(rx: &mut broadcast::Receiver<DeviceEvent>) -> Vec<DeviceEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

/// Each state echoed for `device_id` in `events`, in order.
fn echoes(events: &[DeviceEvent], device_id: &str) -> Vec<DeviceStateValue> {
    events
        .iter()
        .filter(|event| event.device_id == device_id)
        .filter_map(|event| match &event.event_type {
            EventType::AttributeChanged {
                attribute,
                new_value,
                ..
            } if attribute == "state" => serde_json::from_value(new_value.clone()).ok(),
            _ => None,
        })
        .collect()
}

/// Input tracking, caught up: hands the manager `events`, then every event
/// that handling them publishes in turn. Returns those.
async fn track(
    manager: &VirtualDeviceManager,
    rx: &mut broadcast::Receiver<DeviceEvent>,
    events: &[DeviceEvent],
) -> Vec<DeviceEvent> {
    for event in events {
        manager.handle_event(event).await.expect("input tracking");
    }
    let mut published = Vec::new();
    loop {
        let more = drain(rx);
        if more.is_empty() {
            return published;
        }
        for event in &more {
            manager.handle_event(event).await.expect("input tracking");
        }
        published.extend(more);
    }
}

/// Resolves once the hub has answered `n` `PATCHes`, and checks that no
/// other one follows, and that each of `lights` was set once, as it says.
async fn patched(rig: &Rig, lights: &[(&str, DeviceStateValue)]) {
    rig.hub
        .wait("a PATCH per light", |log| log.sets_done >= lights.len())
        .await;
    assert!(
        !rig.hub
            .within(Duration::from_millis(200), |log| log.sets_started.len()
                > lights.len())
            .await,
        "another PATCH: {:?}",
        rig.hub.log()
    );
    let log = rig.hub.log();
    for (id, state) in lights {
        assert_eq!(log.sets_for(id), vec![state.clone()], "{id}'s PATCHes");
    }
}

const TOP: &str = "top";
const MAIN: &str = "main";
const BED: &str = "bed";
const GROUP: &str = "living";
const CONTROLLER: &str = "scenes";

/// Scene controller [`CONTROLLER`], made as the API makes one, with the
/// scene `movie`: [`GROUP`] at 30, instantly. It names only the level.
async fn movie_scenes(rig: &Rig) -> Box<dyn VirtualDevice> {
    let at_30 = DeviceStateValue::Light(LightState {
        is_on: true,
        brightness: Some(30),
        color_temp: None,
        rgb_color: None,
    });
    let scene = Scene {
        name: "movie".to_string(),
        device_states: HashMap::from([(GROUP.to_string(), at_30)]),
        transition_type: TransitionType::Instant,
    };
    let config = config(
        CONTROLLER,
        VirtualDeviceType::SceneController,
        serde_json::json!({ "scenes": { "movie": scene } }),
    );
    let (controller, warnings) = SceneController::create(config, Arc::clone(&rig.store))
        .await
        .expect("scene controller");
    assert_eq!(warnings, Vec::<String>::new());
    Box::new(controller)
}

/// #58: activating a scene that sets a `LightGroupLinear` reaches the
/// group's lights, through the group's plan: one PATCH per light, at the
/// level the group's ranges map the scene's 30 to (the shipped Bedroom
/// Lights' ranges: 80-100, 40-90 and 0-50 put them at 86, 55 and 15). The
/// group reports 30, in the store and its own state, and each device is
/// echoed once. The group's id never reaches the hub.
///
/// The group takes the level it was set to (`set_level`). So its lights'
/// echoes don't re-derive it, and neither do the hub's confirmations of
/// them. And once two of its lights are switched off at the wall, it reads
/// 30 from `top` alone: `top` at 86 lights at any level from 28 to 32, and
/// the group picks the one nearest the level it was set to. With the level
/// it started at (100) it would drift to 32.
///
/// The scene is created before the group, as when the API gets them in
/// that order.
#[tokio::test]
async fn a_scene_that_sets_a_linear_group_reaches_its_lights() {
    let rig = Rig::new(
        &[TOP, MAIN, BED],
        TestHub::new(OnSet::Apply, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let manager = manager(&rig);
    register(&manager, movie_scenes(&rig).await).await;
    let ranges = [(TOP, (80, 100)), (MAIN, (40, 90)), (BED, (0, 50))];
    register(&manager, linear_group(GROUP, &ranges, &rig)).await;
    let mut rx = rig.bus.subscribe();

    let movie = DeviceStateValue::Scene(SceneState {
        scene_name: "movie".to_string(),
        is_active: true,
    });
    manager
        .set_virtual_device_state(&CONTROLLER.to_string(), movie.clone())
        .await
        .expect("movie");
    let lights = [(TOP, lit(86)), (MAIN, lit(55)), (BED, lit(15))];
    for (id, state) in &lights {
        assert_eq!(&rig.stored(id).await, state, "{id}: committed");
    }
    patched(&rig, &lights).await;

    let events = drain(&mut rx);
    for (id, state) in &lights {
        assert_eq!(echoes(&events, id), vec![state.clone()], "{id}'s echo");
    }
    assert_eq!(echoes(&events, GROUP), vec![lit(30)], "the group's echo");
    assert_eq!(echoes(&events, CONTROLLER), vec![movie], "the scene's echo");
    assert_eq!(rig.stored(GROUP).await, lit(30), "the group");
    let own = manager.get_virtual_device_state(&GROUP.to_string()).await;
    assert_eq!(own.expect("the group"), lit(30), "the group's own state");

    // Tracking catches up with the echoes, then with the hub confirming
    // each light: none of it moves the group.
    let tracked = track(&manager, &mut rx, &events).await;
    for (id, _) in &lights {
        rig.pull(id).await;
    }
    let confirmed = drain(&mut rx);
    assert_eq!(
        echoes(&confirmed, TOP),
        vec![lit(86)],
        "the hub confirmed top"
    );
    let tracked = [tracked, track(&manager, &mut rx, &confirmed).await].concat();
    assert!(
        echoes(&tracked, GROUP).is_empty(),
        "re-derived: {tracked:?}"
    );

    // Two lights switched off at the wall.
    for id in [MAIN, BED] {
        rig.hub.report(id, off());
        rig.pull(id).await;
    }
    let moved = drain(&mut rx);
    assert_eq!(echoes(&moved, MAIN), vec![off()], "main went off");
    let tracked = track(&manager, &mut rx, &moved).await;
    assert_eq!(rig.stored(GROUP).await, lit(30), "the group drifted");
    assert!(
        echoes(&tracked, GROUP).is_empty(),
        "the group moved: {tracked:?}"
    );
    rig.shutdown().await;
}

/// #58: a group of groups: a curved `LightGroup` (`inner`) inside a
/// `LightGroupLinear` (`outer`). `outer` at 50 puts `inner` at 40 (its
/// range, 20-60) and its own light `c` at 50 (0-100). `inner` puts its
/// lights where its curves take 40: `a` at 30 (10-60) and `b` at 70
/// (50-100). One PATCH per light, at those levels, and none for either
/// group. Each device is echoed once, and tracking leaves both groups
/// where the write put them.
///
/// `outer` is registered first, before `inner` exists.
#[tokio::test]
async fn a_group_of_groups_reaches_the_lights_of_its_groups() {
    let rig = Rig::new(
        &["a", "b", "c"],
        TestHub::new(OnSet::Apply, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let manager = manager(&rig);
    register(
        &manager,
        linear_group("outer", &[("inner", (20, 60)), ("c", (0, 100))], &rig),
    )
    .await;
    let curves = serde_json::json!({
        "a": { "breakpoints": [[0, 10], [100, 60]] },
        "b": { "breakpoints": [[0, 50], [100, 100]] },
    });
    let inner = config(
        "inner",
        VirtualDeviceType::LightGroup,
        serde_json::json!({ "lights": ["a", "b"], "brightness_curves": curves }),
    );
    register(
        &manager,
        Box::new(LightGroup::new(inner, Arc::clone(&rig.store)).expect("inner")),
    )
    .await;
    let mut rx = rig.bus.subscribe();

    manager
        .set_virtual_device_state(&"outer".to_string(), lit(50))
        .await
        .expect("outer at 50");
    let lights = [("a", lit(30)), ("b", lit(70)), ("c", lit(50))];
    for (id, state) in &lights {
        assert_eq!(&rig.stored(id).await, state, "{id}: committed");
    }
    patched(&rig, &lights).await;

    let events = drain(&mut rx);
    let groups = [("inner", lit(40)), ("outer", lit(50))];
    for (id, state) in lights.iter().chain(&groups) {
        assert_eq!(echoes(&events, id), vec![state.clone()], "{id}'s echo");
    }
    let tracked = track(&manager, &mut rx, &events).await;
    for (id, state) in &groups {
        assert_eq!(&rig.stored(id).await, state, "{id}");
        let own = manager.get_virtual_device_state(&(*id).to_string()).await;
        assert_eq!(&own.expect("group"), state, "{id}'s own state");
        assert!(
            echoes(&tracked, id).is_empty(),
            "{id} re-derived: {tracked:?}"
        );
    }
    rig.shutdown().await;
}

// ---------------------------------------------------------------------
// #72 review: one plan per write, each device once
// ---------------------------------------------------------------------

/// A 1:1 `LightGroup` named `device_id` over `lights`.
fn group(device_id: &str, lights: &[&str], rig: &Rig) -> Box<dyn VirtualDevice> {
    let curves: serde_json::Map<String, serde_json::Value> = lights
        .iter()
        .map(|id| {
            let curve = serde_json::json!({ "breakpoints": [[0, 0], [100, 100]] });
            ((*id).to_string(), curve)
        })
        .collect();
    let config = config(
        device_id,
        VirtualDeviceType::LightGroup,
        serde_json::json!({ "lights": lights, "brightness_curves": curves }),
    );
    Box::new(LightGroup::new(config, Arc::clone(&rig.store)).expect("group"))
}

/// Scene controller `device_id`, made as the API makes one, with the
/// scene `movie`: each of `targets` at its state, instantly.
async fn scenes(
    device_id: &str,
    targets: &[(&str, DeviceStateValue)],
    rig: &Rig,
) -> Box<dyn VirtualDevice> {
    let scene = Scene {
        name: "movie".to_string(),
        device_states: targets
            .iter()
            .map(|(id, state)| ((*id).to_string(), state.clone()))
            .collect(),
        transition_type: TransitionType::Instant,
    };
    let config = config(
        device_id,
        VirtualDeviceType::SceneController,
        serde_json::json!({ "scenes": { "movie": scene } }),
    );
    let (controller, _) = SceneController::create(config, Arc::clone(&rig.store))
        .await
        .expect("scene controller");
    Box::new(controller)
}

fn movie() -> DeviceStateValue {
    DeviceStateValue::Scene(SceneState {
        scene_name: "movie".to_string(),
        is_active: true,
    })
}

/// #72 review, finding 1, at the hub: a scene that sets group `g` to 30
/// and `g`'s lamp to 5 sends the lamp one PATCH, at 5, and `g`'s other
/// light one, at 30, on every fresh rig (fresh `HashMap`s for the scene
/// each time). The lamp used to get a PATCH through each path, and the hub
/// ended at 30 in about half the runs. `g` reports its lights as they are:
/// (5 + 30) / 2 = 17.
#[tokio::test]
async fn a_scene_over_a_group_and_one_of_its_lamps_patches_the_lamp_once() {
    for run in 0..10 {
        let rig = Rig::new(
            &["lamp", "other"],
            TestHub::new(OnSet::Apply, false),
            Pulls::OnDemand,
            SyncConfig::default(),
        )
        .await;
        let manager = manager(&rig);
        register(&manager, group("g", &["lamp", "other"], &rig)).await;
        let targets = [("g", lit(30)), ("lamp", lit(5))];
        register(&manager, scenes(CONTROLLER, &targets, &rig).await).await;

        manager
            .set_virtual_device_state(&CONTROLLER.to_string(), movie())
            .await
            .unwrap_or_else(|e| panic!("run {run}: movie: {e}"));
        patched(&rig, &[("lamp", lit(5)), ("other", lit(30))]).await;
        assert_eq!(rig.stored("g").await, lit(17), "run {run}: g");
        rig.shutdown().await;
    }
}

/// A write that can't be made (a group nested too deep, a scene that sets
/// another scene controller as a light) fails before anything is
/// committed: nothing is stored, echoed or queued, and no PATCH reaches
/// the hub, not even for the physical light the same write sets.
#[tokio::test]
async fn a_write_that_fails_its_expansion_sends_the_hub_nothing() {
    let rig = Rig::new(
        &["a", "x"],
        TestHub::new(OnSet::Apply, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let manager = manager(&rig);
    // g0 > g1 > ... > g5 > x: a scene that sets g0 reaches g5 six levels
    // down.
    let chain = ["g0", "g1", "g2", "g3", "g4", "g5"];
    for (level, id) in chain.iter().enumerate().rev() {
        let member = chain.get(level + 1).copied().unwrap_or("x");
        register(&manager, group(id, &[member], &rig)).await;
    }
    let deep = [("a", lit(50)), ("g0", lit(50))];
    register(&manager, scenes("deep", &deep, &rig).await).await;
    register(&manager, scenes("other", &[("a", lit(10))], &rig).await).await;
    let bad = [("a", lit(50)), ("other", lit(50))];
    register(&manager, scenes("bad", &bad, &rig).await).await;
    let mut rx = rig.bus.subscribe();

    for controller in ["deep", "bad"] {
        let result = manager
            .set_virtual_device_state(&controller.to_string(), movie())
            .await;
        assert!(result.is_err(), "{controller}: {result:?}");
    }
    assert!(
        !rig.hub
            .within(Duration::from_millis(300), |log| !log
                .sets_started
                .is_empty())
            .await,
        "a PATCH went out: {:?}",
        rig.hub.log()
    );
    let stats = rig.engine.get_sync_stats().await;
    assert_eq!(
        (stats.pending_sync_devices, stats.pending_sync_tasks),
        (0, 0),
        "{stats:?}"
    );
    let events = drain(&mut rx);
    assert!(events.is_empty(), "{events:?}");
    for id in ["a", "x"] {
        assert_eq!(rig.stored(id).await, off(), "{id}");
    }
    rig.shutdown().await;
}
