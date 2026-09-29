//! #64: a scene target that names only some fields changes only those.
//!
//! A target that says only `is_on = true` used to be written as the light's
//! whole state, with `brightness: None`. The store showed no brightness for
//! the sync engine's whole protection window (5 s) and up to one pull
//! (2 s) after it, and the hub's own value came back as a second update.
//! Now the scene merges what its target leaves out from the store when it's
//! activated, so the store never shows it unset.
//!
//! The rig is the sync engine's (`lib/v1bectl_sync/tests/common`): a fake
//! hub whose `PATCHes` wait at a gate and whose traffic is logged, and pulls
//! on demand. The steps are ordered on the hub's log, never on sleeps.

#[path = "../../v1bectl_sync/tests/common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{drain_events, json, light, OnSet, Pulls, Rig, TestHub};
use v1bectl_sync::{DeviceStateValue, LightState, SceneState, SyncConfig};
use v1bectl_virtual::{
    Scene, SceneController, TransitionType, VirtualDeviceConfig, VirtualDeviceManager,
    VirtualDeviceTomlConfig, VirtualDeviceType,
};

const LAMP: &str = "lamp";
const CONTROLLER: &str = "scenes";
const SCENE: &str = "lamp_on";

/// Where the scene is made.
#[derive(Clone, Copy, Debug)]
enum Made {
    /// A `virtual_devices/*.toml`, as the server loads one.
    Toml,
    /// The runtime config, as the API's `CreateVirtualDevice` sends it.
    Api,
}

/// Scene controller [`CONTROLLER`] with one scene, [`SCENE`], that says only
/// that [`LAMP`] is on: instant for a `transition_ms` of 0, else a fade.
async fn controller(made: Made, transition_ms: u32, rig: &Rig) -> SceneController {
    match made {
        Made::Toml => {
            let toml = format!(
                "type = \"scene_controller\"\ndevice_id = \"{CONTROLLER}\"\nname = \"Scenes\"\n\
                 [[scenes]]\nname = \"{SCENE}\"\ndisplay_name = \"Lamp on\"\n\
                 [[scenes.devices]]\ndevice_id = \"{LAMP}\"\nstate = {{ type = \"light\", is_on = true }}\n\
                 [settings]\ntransition_duration = {transition_ms}\n"
            );
            let VirtualDeviceTomlConfig::SceneController(config) =
                toml::from_str(&toml).expect("TOML")
            else {
                panic!("not a scene controller: {toml}");
            };
            let (controller, warnings) =
                SceneController::from_toml(&config, Arc::clone(&rig.store))
                    .await
                    .expect("from_toml");
            assert_eq!(warnings, Vec::<String>::new());
            controller
        }
        Made::Api => {
            let only_on = DeviceStateValue::Light(LightState {
                is_on: true,
                brightness: None,
                color_temp: None,
                rgb_color: None,
            });
            let scene = Scene {
                name: SCENE.to_string(),
                device_states: HashMap::from([(LAMP.to_string(), only_on)]),
                transition_type: if transition_ms == 0 {
                    TransitionType::Instant
                } else {
                    TransitionType::Fade {
                        duration_ms: u64::from(transition_ms),
                    }
                },
            };
            let config = VirtualDeviceConfig {
                device_id: CONTROLLER.to_string(),
                device_type: VirtualDeviceType::SceneController,
                name: "Scenes".to_string(),
                description: None,
                enabled: true,
                config: serde_json::json!({ "scenes": { SCENE: scene } }),
            };
            SceneController::new(config, Arc::clone(&rig.store)).expect("scene controller")
        }
    }
}

fn active() -> DeviceStateValue {
    DeviceStateValue::Scene(SceneState {
        scene_name: SCENE.to_string(),
        is_active: true,
    })
}

/// A lamp that is off at 70, on the hub and in the store, and a scene that
/// says only that it's on. The scene's one write is committed through the
/// engine, and its PATCH held at the hub's gate while the lamp is pulled
/// (the hub still says off), then let through, and the lamp pulled again.
///
/// The store shows the lamp at 70 throughout, on from the commit, and one
/// PATCH goes out, at 70. The old whole-state write showed it with no
/// brightness from the commit on, and pushed that.
async fn a_scene_that_names_only_is_on_keeps_the_brightness(made: Made, transition_ms: u32) {
    let case = format!("{made:?}, transition {transition_ms} ms");
    let rig = Rig::new(
        &[LAMP],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    rig.hub.report(LAMP, light(false, 70));
    rig.pull(LAMP).await;
    assert_eq!(rig.stored(LAMP).await, light(false, 70), "{case}: before");

    let manager = VirtualDeviceManager::new(Arc::clone(&rig.store), Arc::clone(&rig.bus));
    manager.attach_sync_engine(Arc::new(rig.engine.clone()));
    manager
        .add_virtual_device(Box::new(controller(made, transition_ms, &rig).await))
        .await
        .expect("register");
    let mut rx = rig.bus.subscribe();

    manager
        .set_virtual_device_state(&CONTROLLER.to_string(), active())
        .await
        .unwrap_or_else(|e| panic!("{case}: activating: {e}"));
    assert_eq!(rig.stored(LAMP).await, light(true, 70), "{case}: committed");

    rig.hub
        .wait(&format!("{case}: the scene's PATCH at the gate"), |log| {
            !log.sets_for(LAMP).is_empty()
        })
        .await;
    rig.pull(LAMP).await;
    assert_eq!(
        rig.stored(LAMP).await,
        light(true, 70),
        "{case}: pulled while its PATCH is held"
    );

    rig.hub.release(1);
    rig.hub
        .wait(&format!("{case}: the PATCH answered"), |log| {
            log.sets_done == 1
        })
        .await;
    rig.pull(LAMP).await;
    assert_eq!(rig.stored(LAMP).await, light(true, 70), "{case}: after");

    // One PATCH, at 70, and nothing after it.
    assert!(
        !rig.hub
            .within(Duration::from_millis(200), |log| log.sets_started.len() > 1)
            .await,
        "{case}: a second PATCH: {:?}",
        rig.hub.log()
    );
    assert_eq!(
        rig.hub.log().sets_for(LAMP),
        vec![light(true, 70)],
        "{case}: the PATCHes"
    );

    // Every state the store showed for the lamp was on at 70: never
    // without a brightness.
    let shown: Vec<_> = drain_events(&mut rx)
        .into_iter()
        .filter(|(id, _, _)| id == LAMP)
        .map(|(_, _, new_value)| new_value)
        .collect();
    assert!(!shown.is_empty(), "{case}: the lamp was never echoed");
    for state in &shown {
        assert_eq!(state, &json(&light(true, 70)), "{case}: shown {shown:?}");
    }
    rig.shutdown().await;
}

#[tokio::test]
async fn an_instant_scene_that_names_only_is_on_keeps_the_brightness() {
    for made in [Made::Toml, Made::Api] {
        a_scene_that_names_only_is_on_keeps_the_brightness(made, 0).await;
    }
}

#[tokio::test]
async fn a_fading_scene_that_names_only_is_on_keeps_the_brightness() {
    for made in [Made::Toml, Made::Api] {
        a_scene_that_names_only_is_on_keeps_the_brightness(made, 200).await;
    }
}
