//! #64 review of #68: a fade or a sequence that names only some fields
//! leaves the others alone, even when they change while it runs.
//!
//! A scene merges what its target leaves out with the device as the store
//! has it. A fade or a sequence waits before the manager commits it, and
//! two writers don't queue behind it: a direct write through the API, and a
//! change on the hub (the IKEA app, a remote) that a pull brings in. Merged
//! only where the transition started (or where a sequence reached the
//! device), the commit put the field back to its old value, and pushed that
//! to the hub. Now each device is merged again where the transition ends.
//!
//! The rig is the sync engine's (`lib/v1bectl_sync/tests/common`): a fake
//! hub whose traffic is logged, and pulls on demand. The transition runs in
//! real time, and the change lands halfway through it.

#[path = "../../v1bectl_sync/tests/common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{OnSet, Pulls, Rig, TestHub};
use v1bectl_sync::{DeviceStateValue, LightState, SceneState, SyncConfig};
use v1bectl_virtual::{
    Scene, SceneController, TransitionType, VirtualDeviceConfig, VirtualDeviceManager,
    VirtualDeviceType,
};

/// Two lamps, so a sequence's first device is one of them whichever order
/// it takes them in.
const LAMPS: [&str; 2] = ["lamp_a", "lamp_b"];
const CONTROLLER: &str = "scenes";
const SCENE: &str = "brighter";

/// How long the transition takes. The colour changes halfway.
const TRANSITION: Duration = Duration::from_secs(2);

fn lamp(is_on: bool, brightness: Option<u8>, color_temp: Option<u16>) -> DeviceStateValue {
    DeviceStateValue::Light(LightState {
        is_on,
        brightness,
        color_temp,
        rgb_color: None,
    })
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    /// A fade over [`TRANSITION`].
    Fade,
    /// A sequence: its first lamp at once, its second after [`TRANSITION`].
    Sequence,
}

#[derive(Clone, Copy, Debug)]
enum Change {
    /// A direct write through the engine, the path the API's
    /// `SetLightState` takes. It doesn't queue behind the manager's
    /// transition.
    DirectWrite,
    /// A change on the hub, which a pull brings into the store.
    OnTheHub,
}

/// A manager over `rig`, committing through its engine, with scene
/// controller [`CONTROLLER`]: one scene, [`SCENE`], that names only the
/// lamps' brightness (80), with `transition`.
async fn scene_controller(rig: &Rig, transition: Transition) -> Arc<VirtualDeviceManager> {
    let transition_ms = u64::try_from(TRANSITION.as_millis()).expect("ms");
    let scene = Scene {
        name: SCENE.to_string(),
        device_states: LAMPS
            .iter()
            .map(|id| ((*id).to_string(), lamp(true, Some(80), None)))
            .collect::<HashMap<_, _>>(),
        transition_type: match transition {
            Transition::Fade => TransitionType::Fade {
                duration_ms: transition_ms,
            },
            Transition::Sequence => TransitionType::Sequence {
                delays_ms: vec![0, transition_ms],
            },
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
    let (controller, warnings) = SceneController::create(config, Arc::clone(&rig.store))
        .await
        .expect("create");
    assert_eq!(warnings, Vec::<String>::new());
    let manager = Arc::new(VirtualDeviceManager::new(
        Arc::clone(&rig.store),
        Arc::clone(&rig.bus),
    ));
    manager.attach_sync_engine(Arc::new(rig.engine.clone()));
    manager
        .add_virtual_device(Box::new(controller))
        .await
        .expect("register");
    manager
}

/// Both lamps on at 50 and 2700 K, and a scene that names only their
/// brightness (80), with `transition`. Halfway through it, the colour of
/// both changes to 4000 K, by `change`. The scene's PATCH for each lamp
/// carries 4000 K, not the 2700 K it started at, and each lamp ends at 80
/// and 4000 K.
async fn a_transition_keeps_a_colour_changed_during_it(transition: Transition, change: Change) {
    let case = format!("{transition:?}, {change:?}");
    let rig = Rig::new(
        &LAMPS,
        TestHub::new(OnSet::Apply, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    for id in LAMPS {
        rig.hub.report(id, lamp(true, Some(50), Some(2700)));
        rig.pull(id).await;
        assert_eq!(
            rig.stored(id).await,
            lamp(true, Some(50), Some(2700)),
            "{case}: {id} before"
        );
    }
    let manager = scene_controller(&rig, transition).await;

    let activation = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            manager
                .set_virtual_device_state(
                    &CONTROLLER.to_string(),
                    DeviceStateValue::Scene(SceneState {
                        scene_name: SCENE.to_string(),
                        is_active: true,
                    }),
                )
                .await
        }
    });

    // Halfway through, the colour changes to 4000 K.
    tokio::time::sleep(TRANSITION / 2).await;
    let recoloured = lamp(true, Some(50), Some(4000));
    match change {
        Change::DirectWrite => {
            for id in LAMPS {
                rig.write(id, recoloured.clone()).await;
            }
            rig.hub
                .wait(&format!("{case}: the direct writes' PATCHes"), |log| {
                    log.sets_done == LAMPS.len()
                })
                .await;
        }
        Change::OnTheHub => {
            for id in LAMPS {
                rig.hub.report(id, recoloured.clone());
                rig.pull(id).await;
            }
        }
    }
    for id in LAMPS {
        assert_eq!(
            rig.stored(id).await,
            recoloured,
            "{case}: {id} took the colour change"
        );
    }
    assert!(
        !activation.is_finished(),
        "{case}: the transition committed before the colour changed, the test proves nothing"
    );
    let sets_before = rig.hub.log().sets_started.len();

    activation
        .await
        .expect("activation task")
        .unwrap_or_else(|e| panic!("{case}: activating: {e}"));
    rig.hub
        .wait(&format!("{case}: the scene's PATCHes"), |log| {
            log.sets_started.len() == sets_before + LAMPS.len()
        })
        .await;

    // The scene names no colour: it leaves the 4000 K alone.
    let ends = lamp(true, Some(80), Some(4000));
    let log = rig.hub.log();
    for id in LAMPS {
        let pushed: Vec<_> = log.sets_started[sets_before..]
            .iter()
            .filter(|(device, _)| device == id)
            .map(|(_, state)| state.clone())
            .collect();
        assert_eq!(
            pushed,
            vec![ends.clone()],
            "{case}: the scene's PATCH of {id}"
        );
        assert_eq!(rig.stored(id).await, ends, "{case}: {id} after");
    }
    rig.shutdown().await;
}

#[tokio::test]
async fn a_fade_keeps_a_colour_changed_by_a_direct_write_during_it() {
    a_transition_keeps_a_colour_changed_during_it(Transition::Fade, Change::DirectWrite).await;
}

#[tokio::test]
async fn a_fade_keeps_a_colour_changed_on_the_hub_during_it() {
    a_transition_keeps_a_colour_changed_during_it(Transition::Fade, Change::OnTheHub).await;
}

#[tokio::test]
async fn a_sequence_keeps_a_colour_changed_by_a_direct_write_during_it() {
    a_transition_keeps_a_colour_changed_during_it(Transition::Sequence, Change::DirectWrite).await;
}

#[tokio::test]
async fn a_sequence_keeps_a_colour_changed_on_the_hub_during_it() {
    a_transition_keeps_a_colour_changed_during_it(Transition::Sequence, Change::OnTheHub).await;
}
