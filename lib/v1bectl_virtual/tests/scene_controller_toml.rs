//! #10: a scene controller's TOML config (`type = "scene_controller"`),
//! mapped onto the runtime's by `SceneController::from_toml`: every field,
//! each target in the shape the store holds its device in, and what fails
//! or is only warned about.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;
use v1bectl_sync::{
    Capability, DeviceId, DeviceInfo, DeviceStateValue, DeviceType, EventBus, LightState,
    OutletState, SceneState, SensorState, StateStore, SwitchState,
};
use v1bectl_virtual::{
    Scene, SceneController, SceneControllerConfig, TransitionType, VirtualDevice,
    VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceManager, VirtualDeviceTomlConfig,
    VirtualDeviceType,
};

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

fn active(scene: &str) -> DeviceStateValue {
    DeviceStateValue::Scene(SceneState {
        scene_name: scene.to_string(),
        is_active: true,
    })
}

/// A store with a light (`lamp`), an outlet as the dummy holds one (`tv`),
/// an outlet as a Dirigera hub reads one (`hub_outlet`: a light that is
/// only on or off), a switch (`remote`) and a sensor (`thermo`).
async fn store() -> Arc<StateStore> {
    let store = StateStore::new();
    let devices = [
        ("lamp", DeviceType::Light, light(false, Some(0), Some(2700))),
        (
            "tv",
            DeviceType::Outlet,
            DeviceStateValue::Outlet(OutletState {
                is_on: true,
                power_consumption: Some(45.5),
                total_energy: Some(123.4),
            }),
        ),
        ("hub_outlet", DeviceType::Light, light(false, None, None)),
        (
            "remote",
            DeviceType::Switch,
            DeviceStateValue::Switch(SwitchState {
                is_pressed: false,
                last_pressed: None,
                battery_level: None,
            }),
        ),
        (
            "thermo",
            DeviceType::Sensor,
            DeviceStateValue::Sensor(SensorState {
                temperature: Some(21.0),
                humidity: None,
                last_updated: 0,
            }),
        ),
    ];
    for (id, device_type, state) in devices {
        let info = DeviceInfo {
            device_id: id.to_string(),
            name: id.to_string(),
            device_type,
            capabilities: vec![Capability::OnOff],
            device_groups: vec![],
            manufacturer: None,
            model: None,
            firmware_version: None,
            battery_powered: false,
            reachable: true,
            last_seen: 0,
            custom_attributes: HashMap::new(),
        };
        store.add_device(info, state).await;
    }
    store
}

/// Scene controller `scenes` with `body` (its scenes and settings), parsed
/// the way the loader parses a `virtual_devices/*.toml`.
fn parse(body: &str) -> SceneControllerConfig {
    let toml =
        format!("type = \"scene_controller\"\ndevice_id = \"scenes\"\nname = \"Scenes\"\n{body}");
    match toml::from_str(&toml) {
        Ok(VirtualDeviceTomlConfig::SceneController(c)) => c,
        other => panic!("not a scene controller: {other:?}\n{toml}"),
    }
}

/// A scene `name` that sets each of `devices`, `(device_id, state)` with
/// `state` an inline TOML table.
fn scene(name: &str, devices: &[(&str, &str)]) -> String {
    let mut toml = format!("[[scenes]]\nname = \"{name}\"\ndisplay_name = \"{name}!\"\n");
    if devices.is_empty() {
        toml.push_str("devices = []\n");
    }
    for (device_id, state) in devices {
        writeln!(
            toml,
            "[[scenes.devices]]\ndevice_id = \"{device_id}\"\nstate = {state}"
        )
        .expect("writing to a String");
    }
    toml
}

async fn from_toml(body: &str) -> Result<(SceneController, Vec<String>), VirtualDeviceError> {
    SceneController::from_toml(&parse(body), store().await).await
}

/// Scene `name` of `controller`, as its runtime config holds it.
fn runtime_scene(controller: &SceneController, name: &str) -> Scene {
    serde_json::from_value(controller.config().config["scenes"][name].clone())
        .unwrap_or_else(|e| panic!("scene {name}: {e}"))
}

fn sorted(mut ids: Vec<DeviceId>) -> Vec<DeviceId> {
    ids.sort();
    ids
}

/// Every field of the TOML lands in the runtime config, and a manager runs
/// it: a scene fades over `transition_duration` and ends at its targets.
#[tokio::test(start_paused = true)]
async fn every_field_maps_onto_the_runtime_config() {
    let body = [
        scene(
            "evening",
            &[
                (
                    "lamp",
                    "{ type = \"light\", is_on = true, brightness = 30, color_temp = 2200 }",
                ),
                ("tv", "{ type = \"outlet\", is_on = false }"),
            ],
        ),
        scene("night", &[("lamp", "{ type = \"light\", is_on = false }")]),
        "[settings]\ntransition_duration = 1500\n".to_string(),
    ]
    .concat();
    let (controller, warnings) = from_toml(&body).await.expect("from_toml");
    assert_eq!(warnings, Vec::<String>::new());

    assert_eq!(controller.device_id(), "scenes");
    assert!(matches!(
        controller.device_type(),
        VirtualDeviceType::SceneController
    ));
    let config = controller.config();
    assert_eq!(config.name, "Scenes");
    assert!(config.enabled);
    assert_eq!(
        config.description.as_deref(),
        Some("Scene controller with 2 scenes")
    );
    assert_eq!(config.config["default_transition_ms"], 1500);
    assert!(config.config.get("default_scene").is_none());

    for (name, targets) in [
        (
            "evening",
            vec![
                ("lamp", light(true, Some(30), Some(2200))),
                ("tv", outlet(false)),
            ],
        ),
        ("night", vec![("lamp", light(false, None, None))]),
    ] {
        let scene = runtime_scene(&controller, name);
        assert_eq!(scene.name, name);
        let want: HashMap<DeviceId, DeviceStateValue> = targets
            .into_iter()
            .map(|(id, state)| (id.to_string(), state))
            .collect();
        assert_eq!(scene.device_states, want, "{name}");
        assert!(
            matches!(
                scene.transition_type,
                TransitionType::Fade { duration_ms: 1500 }
            ),
            "{name}: {:?}",
            scene.transition_type
        );
        assert_eq!(
            config.config["scenes"][name]["display_name"],
            format!("{name}!"),
            "{name}: its display_name is kept"
        );
    }
    assert_eq!(
        sorted(controller.writes_to(&active("evening"))),
        vec!["lamp".to_string(), "tv".to_string()]
    );
    assert_eq!(
        sorted(controller.output_devices()),
        vec!["lamp".to_string(), "tv".to_string()]
    );

    // And the manager runs it as mapped.
    let store = store().await;
    let manager = VirtualDeviceManager::new(store.clone(), Arc::new(EventBus::new(100)));
    let (controller, _) = SceneController::from_toml(&parse(&body), store.clone())
        .await
        .expect("from_toml");
    manager
        .add_virtual_device(Box::new(controller))
        .await
        .expect("register");
    let started = tokio::time::Instant::now();
    manager
        .set_virtual_device_state(&"scenes".to_string(), active("evening"))
        .await
        .expect("activate");
    assert_eq!(started.elapsed(), Duration::from_millis(1500), "fade time");
    // The outlet keeps the readings the scene doesn't name (#64).
    let tv_off = DeviceStateValue::Outlet(OutletState {
        is_on: false,
        power_consumption: Some(45.5),
        total_energy: Some(123.4),
    });
    for (id, want) in [("lamp", light(true, Some(30), Some(2200))), ("tv", tv_off)] {
        let state = store.get_device(&id.to_string()).await.expect(id).state;
        assert_eq!(state, want, "{id}");
    }
}

/// A target is written in the shape the store holds its device in, so the
/// device's gateway can push it, and the store keeps the kind of state it
/// had. A device the store doesn't have is taken at its word.
#[tokio::test]
async fn each_target_takes_the_shape_the_store_holds_its_device_in() {
    let on = "{ type = \"outlet\", is_on = true }";
    let body = scene(
        "s",
        &[
            ("tv", on),
            ("hub_outlet", on),
            ("ghost_outlet", on),
            (
                "ghost_light",
                "{ type = \"light\", is_on = true, brightness = 50 }",
            ),
        ],
    );
    let (controller, warnings) = from_toml(&body).await.expect("from_toml");
    assert_eq!(warnings, Vec::<String>::new());
    let want: HashMap<DeviceId, DeviceStateValue> = [
        ("tv", outlet(true)),
        ("hub_outlet", light(true, None, None)),
        ("ghost_outlet", outlet(true)),
        ("ghost_light", light(true, Some(50), None)),
    ]
    .into_iter()
    .map(|(id, state)| (id.to_string(), state))
    .collect();
    assert_eq!(runtime_scene(&controller, "s").device_states, want);

    // A light the store holds as an outlet: only whether it's on is set,
    // and the rest is warned about.
    let body = scene(
        "s",
        &[(
            "tv",
            "{ type = \"light\", is_on = true, brightness = 50, color_temp = 3000 }",
        )],
    );
    let (controller, warnings) = from_toml(&body).await.expect("from_toml");
    assert_eq!(
        runtime_scene(&controller, "s").device_states["tv"],
        outlet(true)
    );
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("tv is an outlet"), "{warnings:?}");
}

/// `transition_duration` is instant at 0, and a fade from one step up. One
/// shorter than a step, or between steps, is warned about.
#[tokio::test]
async fn transition_duration_is_instant_or_a_fade() {
    for (duration_ms, want, warned) in [
        (0, TransitionType::Instant, None),
        (
            99,
            TransitionType::Instant,
            Some("shorter than one fade step"),
        ),
        (100, TransitionType::Fade { duration_ms: 100 }, None),
        (1000, TransitionType::Fade { duration_ms: 1000 }, None),
        (
            1050,
            TransitionType::Fade { duration_ms: 1050 },
            Some("whole 100 ms steps: 1000 ms"),
        ),
    ] {
        let body = format!(
            "{}[settings]\ntransition_duration = {duration_ms}\n",
            scene("s", &[("lamp", "{ type = \"light\", is_on = true }")])
        );
        let (controller, warnings) = from_toml(&body).await.expect("from_toml");
        let got = runtime_scene(&controller, "s").transition_type;
        assert_eq!(format!("{got:?}"), format!("{want:?}"), "{duration_ms} ms");
        assert_eq!(
            controller.config().config["default_transition_ms"],
            duration_ms,
            "{duration_ms} ms"
        );
        match warned {
            None => assert_eq!(warnings, Vec::<String>::new(), "{duration_ms} ms"),
            Some(warning) => assert!(
                warnings.len() == 1 && warnings[0].contains(warning),
                "{duration_ms} ms: {warnings:?}"
            ),
        }
    }

    // No settings: the TOML's default, a 1 s fade.
    let body = scene("s", &[("lamp", "{ type = \"light\", is_on = true }")]);
    let (controller, _) = from_toml(&body).await.expect("from_toml");
    assert!(matches!(
        runtime_scene(&controller, "s").transition_type,
        TransitionType::Fade { duration_ms: 1000 }
    ));
}

/// A config that is ambiguous, or that can't work, isn't created.
#[tokio::test]
async fn an_ambiguous_or_unworkable_config_fails() {
    let on = "{ type = \"light\", is_on = true }";
    for (body, error) in [
        (
            [scene("evening", &[("lamp", on)]), scene("evening", &[])].concat(),
            "two scenes are named evening",
        ),
        (scene("none", &[("lamp", on)]), "can't be named \"none\""),
        (scene("", &[("lamp", on)]), "can't be named \"\""),
        (
            scene("s", &[("lamp", on), ("lamp", on)]),
            "scene s sets lamp twice",
        ),
        (
            scene(
                "s",
                &[(
                    "lamp",
                    "{ type = \"light\", is_on = true, brightness = 101 }",
                )],
            ),
            "brightness 101, over 100",
        ),
        (scene("s", &[("remote", on)]), "remote, a switch"),
        (scene("s", &[("thermo", on)]), "thermo, a sensor"),
    ] {
        match from_toml(&body).await {
            Err(VirtualDeviceError::Config(message)) => {
                assert!(message.contains(error), "{message:?}, not {error:?}");
            }
            Err(e) => panic!("{error}: failed with {e}"),
            Ok(_) => panic!("{error}: created\n{body}"),
        }
    }
}

/// What the runtime can't honour is created, and warned about, not
/// dropped: a controller with no scenes, a scene with no devices, and a
/// `default_scene`, which nothing activates (it's kept in the config).
#[tokio::test]
async fn what_it_cant_honour_is_warned_about() {
    let (_, warnings) = from_toml("scenes = []\n").await.expect("from_toml");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("no scenes"), "{warnings:?}");

    let (_, warnings) = from_toml(&scene("idle", &[])).await.expect("from_toml");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("scene idle sets no devices"),
        "{warnings:?}"
    );

    let lamp = scene("evening", &[("lamp", "{ type = \"light\", is_on = true }")]);
    for (default_scene, unknown) in [("evening", false), ("nope", true)] {
        let body = format!("{lamp}[settings]\ndefault_scene = \"{default_scene}\"\n");
        let (controller, warnings) = from_toml(&body).await.expect("from_toml");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains(&format!("default_scene {default_scene} is not activated")),
            "{warnings:?}"
        );
        assert_eq!(
            warnings[0].contains("isn't one of its scenes"),
            unknown,
            "{warnings:?}"
        );
        assert_eq!(
            controller.config().config["default_scene"],
            default_scene,
            "kept in the config"
        );
    }
}

/// A device the store doesn't have is no error, as for a button
/// controller's target: the manager flags it once everything is loaded,
/// and setting a scene with it fails there.
#[tokio::test]
async fn an_unknown_device_is_flagged_like_the_other_kinds_do() {
    let on = "{ type = \"light\", is_on = true }";
    let body = scene("evening", &[("lamp", on), ("ghost", on)]);
    let store = store().await;
    let (controller, warnings) = SceneController::from_toml(&parse(&body), store.clone())
        .await
        .expect("from_toml");
    assert_eq!(warnings, Vec::<String>::new());

    let manager = VirtualDeviceManager::new(store, Arc::new(EventBus::new(100)));
    manager
        .add_virtual_device(Box::new(controller))
        .await
        .expect("register");
    assert_eq!(
        manager.dangling_references().await,
        vec![("scenes".to_string(), "ghost".to_string())]
    );
    let error = manager
        .set_virtual_device_state(&"scenes".to_string(), active("evening"))
        .await
        .expect_err("a scene with a device the store doesn't have");
    assert!(error.to_string().contains("ghost"), "{error}");
}

// ---------------------------------------------------------------------
// #64: the API's scene controllers are checked by the same checks
// ---------------------------------------------------------------------

/// A scene of a runtime config, as the API's `CreateVirtualDevice` sends
/// it: `name`, setting each of `targets`, instantly.
fn api_scene(name: &str, targets: &[(&str, DeviceStateValue)]) -> serde_json::Value {
    let device_states: HashMap<DeviceId, DeviceStateValue> = targets
        .iter()
        .map(|(id, state)| ((*id).to_string(), state.clone()))
        .collect();
    serde_json::to_value(Scene {
        name: name.to_string(),
        device_states,
        transition_type: TransitionType::Instant,
    })
    .expect("a scene")
}

/// Scene controller `scenes` from a runtime config with `scenes`, each
/// `(the name it's listed under, the scene)`, created the way the API's
/// `CreateVirtualDevice` creates one.
async fn create(
    scenes: &[(&str, serde_json::Value)],
) -> Result<(SceneController, Vec<String>), VirtualDeviceError> {
    let scenes: serde_json::Map<String, serde_json::Value> = scenes
        .iter()
        .map(|(listed_as, scene)| ((*listed_as).to_string(), scene.clone()))
        .collect();
    let config = VirtualDeviceConfig {
        device_id: "scenes".to_string(),
        device_type: VirtualDeviceType::SceneController,
        name: "Scenes".to_string(),
        description: None,
        enabled: true,
        config: serde_json::json!({ "scenes": scenes }),
    };
    SceneController::create(config, store().await).await
}

/// The message a config that must fail failed with.
fn rejected(
    made: Result<(SceneController, Vec<String>), VirtualDeviceError>,
    case: &str,
) -> String {
    match made {
        Err(VirtualDeviceError::Config(message)) => message,
        Err(e) => panic!("{case}: failed with {e}"),
        Ok(_) => panic!("{case}: created"),
    }
}

/// Each config the TOML path rejects, the API path rejects too, with the
/// same message. A device twice in one scene isn't among them: the API
/// can't express it, as a scene's devices are a map (the API decodes a
/// request's config into one, which keeps only the last of each).
#[tokio::test]
async fn the_api_rejects_what_toml_rejects_with_the_same_message() {
    let on = "{ type = \"light\", is_on = true }";
    let lit = light(true, None, None);
    for (case, toml, api, error) in [
        (
            "two scenes of one name",
            [scene("evening", &[("lamp", on)]), scene("evening", &[])].concat(),
            vec![
                ("evening", api_scene("evening", &[("lamp", lit.clone())])),
                ("evening2", api_scene("evening", &[])),
            ],
            "two scenes are named evening",
        ),
        (
            "a scene named none",
            scene("none", &[("lamp", on)]),
            vec![("none", api_scene("none", &[("lamp", lit.clone())]))],
            "can't be named \"none\"",
        ),
        (
            "a scene with no name",
            scene("", &[("lamp", on)]),
            vec![("", api_scene("", &[("lamp", lit.clone())]))],
            "can't be named \"\"",
        ),
        (
            "a brightness over 100",
            scene(
                "s",
                &[(
                    "lamp",
                    "{ type = \"light\", is_on = true, brightness = 101 }",
                )],
            ),
            vec![(
                "s",
                api_scene("s", &[("lamp", light(true, Some(101), None))]),
            )],
            "brightness 101, over 100",
        ),
        (
            "a switch",
            scene("s", &[("remote", on)]),
            vec![("s", api_scene("s", &[("remote", lit.clone())]))],
            "remote, a switch",
        ),
        (
            "a sensor",
            scene("s", &[("thermo", on)]),
            vec![("s", api_scene("s", &[("thermo", lit.clone())]))],
            "thermo, a sensor",
        ),
    ] {
        let from_toml = rejected(from_toml(&toml).await, &format!("{case}, TOML"));
        assert!(from_toml.contains(error), "{case}: {from_toml:?}");
        let from_api = rejected(create(&api).await, &format!("{case}, API"));
        assert_eq!(from_api, from_toml, "{case}");
    }
}

/// What only the API can express, and can't work, it rejects too: a scene
/// listed under another name than its own (it would show one and answer
/// to the other; one listed under `none` could never be activated), and a
/// target that isn't a light's or an outlet's state.
#[tokio::test]
async fn the_api_rejects_what_only_it_can_express() {
    let lit = light(true, None, None);
    let pressed = DeviceStateValue::Switch(SwitchState {
        is_pressed: true,
        last_pressed: None,
        battery_level: None,
    });
    for (case, api, error) in [
        (
            "listed under another name",
            vec![("a", api_scene("b", &[("lamp", lit.clone())]))],
            "scene \"a\" is named \"b\"",
        ),
        (
            "listed under none",
            vec![("none", api_scene("evening", &[("lamp", lit.clone())]))],
            "scene \"none\" is named \"evening\"",
        ),
        (
            "a switch's state",
            vec![("s", api_scene("s", &[("lamp", pressed)]))],
            "scene s sets lamp to the state of a switch",
        ),
        (
            "no state",
            vec![("s", api_scene("s", &[("lamp", DeviceStateValue::Empty)]))],
            "scene s sets lamp to no state",
        ),
    ] {
        let message = rejected(create(&api).await, case);
        assert!(message.contains(error), "{case}: {message:?}");
    }
}

/// What the TOML path only warns about, the API path warns about too, with
/// the same lines, and creates the controller: warnings stay warnings.
#[tokio::test]
async fn the_api_warns_about_what_toml_warns_about() {
    for (case, toml, api) in [
        ("no scenes", "scenes = []\n".to_string(), vec![]),
        (
            "a scene with no devices",
            scene("idle", &[]),
            vec![("idle", api_scene("idle", &[]))],
        ),
        (
            "a light's level for an outlet",
            scene(
                "s",
                &[(
                    "tv",
                    "{ type = \"light\", is_on = true, brightness = 50, color_temp = 3000 }",
                )],
            ),
            vec![(
                "s",
                api_scene("s", &[("tv", light(true, Some(50), Some(3000)))]),
            )],
        ),
    ] {
        let (_, from_toml) = from_toml(&toml).await.expect(case);
        assert_eq!(from_toml.len(), 1, "{case}: {from_toml:?}");
        let (_, from_api) = create(&api).await.expect(case);
        assert_eq!(from_api, from_toml, "{case}");
    }
}

/// The runtime config a TOML scene controller maps onto passes the API's
/// checks as it is.
#[tokio::test]
async fn a_toml_controllers_runtime_config_passes_the_api_checks() {
    let body = [
        scene(
            "evening",
            &[
                (
                    "lamp",
                    "{ type = \"light\", is_on = true, brightness = 30, color_temp = 2200 }",
                ),
                ("tv", "{ type = \"outlet\", is_on = false }"),
                ("hub_outlet", "{ type = \"outlet\", is_on = true }"),
                ("ghost", "{ type = \"light\", is_on = true }"),
            ],
        ),
        scene("night", &[("lamp", "{ type = \"light\", is_on = false }")]),
    ]
    .concat();
    let (controller, warnings) = from_toml(&body).await.expect("from_toml");
    assert_eq!(warnings, Vec::<String>::new());
    let (_, warnings) = SceneController::create(controller.config().clone(), store().await)
        .await
        .expect("create");
    assert_eq!(warnings, Vec::<String>::new());
}
