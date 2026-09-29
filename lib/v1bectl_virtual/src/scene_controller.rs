use crate::config::{SceneControllerConfig, SceneDeviceState};
use crate::virtual_device::{
    DetachedPlan, VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType,
    VirtualWrite,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use v1bectl_sync::{
    DeviceId, DeviceStateValue, LightState, OutletState, SceneState, SensorState, StateStore,
    SwitchState,
};

/// How long each step of a fade is.
const FADE_STEP: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scene {
    pub name: String,
    /// Each device's target. What one leaves out (a light's `brightness`,
    /// its colour) keeps the device's value, as the store has it where the
    /// scene's transition ends: a scene only changes what it names (#64).
    pub device_states: HashMap<DeviceId, DeviceStateValue>,
    pub transition_type: TransitionType,
}

impl Scene {
    /// Whether activating it waits: a fade of at least one step, or a
    /// sequence with a delay before one of its devices.
    fn waits(&self) -> bool {
        match &self.transition_type {
            TransitionType::Instant => false,
            TransitionType::Fade { duration_ms } => {
                Duration::from_millis(*duration_ms) >= FADE_STEP
            }
            TransitionType::Sequence { delays_ms } => delays_ms
                .iter()
                .take(self.device_states.len())
                .any(|&delay_ms| delay_ms > 0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransitionType {
    Instant,
    Fade { duration_ms: u64 },
    Sequence { delays_ms: Vec<u64> },
}

/// What a write to a scene controller asks of it.
enum Activation {
    /// `none`, or no name: deactivate the current scene. No device is
    /// written.
    Deactivate,
    /// Activate the scene of that name.
    Activate(String, Arc<Scene>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualSceneState {
    pub scene_name: String,
    pub is_active: bool,
}

/// The member states a scene activation has set so far: each device once,
/// at the last state it set, in the order it first set them.
#[derive(Default)]
struct Staged(Vec<(DeviceId, DeviceStateValue)>);

impl Staged {
    fn get(&self, device_id: &DeviceId) -> Option<&DeviceStateValue> {
        self.0
            .iter()
            .find(|(id, _)| id == device_id)
            .map(|(_, state)| state)
    }

    fn set(&mut self, device_id: &DeviceId, state: DeviceStateValue) {
        match self.0.iter_mut().find(|(id, _)| id == device_id) {
            Some((_, staged)) => *staged = state,
            None => self.0.push((device_id.clone(), state)),
        }
    }
}

/// Scene Controller Virtual Device - manages multi-device scenes 🎬
pub struct SceneController {
    config: VirtualDeviceConfig,
    /// Shared with the transitions that run without the manager's lock
    /// (see `plan_write_detached`).
    scenes: HashMap<String, Arc<Scene>>,
    current_scene: Option<String>,
    // kept: parsed from config and retained for upcoming animated scene
    // transitions; transitions are applied instantly for now.
    #[allow(dead_code)]
    transition_duration: Duration,
    current_state: VirtualSceneState,
    state_store: Arc<StateStore>,
}

impl SceneController {
    /// A scene controller from `config`, its runtime config, unchecked. The
    /// API creates one with [`Self::create`], and the server loads one from
    /// TOML with [`Self::from_toml`]: both check the config first.
    pub fn new(
        config: VirtualDeviceConfig,
        state_store: Arc<StateStore>,
    ) -> Result<Self, VirtualDeviceError> {
        // Parse configuration
        let scenes: HashMap<String, Scene> =
            serde_json::from_value(config.config.get("scenes").cloned().unwrap_or_default())
                .map_err(|e| VirtualDeviceError::Config(format!("Invalid scenes config: {e}")))?;
        let scenes = scenes
            .into_iter()
            .map(|(name, scene)| (name, Arc::new(scene)))
            .collect();

        let transition_duration_ms: u64 = serde_json::from_value(
            config
                .config
                .get("default_transition_ms")
                .cloned()
                .unwrap_or(serde_json::Value::Number(serde_json::Number::from(1000))),
        )
        .map_err(|e| VirtualDeviceError::Config(format!("Invalid transition duration: {e}")))?;

        Ok(Self {
            config,
            scenes,
            current_scene: None,
            transition_duration: Duration::from_millis(transition_duration_ms),
            current_state: VirtualSceneState {
                scene_name: "none".to_string(),
                is_active: false,
            },
            state_store,
        })
    }

    /// A scene controller from its runtime config, as the API's
    /// `CreateVirtualDevice` sends it, checked the way [`Self::from_toml`]
    /// checks a TOML one, by the same checks (`check_scenes`), so both
    /// reject the same configs with the same messages (#64). With a line
    /// for each thing it only warns about, for the caller to log.
    ///
    /// On top of those, each scene must be listed under its own `name`. A
    /// scene is activated by the name it's listed under, so one listed
    /// under another would show one name and answer to another. (A TOML
    /// scene always is.)
    ///
    /// Two scenes listed under one name, or a device listed twice in one
    /// scene, can't reach it: the API decodes a request's config into maps,
    /// which keep only the last of each.
    pub async fn create(
        config: VirtualDeviceConfig,
        state_store: Arc<StateStore>,
    ) -> Result<(Self, Vec<String>), VirtualDeviceError> {
        let controller = Self::new(config, state_store)?;
        let warnings = {
            let mut listed: Vec<(&String, &Scene)> = controller
                .scenes
                .iter()
                .map(|(listed_as, scene)| (listed_as, scene.as_ref()))
                .collect();
            listed.sort_by_key(|(listed_as, _)| *listed_as);
            let scenes: Vec<SceneTargets> = listed
                .iter()
                .map(|(_, scene)| SceneTargets::of(scene))
                .collect();
            let warnings = check_scenes(&scenes, &controller.state_store).await?;
            if let Some((listed_as, scene)) = listed
                .iter()
                .find(|(listed_as, scene)| **listed_as != scene.name)
            {
                return Err(VirtualDeviceError::Config(format!(
                    "scene {listed_as:?} is named {:?}: a scene is activated by the name it's listed under, so the two must be the same",
                    scene.name
                )));
            }
            warnings
        };
        Ok((controller, warnings))
    }

    /// A scene controller from its TOML config (`type = "scene_controller"`
    /// in `virtual_devices/*.toml`), as the server loads one (#10), with a
    /// line for each part of that config it can't honour, for the loader to
    /// warn about.
    ///
    /// The TOML maps onto the config [`Self::new`] takes, the one the API's
    /// `CreateVirtualDevice` sends:
    /// - Each `[[scenes]]` is a scene of its `name`. Its `display_name` is
    ///   kept in the config, but nothing at runtime reads it.
    /// - Each of its `devices` is a target (see `toml_state`), in the shape
    ///   the store holds that device in (see `in_store_shape`).
    /// - `settings.transition_duration` is every scene's transition (see
    ///   `toml_transition`): a fade over it, or instant for 0. The TOML
    ///   has no transition per scene, and no sequence.
    /// - `settings.default_scene` is kept in the config, with a warning:
    ///   nothing activates a default scene. Activating it on load would
    ///   switch the lights on every restart of the server.
    ///
    /// Its scenes are checked as the API's are ([`Self::create`]), by the
    /// same checks (`check_scenes`), before any is mapped. It fails on a
    /// config that is ambiguous or can't work: two scenes of one name, a
    /// scene named `none` or with no name (a write of either deactivates
    /// the current scene, so it could never be activated), a device twice
    /// in one scene, a brightness over 100, or a device the store holds as
    /// something a scene can't set (a switch, a sensor).
    ///
    /// A device the store doesn't have is no error, as for the other
    /// virtual devices: it may be a virtual device that loads later. The
    /// manager warns about each one still missing once all are loaded (see
    /// [`crate::VirtualDeviceManager::dangling_references`]), and activating
    /// a scene that sets one fails at it.
    pub async fn from_toml(
        toml: &SceneControllerConfig,
        state_store: Arc<StateStore>,
    ) -> Result<(Self, Vec<String>), VirtualDeviceError> {
        let mut warnings = Vec::new();
        let transition = toml_transition(toml.settings.transition_duration, &mut warnings);

        let checked: Vec<SceneTargets> = toml
            .scenes
            .iter()
            .map(|scene| SceneTargets {
                name: &scene.name,
                targets: scene
                    .devices
                    .iter()
                    .map(|device| (&device.device_id, toml_state(&device.state)))
                    .collect(),
            })
            .collect();
        warnings.extend(check_scenes(&checked, &state_store).await?);

        let mut scenes = serde_json::Map::new();
        for (scene, checked) in toml.scenes.iter().zip(&checked) {
            let name = &scene.name;
            let mut device_states = HashMap::new();
            for (device_id, target) in &checked.targets {
                let current = state_store.get_device(device_id).await.map(|d| d.state);
                device_states.insert(
                    (*device_id).clone(),
                    in_store_shape(target, current.as_ref()),
                );
            }

            let runtime = Scene {
                name: name.clone(),
                device_states,
                transition_type: transition.clone(),
            };
            let mut value = serde_json::to_value(&runtime).map_err(|e| {
                VirtualDeviceError::Config(format!("scene {name} doesn't serialize: {e}"))
            })?;
            if let Some(fields) = value.as_object_mut() {
                fields.insert(
                    "display_name".to_string(),
                    scene.display_name.clone().into(),
                );
            }
            scenes.insert(name.clone(), value);
        }

        if let Some(default_scene) = &toml.settings.default_scene {
            let unknown = if scenes.contains_key(default_scene) {
                ""
            } else {
                " (and it isn't one of its scenes)"
            };
            warnings.push(format!(
                "default_scene {default_scene} is not activated{unknown}: nothing activates a default scene, so activate it through the API"
            ));
        }

        let mut config = serde_json::json!({
            "scenes": scenes,
            "default_transition_ms": toml.settings.transition_duration,
        });
        if let Some(default_scene) = &toml.settings.default_scene {
            config["default_scene"] = default_scene.clone().into();
        }

        let controller = Self::new(
            VirtualDeviceConfig {
                device_id: toml.device_id.clone(),
                name: toml.name.clone(),
                description: Some(format!(
                    "Scene controller with {} scenes",
                    toml.scenes.len()
                )),
                enabled: true,
                device_type: VirtualDeviceType::SceneController,
                config,
            },
            state_store,
        )?;
        Ok((controller, warnings))
    }

    /// What writing `new_state` asks of this controller. It only looks the
    /// scene up, so it's quick, and reads nothing that can change.
    fn activation(&self, new_state: &DeviceStateValue) -> Result<Activation, VirtualDeviceError> {
        // Parse scene activation from scene state
        let DeviceStateValue::Scene(scene_state) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        let scene_name = &scene_state.scene_name;
        if scene_name == "none" || scene_name.is_empty() {
            return Ok(Activation::Deactivate);
        }
        self.scenes.get(scene_name).map_or_else(
            || {
                Err(VirtualDeviceError::Config(format!(
                    "Scene not found: {scene_name}"
                )))
            },
            |scene| Ok(Activation::Activate(scene_name.clone(), Arc::clone(scene))),
        )
    }

    /// The write `activation` takes: its devices where the scene's
    /// transition leaves them (see [`Self::plan_activation`]), and the scene
    /// active, or no device and no scene active for a deactivation. It
    /// borrows nothing of the controller, so it can run without the
    /// manager's lock (see [`VirtualDevice::plan_write_detached`]).
    async fn plan(store: &StateStore, activation: Activation) -> VirtualWrite {
        match activation {
            Activation::Deactivate => VirtualWrite {
                members: Vec::new(),
                state: DeviceStateValue::Scene(SceneState {
                    scene_name: "none".to_string(),
                    is_active: false,
                }),
            },
            Activation::Activate(scene_name, scene) => VirtualWrite {
                members: Self::plan_activation(store, &scene).await,
                state: DeviceStateValue::Scene(SceneState {
                    scene_name,
                    is_active: true,
                }),
            },
        }
    }

    /// The member writes that activating `scene` takes: where its
    /// transition leaves each of its devices, in the order it first sets
    /// them.
    ///
    /// The transition runs as it always has, steps and delays included, but
    /// on the scene's own copy of its devices' states (#55). It writes
    /// nothing: the manager commits where it ends. Its steps used to go into
    /// the store, where the manager only ever committed the last ones, and a
    /// pull in between took them for outside changes. A device the store
    /// doesn't have ends the transition where writing it used to fail, as
    /// its last write: the manager's commit of it fails the activation
    /// there.
    ///
    /// Each target is merged with the device as the store has it (see
    /// [`merged`]), so a scene only changes what it names (#64). For an
    /// instant scene, that's the store as the manager commits it: it plans
    /// and commits in one hold of its lock. A fade or a sequence waits
    /// before the commit, and a write that doesn't queue behind it (a direct
    /// write through the API, a change on the hub that a pull brings in)
    /// can change a field it doesn't name meanwhile. So where it ends, each
    /// device is merged again, with the store as it is then (see
    /// [`Self::merge_at_end`]): that change is kept, not reverted to where
    /// the transition started it. A fade's steps interpolate toward its
    /// targets merged with where it starts them.
    async fn plan_activation(
        store: &StateStore,
        scene: &Scene,
    ) -> Vec<(DeviceId, DeviceStateValue)> {
        let mut staged = Staged::default();
        match scene.transition_type {
            TransitionType::Instant => {
                // Set all devices immediately
                for (device_id, target) in &scene.device_states {
                    if !Self::stage_target(store, &mut staged, device_id, target).await {
                        break;
                    }
                }
            }
            TransitionType::Fade { duration_ms } => {
                // Calculate intermediate steps for smooth transitions
                let duration = Duration::from_millis(duration_ms);
                let steps = (duration.as_millis() / FADE_STEP.as_millis()) as usize;

                if steps == 0 {
                    // Just set immediately if duration too short
                    for (device_id, target) in &scene.device_states {
                        if !Self::stage_target(store, &mut staged, device_id, target).await {
                            break;
                        }
                    }
                    return staged.0;
                }

                // Each device's target, merged with where the fade starts
                // it, at its first step: what its steps interpolate toward,
                // so what the scene leaves out stays where it is at every
                // step. Where the fade ends, that's merged again, with the
                // store as it is then.
                let mut targets = Staged::default();
                'fade: for step in 0..=steps {
                    // `steps` is a fade duration in 100ms increments; not
                    // provably bounded to f32's 23-bit mantissa, but scene
                    // fades are seconds-to-minutes long in practice.
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "steps is a fade-duration/100ms step count, far below f32's precision limit in practice"
                    )]
                    let progress = step as f32 / steps as f32;

                    for (device_id, target_state) in &scene.device_states {
                        let started = staged.get(device_id).cloned();
                        let (current_state, target) =
                            match started.zip(targets.get(device_id).cloned()) {
                                Some(fading) => fading,
                                None => match store.get_device(device_id).await {
                                    // Where it starts: the device as the
                                    // store has it.
                                    Some(device) => {
                                        let target = merged(target_state, &device.state);
                                        targets.set(device_id, target.clone());
                                        (device.state, target)
                                    }
                                    // Not in the store: this step is its
                                    // last write.
                                    None => (
                                        Self::get_default_state_for_target(target_state),
                                        target_state.clone(),
                                    ),
                                },
                            };

                        let interpolated_state =
                            Self::interpolate_states(&current_state, &target, progress);
                        if !Self::stage(store, &mut staged, device_id, interpolated_state).await {
                            break 'fade;
                        }
                    }

                    if step < steps {
                        tokio::time::sleep(FADE_STEP).await;
                    } else {
                        // Where it ends, what the scene leaves out is where
                        // the device is now.
                        Self::merge_at_end(store, scene, &mut staged).await;
                    }
                }
            }
            TransitionType::Sequence { ref delays_ms } => {
                // Activate devices in sequence with specified delays
                for (i, (device_id, target)) in scene.device_states.iter().enumerate() {
                    if let Some(&delay_ms) = delays_ms.get(i) {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                    if !Self::stage_target(store, &mut staged, device_id, target).await {
                        break;
                    }
                }
                // A device it reached before a delay is merged again: what
                // the scene leaves out is where the device is now.
                Self::merge_at_end(store, scene, &mut staged).await;
            }
        }

        staged.0
    }

    /// Each device `staged` holds, at its target in `scene` merged with the
    /// device as the store has it now (see [`merged`]): where a transition
    /// that waited ends, right before the manager commits it (#64). A field
    /// the scene doesn't name that changed while it waited, by a write that
    /// doesn't queue behind it (a direct write through the API, a change on
    /// the hub that a pull brought in), keeps its new value. Merged where
    /// the transition started, or where a sequence reached the device, the
    /// commit would put the old one back, on the hub too.
    ///
    /// A device the store doesn't have stays at the scene's target as it
    /// is, for the manager's commit of it to fail at.
    async fn merge_at_end(store: &StateStore, scene: &Scene, staged: &mut Staged) {
        for (device_id, state) in &mut staged.0 {
            let Some(target) = scene.device_states.get(device_id) else {
                continue;
            };
            if let Some(device) = store.get_device(device_id).await {
                *state = merged(target, &device.state);
            }
        }
    }

    /// Stage `target` for `device_id`, merged with the device as the store
    /// has it now (see [`merged`]), where the activation used to write it
    /// into the store. Returns whether the store has the device: a write to
    /// one it doesn't have failed, and ended the activation. That one is
    /// staged at the scene's target as it is, for the manager's commit of
    /// it to fail at, as before.
    async fn stage_target(
        store: &StateStore,
        staged: &mut Staged,
        device_id: &DeviceId,
        target: &DeviceStateValue,
    ) -> bool {
        let current = store.get_device(device_id).await;
        let state = current
            .as_ref()
            .map_or_else(|| target.clone(), |device| merged(target, &device.state));
        staged.set(device_id, state);
        current.is_some()
    }

    /// Stage `state` for `device_id`, where the activation used to write it
    /// into the store. Returns whether the store has the device: a write to
    /// one it doesn't have failed, and ended the activation.
    async fn stage(
        store: &StateStore,
        staged: &mut Staged,
        device_id: &DeviceId,
        state: DeviceStateValue,
    ) -> bool {
        staged.set(device_id, state);
        store.get_device(device_id).await.is_some()
    }

    /// Get default state for a target device type
    fn get_default_state_for_target(target_state: &DeviceStateValue) -> DeviceStateValue {
        match target_state {
            DeviceStateValue::Light(_) => DeviceStateValue::Light(LightState {
                is_on: false,
                brightness: Some(0),
                color_temp: Some(2700),
                rgb_color: None,
            }),
            DeviceStateValue::Switch(_) => DeviceStateValue::Switch(SwitchState {
                is_pressed: false,
                last_pressed: None,
                battery_level: None,
            }),
            DeviceStateValue::Sensor(_) => DeviceStateValue::Sensor(SensorState {
                temperature: None,
                humidity: None,
                last_updated: chrono::Utc::now().timestamp_millis().cast_unsigned(),
            }),
            _ => target_state.clone(),
        }
    }

    /// Interpolate between two device states
    fn interpolate_states(
        current: &DeviceStateValue,
        target: &DeviceStateValue,
        progress: f32,
    ) -> DeviceStateValue {
        match (current, target) {
            (DeviceStateValue::Light(current_light), DeviceStateValue::Light(target_light)) => {
                let interpolated_brightness = if let (Some(c), Some(t)) =
                    (current_light.brightness, target_light.brightness)
                {
                    let interpolated = f32::from(c) + progress * (f32::from(t) - f32::from(c));
                    // interpolated is a weighted average of two u8 values,
                    // so it's always within u8 range; clippy can't see that.
                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "weighted average of two u8 brightness values, always in u8 range"
                    )]
                    let brightness = interpolated.round() as u8;
                    Some(brightness)
                } else {
                    target_light.brightness
                };

                let interpolated_temp = if let (Some(c), Some(t)) =
                    (current_light.color_temp, target_light.color_temp)
                {
                    let interpolated = f32::from(c) + progress * (f32::from(t) - f32::from(c));
                    // Same reasoning as brightness above, for u16 color temp.
                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "weighted average of two u16 color-temp values, always in u16 range"
                    )]
                    let color_temp = interpolated.round() as u16;
                    Some(color_temp)
                } else {
                    target_light.color_temp
                };

                DeviceStateValue::Light(LightState {
                    is_on: if progress >= 0.5 {
                        target_light.is_on
                    } else {
                        current_light.is_on
                    },
                    brightness: interpolated_brightness,
                    color_temp: interpolated_temp,
                    rgb_color: if progress >= 0.5 {
                        target_light.rgb_color.clone()
                    } else {
                        current_light.rgb_color.clone()
                    },
                })
            }
            _ => {
                // For non-light states, just switch at 50% progress
                if progress >= 0.5 {
                    target.clone()
                } else {
                    current.clone()
                }
            }
        }
    }

    /// Get all device IDs involved in scenes
    fn get_all_scene_devices(&self) -> Vec<DeviceId> {
        let mut devices = std::collections::HashSet::new();
        for scene in self.scenes.values() {
            for device_id in scene.device_states.keys() {
                devices.insert(device_id.clone());
            }
        }
        devices.into_iter().collect()
    }
}

/// `target`, the state a scene sets a device to, as activating the scene
/// writes it over `current`, the device as the store has it (#64):
/// - In the shape the store holds the device in (see [`in_store_shape`]),
///   so its gateway can push it. A Dirigera hub's outlet is a light there.
/// - With what the target leaves out kept from `current`, so a scene only
///   changes what it names: a light's `brightness` and its colour, and an
///   outlet's readings. A light shows one colour: a target that names
///   `color_temp` or `rgb_color` sets both, and one that names neither
///   keeps both.
///
/// A field a target left out used to be written as none, with the rest of
/// the device's state: the store showed a light with no brightness for the
/// sync engine's protection window and up to one pull after it, until the
/// hub's own value came back as a second update.
fn merged(target: &DeviceStateValue, current: &DeviceStateValue) -> DeviceStateValue {
    match (in_store_shape(target, Some(current)), current) {
        (DeviceStateValue::Light(target), DeviceStateValue::Light(current)) => {
            let (color_temp, rgb_color) =
                if target.color_temp.is_some() || target.rgb_color.is_some() {
                    (target.color_temp, target.rgb_color)
                } else {
                    (current.color_temp, current.rgb_color.clone())
                };
            DeviceStateValue::Light(LightState {
                is_on: target.is_on,
                brightness: target.brightness.or(current.brightness),
                color_temp,
                rgb_color,
            })
        }
        (DeviceStateValue::Outlet(target), DeviceStateValue::Outlet(current)) => {
            DeviceStateValue::Outlet(OutletState {
                is_on: target.is_on,
                power_consumption: target.power_consumption.or(current.power_consumption),
                total_energy: target.total_energy.or(current.total_energy),
            })
        }
        (shaped, _) => shaped,
    }
}

/// `target` in the shape the store holds its device in, `current`, so the
/// device's gateway can push it, and the store keeps the kind of state it
/// holds for it:
/// - A light the store holds as an outlet is an outlet, on or off only.
/// - An outlet the store holds as a light is a light that is only on or
///   off. That is how a Dirigera hub's outlets are read, and the only kind
///   of state its gateway writes to one.
/// - Anything else is taken as it is, as is a target for a device the
///   store doesn't have.
fn in_store_shape(
    target: &DeviceStateValue,
    current: Option<&DeviceStateValue>,
) -> DeviceStateValue {
    match (target, current) {
        (DeviceStateValue::Light(light), Some(DeviceStateValue::Outlet(_))) => {
            DeviceStateValue::Outlet(OutletState {
                is_on: light.is_on,
                power_consumption: None,
                total_energy: None,
            })
        }
        (DeviceStateValue::Outlet(outlet), Some(DeviceStateValue::Light(_))) => {
            DeviceStateValue::Light(LightState {
                is_on: outlet.is_on,
                brightness: None,
                color_temp: None,
                rgb_color: None,
            })
        }
        _ => target.clone(),
    }
}

/// The transition of every scene of a TOML scene controller, from its
/// `transition_duration` in ms: a fade over it, or instant for 0.
///
/// A fade runs in whole [`FADE_STEP`]s. So one shorter than a step would
/// set its devices at once, and is instant, with a warning. One between
/// steps ends at the step before it, with a warning too.
fn toml_transition(duration_ms: u32, warnings: &mut Vec<String>) -> TransitionType {
    let step_ms = FADE_STEP.as_millis();
    let requested = u128::from(duration_ms);
    if duration_ms == 0 {
        return TransitionType::Instant;
    }
    if requested < step_ms {
        warnings.push(format!(
            "transition_duration {duration_ms} ms is shorter than one fade step ({step_ms} ms): its scenes are set instantly"
        ));
        return TransitionType::Instant;
    }
    let faded = requested / step_ms * step_ms;
    if faded != requested {
        warnings.push(format!(
            "transition_duration {duration_ms} ms fades in whole {step_ms} ms steps: {faded} ms"
        ));
    }
    TransitionType::Fade {
        duration_ms: u64::from(duration_ms),
    }
}

/// The state a TOML scene target, `state`, sets its device to, as the TOML
/// says it: a light, with `rgb_color` unset (the TOML has none), or an
/// outlet. [`SceneController::from_toml`] writes it in the shape the store
/// holds the device in (see [`in_store_shape`]).
fn toml_state(state: &SceneDeviceState) -> DeviceStateValue {
    match *state {
        SceneDeviceState::Light {
            is_on,
            brightness,
            color_temp,
        } => DeviceStateValue::Light(LightState {
            is_on,
            brightness,
            color_temp,
            rgb_color: None,
        }),
        SceneDeviceState::Outlet { is_on } => DeviceStateValue::Outlet(OutletState {
            is_on,
            power_consumption: None,
            total_energy: None,
        }),
    }
}

/// One scene of a scene controller's config, as [`check_scenes`] checks
/// it: its name, and each device it sets with its target, in the order the
/// config lists them, repeats and all.
struct SceneTargets<'a> {
    name: &'a str,
    targets: Vec<(&'a DeviceId, DeviceStateValue)>,
}

impl<'a> SceneTargets<'a> {
    /// `scene` of a runtime config, its devices in order of their ids.
    fn of(scene: &'a Scene) -> Self {
        let mut targets: Vec<(&DeviceId, DeviceStateValue)> = scene
            .device_states
            .iter()
            .map(|(device_id, target)| (device_id, target.clone()))
            .collect();
        targets.sort_by_key(|(device_id, _)| *device_id);
        Self {
            name: &scene.name,
            targets,
        }
    }
}

/// The checks a scene controller's scenes pass before it's created, from
/// TOML ([`SceneController::from_toml`]) or through the API
/// ([`SceneController::create`]), so both reject the same configs with the
/// same messages (#64). Returns a line for each thing it only warns about.
///
/// It fails on a config that is ambiguous or can't work:
/// - two scenes of one name;
/// - a scene named `none` or with no name: a write of either deactivates
///   the current scene, so it could never be activated;
/// - a device twice in one scene;
/// - a brightness over 100;
/// - a device the store holds as something a scene can't set (a switch, a
///   sensor), or a target that isn't a light's or an outlet's state.
///
/// It warns about a config with no scenes, a scene with no devices, and a
/// light's brightness or colour for a device the store holds as an outlet:
/// only whether it's on is set (see [`in_store_shape`]).
///
/// A device the store doesn't have is no error, as for the other virtual
/// devices: it may be a virtual device that loads later.
async fn check_scenes(
    scenes: &[SceneTargets<'_>],
    store: &StateStore,
) -> Result<Vec<String>, VirtualDeviceError> {
    let mut warnings = Vec::new();
    if scenes.is_empty() {
        warnings.push("it has no scenes: there is nothing to activate".to_string());
    }

    let mut names = HashSet::new();
    for scene in scenes {
        let name = scene.name;
        if name.is_empty() || name == "none" {
            return Err(VirtualDeviceError::Config(format!(
                "a scene can't be named {name:?}: activating `none` or no name deactivates the current scene"
            )));
        }
        if !names.insert(name) {
            return Err(VirtualDeviceError::Config(format!(
                "two scenes are named {name}"
            )));
        }
        if scene.targets.is_empty() {
            warnings.push(format!(
                "scene {name} sets no devices: activating it only marks it active"
            ));
        }

        let mut devices = HashSet::new();
        for (device_id, target) in &scene.targets {
            check_target(name, device_id, target, store, &mut warnings).await?;
            if !devices.insert(*device_id) {
                return Err(VirtualDeviceError::Config(format!(
                    "scene {name} sets {device_id} twice"
                )));
            }
        }
    }
    Ok(warnings)
}

/// [`check_scenes`] for `target`, the state scene `scene` sets `device_id`
/// to.
async fn check_target(
    scene: &str,
    device_id: &DeviceId,
    target: &DeviceStateValue,
    store: &StateStore,
    warnings: &mut Vec<String>,
) -> Result<(), VirtualDeviceError> {
    let (brightness, names_colour) = match target {
        DeviceStateValue::Light(light) => (
            light.brightness,
            light.color_temp.is_some() || light.rgb_color.is_some(),
        ),
        DeviceStateValue::Outlet(_) => (None, false),
        DeviceStateValue::Empty => {
            return Err(VirtualDeviceError::Config(format!(
                "scene {scene} sets {device_id} to no state: a scene sets only lights and outlets"
            )));
        }
        other => {
            return Err(VirtualDeviceError::Config(format!(
                "scene {scene} sets {device_id} to the state of a {}: a scene sets only lights and outlets",
                state_kind(other)
            )));
        }
    };
    if let Some(level) = brightness.filter(|&level| level > 100) {
        return Err(VirtualDeviceError::Config(format!(
            "scene {scene} sets {device_id} to brightness {level}, over 100"
        )));
    }

    match store.get_device(device_id).await.map(|device| device.state) {
        None | Some(DeviceStateValue::Light(_)) => {}
        Some(DeviceStateValue::Outlet(_)) => {
            if brightness.is_some() || names_colour {
                warnings.push(format!(
                    "scene {scene}: {device_id} is an outlet, so only whether it's on is set (not its brightness or colour)"
                ));
            }
        }
        Some(other) => {
            return Err(VirtualDeviceError::Config(format!(
                "scene {scene} sets {device_id}, a {}: a scene sets only lights and outlets",
                state_kind(&other)
            )));
        }
    }
    Ok(())
}

/// What kind of device holds `state`, for an error message.
fn state_kind(state: &DeviceStateValue) -> &'static str {
    match state {
        DeviceStateValue::Light(_) => "light",
        DeviceStateValue::Outlet(_) => "outlet",
        DeviceStateValue::Switch(_) => "switch",
        DeviceStateValue::Sensor(_) => "sensor",
        DeviceStateValue::MotionSensor(_) => "motion sensor",
        DeviceStateValue::Scene(_) => "scene controller",
        DeviceStateValue::Timer(_) => "timer",
        DeviceStateValue::Empty => "device without a state (a button controller)",
    }
}

#[async_trait]
impl VirtualDevice for SceneController {
    fn device_id(&self) -> &DeviceId {
        &self.config.device_id
    }

    fn device_type(&self) -> VirtualDeviceType {
        VirtualDeviceType::SceneController
    }

    fn config(&self) -> &VirtualDeviceConfig {
        &self.config
    }

    /// Its devices where the scene's transition leaves them (see
    /// `plan_activation`), and the scene active. `none` (or no name)
    /// deactivates the current scene, and writes no device.
    async fn plan_write(
        &self,
        new_state: DeviceStateValue,
    ) -> Result<VirtualWrite, VirtualDeviceError> {
        Ok(Self::plan(&self.state_store, self.activation(&new_state)?).await)
    }

    /// [`Self::plan_write`] as a plan of its own, for an activation whose
    /// transition waits: a fade, or a sequence with a delay (#58). It holds
    /// the scene and the store, and nothing of the controller, so its
    /// delays pass without the manager's lock.
    ///
    /// An activation that doesn't wait (an instant scene, a fade shorter
    /// than a step, a deactivation, or a write that fails at once) is
    /// `None`: the manager plans and commits it under one hold of its lock,
    /// as before.
    fn plan_write_detached(&self, new_state: &DeviceStateValue) -> Option<DetachedPlan> {
        let Ok(Activation::Activate(scene_name, scene)) = self.activation(new_state) else {
            return None;
        };
        if !scene.waits() {
            return None;
        }
        let store = Arc::clone(&self.state_store);
        Some(Box::pin(async move {
            Ok(Self::plan(&store, Activation::Activate(scene_name, scene)).await)
        }))
    }

    /// The devices of the scene `new_state` activates: all its transition
    /// can write. Its other scenes' devices aren't queued on, so a write to
    /// one of those doesn't wait for this scene's fade (#58). A
    /// deactivation, or a write that fails at once, writes none.
    fn writes_to(&self, new_state: &DeviceStateValue) -> Vec<DeviceId> {
        match self.activation(new_state) {
            Ok(Activation::Activate(_, scene)) => scene.device_states.keys().cloned().collect(),
            Ok(Activation::Deactivate) | Err(_) => Vec::new(),
        }
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        if let DeviceStateValue::Scene(SceneState {
            scene_name,
            is_active,
        }) = state
        {
            self.current_scene = is_active.then(|| scene_name.clone());
            self.current_state = VirtualSceneState {
                scene_name,
                is_active,
            };
        }
    }

    fn current_state(&self) -> DeviceStateValue {
        DeviceStateValue::Scene(SceneState {
            scene_name: self.current_state.scene_name.clone(),
            is_active: self.current_state.is_active,
        })
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        self.get_all_scene_devices()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use v1bectl_sync::{Capability, DeviceInfo, DeviceType, RgbColor};

    fn light(is_on: bool, brightness: u8) -> DeviceStateValue {
        DeviceStateValue::Light(LightState {
            is_on,
            brightness: Some(brightness),
            color_temp: Some(2700),
            rgb_color: None,
        })
    }

    /// A store with lights `ids`, all off.
    async fn store_with(ids: &[&str]) -> Arc<StateStore> {
        let store = StateStore::new();
        for id in ids {
            let info = DeviceInfo {
                device_id: (*id).to_string(),
                name: (*id).to_string(),
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
            };
            store.add_device(info, light(false, 0)).await;
        }
        store
    }

    /// A scene controller whose one scene, `evening`, takes each of
    /// `devices` to its state with `transition`.
    fn controller(
        store: &Arc<StateStore>,
        devices: &[(&str, DeviceStateValue)],
        transition: &TransitionType,
    ) -> SceneController {
        let device_states: HashMap<DeviceId, DeviceStateValue> = devices
            .iter()
            .map(|(id, state)| ((*id).to_string(), state.clone()))
            .collect();
        let config = VirtualDeviceConfig {
            device_id: "scene".to_string(),
            device_type: VirtualDeviceType::SceneController,
            name: "Scene".to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({ "scenes": { "evening": {
                "name": "evening",
                "device_states": device_states,
                "transition_type": transition,
            } } }),
        };
        SceneController::new(config, store.clone()).expect("scene")
    }

    fn evening() -> DeviceStateValue {
        DeviceStateValue::Scene(SceneState {
            scene_name: "evening".to_string(),
            is_active: true,
        })
    }

    /// The scene's devices in the order its transition takes them: the
    /// order of its `device_states`.
    fn order(controller: &SceneController) -> Vec<DeviceId> {
        controller.scenes["evening"]
            .device_states
            .keys()
            .cloned()
            .collect()
    }

    /// #58: a sequence takes its devices in order, the i-th after the i-th
    /// delay, and hands them to the manager in that order, each at its
    /// target, once every delay has passed. (A paused clock: the delays
    /// pass at once and measure exactly.)
    #[tokio::test(start_paused = true)]
    async fn a_sequence_plans_its_devices_in_order_after_their_delays() {
        let store = store_with(&["a", "b", "c"]).await;
        let targets = [
            ("a", light(true, 10)),
            ("b", light(true, 20)),
            ("c", light(true, 30)),
        ];
        let sequence = TransitionType::Sequence {
            delays_ms: vec![100, 200, 300],
        };
        let controller = controller(&store, &targets, &sequence);
        let want: Vec<(DeviceId, DeviceStateValue)> = order(&controller)
            .into_iter()
            .map(|id| {
                let (_, target) = targets.iter().find(|(t, _)| *t == id).unwrap();
                (id, target.clone())
            })
            .collect();

        let started = tokio::time::Instant::now();
        let write = controller.plan_write(evening()).await.expect("plan");
        assert_eq!(started.elapsed(), Duration::from_millis(600), "delays");
        assert_eq!(write.members, want, "in the scene's order, at the targets");
        assert_eq!(write.state, evening());
    }

    /// #58: a sequence that reaches a device the store doesn't have stops
    /// there, having slept only through the delays up to it. The missing
    /// device is its last write, where the manager's commit fails the
    /// activation; the devices after it are never planned.
    #[tokio::test(start_paused = true)]
    async fn a_sequence_stops_at_a_missing_device_after_only_its_delays() {
        let store = store_with(&["a", "b"]).await;
        let targets = [
            ("a", light(true, 10)),
            ("b", light(true, 20)),
            ("missing", light(true, 30)),
        ];
        let delays_ms = vec![100, 200, 400];
        let sequence = TransitionType::Sequence {
            delays_ms: delays_ms.clone(),
        };
        let controller = controller(&store, &targets, &sequence);
        let order = order(&controller);
        let at = order.iter().position(|id| id == "missing").unwrap();
        let want: Vec<(DeviceId, DeviceStateValue)> = order[..=at]
            .iter()
            .map(|id| {
                let (_, target) = targets.iter().find(|(t, _)| *t == id.as_str()).unwrap();
                (id.clone(), target.clone())
            })
            .collect();

        let started = tokio::time::Instant::now();
        let write = controller.plan_write(evening()).await.expect("plan");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(delays_ms[..=at].iter().sum()),
            "the delays up to the missing device (at {at} of {order:?})"
        );
        assert_eq!(write.members, want, "up to the missing device");
    }

    /// #58: only an activation that waits is planned apart from the
    /// controller, without the manager's lock: a fade of at least one step,
    /// or a sequence with a delay before one of its devices. Everything
    /// else is planned and committed in one hold of the lock, as before.
    #[tokio::test]
    async fn only_an_activation_that_waits_is_planned_detached() {
        let store = store_with(&["a", "b"]).await;
        let targets = [("a", light(true, 10)), ("b", light(true, 20))];
        let named = |scene_name: &str| {
            DeviceStateValue::Scene(SceneState {
                scene_name: scene_name.to_string(),
                is_active: true,
            })
        };
        let sequence = |delays_ms: &[u64]| TransitionType::Sequence {
            delays_ms: delays_ms.to_vec(),
        };
        for (transition, waits) in [
            (TransitionType::Instant, false),
            (TransitionType::Fade { duration_ms: 99 }, false),
            (TransitionType::Fade { duration_ms: 100 }, true),
            (sequence(&[]), false),
            (sequence(&[0, 0]), false),
            // Only two devices: a third delay never runs.
            (sequence(&[0, 0, 500]), false),
            (sequence(&[0, 100]), true),
        ] {
            let controller = controller(&store, &targets, &transition);
            assert_eq!(
                controller.plan_write_detached(&evening()).is_some(),
                waits,
                "{transition:?}"
            );
            for other in [named("none"), named(""), named("nope"), light(true, 1)] {
                assert!(
                    controller.plan_write_detached(&other).is_none(),
                    "{transition:?}: {other:?}"
                );
            }
        }

        // A detached plan is the one `plan_write` makes.
        let controller = controller(&store, &targets, &sequence(&[0, 100]));
        let detached = controller.plan_write_detached(&evening()).expect("waits");
        let planned = controller.plan_write(evening()).await.expect("plan");
        assert_eq!(detached.await.expect("detached plan"), planned);
    }

    /// #58: a fade that reaches a device the store doesn't have stops at
    /// its first step, before its first delay. Each device it took up to
    /// there is where the first step puts it: where it is.
    #[tokio::test(start_paused = true)]
    async fn a_fade_stops_at_a_missing_device_at_its_first_step() {
        let store = store_with(&["a"]).await;
        let targets = [("a", light(true, 80)), ("missing", light(true, 80))];
        let fade = TransitionType::Fade { duration_ms: 1000 };
        let controller = controller(&store, &targets, &fade);
        let order = order(&controller);
        let at = order.iter().position(|id| id == "missing").unwrap();
        // `a` at progress 0 is `a` as it is: off. The missing device starts
        // from off too, at level 0.
        let want: Vec<(DeviceId, DeviceStateValue)> = order[..=at]
            .iter()
            .map(|id| (id.clone(), light(false, 0)))
            .collect();

        let started = tokio::time::Instant::now();
        let write = controller.plan_write(evening()).await.expect("plan");
        assert_eq!(started.elapsed(), Duration::ZERO, "it slept");
        assert_eq!(
            write.members, want,
            "the first step, up to the missing device"
        );
    }

    fn lamp(
        is_on: bool,
        brightness: Option<u8>,
        color_temp: Option<u16>,
        rgb_color: Option<RgbColor>,
    ) -> DeviceStateValue {
        DeviceStateValue::Light(LightState {
            is_on,
            brightness,
            color_temp,
            rgb_color,
        })
    }

    fn outlet(is_on: bool, power_consumption: Option<f32>) -> DeviceStateValue {
        DeviceStateValue::Outlet(OutletState {
            is_on,
            power_consumption,
            total_energy: power_consumption.map(|watts| watts * 2.0),
        })
    }

    /// #64: a target keeps what it leaves out from the device as the store
    /// has it, in the shape the store has it in. A light's colour is one
    /// thing: naming either kind sets both.
    #[test]
    fn a_target_keeps_what_it_leaves_out() {
        let red = || Some(RgbColor { r: 255, g: 0, b: 0 });
        for (case, current, target, want) in [
            (
                "only is_on",
                lamp(false, Some(70), Some(2700), None),
                lamp(true, None, None, None),
                lamp(true, Some(70), Some(2700), None),
            ),
            (
                "a brightness",
                lamp(false, Some(70), Some(2700), None),
                lamp(true, Some(30), None, None),
                lamp(true, Some(30), Some(2700), None),
            ),
            (
                "a colour temperature over an RGB colour",
                lamp(false, Some(70), None, red()),
                lamp(true, None, Some(4000), None),
                lamp(true, Some(70), Some(4000), None),
            ),
            (
                "an RGB colour over a colour temperature",
                lamp(false, Some(70), Some(2700), None),
                lamp(true, None, None, red()),
                lamp(true, Some(70), None, red()),
            ),
            (
                "no colour, over an RGB colour",
                lamp(false, Some(70), None, red()),
                lamp(true, None, None, None),
                lamp(true, Some(70), None, red()),
            ),
            (
                "everything",
                lamp(false, Some(70), Some(2700), None),
                lamp(true, Some(30), Some(2200), None),
                lamp(true, Some(30), Some(2200), None),
            ),
            (
                "an outlet keeps its readings",
                outlet(true, Some(45.5)),
                outlet(false, None),
                outlet(false, Some(45.5)),
            ),
            (
                "an outlet the store holds as a light (a Dirigera hub's)",
                lamp(false, None, None, None),
                outlet(true, None),
                lamp(true, None, None, None),
            ),
            (
                "an outlet over a light the store has a level for",
                lamp(false, Some(70), Some(2700), None),
                outlet(true, None),
                lamp(true, Some(70), Some(2700), None),
            ),
            (
                "a light the store holds as an outlet",
                outlet(true, Some(45.5)),
                lamp(false, Some(30), Some(2200), None),
                outlet(false, Some(45.5)),
            ),
        ] {
            assert_eq!(merged(&target, &current), want, "{case}");
        }
    }

    /// #64: a fade whose target leaves a field out keeps it where the fade
    /// starts it, at every step, and ends at the target merged with the
    /// device where it ends: here, where it started, as nothing changed it.
    #[tokio::test(start_paused = true)]
    async fn a_fade_keeps_what_its_target_leaves_out() {
        let store = store_with(&["a"]).await;
        store
            .update_device_state(&"a".to_string(), lamp(false, Some(70), Some(2200), None))
            .await
            .expect("a");
        let fade = TransitionType::Fade { duration_ms: 1000 };
        let controller = controller(&store, &[("a", lamp(true, None, None, None))], &fade);

        let started = tokio::time::Instant::now();
        let write = controller.plan_write(evening()).await.expect("plan");
        assert_eq!(started.elapsed(), Duration::from_secs(1), "fade time");
        assert_eq!(
            write.members,
            vec![("a".to_string(), lamp(true, Some(70), Some(2200), None))]
        );

        // Every step: interpolating from where it starts to the merged
        // target keeps the level and the colour.
        let start = lamp(false, Some(70), Some(2200), None);
        let target = merged(&lamp(true, None, None, None), &start);
        for progress in [0.0, 0.3, 0.5, 1.0] {
            let DeviceStateValue::Light(step) =
                SceneController::interpolate_states(&start, &target, progress)
            else {
                panic!("a light");
            };
            assert_eq!(
                (step.brightness, step.color_temp),
                (Some(70), Some(2200)),
                "{progress}"
            );
        }
    }

    /// #64 review: a fade or a sequence merges what its targets leave out
    /// again where it ends, with the store as it is then. A colour changed
    /// while it waits, by a write that doesn't queue behind it, is kept, not
    /// put back to where the transition started (or where the sequence
    /// reached the device). A paused clock: the change lands halfway,
    /// exactly.
    #[tokio::test(start_paused = true)]
    async fn a_transition_keeps_what_changes_while_it_waits() {
        let brighter = lamp(true, Some(80), None, None);
        for transition in [
            TransitionType::Fade { duration_ms: 1000 },
            TransitionType::Sequence {
                delays_ms: vec![0, 1000],
            },
        ] {
            let store = store_with(&["a", "b"]).await;
            let set_all = |state: DeviceStateValue| {
                let store = Arc::clone(&store);
                async move {
                    for id in ["a", "b"] {
                        store
                            .update_device_state(&id.to_string(), state.clone())
                            .await
                            .expect(id);
                    }
                }
            };
            set_all(lamp(true, Some(50), Some(2700), None)).await;
            let targets = [("a", brighter.clone()), ("b", brighter.clone())];
            let controller = controller(&store, &targets, &transition);

            let recolour = async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                set_all(lamp(true, Some(50), Some(4000), None)).await;
            };
            let (write, ()) = tokio::join!(controller.plan_write(evening()), recolour);
            let mut members = write.expect("plan").members;
            members.sort_by(|(a, _), (b, _)| a.cmp(b));
            let ends = lamp(true, Some(80), Some(4000), None);
            assert_eq!(
                members,
                vec![("a".to_string(), ends.clone()), ("b".to_string(), ends)],
                "{transition:?}"
            );
        }
    }

    #[test]
    fn test_light_state_interpolation() {
        let current = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(20),
            color_temp: Some(2700),
            rgb_color: None,
        });

        let target = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(80),
            color_temp: Some(4000),
            rgb_color: None,
        });

        // Test midpoint interpolation
        let interpolated = SceneController::interpolate_states(&current, &target, 0.5);
        if let DeviceStateValue::Light(light) = interpolated {
            assert_eq!(light.brightness, Some(50));
            assert_eq!(light.color_temp, Some(3350));
        } else {
            panic!("Expected light state");
        }
    }
}
