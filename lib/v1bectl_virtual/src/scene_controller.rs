use crate::virtual_device::{
    DetachedPlan, VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType,
    VirtualWrite,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use v1bectl_sync::{
    DeviceId, DeviceStateValue, LightState, SceneState, SensorState, StateStore, SwitchState,
};

/// How long each step of a fade is.
const FADE_STEP: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scene {
    pub name: String,
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
    async fn plan_activation(
        store: &StateStore,
        scene: &Scene,
    ) -> Vec<(DeviceId, DeviceStateValue)> {
        let mut staged = Staged::default();
        match scene.transition_type {
            TransitionType::Instant => {
                // Set all devices immediately
                for (device_id, state) in &scene.device_states {
                    if !Self::stage(store, &mut staged, device_id, state.clone()).await {
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
                    for (device_id, state) in &scene.device_states {
                        if !Self::stage(store, &mut staged, device_id, state.clone()).await {
                            break;
                        }
                    }
                    return staged.0;
                }

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
                        let current_state = match staged.get(device_id) {
                            Some(state) => state.clone(),
                            None => store.get_device(device_id).await.map_or_else(
                                || Self::get_default_state_for_target(target_state),
                                |ds| ds.state,
                            ),
                        };

                        let interpolated_state =
                            Self::interpolate_states(&current_state, target_state, progress);
                        if !Self::stage(store, &mut staged, device_id, interpolated_state).await {
                            break 'fade;
                        }
                    }

                    if step < steps {
                        tokio::time::sleep(FADE_STEP).await;
                    }
                }
            }
            TransitionType::Sequence { ref delays_ms } => {
                // Activate devices in sequence with specified delays
                for (i, (device_id, state)) in scene.device_states.iter().enumerate() {
                    if let Some(&delay_ms) = delays_ms.get(i) {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                    if !Self::stage(store, &mut staged, device_id, state.clone()).await {
                        break;
                    }
                }
            }
        }

        staged.0
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
    use v1bectl_sync::{Capability, DeviceInfo, DeviceType};

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
