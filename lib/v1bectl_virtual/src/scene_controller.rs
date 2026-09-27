use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use v1bectl_sync::{
    DeviceId, DeviceStateValue, LightState, SceneState, SensorState, StateStore, SwitchState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scene {
    pub name: String,
    pub device_states: HashMap<DeviceId, DeviceStateValue>,
    pub transition_type: TransitionType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransitionType {
    Instant,
    Fade { duration_ms: u64 },
    Sequence { delays_ms: Vec<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualSceneState {
    pub scene_name: String,
    pub is_active: bool,
}

/// Scene Controller Virtual Device - manages multi-device scenes 🎬
pub struct SceneController {
    config: VirtualDeviceConfig,
    scenes: HashMap<String, Scene>,
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

    /// Activate a scene with its configured transition
    async fn activate_scene(&mut self, scene: &Scene) -> Result<(), VirtualDeviceError> {
        match scene.transition_type {
            TransitionType::Instant => {
                // Set all devices immediately
                for (device_id, state) in &scene.device_states {
                    self.state_store
                        .update_device_state(device_id, state.clone())
                        .await?;
                }
            }
            TransitionType::Fade { duration_ms } => {
                // Calculate intermediate steps for smooth transitions
                let duration = Duration::from_millis(duration_ms);
                let step_duration = Duration::from_millis(100); // 100ms steps
                let steps = (duration.as_millis() / step_duration.as_millis()) as usize;

                if steps == 0 {
                    // Just set immediately if duration too short
                    for (device_id, state) in &scene.device_states {
                        self.state_store
                            .update_device_state(device_id, state.clone())
                            .await?;
                    }
                    return Ok(());
                }

                for step in 0..=steps {
                    // `steps` is a fade duration in 100ms increments; not
                    // provably bounded to f32's 23-bit mantissa, but scene
                    // fades are seconds-to-minutes long in practice.
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "steps is a fade-duration/100ms step count, far below f32's precision limit in practice"
                    )]
                    let progress = step as f32 / steps as f32;

                    for (device_id, target_state) in &scene.device_states {
                        let current_state =
                            self.state_store.get_device(device_id).await.map_or_else(
                                || Self::get_default_state_for_target(target_state),
                                |ds| ds.state,
                            );

                        let interpolated_state =
                            Self::interpolate_states(&current_state, target_state, progress);
                        self.state_store
                            .update_device_state(device_id, interpolated_state)
                            .await?;
                    }

                    if step < steps {
                        tokio::time::sleep(step_duration).await;
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
                    self.state_store
                        .update_device_state(device_id, state.clone())
                        .await?;
                }
            }
        }

        Ok(())
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

    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        // Parse scene activation from scene state
        let scene_name = match new_state {
            DeviceStateValue::Scene(ref scene_state) => scene_state.scene_name.clone(),
            _ => return Err(VirtualDeviceError::InvalidStateType),
        };

        if scene_name == "none" || scene_name.is_empty() {
            // Deactivate current scene
            self.current_scene = None;
            self.current_state = VirtualSceneState {
                scene_name: "none".to_string(),
                is_active: false,
            };
            return Ok(());
        }

        if let Some(scene) = self.scenes.get(&scene_name).cloned() {
            // Activate the scene
            self.activate_scene(&scene).await?;
            self.current_scene = Some(scene_name.clone());
            self.current_state = VirtualSceneState {
                scene_name,
                is_active: true,
            };
            Ok(())
        } else {
            Err(VirtualDeviceError::Config(format!(
                "Scene not found: {scene_name}"
            )))
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
