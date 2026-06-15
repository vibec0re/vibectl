use crate::virtual_device::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use v1bectl_sync::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrightnessCurve {
    /// Breakpoints as (group_brightness, device_brightness) pairs
    pub breakpoints: Vec<(u8, u8)>,
}

impl BrightnessCurve {
    /// Interpolate device brightness from group brightness using curve
    pub fn interpolate(&self, group_brightness: u8) -> u8 {
        if self.breakpoints.is_empty() {
            return group_brightness;
        }

        // Sort breakpoints by group brightness
        let mut points = self.breakpoints.clone();
        points.sort_by_key(|&(g, _)| g);

        // Find the two points to interpolate between
        if group_brightness <= points[0].0 {
            return points[0].1;
        }

        if group_brightness >= points.last().unwrap().0 {
            return points.last().unwrap().1;
        }

        // Linear interpolation between two points
        for window in points.windows(2) {
            let (gb1, db1) = window[0];
            let (gb2, db2) = window[1];

            if group_brightness >= gb1 && group_brightness <= gb2 {
                if gb2 == gb1 {
                    return db1;
                }

                let ratio = (group_brightness - gb1) as f32 / (gb2 - gb1) as f32;
                let interpolated = db1 as f32 + ratio * (db2 as f32 - db1 as f32);
                return interpolated.round() as u8;
            }
        }

        group_brightness
    }
}

/// Light Group Virtual Device - controls multiple lights as one unit 💡
pub struct LightGroup {
    config: VirtualDeviceConfig,
    lights: Vec<DeviceId>,
    brightness_curves: HashMap<DeviceId, BrightnessCurve>,
    current_state: LightState,
    state_store: Arc<StateStore>,
}

impl LightGroup {
    pub fn new(
        config: VirtualDeviceConfig,
        state_store: Arc<StateStore>,
    ) -> Result<Self, VirtualDeviceError> {
        // Parse configuration
        let lights: Vec<DeviceId> =
            serde_json::from_value(config.config.get("lights").cloned().unwrap_or_default())
                .map_err(|e| VirtualDeviceError::Config(format!("Invalid lights config: {}", e)))?;

        let brightness_curves: HashMap<DeviceId, BrightnessCurve> = serde_json::from_value(
            config
                .config
                .get("brightness_curves")
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|e| VirtualDeviceError::Config(format!("Invalid brightness curves: {}", e)))?;

        // Validate that all lights have curves
        for light_id in &lights {
            if !brightness_curves.contains_key(light_id) {
                return Err(VirtualDeviceError::Config(format!(
                    "Missing brightness curve for light: {}",
                    light_id
                )));
            }
        }

        Ok(Self {
            config,
            lights,
            brightness_curves,
            current_state: LightState {
                is_on: false,
                brightness: Some(0),
                color_temp: Some(2700),
                rgb_color: None,
            },
            state_store,
        })
    }

    /// Apply brightness curves to all member lights
    async fn apply_brightness_curves(&self) -> Result<(), VirtualDeviceError> {
        for light_id in &self.lights {
            let curve = &self.brightness_curves[light_id];
            let device_brightness = if self.current_state.is_on {
                curve.interpolate(self.current_state.brightness.unwrap_or(100))
            } else {
                0
            };

            let device_state = LightState {
                is_on: device_brightness > 0,
                brightness: Some(device_brightness),
                color_temp: self.current_state.color_temp,
                rgb_color: self.current_state.rgb_color.clone(),
            };

            // Set physical light state
            self.state_store
                .update_device_state(light_id, DeviceStateValue::Light(device_state))
                .await?;
        }

        Ok(())
    }

    /// Calculate group state from member light states
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut total_brightness = 0u32;
        let mut lights_on = 0;
        let mut any_on = false;

        for light_id in &self.lights {
            if let Some(device_state) = self.state_store.get_device(light_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    if light_state.is_on {
                        any_on = true;
                        lights_on += 1;
                        total_brightness += light_state.brightness.unwrap_or(100) as u32;
                    }
                }
            }
        }

        self.current_state.is_on = any_on;
        if lights_on > 0 {
            self.current_state.brightness = Some((total_brightness / lights_on as u32) as u8);
        } else {
            self.current_state.brightness = Some(0);
        }

        Ok(())
    }
}

#[async_trait]
impl VirtualDevice for LightGroup {
    fn device_id(&self) -> &DeviceId {
        &self.config.device_id
    }

    fn device_type(&self) -> VirtualDeviceType {
        VirtualDeviceType::LightGroup
    }

    fn config(&self) -> &VirtualDeviceConfig {
        &self.config
    }

    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        match new_state {
            DeviceStateValue::Light(light_state) => {
                // Update virtual group state
                self.current_state = light_state;

                // Apply to all member lights using brightness curves
                self.apply_brightness_curves().await?;

                Ok(())
            }
            _ => Err(VirtualDeviceError::InvalidStateType),
        }
    }

    async fn on_input_changed(
        &mut self,
        device_id: &DeviceId,
        _new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        // Only react to changes from our member lights
        if !self.lights.contains(device_id) {
            return Ok(());
        }

        // Recalculate group state based on member light changes
        self.calculate_group_state().await?;

        Ok(())
    }

    fn current_state(&self) -> DeviceStateValue {
        DeviceStateValue::Light(self.current_state.clone())
    }

    fn input_devices(&self) -> Vec<DeviceId> {
        self.lights.clone()
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        self.lights.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_brightness_curve_interpolation() {
        let curve = BrightnessCurve {
            breakpoints: vec![(0, 0), (25, 50), (75, 75), (100, 100)],
        };

        assert_eq!(curve.interpolate(0), 0);
        assert_eq!(curve.interpolate(12), 24); // ~halfway between 0-25: 50*12/25 = 24 (floored)
        assert_eq!(curve.interpolate(25), 50);
        assert_eq!(curve.interpolate(50), 63); // Halfway between 25-75: 62.5 rounds to 63
        assert_eq!(curve.interpolate(75), 75);
        assert_eq!(curve.interpolate(100), 100);
    }

    #[test]
    fn test_brightness_curve_edge_cases() {
        let curve = BrightnessCurve {
            breakpoints: vec![(20, 100), (80, 0)],
        };

        assert_eq!(curve.interpolate(0), 100); // Below first point
        assert_eq!(curve.interpolate(50), 50); // Midpoint
        assert_eq!(curve.interpolate(100), 0); // Above last point
    }
}
