// 🔥 LINEAR LIGHT GROUP - IMPROVED BRIGHTNESS MAPPING! 💖

use crate::light_group::fanned_out;
use crate::virtual_device::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use v1bectl_sync::*;

/// Light Group with Linear Brightness Mapping
/// Maps input brightness (0-100) to member-specific ranges [min, max]
pub struct LightGroupLinear {
    config: VirtualDeviceConfig,
    members: HashMap<String, String>, // name -> device_id
    brightness_ranges: HashMap<String, (u8, u8)>, // name -> (min, max)
    current_state: LightState,
    state_store: Arc<StateStore>,
}

impl LightGroupLinear {
    pub fn new(
        config: VirtualDeviceConfig,
        members: HashMap<String, String>,
        brightness_ranges: HashMap<String, (u8, u8)>,
        state_store: Arc<StateStore>,
    ) -> Result<Self, VirtualDeviceError> {
        // Validate all members have brightness ranges
        for name in members.keys() {
            if !brightness_ranges.contains_key(name) {
                return Err(VirtualDeviceError::Config(format!(
                    "Missing brightness range for member: {}",
                    name
                )));
            }
        }

        Ok(Self {
            config,
            members,
            brightness_ranges,
            current_state: LightState {
                is_on: false,
                brightness: Some(0),
                color_temp: Some(2700),
                rgb_color: None,
            },
            state_store,
        })
    }

    /// Map group brightness to member brightness using linear interpolation
    fn map_brightness(&self, member_name: &str, group_brightness: u8) -> u8 {
        if let Some((min, max)) = self.brightness_ranges.get(member_name) {
            // Linear mapping from 0-100 to [min, max]
            if group_brightness == 0 {
                return 0; // Off is always off
            }

            // Map 1-100 to min-max range
            let range = (*max as f32) - (*min as f32);
            let normalized = (group_brightness as f32) / 100.0;
            let mapped = (*min as f32) + (normalized * range);

            // Clamp to valid range
            mapped.round().clamp(0.0, 100.0) as u8
        } else {
            group_brightness // Fallback to direct mapping
        }
    }

    /// What the group state `group` fans out to the member called `name`.
    fn member_state(&self, name: &str, group: &LightState) -> LightState {
        let member_brightness = if group.is_on {
            self.map_brightness(name, group.brightness.unwrap_or(100))
        } else {
            0
        };

        LightState {
            is_on: member_brightness > 0,
            brightness: Some(member_brightness),
            color_temp: group.color_temp,
            rgb_color: group.rgb_color.clone(),
        }
    }

    /// Apply the mapped brightness for the group state `group` to all member lights
    async fn apply_brightness_mapping(&self, group: &LightState) -> Result<(), VirtualDeviceError> {
        for (name, device_id) in &self.members {
            let device_state = self.member_state(name, group);
            let member_brightness = device_state.brightness.unwrap_or(0);

            // Update member light state
            self.state_store
                .update_device_state(device_id, DeviceStateValue::Light(device_state))
                .await?;

            tracing::debug!(
                "🔥 Updated {} ({}) to {}% (group: {}%)",
                name,
                device_id,
                member_brightness,
                group.brightness.unwrap_or(100)
            );
        }

        Ok(())
    }

    /// Calculate group state from member states (inverse mapping)
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut total_brightness = 0u32;
        let mut lights_on = 0;
        let mut any_on = false;

        for device_id in self.members.values() {
            if let Some(device_state) = self.state_store.get_device(device_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    if light_state.is_on {
                        any_on = true;
                        lights_on += 1;
                        // TODO: Inverse map member brightness to group brightness
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
impl VirtualDevice for LightGroupLinear {
    fn device_id(&self) -> &DeviceId {
        &self.config.device_id
    }

    fn device_type(&self) -> VirtualDeviceType {
        VirtualDeviceType::LightGroupLinear
    }

    fn config(&self) -> &VirtualDeviceConfig {
        &self.config
    }

    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        match new_state {
            DeviceStateValue::Light(light_state) => {
                // Apply linear brightness mapping to all members, and only
                // then take the new state: if a member fails, the group keeps
                // its old one (the manager then re-derives it from the
                // members that did change).
                self.apply_brightness_mapping(&light_state).await?;
                self.current_state = light_state;

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
        if !self.members.values().any(|id| id == device_id) {
            return Ok(());
        }

        // Recalculate group state based on member changes
        self.calculate_group_state().await?;

        Ok(())
    }

    fn accounts_for(&self, input: &DeviceId, state: &DeviceStateValue) -> bool {
        self.members.iter().any(|(name, device_id)| {
            device_id == input && fanned_out(&self.member_state(name, &self.current_state), state)
        })
    }

    fn current_state(&self) -> DeviceStateValue {
        DeviceStateValue::Light(self.current_state.clone())
    }

    fn input_devices(&self) -> Vec<DeviceId> {
        self.members.values().cloned().collect()
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        self.members.values().cloned().collect()
    }
}
