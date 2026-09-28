// 🔥 LINEAR LIGHT GROUP - IMPROVED BRIGHTNESS MAPPING! 💖

use crate::light_group::{fanned_out, initial_group_state, re_derive, resolve_write};
use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType, VirtualWrite,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use v1bectl_sync::{DeviceId, DeviceState, DeviceStateValue, LightState, StateStore};

/// Light Group with Linear Brightness Mapping
/// Maps input brightness (0-100) to member-specific ranges [min, max]
///
/// Like [`crate::LightGroup`], it reads its members from the store but
/// never writes them: a write to it is a [`VirtualWrite`] that the manager
/// commits (#55).
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
                    "Missing brightness range for member: {name}"
                )));
            }
        }

        Ok(Self {
            config,
            members,
            brightness_ranges,
            current_state: initial_group_state(),
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
            let range = f32::from(*max) - f32::from(*min);
            let normalized = f32::from(group_brightness) / 100.0;
            let mapped = f32::from(*min) + (normalized * range);

            // Clamp to valid range, then cast: the clamp bounds `mapped` to
            // [0.0, 100.0], so the cast to u8 never truncates or loses sign.
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "just clamped to [0.0, 100.0] above, so this is always in u8 range"
            )]
            let result = mapped.round().clamp(0.0, 100.0) as u8;
            result
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

    /// Calculate group state from member states (inverse mapping, see
    /// [`re_derive`]): with no member lit, the group keeps its level.
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut on_levels = Vec::new();
        for device_id in self.members.values() {
            if let Some(device_state) = self.state_store.get_device(device_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    if light_state.is_on {
                        // TODO: Inverse map member brightness to group brightness
                        on_levels.push(light_state.brightness.unwrap_or(100));
                    }
                }
            }
        }

        re_derive(&mut self.current_state, &on_levels);
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

    /// Each member at its range's level for the group state the write
    /// resolves to (`resolve_write`).
    async fn plan_write(
        &self,
        new_state: DeviceStateValue,
    ) -> Result<VirtualWrite, VirtualDeviceError> {
        let DeviceStateValue::Light(asked) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        let group = resolve_write(&self.current_state, asked);
        let members = self
            .members
            .iter()
            .map(|(name, device_id)| {
                let state = self.member_state(name, &group);
                tracing::debug!(
                    "🔥 {} ({}) goes to {}% (group: {}%)",
                    name,
                    device_id,
                    state.brightness.unwrap_or(0),
                    group.brightness.unwrap_or(100)
                );
                (device_id.clone(), DeviceStateValue::Light(state))
            })
            .collect();
        Ok(VirtualWrite {
            members,
            state: DeviceStateValue::Light(group),
        })
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        if let DeviceStateValue::Light(group) = state {
            self.current_state = group;
        }
    }

    async fn seed_from_inputs(&mut self) -> Result<(), VirtualDeviceError> {
        self.calculate_group_state().await
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
