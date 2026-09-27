use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use v1bectl_sync::{DeviceId, DeviceState, DeviceStateValue, LightState, StateStore};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrightnessCurve {
    /// Breakpoints as (`group_brightness`, `device_brightness`) pairs
    pub breakpoints: Vec<(u8, u8)>,
}

impl BrightnessCurve {
    /// Interpolate device brightness from group brightness using curve
    #[must_use]
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

                let ratio = f32::from(group_brightness - gb1) / f32::from(gb2 - gb1);
                let interpolated = f32::from(db1) + ratio * (f32::from(db2) - f32::from(db1));
                // `ratio` is in [0.0, 1.0] (checked above) and db1/db2 are
                // u8, so `interpolated` is bounded within [db1, db2] (or
                // [db2, db1]) and the round-trip back to u8 never truncates
                // or loses sign; clippy can't see that boundedness.
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "interpolated is a linear interpolation between two u8 endpoints, always in u8 range"
                )]
                let result = interpolated.round() as u8;
                return result;
            }
        }

        group_brightness
    }
}

/// Whether a member that holds `actual` is where fanning `target` out to it
/// would put it, as far as re-deriving its group goes: on/off, and the level
/// while on. That is all a re-derive reads. Colour doesn't count: it's never
/// derived back into a group, and a hub reports it its own way (a bulb
/// without colour temperature has none). Nor does the level of a light
/// that's off.
pub(crate) fn fanned_out(target: &LightState, actual: &DeviceStateValue) -> bool {
    let DeviceStateValue::Light(actual) = actual else {
        return false;
    };
    actual.is_on == target.is_on && (!target.is_on || actual.brightness == target.brightness)
}

/// The level a group starts at when none of its members is on (#16), so a
/// plain `on` right after start lights them. A group only uses it until it
/// has a level of its own, and with none there is nothing to restore. Full
/// brightness is what a plain wall switch gives the room; anything lower
/// would be a guessed dim. At 100 every member of a linear group lights, at
/// the top of its range.
pub const DEFAULT_GROUP_LEVEL: u8 = 100;

/// The level of the group state `group`: its brightness, or
/// [`DEFAULT_GROUP_LEVEL`] if it has none (or 0).
fn level_of(group: &LightState) -> u8 {
    group
        .brightness
        .filter(|&level| level > 0)
        .unwrap_or(DEFAULT_GROUP_LEVEL)
}

/// The state a group takes for the write `asked`, when it was at `current`.
///
/// A group always has a level, on or off: the last non-zero one it was set
/// to or derived (#16). So its state says what the next plain `on` lights,
/// and a client that merges `is_on: true` into it, as the API does, gets
/// exactly that. `on` without a level restores it. A level of 0 is off, and
/// keeps it for the next `on`.
pub(crate) fn resolve_write(current: &LightState, asked: LightState) -> LightState {
    match asked.brightness {
        Some(level) if level > 0 => asked,
        Some(_) => LightState {
            is_on: false,
            brightness: Some(level_of(current)),
            ..asked
        },
        None => LightState {
            brightness: Some(level_of(current)),
            ..asked
        },
    }
}

/// Re-derive the group state `group` from `on_levels`, the levels of its
/// members that are on: on if any of them is lit, at the average level of
/// those that are. With none lit it only goes off and keeps its level
/// (#16). Re-deriving it to 0 would make the next plain `on` light nothing.
///
/// A member that's on at level 0 isn't lit (#34 review, nit 3). The TUI's
/// `-` can leave a light there. Counting it made a group whose members were
/// all at 0 `{on, <old level>}`, with nothing lit. Averaging it in would
/// dim the group below its lit members.
pub(crate) fn re_derive(group: &mut LightState, on_levels: &[u8]) {
    let lit: Vec<u32> = on_levels
        .iter()
        .filter(|&&level| level > 0)
        .map(|&level| u32::from(level))
        .collect();
    group.is_on = !lit.is_empty();
    let average = u32::try_from(lit.len())
        .ok()
        .and_then(|count| lit.iter().sum::<u32>().checked_div(count))
        .and_then(|average| u8::try_from(average).ok());
    if let Some(average) = average {
        group.brightness = Some(average);
    }
}

/// What a group starts as, before [`VirtualDevice::seed_from_inputs`]: off,
/// at [`DEFAULT_GROUP_LEVEL`].
pub(crate) fn initial_group_state() -> LightState {
    LightState {
        is_on: false,
        brightness: Some(DEFAULT_GROUP_LEVEL),
        color_temp: Some(2700),
        rgb_color: None,
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
                .map_err(|e| VirtualDeviceError::Config(format!("Invalid lights config: {e}")))?;

        let brightness_curves: HashMap<DeviceId, BrightnessCurve> = serde_json::from_value(
            config
                .config
                .get("brightness_curves")
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|e| VirtualDeviceError::Config(format!("Invalid brightness curves: {e}")))?;

        // Validate that all lights have curves
        for light_id in &lights {
            if !brightness_curves.contains_key(light_id) {
                return Err(VirtualDeviceError::Config(format!(
                    "Missing brightness curve for light: {light_id}"
                )));
            }
        }

        Ok(Self {
            config,
            lights,
            brightness_curves,
            current_state: initial_group_state(),
            state_store,
        })
    }

    /// What the group state `group` fans out to `light_id`, one of its lights.
    fn member_state(&self, light_id: &DeviceId, group: &LightState) -> LightState {
        let curve = &self.brightness_curves[light_id];
        let device_brightness = if group.is_on {
            curve.interpolate(group.brightness.unwrap_or(100))
        } else {
            0
        };

        LightState {
            is_on: device_brightness > 0,
            brightness: Some(device_brightness),
            color_temp: group.color_temp,
            rgb_color: group.rgb_color.clone(),
        }
    }

    /// Apply brightness curves for the group state `group` to all member lights
    async fn apply_brightness_curves(&self, group: &LightState) -> Result<(), VirtualDeviceError> {
        for light_id in &self.lights {
            let device_state = self.member_state(light_id, group);

            // Set physical light state
            self.state_store
                .update_device_state(light_id, DeviceStateValue::Light(device_state))
                .await?;
        }

        Ok(())
    }

    /// Calculate group state from member light states (see [`re_derive`]):
    /// with no member lit, the group keeps its level.
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut on_levels = Vec::new();
        for light_id in &self.lights {
            if let Some(device_state) = self.state_store.get_device(light_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    if light_state.is_on {
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
                // Apply to all member lights using brightness curves, and
                // only then take the new state: if a member fails, the group
                // keeps its old one (the manager then re-derives it from the
                // members that did change).
                let light_state = resolve_write(&self.current_state, light_state);
                self.apply_brightness_curves(&light_state).await?;
                self.current_state = light_state;

                Ok(())
            }
            _ => Err(VirtualDeviceError::InvalidStateType),
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
        if !self.lights.contains(device_id) {
            return Ok(());
        }

        // Recalculate group state based on member light changes
        self.calculate_group_state().await?;

        Ok(())
    }

    fn accounts_for(&self, input: &DeviceId, state: &DeviceStateValue) -> bool {
        self.lights.contains(input)
            && fanned_out(&self.member_state(input, &self.current_state), state)
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

    /// #34 review, nit 3: a member that's on at level 0 isn't lit. A group
    /// whose members are all there is off, at the level it had. Before, it
    /// came out `{on, 60}` with nothing lit. Next to a lit member, a member at
    /// 0 doesn't pull the group's level down.
    #[test]
    fn re_derive_counts_members_on_at_level_0_as_unlit() {
        let group = |is_on, level| LightState {
            is_on,
            brightness: Some(level),
            color_temp: Some(2700),
            rgb_color: None,
        };
        for (on_levels, want) in [
            (&[0, 0][..], group(false, 60)),
            (&[][..], group(false, 60)),
            (&[0, 50][..], group(true, 50)),
            (&[40, 80][..], group(true, 60)),
        ] {
            let mut derived = group(false, 60);
            re_derive(&mut derived, on_levels);
            assert_eq!(derived, want, "members on at {on_levels:?}");
        }
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
