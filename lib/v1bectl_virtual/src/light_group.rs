use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType, VirtualWrite,
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

/// Re-derive the group state `group` from `on_levels`, a level for each of
/// its members that's on: the group level its range or curve inverts its
/// own level to ([`invert`], #10, #66). On if any of them is lit, at the
/// average of those levels. With none lit it only goes off and keeps its
/// level (#16). Re-deriving it to 0 would make the next plain `on` light
/// nothing.
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

/// A set of group levels, 1..=100 (bit `n` is level `n`): the levels a
/// member's level can invert to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Levels(u128);

impl Levels {
    /// Every level a lit group can be at.
    const ALL: Self = Self(((1 << 101) - 1) & !1);

    /// The group levels whose fan-out `fan_out` lights a member at `level`,
    /// or nearest to it if none does. So a member dimmed below its range
    /// inverts to the levels that put it at the bottom of it, one turned up
    /// past its range to those that put it at the top. Never empty.
    ///
    /// It scans the fan-out itself, so it holds for any shape of it: rising,
    /// falling (a range with `min > max`, a curve that goes down), flat, or
    /// neither.
    pub(crate) fn inverting(level: u8, fan_out: impl Fn(u8) -> u8) -> Self {
        let mut nearest = u8::MAX;
        let mut levels = 0;
        for group_level in 1..=100 {
            let off_by = fan_out(group_level).abs_diff(level);
            if off_by < nearest {
                nearest = off_by;
                levels = 0;
            }
            if off_by == nearest {
                levels |= 1 << group_level;
            }
        }
        Self(levels)
    }

    /// The levels in both.
    fn and(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// The level nearest `target` of these, the lower of two as near. None
    /// if there are none.
    fn nearest(self, target: u8) -> Option<u8> {
        (1..=100)
            .filter(|&level| self.0 & (1 << level) != 0)
            .min_by_key(|&level: &u8| (level.abs_diff(target), level))
    }
}

#[cfg(test)]
impl Levels {
    /// The set of `levels`.
    pub(crate) fn of(levels: &[u8]) -> Self {
        Self(levels.iter().fold(0, |bits, &level| bits | 1 << level))
    }
}

/// The group level each lit member's level inverts to, for [`re_derive`] to
/// average as it averaged their raw levels before (#10, #66), and the level
/// they all invert to, if they do. `lit` holds, for each member that's lit,
/// the levels that light it where it is ([`Levels::inverting`] over the
/// group's own fan-out: a [`crate::LightGroupLinear`]'s ranges, a
/// [`LightGroup`]'s curves). `set_level` is the group's: the level it last
/// put its members at (a write it took), or found every lit member at (rule
/// 1 below).
///
/// Rounding and clamping give most members several candidates: a range or a
/// curve that climbs slower than the group lights one at the same level for
/// neighbouring group levels, a range that ends above 100 is flat at the
/// top, and a range with `min == max`, or a curve between two breakpoints
/// at the same level, is flat throughout. Which one a member inverts to:
///
/// 1. If some level is a candidate of every lit member, they are exactly
///    where that level puts them, and each inverts to the one of those
///    levels nearest `set_level`. The group reads it, and it becomes the set
///    level. So a group set to a level reads that level back, and one that
///    starts on members some level put where they are reads a level that
///    puts them there.
/// 2. Otherwise each inverts on its own, to its candidate nearest
///    `set_level`. A member still where the group put it inverts to the set
///    level itself, and one moved away (dimmed at the wall) to the level
///    nearest it that puts the member where it is now.
///
/// Ties go to the lower level. A linear range is monotonic, and a curve
/// needn't be, but nothing here relies on either: a fan-out that rises and
/// falls again gives a member candidates on both sides, and it still
/// inverts to the nearest.
///
/// The result depends only on the members and `set_level`, and rule 1 moves
/// the set level to a level that's still a candidate of each. So
/// re-deriving from members that haven't moved lands on the same state.
pub(crate) fn invert(lit: &[Levels], set_level: u8) -> (Vec<u8>, Option<u8>) {
    if lit.is_empty() {
        return (Vec::new(), None);
    }
    let every = lit
        .iter()
        .fold(Levels::ALL, |every, levels| every.and(*levels));
    if let Some(level) = every.nearest(set_level) {
        return (vec![level; lit.len()], Some(level));
    }
    let levels = lit
        .iter()
        .filter_map(|levels| levels.nearest(set_level))
        .collect();
    (levels, None)
}

/// Light Group Virtual Device - controls multiple lights as one unit 💡
///
/// It reads its members from the store, to derive its own state from them,
/// but never writes them: a write to it is a [`VirtualWrite`] that the
/// manager commits (#55). It re-derives its state from theirs by inverting
/// their curves, so a group set to a level reads that level back (#66).
pub struct LightGroup {
    config: VirtualDeviceConfig,
    lights: Vec<DeviceId>,
    brightness_curves: HashMap<DeviceId, BrightnessCurve>,
    current_state: LightState,
    /// The level the group last put its members at (a write it took), or
    /// found every lit member at (a re-derive): the level the members'
    /// levels are inverted towards (see [`invert`]). It starts where the
    /// group's own level does.
    set_level: u8,
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

        // #71: the TOML path only ever builds a 2-point curve from a
        // `min`/`max` pair, checked before it's built (`v1bectl_server`).
        // The API's `CreateVirtualDevice` takes a `LightGroup`'s raw
        // breakpoints straight from the client, so they're checked here
        // too, the same way #68 checks a scene's brightness: every
        // currently-valid curve stays valid — rising, falling (`min >
        // max`), a dip or a plateau — as long as each breakpoint is a
        // brightness, 0..=100, and there are at least two of them to
        // interpolate between.
        for (light_id, curve) in &brightness_curves {
            if curve.breakpoints.len() < 2 {
                return Err(VirtualDeviceError::Config(format!(
                    "{light_id}'s brightness curve needs at least 2 breakpoints, has {}",
                    curve.breakpoints.len()
                )));
            }
            if let Some(&(group_level, device_level)) = curve
                .breakpoints
                .iter()
                .find(|&&(group_level, device_level)| group_level > 100 || device_level > 100)
            {
                return Err(VirtualDeviceError::Config(format!(
                    "{light_id}'s brightness curve is out of range ({group_level}, {device_level}); both values of a breakpoint must be 0-100"
                )));
            }
        }

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
            set_level: DEFAULT_GROUP_LEVEL,
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

    /// Calculate group state from member light states (see [`re_derive`]):
    /// with no member lit, the group keeps its level. Each lit member
    /// counts at the group level its curve inverts its level to
    /// ([`invert`]), not at its own (#66). A member on at level 0 isn't lit
    /// (`re_derive`), so it has nothing to invert.
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut lit = Vec::new();
        for light_id in &self.lights {
            if let Some(device_state) = self.state_store.get_device(light_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    let level = light_state.brightness.unwrap_or(100);
                    if light_state.is_on && level > 0 {
                        let curve = &self.brightness_curves[light_id];
                        lit.push(Levels::inverting(level, |group_level| {
                            curve.interpolate(group_level)
                        }));
                    }
                }
            }
        }

        let (on_levels, every) = invert(&lit, self.set_level);
        re_derive(&mut self.current_state, &on_levels);
        if let Some(level) = every {
            self.set_level = level;
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

    /// Each light at its brightness curve's level for the group state the
    /// write resolves to (`resolve_write`), in the order of its lights.
    async fn plan_write(
        &self,
        new_state: DeviceStateValue,
    ) -> Result<VirtualWrite, VirtualDeviceError> {
        let DeviceStateValue::Light(asked) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        let group = resolve_write(&self.current_state, asked);
        let members = self
            .lights
            .iter()
            .map(|id| {
                let state = DeviceStateValue::Light(self.member_state(id, &group));
                (id.clone(), state)
            })
            .collect();
        Ok(VirtualWrite {
            members,
            state: DeviceStateValue::Light(group),
        })
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        if let DeviceStateValue::Light(group) = state {
            if let Some(level) = group.brightness.filter(|&level| level > 0) {
                self.set_level = level;
            }
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
    use v1bectl_sync::{Capability, DeviceInfo, DeviceType};

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

    /// A member's name, and its curve's breakpoints.
    type Curves = Vec<(String, Vec<(u8, u8)>)>;

    fn light(is_on: bool, brightness: u8) -> LightState {
        LightState {
            is_on,
            brightness: Some(brightness),
            color_temp: Some(2700),
            rgb_color: None,
        }
    }

    fn light_info(device_id: &str) -> DeviceInfo {
        DeviceInfo {
            device_id: device_id.to_string(),
            name: device_id.to_string(),
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
        }
    }

    /// 1:1: a member at the group's own level.
    const IDENTITY: (&str, &[(u8, u8)]) = ("identity", &[(0, 0), (100, 100)]);
    /// What `v1bectl_server` makes of a TOML `light_group` curve with
    /// `min = 10` and `max = 60` (#62): half a level per group level, from
    /// 10 up to 60.
    const TOML: (&str, &[(u8, u8)]) = ("toml", &[(0, 10), (100, 60)]);
    /// Holds 40 from group level 30 to 60.
    const PLATEAU: (&str, &[(u8, u8)]) = ("plateau", &[(0, 0), (30, 40), (60, 40), (100, 100)]);
    /// Dims to 20 at 50, and brightens again: a curve needn't rise.
    const DIP: (&str, &[(u8, u8)]) = ("dip", &[(0, 100), (50, 20), (100, 100)]);

    fn curves(curves: &[(&str, &[(u8, u8)])]) -> Curves {
        curves
            .iter()
            .map(|&(name, breakpoints)| (name.to_string(), breakpoints.to_vec()))
            .collect()
    }

    /// Hand-made groups: one over each curve, and one over all of them.
    fn groups() -> Vec<(&'static str, Curves)> {
        vec![
            ("identity", curves(&[IDENTITY])),
            ("toml", curves(&[TOML])),
            ("plateau", curves(&[PLATEAU])),
            ("dip", curves(&[DIP])),
            ("all four", curves(&[IDENTITY, TOML, PLATEAU, DIP])),
        ]
    }

    /// A light group over `curves`, each member a light named after its
    /// curve, off in a new store. Its config is the one `v1bectl_server`
    /// and the API's `CreateVirtualDevice` build.
    async fn group_over(curves: &Curves) -> (LightGroup, Arc<StateStore>) {
        let store = StateStore::new();
        for (name, _) in curves {
            let off = DeviceStateValue::Light(light(false, 0));
            store.add_device(light_info(name), off).await;
        }
        let lights: Vec<&String> = curves.iter().map(|(name, _)| name).collect();
        let brightness_curves: serde_json::Map<String, serde_json::Value> = curves
            .iter()
            .map(|(name, breakpoints)| {
                let curve = serde_json::json!({ "breakpoints": breakpoints });
                (name.clone(), curve)
            })
            .collect();
        let config = VirtualDeviceConfig {
            device_id: "g".to_string(),
            device_type: VirtualDeviceType::LightGroup,
            name: "g".to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({
                "lights": lights,
                "brightness_curves": brightness_curves,
            }),
        };
        let group = LightGroup::new(config, Arc::clone(&store)).expect("light group");
        (group, store)
    }

    /// Where the curve of `name`, one of the members of `group`, puts it
    /// at the group level `level`.
    fn fan_out(group: &LightGroup, name: &str, level: u8) -> u8 {
        group.brightness_curves[name].interpolate(level)
    }

    /// Put each member where `write` puts it, in the store.
    async fn commit_members(store: &StateStore, write: VirtualWrite) {
        for (device_id, state) in write.members {
            store
                .update_device_state(&device_id, state)
                .await
                .expect("member");
        }
    }

    /// `group` set to `asked`: its members where the write puts them, and
    /// its new state taken, as the manager commits a write.
    async fn set(group: &mut LightGroup, store: &StateStore, asked: LightState) {
        let write = group
            .plan_write(DeviceStateValue::Light(asked))
            .await
            .expect("plan");
        group.take_state(write.state.clone());
        commit_members(store, write).await;
    }

    /// `name`, one of the members of `group`, moved to `state` from outside
    /// (the wall), and the group re-derived from it.
    async fn moved(group: &mut LightGroup, store: &StateStore, name: &str, state: LightState) {
        let id = name.to_string();
        let state = DeviceStateValue::Light(state);
        store.update_device_state(&id, state).await.expect("member");
        let input = store.get_device(&id).await.expect("member");
        group
            .on_input_changed(&id, &input)
            .await
            .expect("re-derive");
    }

    /// #66: a group set to any level, 0 to 100, reads that level back when
    /// it re-derives from the members the write put where they are: each
    /// member inverts to the level it was set from. Before, the members'
    /// own levels were averaged: "all four" at 50 (`identity` at 50, `toml`
    /// at 35, `plateau` at 40, `dip` at 20) read back as 36. (A group set to
    /// 0 goes off, keeping its level.)
    #[tokio::test]
    async fn a_group_set_to_a_level_reads_it_back_from_its_members() {
        for (name, curves) in groups() {
            let (mut group, store) = group_over(&curves).await;
            for level in 0..=100 {
                set(&mut group, &store, light(true, level)).await;
                let state = group.current_state();

                group.calculate_group_state().await.expect("re-derive");
                assert_eq!(group.current_state(), state, "{name} set to {level}");
            }
        }
    }

    /// A group that starts on members some level put where they are (the
    /// server restarted since) has only the level it starts at, 100, to
    /// invert towards. It reads the level nearest 100 that puts every lit
    /// member where it is: the highest. That's the level it was set to, or
    /// one that lights them the same (`dip` lights a member the same on
    /// both sides of 50), so it accounts for each of them.
    #[tokio::test]
    async fn a_group_started_on_its_members_reads_the_highest_level_that_puts_them_there() {
        for (name, curves) in groups() {
            for level in 1..=100 {
                let (mut group, store) = group_over(&curves).await;
                let asked = DeviceStateValue::Light(light(true, level));
                commit_members(&store, group.plan_write(asked).await.expect("plan")).await;
                // Where `at` puts the members that `level` lights.
                let fanned = |at: u8| -> Vec<u8> {
                    curves
                        .iter()
                        .filter(|(member, _)| fan_out(&group, member, level) > 0)
                        .map(|(member, _)| fan_out(&group, member, at))
                        .collect()
                };
                let highest = (level..=100)
                    .rev()
                    .find(|&at| fanned(at) == fanned(level))
                    .expect("`level` itself puts them there");
                let lit = !fanned(level).is_empty();

                group.seed_from_inputs().await.expect("seed");
                let want = if lit {
                    light(true, highest)
                } else {
                    light(false, DEFAULT_GROUP_LEVEL)
                };
                assert_eq!(
                    group.current_state(),
                    DeviceStateValue::Light(want),
                    "{name} started on members at {level}"
                );
                // A member that's off doesn't count (`re_derive`).
                for (member, _) in &curves {
                    if fan_out(&group, member, level) == 0 {
                        continue;
                    }
                    let input = store.get_device(member).await.expect("member").state;
                    assert!(
                        group.accounts_for(member, &input),
                        "{name} started at {level} doesn't account for {member}"
                    );
                }
            }
        }
    }

    /// The group levels a member's level inverts to: those its curve lights
    /// it at, and below or above what it can reach, those that light it
    /// nearest. `toml` (10-60) moves one level for every two of the
    /// group's, and `plateau` holds 40 from 30 to 60.
    #[tokio::test]
    async fn a_member_level_inverts_to_the_group_levels_its_curve_lights_it_at() {
        let (group, _store) = group_over(&curves(&[TOML, PLATEAU])).await;
        let toml = |level| fan_out(&group, "toml", level);
        let plateau = |level| fan_out(&group, "plateau", level);
        let levels = Levels::of;

        assert_eq!(Levels::inverting(35, toml), levels(&[49, 50]));
        assert_eq!(Levels::inverting(20, toml), levels(&[19, 20]));
        assert_eq!(Levels::inverting(5, toml), levels(&[1, 2]), "below");
        assert_eq!(Levels::inverting(90, toml), levels(&[99, 100]), "above");
        let from_30_to_60: Vec<u8> = (30..=60).collect();
        assert_eq!(Levels::inverting(40, plateau), levels(&from_30_to_60));
    }

    /// #66: the group over `toml`, `plateau` and `identity`, set to 50, puts
    /// them at 35, 40 and 50. Dimming `toml` to 20 at the wall re-derives
    /// the group: 20 is where 19 and 20 put `toml`, which inverts to the
    /// one nearer 50, 20. `plateau` and `identity` are still where 50 put
    /// them and invert to it. (20 + 50 + 50) / 3 is 40. Before, it was the
    /// members' own average, (20 + 40 + 50) / 3 = 36. Re-deriving again
    /// changes nothing, and turning `toml` back to 35 reads 50 again: the
    /// group still inverts towards the level it was set to.
    #[tokio::test]
    async fn a_member_dimmed_at_the_wall_counts_at_the_level_that_puts_it_there() {
        let (mut group, store) = group_over(&curves(&[TOML, PLATEAU, IDENTITY])).await;
        set(&mut group, &store, light(true, 50)).await;
        for (member, level) in [("toml", 35), ("plateau", 40), ("identity", 50)] {
            let state = store.get_device(&member.to_string()).await.expect(member);
            assert_eq!(state.state, DeviceStateValue::Light(light(true, level)));
        }

        moved(&mut group, &store, "toml", light(true, 20)).await;
        let at_40 = DeviceStateValue::Light(light(true, 40));
        assert_eq!(group.current_state(), at_40, "toml dimmed to 20");
        group.calculate_group_state().await.expect("re-derive");
        assert_eq!(group.current_state(), at_40, "re-deriving again moved it");

        moved(&mut group, &store, "toml", light(true, 35)).await;
        let at_50 = DeviceStateValue::Light(light(true, 50));
        assert_eq!(group.current_state(), at_50, "toml back at 35");
    }

    /// A member turned off at the wall leaves the others to say where the
    /// group is. Before, the group over `toml`, `plateau` and `identity` at
    /// 50 with `identity` off read as 37, the average of 35 and 40.
    #[tokio::test]
    async fn a_member_turned_off_leaves_the_group_at_its_level() {
        let (mut group, store) = group_over(&curves(&[TOML, PLATEAU, IDENTITY])).await;
        set(&mut group, &store, light(true, 50)).await;

        moved(&mut group, &store, "identity", light(false, 0)).await;
        assert_eq!(
            group.current_state(),
            DeviceStateValue::Light(light(true, 50))
        );
    }

    /// A group that finds every lit member where one level puts them (rule
    /// 1 of [`invert`]) takes that level as its set level, as a write would
    /// have. The group over `toml`, `plateau` and `identity` starts on
    /// members where 50 put them (35, 40 and 50), and reads 50. Dimming
    /// `identity` to 20 at the wall then inverts `toml` (49, 50) and
    /// `plateau` (30 to 60) to 50, the level found: (50 + 50 + 20) / 3 is
    /// 40. Inverting towards the level it started at, 100, instead would
    /// put `plateau` at 60 and read 43.
    #[tokio::test]
    async fn a_group_inverts_towards_the_level_it_found_its_members_at() {
        let (mut group, store) = group_over(&curves(&[TOML, PLATEAU, IDENTITY])).await;
        let at_50 = DeviceStateValue::Light(light(true, 50));
        commit_members(&store, group.plan_write(at_50.clone()).await.expect("plan")).await;
        group.seed_from_inputs().await.expect("seed");
        assert_eq!(group.current_state(), at_50, "started on members at 50");

        moved(&mut group, &store, "identity", light(true, 20)).await;
        assert_eq!(
            group.current_state(),
            DeviceStateValue::Light(light(true, 40)),
            "identity dimmed to 20"
        );
    }

    /// A fan-out that falls and rises again (a V, dimmest at 50) gives a
    /// member candidates on both sides. It inverts to the one nearest the
    /// set level, the lower of two as near. A linear range can't do this,
    /// and a curve can (`DIP`), but nothing in the inversion relies on
    /// either.
    #[test]
    fn a_fan_out_that_falls_and_rises_inverts_to_the_nearest_candidate() {
        let v = |level: u8| level.abs_diff(50) * 2;
        let at_20 = Levels::inverting(20, v);
        assert_eq!(at_20, Levels(1 << 40 | 1 << 60));
        assert_eq!(at_20.nearest(55), Some(60));
        assert_eq!(at_20.nearest(50), Some(40), "a tie goes to the lower");
        assert_eq!(at_20.nearest(0), Some(40));
        // Out of reach: 98 (at levels 1 and 99) and 100 (at level 100) are
        // as near. Levels 1 and 99 are as near 50, too.
        let at_99 = Levels::inverting(99, v);
        assert_eq!(at_99, Levels(1 << 1 | 1 << 99 | 1 << 100));
        assert_eq!(at_99.nearest(50), Some(1));
        assert_eq!(at_99.nearest(100), Some(100));

        assert_eq!(invert(&[at_20], 55), (vec![60], Some(60)));
        // Nothing lights both where they are: each on its own.
        let at_100 = Levels::inverting(100, v);
        assert_eq!(invert(&[at_20, at_100], 55), (vec![60, 100], None));
        assert_eq!(invert(&[], 55), (vec![], None));
    }
}
