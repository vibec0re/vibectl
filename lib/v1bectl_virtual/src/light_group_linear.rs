// 🔥 LINEAR LIGHT GROUP - IMPROVED BRIGHTNESS MAPPING! 💖

use crate::light_group::{
    fanned_out, initial_group_state, re_derive, resolve_write, DEFAULT_GROUP_LEVEL,
};
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
/// commits (#55). It re-derives its state from theirs by inverting their
/// ranges, so a group set to a level reads that level back (#10).
pub struct LightGroupLinear {
    config: VirtualDeviceConfig,
    members: HashMap<String, String>, // name -> device_id
    brightness_ranges: HashMap<String, (u8, u8)>, // name -> (min, max)
    current_state: LightState,
    /// The level the group last put its members at (a write it took), or
    /// found every lit member at (a re-derive): the level the members'
    /// levels are inverted towards (see [`invert`]). It starts where the
    /// group's own level does.
    set_level: u8,
    state_store: Arc<StateStore>,
}

/// A set of group levels, 1..=100 (bit `n` is level `n`): the levels a
/// member's level can invert to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Levels(u128);

impl Levels {
    /// Every level a lit group can be at.
    const ALL: Self = Self(((1 << 101) - 1) & !1);

    /// The group levels whose fan-out `fan_out` lights a member at `level`,
    /// or nearest to it if none does. So a member dimmed below its range
    /// inverts to the levels that put it at the bottom of it, one turned up
    /// past its range to those that put it at the top. Never empty.
    ///
    /// It scans the fan-out itself, so it holds for any shape of it: rising,
    /// falling (a range with `min > max`), flat, or neither.
    fn inverting(level: u8, fan_out: impl Fn(u8) -> u8) -> Self {
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

/// The group level each lit member's level inverts to, for [`re_derive`] to
/// average as it averaged their raw levels before (#10), and the level they
/// all invert to, if they do. `lit` holds, for each member that's lit, the
/// levels that light it where it is ([`Levels::inverting`]). `set_level` is
/// the group's (see [`LightGroupLinear`]).
///
/// Rounding and clamping give most members several candidates: a range
/// narrower than 0..=100 lights one at the same level for neighbouring group
/// levels, one that ends above 100 is flat at the top, and one with
/// `min == max` is flat throughout. Which one a member inverts to:
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
/// Ties go to the lower level. A linear range is monotonic, but nothing
/// here relies on that: a fan-out that rises and falls again gives a member
/// candidates on both sides, and it still inverts to the nearest.
///
/// The result depends only on the members and `set_level`, and rule 1 moves
/// the set level to a level that's still a candidate of each. So
/// re-deriving from members that haven't moved lands on the same state.
fn invert(lit: &[Levels], set_level: u8) -> (Vec<u8>, Option<u8>) {
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
            set_level: DEFAULT_GROUP_LEVEL,
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

    /// Calculate group state from member states (see [`re_derive`]): with
    /// no member lit, the group keeps its level. Each lit member counts at
    /// the group level its range inverts its level to ([`invert`]), not at
    /// its own (#10). A member on at level 0 isn't lit (`re_derive`), so it
    /// has nothing to invert.
    async fn calculate_group_state(&mut self) -> Result<(), VirtualDeviceError> {
        let mut lit = Vec::new();
        for (name, device_id) in &self.members {
            if let Some(device_state) = self.state_store.get_device(device_id).await {
                if let DeviceStateValue::Light(light_state) = device_state.state {
                    let level = light_state.brightness.unwrap_or(100);
                    if light_state.is_on && level > 0 {
                        lit.push(Levels::inverting(level, |group_level| {
                            self.map_brightness(name, group_level)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VirtualDeviceTomlConfig;
    use std::path::Path;
    use v1bectl_sync::{Capability, DeviceInfo, DeviceType};

    /// Member name and `[min, max]` range, as a config has them.
    type Ranges = Vec<(String, (u8, u8))>;

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

    /// The ranges of the shipped `virtual_devices/bedroom_lights.toml`,
    /// read from the file: `top` 80-100, `main` 40-90, `bed` 0-50.
    fn bedroom_lights() -> Ranges {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../virtual_devices/bedroom_lights.toml");
        let toml = std::fs::read_to_string(&path).expect("bedroom_lights.toml");
        match toml::from_str(&toml) {
            Ok(VirtualDeviceTomlConfig::LightGroupLinear(c)) => c
                .brightness
                .into_iter()
                .map(|(name, [min, max])| (name, (min, max)))
                .collect(),
            other => panic!("bedroom_lights.toml is no linear group: {other:?}"),
        }
    }

    fn ranges(ranges: &[(&str, (u8, u8))]) -> Ranges {
        ranges
            .iter()
            .map(|&(name, range)| (name.to_string(), range))
            .collect()
    }

    const IDENTITY: (&str, (u8, u8)) = ("identity", (0, 100));
    /// 2.5 member levels per group level, and flat at 100 from 40 up.
    const STEEP: (&str, (u8, u8)) = ("steep", (0, 250));
    /// 70 at every group level.
    const FLAT: (&str, (u8, u8)) = ("flat", (70, 70));
    /// Dims as the group brightens.
    const FALLING: (&str, (u8, u8)) = ("falling", (90, 40));

    /// The shipped Bedroom Lights, and hand-made groups: one over each kind
    /// of range, and one over all of them.
    fn groups() -> Vec<(&'static str, Ranges)> {
        vec![
            ("bedroom_lights.toml", bedroom_lights()),
            ("identity", ranges(&[IDENTITY])),
            ("steep", ranges(&[STEEP])),
            ("flat", ranges(&[FLAT])),
            ("falling", ranges(&[FALLING])),
            ("all four", ranges(&[IDENTITY, STEEP, FLAT, FALLING])),
        ]
    }

    /// A linear group over `ranges`, each member a light named after its
    /// range, off in a new store.
    async fn group_over(ranges: &Ranges) -> (LightGroupLinear, Arc<StateStore>) {
        let store = StateStore::new();
        for (name, _) in ranges {
            let off = DeviceStateValue::Light(light(false, 0));
            store.add_device(light_info(name), off).await;
        }
        let members = ranges
            .iter()
            .map(|(name, _)| (name.clone(), name.clone()))
            .collect();
        let config = VirtualDeviceConfig {
            device_id: "g".to_string(),
            device_type: VirtualDeviceType::LightGroupLinear,
            name: "g".to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let ranges = ranges.iter().cloned().collect();
        let group = LightGroupLinear::new(config, members, ranges, Arc::clone(&store))
            .expect("linear group");
        (group, store)
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
    async fn set(group: &mut LightGroupLinear, store: &StateStore, asked: LightState) {
        let write = group
            .plan_write(DeviceStateValue::Light(asked))
            .await
            .expect("plan");
        group.take_state(write.state.clone());
        commit_members(store, write).await;
    }

    /// `name`, one of the members of `group`, moved to `state` from outside
    /// (the wall), and the group re-derived from it.
    async fn moved(
        group: &mut LightGroupLinear,
        store: &StateStore,
        name: &str,
        state: LightState,
    ) {
        let id = name.to_string();
        let state = DeviceStateValue::Light(state);
        store.update_device_state(&id, state).await.expect("member");
        let input = store.get_device(&id).await.expect("member");
        group
            .on_input_changed(&id, &input)
            .await
            .expect("re-derive");
    }

    /// #10: a group set to any level, 0 to 100, reads that level back when
    /// it re-derives from the members the write put where they are: each
    /// member inverts to the level it was set from. Before, the members'
    /// own levels were averaged, and Bedroom Lights at 50 read back as 60.
    /// (A group set to 0 goes off, keeping its level.)
    #[tokio::test]
    async fn a_group_set_to_a_level_reads_it_back_from_its_members() {
        for (name, ranges) in groups() {
            let (mut group, store) = group_over(&ranges).await;
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
    /// one that lights them the same, so it accounts for each of them.
    #[tokio::test]
    async fn a_group_started_on_its_members_reads_the_highest_level_that_puts_them_there() {
        for (name, ranges) in groups() {
            for level in 1..=100 {
                let (mut group, store) = group_over(&ranges).await;
                let asked = DeviceStateValue::Light(light(true, level));
                commit_members(&store, group.plan_write(asked).await.expect("plan")).await;
                // Where `at` puts the members that `level` lights.
                let fan_out = |at: u8| -> Vec<u8> {
                    ranges
                        .iter()
                        .filter(|(member, _)| group.map_brightness(member, level) > 0)
                        .map(|(member, _)| group.map_brightness(member, at))
                        .collect()
                };
                let highest = (level..=100)
                    .rev()
                    .find(|&at| fan_out(at) == fan_out(level))
                    .expect("`level` itself puts them there");
                let lit = !fan_out(level).is_empty();

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
                for (member, _) in &ranges {
                    if group.map_brightness(member, level) == 0 {
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

    /// #65 review, finding 2: a group that finds every lit member where one
    /// level puts them (rule 1 of [`invert`]) takes that level as its set
    /// level, as a write would have. Bedroom Lights starts on members where
    /// 50 put them (`top` 90, `main` 65, `bed` 25), and reads 50. Dimming
    /// `bed` to 11 at the wall then inverts `top` (48 to 52) and `main` (49,
    /// 50) to 50, the level found, and `bed` (21, 22) to 22: (50 + 50 + 22)
    /// / 3 is 40. Inverting towards the level it started at, 100, instead
    /// would put `top` at 52 and read 41.
    #[tokio::test]
    async fn a_group_inverts_towards_the_level_it_found_its_members_at() {
        let (mut group, store) = group_over(&bedroom_lights()).await;
        let at_50 = DeviceStateValue::Light(light(true, 50));
        commit_members(&store, group.plan_write(at_50.clone()).await.expect("plan")).await;
        group.seed_from_inputs().await.expect("seed");
        assert_eq!(group.current_state(), at_50, "started on members at 50");

        moved(&mut group, &store, "bed", light(true, 11)).await;
        assert_eq!(
            group.current_state(),
            DeviceStateValue::Light(light(true, 40)),
            "bed dimmed to 11"
        );
    }

    /// #10: Bedroom Lights at 50 puts `top` at 90, `main` at 65 and `bed` at
    /// 25. Dimming `bed` to 10 at the wall re-derives the group: `top` and
    /// `main` are still where 50 put them and invert to it, and 10 is where
    /// 19 and 20 put `bed`, which inverts to the nearer, 20. The average is
    /// 40. Before, it was the members' own, 55. Re-deriving again changes
    /// nothing, and turning `bed` back to 25 reads 50 again: the group
    /// still inverts towards the level it was set to.
    #[tokio::test]
    async fn a_member_dimmed_at_the_wall_counts_at_the_level_that_puts_it_there() {
        let (mut group, store) = group_over(&bedroom_lights()).await;
        set(&mut group, &store, light(true, 50)).await;
        for (member, level) in [("top", 90), ("main", 65), ("bed", 25)] {
            let state = store.get_device(&member.to_string()).await.expect(member);
            assert_eq!(state.state, DeviceStateValue::Light(light(true, level)));
        }

        moved(&mut group, &store, "bed", light(true, 10)).await;
        let at_40 = DeviceStateValue::Light(light(true, 40));
        assert_eq!(group.current_state(), at_40, "bed dimmed to 10");
        group.calculate_group_state().await.expect("re-derive");
        assert_eq!(group.current_state(), at_40, "re-deriving again moved it");

        moved(&mut group, &store, "bed", light(true, 25)).await;
        let at_50 = DeviceStateValue::Light(light(true, 50));
        assert_eq!(group.current_state(), at_50, "bed back at 25");
    }

    /// A member turned off at the wall leaves the others to say where the
    /// group is. Before, Bedroom Lights at 50 with `bed` off read as 77,
    /// the average of `top` and `main`.
    #[tokio::test]
    async fn a_member_turned_off_leaves_the_group_at_its_level() {
        let (mut group, store) = group_over(&bedroom_lights()).await;
        set(&mut group, &store, light(true, 50)).await;

        moved(&mut group, &store, "bed", light(false, 0)).await;
        assert_eq!(
            group.current_state(),
            DeviceStateValue::Light(light(true, 50))
        );
    }

    /// The levels that light a member where it is, and below or above the
    /// range it can reach, the bottom or top of it: the shipped `top`
    /// (80-100) moves one level for every five of the group's.
    #[tokio::test]
    async fn a_member_level_inverts_to_the_group_levels_that_light_it_there() {
        let levels = |set: &[u8]| Levels(set.iter().fold(0, |bits, &level| bits | 1 << level));
        let (group, _store) = group_over(&bedroom_lights()).await;
        let top = |level| group.map_brightness("top", level);

        assert_eq!(Levels::inverting(90, top), levels(&[48, 49, 50, 51, 52]));
        assert_eq!(Levels::inverting(81, top), levels(&[3, 4, 5, 6, 7]));
        assert_eq!(Levels::inverting(30, top), levels(&[1, 2]), "below");
        assert_eq!(Levels::inverting(100, top), levels(&[98, 99, 100]));
    }

    /// A fan-out that falls and rises again (a V, dimmest at 50) gives a
    /// member candidates on both sides. It inverts to the one nearest the
    /// set level, the lower of two as near. A linear range can't do this,
    /// but nothing in the inversion relies on that.
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
