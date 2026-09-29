# Virtual Device System Design

## Concept Overview

Virtual devices are software-defined devices that combine multiple physical devices to create new automation behaviors. They exist only in the v1bectl server but appear as regular devices to API clients.

## Core Architecture

### Virtual Device Trait

A virtual device never writes anything itself. A write to it is a **plan**:
the device says what the write takes (its members' new states, in order, and
its own), and the manager **commits** that plan. Only once every member
write is committed does the device take its new state. 🔥

```rust
#[async_trait]
trait VirtualDevice: Send + Sync {
    fn device_id(&self) -> &DeviceId;
    fn device_type(&self) -> VirtualDeviceType;
    fn config(&self) -> &VirtualDeviceConfig;

    // What it takes to set this device to `new_state`. Writes NOTHING:
    // not the store, not its members, not even itself (it's `&self`).
    async fn plan_write(&self, new_state: DeviceStateValue) -> Result<VirtualWrite, VirtualDeviceError>;

    // The same plan as a future that owns what it needs, for a write whose
    // plan waits (a scene's fade or sequence). The manager runs it without
    // its lock. Default: None (plan with `plan_write`, under the lock).
    fn plan_write_detached(&self, new_state: &DeviceStateValue) -> Option<DetachedPlan> { None }

    // Every device a write of `new_state` may write: the manager queues the
    // write on them (and this device) for its whole duration. Default:
    // outputs. Must not list too few (a debug build asserts it).
    fn writes_to(&self, new_state: &DeviceStateValue) -> Vec<DeviceId> { self.output_devices() }

    // The manager committed every member write of a plan: take its state.
    // If one failed, this isn't called, and the device keeps its old state.
    fn take_state(&mut self, state: DeviceStateValue);

    // Take the state its inputs give it, once, when it's registered
    async fn seed_from_inputs(&mut self) -> Result<(), VirtualDeviceError> { Ok(()) }

    // Called when input device states change (re-derive from them)
    async fn on_input_changed(&mut self, device_id: &DeviceId, new_state: &DeviceState) -> Result<(), VirtualDeviceError> { Ok(()) }

    // Whether its state already accounts for `input` at `state`: the echo
    // of its own write, which the manager then doesn't re-derive it from.
    // A light group would read back the level it was set to anyway (#10,
    // #66); a device whose re-derive is lossy would drift.
    fn accounts_for(&self, input: &DeviceId, state: &DeviceStateValue) -> bool { false }

    // The writes it asks for in reaction to an input's event (a button
    // controller's action for a press). The manager makes them.
    fn reactions(&self, event: &DeviceEvent) -> Vec<ButtonAction> { Vec::new() }

    // Get current virtual device state
    fn current_state(&self) -> DeviceStateValue;

    // Which devices this virtual device depends on
    fn input_devices(&self) -> Vec<DeviceId>;

    // Which devices this virtual device controls
    fn output_devices(&self) -> Vec<DeviceId>;
}

// What a write takes
struct VirtualWrite {
    members: Vec<(DeviceId, DeviceStateValue)>, // committed in this order
    state: DeviceStateValue,                    // the device's own, once they all are
}
```

#### ✍️ How a write flows

1. The API (or a button action) calls
   `VirtualDeviceManager::set_virtual_device_state(device_id, new_state)`.
2. Under the manager's `virtual_devices` lock, the device plans the write:
   `plan_write` returns a `VirtualWrite`. Nothing has changed anywhere yet.
   (A plan that waits runs without the lock: see ⏱️ below.)
3. A member that is a virtual device itself (a scene's light group, a group
   in a group) plans its own part of the write, and so on down: the manager
   **expands** the whole write before it commits any of it (see 🪆 below).
4. The manager commits each member write, in order:
   - **A physical member, with the sync engine attached:** the write goes
     through `SyncEngine::apply_optimistic_update`, the call a direct write
     to that light makes. It arms the light's 🛡️ protection window, then
     writes the store, echoes the change and queues the push to the hub.
   - **A physical member with no engine** (the dummy and test setups): the
     manager writes the store and echoes it itself.
   - **A virtual member:** its own members first, the same way, then it
     takes its new state, is stored, and is echoed if the write changed it.
   - A physical member the write leaves as it is is skipped. A member the
     store doesn't have fails the write right there: the members before it
     stay committed, and the ones after it are never written.
5. Every member committed: the manager hands the device its new state
   (`take_state`), stores it and echoes it. If a member failed, the device
   (and every group on the way down to that member) keeps its old state, and
   input tracking re-derives it from what actually happened.

#### 🛡️ Why a device must never write the store itself (#55)

The sync engine's pull worker reconciles the store with the hub every couple
of seconds. It snapshots the store without the sync buffer's lock, and if a
light's stored state differs from what the hub reports with nothing
protecting it, it takes that for an outside change (someone flipped a
physical switch) and puts the hub's value back (`GatewayWins`). What
protects a write is its protection window, and `apply_optimistic_update`
arms it **before** it touches the store.

Light groups used to write their members into the store themselves, and the
manager pushed them through the engine afterwards. A pull that landed in
between found the store ahead of the hub with nothing pending, and reverted
the member. One that landed before the manager read the members back left
them reverted **and** never pushed. 💔 So:

- `plan_write` takes `&self` and writes nothing. The manager is the only
  one that commits, and it hands each physical member to the engine
  *before* anything changes the store. Writing the store first, even just
  before calling the engine, reopens the gap
  (`lib/v1bectl_virtual/tests/commit_order_race.rs` pins that order, and
  `tests/group_write_race.rs` pins "planning writes nothing").
- A device may *read* the store: a group derives its own state from its
  members, and a fade starts from where its lights are.

#### ⏱️ Plans that wait (#58)

A scene's fade or sequence waits between its steps. It hands the manager its
plan from `plan_write_detached`, as a future that owns its scene and the
store, so the manager runs it **without** its `virtual_devices` lock and
takes that lock only to commit where the transition ends.

What keeps writes in order is a 🚦 **write queue**. Every manager write (a
group write, a scene activation, a button action, an add or a removal) joins
it in the order it asks, with the devices it writes: its own, every
member's (`writes_to`), and, through each member that is virtual, every
device that one can write in turn, all the way down (see 🪆 below). It goes
once every write that joined before it and shares a device with it is
over, **whether that one is running or still
waiting itself**, and it stays in the queue until it has committed, a
fade's delays included. So:

- **A write that overlaps a transition** (it shares a light with it, or it's
  to the same scene controller) waits for the transition's commit, and lands
  after it. The newest write ends up showing, exactly as when every write
  queued on the manager's lock: an "all off" pressed 3 s into a 10 s sunset
  fade turns the lights off once the fade has committed, and they stay off.
- **Writes that overlap each other land in the order they asked**, even
  through a write that's still waiting. Press "all on" (a big group, which
  waits for the fade), then switch one of its lamps off: the lamp switches
  off after "all on", and stays off.
- **A write that overlaps nothing in flight** goes ahead at once. A
  one-second fade no longer holds up every other virtual write for its whole
  second. 🔓
- **A write dropped part-way** (a client that went away) leaves the queue at
  once, commits nothing, and never holds up the writes behind it.
- A direct write to a light through the API never went through the manager,
  so it lands at once, and a fade over that light overwrites it when it
  commits, as it always did.

A write waits only for writes that asked before it, and it waits before it
takes the `virtual_devices` lock, which nothing holds while it waits in the
queue. So writes can't deadlock. The queue only ever holds the writes in
flight: each one leaves it when it's over, however it ends. If a device a
waiting write names was *replaced* ahead of it (removed and re-added under
the same id, as a config reload does — a removal alone only drops devices
from what a write names, already covered, so it never forces this), the
write joins again at the back with the devices as they are now. It loses its
place, even on a device it already held, so an older write can end up
landing last; keeping its place would risk a deadlock instead.

Input tracking and resync don't queue, so they don't wait for a fade
themselves. But tracking runs a button press's actions in turn, and a press
that overlaps a fade waits for it. Until the fade commits, the presses and
group re-derivations behind that press wait too. That's how it was before
#58, when tracking waited for every fade.

Cancelling a fade when a newer write to its lights comes in, instead of
making that write wait, would be a possible later improvement. It changes
what shows, so it's the owner's call, and it isn't done.

A write whose plan doesn't wait (a light group, an instant scene) keeps the
default `None`, and is planned and committed in one hold of the lock. A
group needs that: its plan starts from its own state (a plain `on` restores
its level), so no other write may land in between.

#### 🪆 Virtual members: a write fans out through them (#58)

A member of a write can be a virtual device itself: a scene that sets a
light group ("movie: the living room at 30 %"), a group that has groups
among its members, a button action on a group of groups. The write reaches
that member's lights **through the member's own plan**:

1. **Expansion.** Before anything is committed, the manager asks each
   virtual member for its `plan_write` of the state listed for it, and
   expands that plan the same way, down to the physical lights
   (`VirtualDeviceManager::expand`). A member is planned from its state
   before the write, in the same hold of the lock as the commit: only the
   device written may plan without the lock (⏱️ above).
2. **Commit.** Each physical light goes through the engine as always. Each
   virtual member, once its own members are committed, takes its new state
   (`take_state`, so a group's `set_level` moves to the level it was set
   to), is stored, and is echoed if the write changed it, like a physical
   member. The device written is echoed as always. So a scene over a group
   echoes each light once, the group once and the scene once.
3. **Echoes.** It all happens in one hold of the lock, so by the time input
   tracking sees the lights' echoes, the group already accounts for them
   (`accounts_for`): they're its own write, and don't re-derive it. Nor does
   the group's echo re-derive an outer group: the outer one's state already
   puts it there.
4. **The queue.** A write joins the 🚦 queue with its whole transitive device
   set up front (`write_keys`): the device, the members its plan may write
   (`writes_to`), and every device each virtual one among them can write
   (its `output_devices`, since its state isn't known until its parent
   plans it), and theirs. So a scene that sets group `g` waits for a fade of
   one of `g`'s lights, and a later write to that light alone waits for the
   scene: they land in the order they asked, and the newest shows. #60's
   retry works on the same set: if a group the write reaches is replaced
   while it waits, it rejoins with the group's new members.
5. **Cycles and depth.** A write that would reach a virtual device already
   on its path (`A` contains `B` contains `A`) fails with
   `VirtualDeviceError::Cycle`, and one that would go more than
   `MAX_NESTING` (4) levels below the device written fails with
   `VirtualDeviceError::TooDeep`. A virtual member whose plan fails (a scene
   that sets another scene controller as if it were a light) fails the write
   with `VirtualDeviceError::Member`. All three fail **before anything is
   committed**. A device whose members would lead back to it isn't even
   registered: `add_virtual_device` rejects it with the cycle, whichever of
   the two comes second. A device reached through two different members (a
   light in two groups of one scene) is no cycle: it's written each time, in
   order.
6. **Load order doesn't matter.** Whether a member is virtual is resolved
   when a write reaches it, not when the device naming it loads. The files
   in `virtual_devices/` load in name order, but a scene or a group may name
   a group from a file that loads after it, and a scene's checks and its
   TOML mapping treat a virtual device the store already has as one it
   doesn't have yet. So both orders give the same devices, from TOML and
   through the API alike. The manager's "references unknown device" warning
   at `start` is the only load-time signal.

Nesting a group is naming it as a member. Here the downstairs group has the
living room's curved group (`living_room_lights`, the `light_group` example
under Configuration System below) as one of its members, next to the
hallway light:

```toml
type = "light_group_linear"
device_id = "downstairs_lights"
name = "Downstairs"

[members]
living = "living_room_lights"   # a light group: fans out to its own lights
hall = "light_hallway"

[brightness]
living = [20, 100]
hall = [0, 100]
```

### Virtual Device Manager
```rust
struct VirtualDeviceManager {
    virtual_devices: Arc<RwLock<HashMap<DeviceId, Box<dyn VirtualDevice>>>>,
    input_mappings: Arc<RwLock<HashMap<DeviceId, Vec<DeviceId>>>>,  // input -> virtual
    output_mappings: Arc<RwLock<HashMap<DeviceId, Vec<DeviceId>>>>, // virtual -> outputs
    state_store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
    sync_engine: Arc<OnceLock<Arc<SyncEngine>>>, // the physical write path
}

impl VirtualDeviceManager {
    // Called from the API (simplified: see "How a write flows")
    async fn set_virtual_device_state(&self, device_id: &DeviceId, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        let mut devices = self.virtual_devices.write().await;
        let device = devices.get(device_id).ok_or(VirtualDeviceError::DeviceNotFound(device_id.clone()))?;
        let write = device.plan_write(new_state).await?; // writes nothing
        // 🪆 each virtual member plans its part, down to the lights; a cycle,
        // too deep a nesting or a member that can't plan fails here, with
        // nothing committed (#58)
        let expanded = Self::expand(&devices, vec![device_id.clone()], write).await?;
        self.commit_expanded(&mut devices, expanded).await
    }

    // Commit an expanded write: each member in order, then the device's own state
    async fn commit_expanded(&self, devices: &mut Devices, expanded: Expanded) -> Result<(), VirtualDeviceError> {
        for member in expanded.members {
            match member {
                // 🛡️ the engine arms protection, THEN writes the store
                Member::Physical(member_id, state) => self.commit_member_write(&member_id, state).await?,
                // its lights first, then it takes its state (store + echo)
                Member::Virtual(inner) => self.commit_expanded(devices, inner).await?,
            }
        }
        let device = devices.get_mut(&expanded.device_id).unwrap();
        device.take_state(expanded.state);
        self.store_state(&expanded.device_id, device.current_state()).await // store + echo
    }

    // Input tracking: called for every state echo on the event bus
    async fn handle_device_state_change(&self, device_id: &DeviceId, new_state: &DeviceState) {
        let mut devices = self.virtual_devices.write().await;
        for vd_id in self.input_mappings.read().await.get(device_id).into_iter().flatten() {
            if let Some(virtual_device) = devices.get_mut(vd_id) {
                if virtual_device.accounts_for(device_id, &new_state.state) {
                    continue; // the echo of its own write
                }
                let _ = virtual_device.on_input_changed(device_id, new_state).await;
                // ...then store + echo its new state, if it moved
            }
        }
    }
}
```

## Virtual Device Types

### 1. Light Groups
**Purpose**: Control multiple lights as a single unit with intelligent brightness mapping

```rust
struct LightGroup {
    device_id: DeviceId,
    lights: Vec<DeviceId>,
    brightness_curves: HashMap<DeviceId, BrightnessCurve>,
    current_state: LightState,
    set_level: u8,                // what its lights' levels invert towards (#66)
    state_store: Arc<StateStore>, // read-only: derives the group from its lights
}

// Example: Living room group with 3 lights
// 0-25%: Only ambient light
// 25-75%: Ambient + main light  
// 75-100%: All lights at full
struct BrightnessCurve {
    breakpoints: Vec<(u8, u8)>, // (group_brightness, device_brightness)
}

impl VirtualDevice for LightGroup {
    async fn plan_write(&self, new_state: DeviceStateValue) -> Result<VirtualWrite, VirtualDeviceError> {
        let DeviceStateValue::Light(asked) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        // An `on` without a level restores the group's level (#16)
        let group = resolve_write(&self.current_state, asked);

        // Apply brightness curves to each physical light: planned, NOT written!
        let members = self.lights.iter().map(|light_id| {
            let curve = &self.brightness_curves[light_id];
            let device_brightness = if group.is_on {
                curve.interpolate(group.brightness.unwrap_or(100))
            } else {
                0
            };
            let device_state = LightState {
                is_on: device_brightness > 0,
                brightness: Some(device_brightness),
                ..group.clone()
            };
            (light_id.clone(), DeviceStateValue::Light(device_state))
        }).collect();

        // The manager commits the lights through the sync engine, in order
        Ok(VirtualWrite { members, state: DeviceStateValue::Light(group) })
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        // Every light is committed: the group is where the write put it
        if let DeviceStateValue::Light(group) = state {
            if let Some(level) = group.brightness.filter(|&level| level > 0) {
                self.set_level = level;
            }
            self.current_state = group;
        }
    }
}
```

#### 🎚️ Reading the level back (#10, #66)

When a member changes from outside (a wall switch, the hub app), the manager
re-derives the group from its members: on if any is lit, at the average
level of those that are. Each lit member counts at the **group** level its
curve (or, for `LightGroupLinear`, its range) inverts its own level to, not
at its own level. Averaging the raw levels mixed member and group units, so
a group drifted off the level it was set to after any member change.

A member usually has several candidates (a curve that climbs slower than the
group lights it at the same level for neighbouring group levels, a flat
stretch for many). The group picks among them towards its `set_level`: the
level it last took from a write, or found every lit member at.

1. If some level is a candidate of every lit member, each inverts to the one
   of those nearest `set_level`, and that becomes the set level.
2. Otherwise each inverts to its own candidate nearest `set_level`.
3. Ties go to the lower level.

So a group set to a level reads that level back, a member turned off at the
wall leaves it there, and a member dimmed at the wall moves it only by that
member's share. Over the curves `[[0, 10], [100, 60]]`, `[[0, 0], [30, 40],
[60, 40], [100, 100]]` and 1:1, a group at 50 puts its members at 35, 40 and
50. Turning the 1:1 one off keeps it at 50 (the raw average read 37), and
dimming the first to 20 reads (20 + 50 + 50) / 3 = 40 (the raw average read
36). `lib/v1bectl_virtual/tests/light_group_levels.rs` drives that case end
to end, and `linear_group_levels.rs` the shipped Bedroom Lights.

### 2. Scene Controller
**Purpose**: Activate predefined multi-device scenes with smooth transitions

```rust
struct SceneController {
    device_id: DeviceId,
    scenes: HashMap<String, Arc<Scene>>, // shared with the plans that wait
    current_scene: Option<String>,
    state_store: Arc<StateStore>,        // read-only: where a fade starts from
}

struct Scene {
    name: String,
    device_states: HashMap<DeviceId, DeviceStateValue>,
    transition_type: TransitionType,
}

enum TransitionType {
    Instant,
    Fade { duration_ms: u64 },        // in 100 ms steps
    Sequence { delays_ms: Vec<u64> }, // the i-th delay before the i-th device
}

impl VirtualDevice for SceneController {
    async fn plan_write(&self, new_state: DeviceStateValue) -> Result<VirtualWrite, VirtualDeviceError> {
        let DeviceStateValue::Scene(scene_state) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        let scene = self.scenes.get(&scene_state.scene_name).ok_or_else(|| {
            VirtualDeviceError::Config(format!("Scene not found: {}", scene_state.scene_name))
        })?;
        Ok(VirtualWrite {
            members: plan_transition(&self.state_store, scene).await,
            state: DeviceStateValue::Scene(SceneState { scene_name: scene_state.scene_name, is_active: true }),
        })
    }

    // ⏱️ A fade or a sequence waits: hand the manager a plan that owns its
    // scene and the store, so its delays pass without the manager's lock (#58)
    fn plan_write_detached(&self, new_state: &DeviceStateValue) -> Option<DetachedPlan> {
        let DeviceStateValue::Scene(scene_state) = new_state else { return None };
        let scene = Arc::clone(self.scenes.get(&scene_state.scene_name)?);
        if !scene.waits() {
            return None; // instant: plan and commit in one hold of the lock
        }
        let (store, scene_name) = (Arc::clone(&self.state_store), scene_state.scene_name.clone());
        Some(Box::pin(async move {
            Ok(VirtualWrite {
                members: plan_transition(&store, &scene).await,
                state: DeviceStateValue::Scene(SceneState { scene_name, is_active: true }),
            })
        }))
    }

    // 🚦 Only the asked-for scene's lights: a write to a light of another
    // scene of this controller doesn't wait for this one's fade
    fn writes_to(&self, new_state: &DeviceStateValue) -> Vec<DeviceId> {
        match new_state {
            DeviceStateValue::Scene(s) => self.scenes.get(&s.scene_name)
                .map(|scene| scene.device_states.keys().cloned().collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn take_state(&mut self, state: DeviceStateValue) {
        if let DeviceStateValue::Scene(SceneState { scene_name, is_active }) = state {
            self.current_scene = is_active.then_some(scene_name);
        }
    }
}

// The transition runs as it always has, steps and delays included, but on
// the scene's own copy of its lights' states (`staged`). Only where it ENDS
// goes to the manager, to commit. Its steps used to go into the store, where
// a pull took them for outside changes. (A light the store doesn't have ends
// the transition there, and the manager's commit of it fails the activation.)
//
// 🎯 Each target is merged with the light as the store has it (#64): what
// the target leaves out keeps the light's value, so a scene only changes what
// it names. See "A scene only changes what it names" below.
async fn plan_transition(store: &StateStore, scene: &Scene) -> Vec<(DeviceId, DeviceStateValue)> {
    let mut staged = Staged::default();
    match &scene.transition_type {
        TransitionType::Instant => {
            for (device_id, target) in &scene.device_states {
                staged.set(device_id, merged(target, &stored(store, device_id).await));
            }
        }
        TransitionType::Fade { duration_ms } => {
            let steps = duration_ms / 100; // 100ms steps (under one step: instant)
            let mut targets = Staged::default(); // merged with where the fade starts
            for step in 0..=steps {
                let progress = step as f32 / steps as f32;
                for (device_id, target) in &scene.device_states {
                    let (current_state, target) = match staged.get(device_id) {
                        Some(state) => (state.clone(), targets.get(device_id).clone()),
                        // where it starts: the light as the store has it
                        None => {
                            let start = stored(store, device_id).await;
                            targets.set(device_id, merged(target, &start));
                            (start, targets.get(device_id).clone())
                        }
                    };
                    staged.set(device_id, interpolate_states(&current_state, &target, progress));
                }
                if step < steps {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    merge_at_end(store, scene, &mut staged).await; // the store as it is NOW
                }
            }
        }
        TransitionType::Sequence { delays_ms } => {
            for (i, (device_id, target)) in scene.device_states.iter().enumerate() {
                if let Some(delay_ms) = delays_ms.get(i) {
                    tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                }
                staged.set(device_id, merged(target, &stored(store, device_id).await));
            }
            merge_at_end(store, scene, &mut staged).await; // the store as it is NOW
        }
    }
    staged.into_members() // each light once, at its last state
}

// Where a transition that waited ends: each staged light at its target,
// merged again with the light as the store has it now. What changed while it
// waited (a direct write, a change on the hub) is kept, not reverted.
async fn merge_at_end(store: &StateStore, scene: &Scene, staged: &mut Staged) {
    for (device_id, state) in staged.iter_mut() {
        *state = merged(&scene.device_states[device_id], &stored(store, device_id).await);
    }
}
```

#### 🎯 A scene only changes what it names (#64)

A target may leave fields out: a TOML light with only `is_on`, or a runtime
`LightState` with `brightness: None`. Activating the scene merges each
target with the device as the store has it (`merged` in
`lib/v1bectl_virtual/src/scene_controller.rs`):

- A light keeps its `brightness` if the target has none, and its colour if
  the target names neither `color_temp` nor `rgb_color`. A light shows one
  colour, so a target that names either sets both.
- An outlet keeps its readings (`power_consumption`, `total_energy`).
- The target takes the shape the store holds the device in: an outlet the
  store holds as a light (a Dirigera hub's) is written as a light that is
  only on or off, and a light the store holds as an outlet as an outlet.
- **When:** an instant scene merges as it plans, and the manager commits it
  in the same hold of its lock. A fade or a sequence waits before the
  commit, and two writers don't queue behind it: a direct write through the
  API (`SetLightState`), and a change on the hub (the IKEA app, a remote)
  that a pull brings in. So where it ends, it merges each device **again**,
  with the store as it is then (`merge_at_end`). A field it doesn't name
  that changed while it ran keeps its new value, instead of being put back
  to where the transition started (and pushed to the hub like that). A
  fade's steps interpolate toward the target merged with where it starts
  the device, so what the target leaves out stays where it is at every step.
- A device the store doesn't have is staged as the target says, and the
  manager's commit fails at it, as before.

Before #64, a field a target left out was written as none, as part of the
device's whole state. Nothing fought the hub over it (one PATCH, which the
Dirigera gateway sends without the missing fields), but the store showed the
light with no brightness for the sync engine's whole protection window (5 s)
and up to one pull (2 s) after it, and then the hub's own value came back as
a second update.

### 3. Conditional Automation
**Purpose**: React to sensor changes and time events with complex logic

```rust
struct ConditionalDevice {
    device_id: DeviceId,
    rules: Vec<AutomationRule>,
    schedule: Option<Schedule>,
    enabled: bool,
}

struct AutomationRule {
    name: String,
    conditions: Vec<Condition>,
    actions: Vec<Action>,
    cooldown: Option<Duration>,
    last_triggered: Option<Instant>,
}

enum Condition {
    DeviceState { device_id: DeviceId, state: DeviceStateValue },
    TimeRange { start: Time, end: Time },
    SensorThreshold { device_id: DeviceId, threshold: f32, comparison: Comparison },
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
}

enum Action {
    SetDevice { device_id: DeviceId, state: DeviceStateValue },
    ActivateScene { scene_name: String },
    Wait { duration: Duration },
    SendNotification { message: String },
}

impl VirtualDevice for ConditionalDevice {
    async fn on_input_changed(&mut self, device_id: &DeviceId, new_state: &DeviceState) -> Result<(), VirtualDeviceError> {
        if !self.enabled {
            return Ok(());
        }
        
        for rule in &mut self.rules {
            // Check cooldown
            if let Some(last_triggered) = rule.last_triggered {
                if let Some(cooldown) = rule.cooldown {
                    if last_triggered.elapsed() < cooldown {
                        continue;
                    }
                }
            }
            
            // Evaluate conditions
            let mut should_trigger = true;
            for condition in &rule.conditions {
                if !self.evaluate_condition(condition, device_id, new_state).await? {
                    should_trigger = false;
                    break;
                }
            }
            
            if should_trigger {
                // Asks the manager for its writes (like `reactions`): never
                // writes a device, or the store, itself
                self.execute_actions(&rule.actions).await?;
                rule.last_triggered = Some(Instant::now());
            }
        }
        
        Ok(())
    }
}
```

### 4. Timer Device
**Purpose**: Schedule delayed actions and recurring events

```rust
struct TimerDevice {
    device_id: DeviceId,
    timers: HashMap<String, Timer>,
    scheduler: Arc<Scheduler>,
}

struct Timer {
    name: String,
    timer_type: TimerType,
    actions: Vec<Action>,
    enabled: bool,
}

enum TimerType {
    OneShot { delay: Duration },
    Recurring { interval: Duration },
    Cron { expression: String },
    Sunrise { offset: Duration },
    Sunset { offset: Duration },
}

impl VirtualDevice for TimerDevice {
    // Starting, stopping or resetting a timer writes no light, so the plan
    // is just the timer's new state. It checks the write, and changes nothing.
    async fn plan_write(&self, new_state: DeviceStateValue) -> Result<VirtualWrite, VirtualDeviceError> {
        let DeviceStateValue::Timer(ref timer_state) = new_state else {
            return Err(VirtualDeviceError::InvalidStateType);
        };
        if !self.timers.contains_key(&timer_state.timer_name) {
            return Err(VirtualDeviceError::Config(format!("Timer not found: {}", timer_state.timer_name)));
        }
        Ok(VirtualWrite { members: Vec::new(), state: new_state })
    }

    // The write is committed: now start, stop or reset the timer
    fn take_state(&mut self, state: DeviceStateValue) {
        if let DeviceStateValue::Timer(timer_state) = state {
            match timer_state.action {
                TimerAction::Start => self.start_timer(&timer_state.timer_name),
                TimerAction::Stop => self.stop_timer(&timer_state.timer_name),
                TimerAction::Reset => self.reset_timer(&timer_state.timer_name),
            }
        }
    }
}
```

When a timer fires, its actions are writes like any other: it asks the
manager for them (the way a button controller's `reactions` do), and the
manager commits them. It never writes a light, or the store, itself. 🛡️

## Configuration System

The server loads every `*.toml` file under `virtual_devices/` at startup, in
the order of their names (`v1bectl_virtual::load_virtual_devices_from_dir`,
in `lib/v1bectl_virtual/src/config.rs`; nothing a file loads to depends on
that order, see 🪆 above): each file holds one
`VirtualDeviceTomlConfig`, tagged by its `type` field. A file that fails to
parse is logged and skipped; the rest still load. Parsing a file is only
half the story — building the registered device from it is
`v1bectl_server`'s job, one `match` arm per `type`
(`v1bectl_server/src/main.rs`), and the notes below say where that arm falls
short of what the file says.

Four `type`s exist today (`config.rs`):

### `light_group_linear`

A 1:1 mapping from a group level to each member's own range. This is the
type the shipped example uses (`virtual_devices/bedroom_lights.toml`):

```toml
type = "light_group_linear"
device_id = "virtual_bedroom_lights"
name = "Bedroom Lights"

[members]
top = "light_bedroom"
main = "light_living_room"
bed = "light_kitchen"

# name -> [min, max]: the group's 0..100 maps onto this member's range.
[brightness]
top = [80, 100]
main = [40, 90]
bed = [0, 50]

[settings]
transition_time = 200
```

`members` maps a name to a device id, and `brightness` maps the same name to
its `[min, max]` range (`LightGroupLinearConfig`). `settings.transition_time`
(ms) defaults to 500 if left out. This type is fully wired: `v1bectl_server`
builds a `LightGroupLinear` straight from `members` and `brightness`
(`lib/v1bectl_virtual/tests/dummy_scenario.rs` drives the shipped file
against the dummy hub).

### `button_controller`

Binds one physical button to actions on a light or group. The shipped
example (`virtual_devices/button_ctrl.toml`):

```toml
type = "button_controller"
device_id = "ctrl_lightgroup_bed"
name = "Bedroom Lights Controller"
button = "switch_hallway"

# [command, target, amount]: toggle, on, off, inc, dec, set.
press_on = ["toggle", "virtual_bedroom_lights"]
press_double = ["set", "virtual_bedroom_lights", 100]
press_on_long = ["inc", "virtual_bedroom_lights", 10]
```

`press_off` defaults to `[]` (nothing on release, as a toggle needs it);
`press_off_long` and `press_double` are optional too
(`ButtonControllerConfig`). Also fully wired, and driven against the dummy
hub the same way.

### `light_group`

The curve-based group type (`LightGroupConfig`):

```toml
type = "light_group"
device_id = "living_room_lights"
name = "Living Room Lights"
members = ["light_1", "light_2", "light_3"]

[brightness_curves.light_1]
min = 0
max = 100

[brightness_curves.light_2]
min = 20
max = 90

[settings]
aggregation = "average"   # average, min, max or any
transition_time = 500
exclude = []
```

A member may end in `*` to match every physical device id with that prefix;
`settings.exclude` (patterns or plain ids) then drops any of those back out
before the group is built. A wildcard never matches a virtual device: which
of those are loaded yet depends on the order the files load in. To nest a
group, name it (see 🪆 above, #58). `v1bectl_server` maps each member's
`brightness_curves` entry into the two-breakpoint curve `LightGroup` expects:
group 0 -> `min`, group 100 -> `max` (#62). `light_3` above gets no entry, so
it falls back to a 1:1 curve (group brightness = device brightness), logged
at debug; a member only reached through a `*` wildcard always falls back the
same way, since the file has no way to give a curve to an id it doesn't
name. A curve with `min` or `max` over 100 fails the whole group, logged
like any other creation failure.

`LightGroup` itself always averages over the members that are on, each at
the group level its curve inverts its own level to (see 🎚️ above, #66),
and writes every member instantly (like `light_group_linear` above,
`transition_time` isn't wired up on either type yet), so
`settings.aggregation` other than `"average"` and a `settings.transition_time`
other than its 500 ms default are not honoured — each logs a `warn!` once at
load rather than being silently dropped. `settings.exclude` *is* honoured,
since it's applied to the wildcard match itself, before the group is built.

### `scene_controller`

Named scenes of device states, all with the same transition
(`SceneControllerConfig`):

```toml
type = "scene_controller"
device_id = "evening_scene"
name = "Evening Scene"

[[scenes]]
name = "cozy"
display_name = "Cozy"

[[scenes.devices]]
device_id = "light_1"
[scenes.devices.state]
type = "light"
is_on = true
brightness = 30
color_temp = 2700

[[scenes.devices]]
device_id = "outlet_1"
[scenes.devices.state]
type = "outlet"
is_on = true

[settings]
transition_duration = 2000
default_scene = "cozy"
```

Each scene device has a `device_id` and a tagged `state`: `type = "light"`
(`is_on`, optional `brightness`, optional `color_temp`) or `type = "outlet"`
(`is_on`). `v1bectl_server` builds a `SceneController` from it with
`SceneController::from_toml` (`lib/v1bectl_virtual/src/scene_controller.rs`)
and registers it like the types above (#10). The file maps onto the same
runtime config the API's `CreateVirtualDevice` takes:

| TOML | Runtime |
|---|---|
| `device_id`, `name` | the controller's id and name |
| `[[scenes]]` `name` | a scene of that name, which `ActivateScene` sets |
| `display_name` | kept in the controller's config; nothing at runtime reads it |
| `[[scenes.devices]]` | that device's target in the scene, in the shape the store holds a physical device in; a virtual one (a light group) is resolved when the scene is set (#58) |
| `type = "light"` | a light state (`is_on`, `brightness`, `color_temp`); `rgb_color` is unset |
| `type = "outlet"` | an outlet state if the store holds the device as one (the dummy's `outlet_tv`), or an on/off light state if it holds it as a light, which is how a Dirigera hub reads its outlets |
| `settings.transition_duration` (ms, default 1000) | every scene's transition: `Instant` at 0, else a `Fade` over it, in whole 100 ms steps |
| `settings.default_scene` | kept in the config, with a warning: nothing activates it |

The file can't express a transition per scene, or a `Sequence` with its
delays (see 🚦 above for both). A light field it leaves out (`brightness`,
`color_temp`) is unset in the target, as in a scene the API creates, and
keeps the device's value when the scene is activated: a scene only changes
what it names (see 🎯 above, #64).

When the server loads the file, it checks it the way it checks the other
types:
- ❌ **Not created** (logged; the other files still load): two scenes of one
  name, a scene named `none` or with no name (setting either deactivates, so
  it could never be set), a device twice in one scene, a `brightness` over
  100, or a physical device the store holds as something a scene can't set
  (a switch, a sensor).
- ⚠️ **Created, with a warning**: no scenes, a scene with no devices, a
  `default_scene`, a `transition_duration` shorter than one 100 ms step (set
  instantly) or between steps (rounded down), and `brightness`/`color_temp`
  for a physical device the store holds as an outlet (only `is_on` is set).
- A device the store doesn't have is no error, as for a button controller's
  target: it may be a virtual device that loads later. The manager's `start`
  warns about each one still missing (`dangling_references`), and setting a
  scene with one fails at that device.
- 🪆 **A virtual device is checked the same, loaded or not** (#58): a scene
  that sets a light group loads whether the group's file comes before it or
  after, and the group's lights get the scene's level through the group when
  it's set. A target that isn't a light's to take (another scene
  controller) is only found out then: setting the scene fails, with nothing
  committed. It used to be rejected at load, but only if that controller's
  file happened to load first (#63 review, finding 2).

✅ **The API checks a scene controller the same way (#64).** Its
`CreateVirtualDevice` builds one with `SceneController::create`, which runs
the same checks as `from_toml` (`check_scenes`), so both reject the same
configs with the same messages; the API answers `CREATE_FAILED` with it, and
logs the warnings. The TOML-only warnings (`default_scene`,
`transition_duration`) have no runtime counterpart. On top of that, the API
rejects what only it can express: a scene listed under another name than its
own `name` (it would show one and be set by the other), and a target that
isn't a light's or an outlet's state. Two scenes under one name, or a device
twice in one scene, can't reach it: the request's config is decoded into
maps, which keep only the last of each.

`v1bectl_server/src/tests.rs` loads two such files
(`v1bectl_server/tests/fixtures/virtual_devices/`) the way the server does,
and sets their scenes against the dummy hub: an instant one and a 2 s fade.
It also loads `virtual_devices_nested/`, where a scene's file sorts before
the file of the light group it sets, and sets that scene through the group.

## API Integration

Virtual devices appear as regular devices in the API but with special device types:

```rust
// API calls work the same
GET /devices/living_room_group
-> {
  "device_id": "living_room_group",
  "device_type": "VirtualLightGroup",
  "state": {
    "is_on": true,
    "brightness": 75
  }
}

POST /devices/living_room_group/state
{
  "brightness": 50
}
-> Triggers brightness curve calculations for all member lights
```

## Testing Strategy

### Unit Tests
- Individual virtual device logic
- Brightness curve calculations
- Condition evaluation
- Action execution

### Integration Tests  
- Multi-device scene activation
- Complex automation scenarios
- Timer scheduling accuracy
- Virtual device API compatibility

### Performance Tests
- Large light group coordination
- Rapid state changes
- Memory usage with many virtual devices
- Concurrent virtual device triggers