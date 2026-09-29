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
    // of its own write, which must not re-derive it (lossy!)
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
3. The manager commits each member write, in order (`commit_member_write`):
   - **A physical member, with the sync engine attached:** the write goes
     through `SyncEngine::apply_optimistic_update`, the call a direct write
     to that light makes. It arms the light's 🛡️ protection window, then
     writes the store, echoes the change and queues the push to the hub.
   - **A virtual member, or no engine** (the dummy and test setups): the
     manager writes the store and echoes it itself.
   - A member the write leaves as it is is skipped. A member the store
     doesn't have fails the write right there: the members before it stay
     committed, and the ones after it are never written.
4. Every member committed: the manager hands the device its new state
   (`take_state`), stores it and echoes it. If a member failed, the device
   keeps its old state, and input tracking re-derives it from what actually
   happened.

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
it in the order it asks, with the devices it writes: its own and every
member's (`writes_to`). It goes once every write that joined before it and
shares a device with it is over, **whether that one is running or still
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
waiting write names was added or removed ahead of it, the write joins again
at the back with the devices as they are now.

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
        for (member_id, state) in write.members {
            // 🛡️ the engine arms protection, THEN writes the store
            self.commit_member_write(&member_id, state).await?;
        }
        let device = devices.get_mut(device_id).unwrap();
        device.take_state(write.state);
        self.store_state(device_id, device.current_state()).await // store + echo
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
            self.current_state = group;
        }
    }
}
```

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
async fn plan_transition(store: &StateStore, scene: &Scene) -> Vec<(DeviceId, DeviceStateValue)> {
    let mut staged = Staged::default();
    match &scene.transition_type {
        TransitionType::Instant => {
            for (device_id, state) in &scene.device_states {
                staged.set(device_id, state.clone());
            }
        }
        TransitionType::Fade { duration_ms } => {
            let steps = duration_ms / 100; // 100ms steps (under one step: instant)
            for step in 0..=steps {
                let progress = step as f32 / steps as f32;
                for (device_id, target_state) in &scene.device_states {
                    let current_state = match staged.get(device_id) {
                        Some(state) => state.clone(),
                        // where it starts: the light as the store has it
                        None => store.get_device(device_id).await.map_or_else(|| default_for(target_state), |d| d.state),
                    };
                    staged.set(device_id, interpolate_states(&current_state, target_state, progress));
                }
                if step < steps {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        TransitionType::Sequence { delays_ms } => {
            for (i, (device_id, state)) in scene.device_states.iter().enumerate() {
                if let Some(delay_ms) = delays_ms.get(i) {
                    tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                }
                staged.set(device_id, state.clone());
            }
        }
    }
    staged.into_members() // each light once, at its last state
}
```

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

### Virtual Device Definition Format
```rust
#[derive(Serialize, Deserialize)]
struct VirtualDeviceConfig {
    device_id: DeviceId,
    device_type: VirtualDeviceType,
    name: String,
    description: Option<String>,
    enabled: bool,
    config: serde_json::Value, // Type-specific configuration
}

// Example configurations
// Light Group
{
  "device_id": "living_room_group",
  "device_type": "LightGroup",
  "name": "Living Room Lights",
  "enabled": true,
  "config": {
    "lights": ["light_1", "light_2", "light_3"],
    "brightness_curves": {
      "light_1": {"breakpoints": [[0, 0], [25, 50], [100, 100]]},
      "light_2": {"breakpoints": [[0, 0], [25, 0], [50, 100]]},
      "light_3": {"breakpoints": [[0, 0], [75, 0], [100, 100]]}
    }
  }
}

// Scene Controller
{
  "device_id": "evening_scene",
  "device_type": "SceneController", 
  "name": "Evening Scene",
  "enabled": true,
  "config": {
    "scenes": {
      "cozy": {
        "device_states": {
          "light_1": {"is_on": true, "brightness": 30, "color_temp": 2700},
          "light_2": {"is_on": false}
        },
        "transition_type": {"Fade": {"duration": "2s"}}
      }
    }
  }
}
```

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