# Virtual Device System Design

## Concept Overview

Virtual devices are software-defined devices that combine multiple physical devices to create new automation behaviors. They exist only in the v1bectl server but appear as regular devices to API clients.

## Core Architecture

### Virtual Device Trait
```rust
#[async_trait]
trait VirtualDevice: Send + Sync {
    fn device_id(&self) -> &DeviceId;
    fn device_type(&self) -> VirtualDeviceType;
    fn config(&self) -> &VirtualDeviceConfig;
    
    // Called when virtual device state should change
    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError>;
    
    // Called when input device states change
    async fn on_input_changed(&mut self, device_id: &DeviceId, new_state: &DeviceState) -> Result<(), VirtualDeviceError>;
    
    // Get current virtual device state
    fn current_state(&self) -> DeviceStateValue;
    
    // Which physical devices this virtual device depends on
    fn input_devices(&self) -> Vec<DeviceId>;
    
    // Which physical devices this virtual device controls
    fn output_devices(&self) -> Vec<DeviceId>;
}
```

### Virtual Device Manager
```rust
struct VirtualDeviceManager {
    virtual_devices: HashMap<DeviceId, Box<dyn VirtualDevice>>,
    input_mappings: HashMap<DeviceId, Vec<DeviceId>>, // physical -> virtual
    output_mappings: HashMap<DeviceId, Vec<DeviceId>>, // virtual -> physical
    state_store: Arc<StateStore>,
}

impl VirtualDeviceManager {
    // Called by state store when physical device changes
    async fn handle_device_state_change(&mut self, device_id: &DeviceId, new_state: &DeviceState) {
        if let Some(virtual_device_ids) = self.input_mappings.get(device_id) {
            for vd_id in virtual_device_ids {
                if let Some(virtual_device) = self.virtual_devices.get_mut(vd_id) {
                    let _ = virtual_device.on_input_changed(device_id, new_state).await;
                }
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
    current_brightness: u8,
    is_on: bool,
}

// Example: Living room group with 3 lights
// 0-25%: Only ambient light
// 25-75%: Ambient + main light  
// 75-100%: All lights at full
struct BrightnessCurve {
    breakpoints: Vec<(u8, u8)>, // (group_brightness, device_brightness)
}

impl VirtualDevice for LightGroup {
    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        match new_state {
            DeviceStateValue::Light(light_state) => {
                self.is_on = light_state.is_on;
                if let Some(brightness) = light_state.brightness {
                    self.current_brightness = brightness;
                    
                    // Apply brightness curves to each physical light
                    for light_id in &self.lights {
                        let curve = &self.brightness_curves[light_id];
                        let device_brightness = curve.interpolate(brightness);
                        
                        let device_state = LightState {
                            is_on: device_brightness > 0,
                            brightness: Some(device_brightness),
                            ..light_state
                        };
                        
                        // Send to state store
                        self.state_store.set_device_state(light_id, DeviceStateValue::Light(device_state)).await?;
                    }
                }
                Ok(())
            }
            _ => Err(VirtualDeviceError::InvalidStateType)
        }
    }
}
```

### 2. Scene Controller
**Purpose**: Activate predefined multi-device scenes with smooth transitions

```rust
struct SceneController {
    device_id: DeviceId,
    scenes: HashMap<String, Scene>,
    current_scene: Option<String>,
    transition_duration: Duration,
}

struct Scene {
    name: String,
    device_states: HashMap<DeviceId, DeviceStateValue>,
    transition_type: TransitionType,
}

enum TransitionType {
    Instant,
    Fade { duration: Duration },
    Sequence { delays: Vec<Duration> },
}

impl VirtualDevice for SceneController {
    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        match new_state {
            DeviceStateValue::Scene(scene_state) => {
                if let Some(scene) = self.scenes.get(&scene_state.scene_name) {
                    self.activate_scene(scene).await?;
                    self.current_scene = Some(scene_state.scene_name);
                }
                Ok(())
            }
            _ => Err(VirtualDeviceError::InvalidStateType)
        }
    }
    
    async fn activate_scene(&self, scene: &Scene) -> Result<(), VirtualDeviceError> {
        match scene.transition_type {
            TransitionType::Instant => {
                // Set all devices immediately
                for (device_id, state) in &scene.device_states {
                    self.state_store.set_device_state(device_id, state.clone()).await?;
                }
            }
            TransitionType::Fade { duration } => {
                // Calculate intermediate steps for smooth transitions
                let steps = (duration.as_millis() / 100) as usize; // 100ms steps
                for step in 0..=steps {
                    let progress = step as f32 / steps as f32;
                    for (device_id, target_state) in &scene.device_states {
                        let current_state = self.state_store.get_device_state(device_id).await?;
                        let interpolated_state = interpolate_states(&current_state, target_state, progress);
                        self.state_store.set_device_state(device_id, interpolated_state).await?;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            TransitionType::Sequence { delays } => {
                // Activate devices in sequence with specified delays
                for (i, (device_id, state)) in scene.device_states.iter().enumerate() {
                    if let Some(delay) = delays.get(i) {
                        tokio::time::sleep(*delay).await;
                    }
                    self.state_store.set_device_state(device_id, state.clone()).await?;
                }
            }
        }
        Ok(())
    }
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
    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        match new_state {
            DeviceStateValue::Timer(timer_state) => {
                match timer_state.action {
                    TimerAction::Start => self.start_timer(&timer_state.timer_name).await?,
                    TimerAction::Stop => self.stop_timer(&timer_state.timer_name).await?,
                    TimerAction::Reset => self.reset_timer(&timer_state.timer_name).await?,
                }
                Ok(())
            }
            _ => Err(VirtualDeviceError::InvalidStateType)
        }
    }
}
```

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