// 🔥 BUTTON CONTROLLER - REACTS TO BUTTON EVENTS! 💖

use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use v1bectl_sync::{
    ButtonPressType, DeviceEvent, DeviceId, DeviceStateValue, EventBus, EventType, LightState,
    StateStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ButtonCommand {
    Inc, // Increment brightness
    Dec, // Decrement brightness
    Set, // Set specific value
    On,  // Turn on
    Off, // Turn off
}

impl ButtonCommand {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "inc" => Some(Self::Inc),
            "dec" => Some(Self::Dec),
            "set" => Some(Self::Set),
            "on" => Some(Self::On),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

/// One configured button action, `[command, target, amount]`: what a press
/// does to `target`, a light or a light group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ButtonAction {
    pub command: ButtonCommand,
    pub target: DeviceId,
    /// The `inc`/`dec` step (default 10), or the `set` level (default 50).
    pub amount: Option<u8>,
}

impl ButtonAction {
    /// Parse `[command, target]` or `[command, target, amount]`.
    pub fn parse(action: &[serde_json::Value]) -> Result<Self, VirtualDeviceError> {
        if action.len() < 2 {
            return Err(VirtualDeviceError::Config(
                "Invalid action format".to_string(),
            ));
        }

        let cmd_str = action[0]
            .as_str()
            .ok_or_else(|| VirtualDeviceError::Config("Command must be string".to_string()))?;
        let target = action[1]
            .as_str()
            .ok_or_else(|| VirtualDeviceError::Config("Device ID must be string".to_string()))?;
        let command = ButtonCommand::from_str(cmd_str)
            .ok_or_else(|| VirtualDeviceError::Config(format!("Unknown command: {cmd_str}")))?;
        let amount = action
            .get(2)
            .and_then(serde_json::Value::as_u64)
            .map(|amount| u8::try_from(amount.min(100)).unwrap_or(100));

        Ok(Self {
            command,
            target: target.to_string(),
            amount,
        })
    }

    /// The state this action moves a light at `light` to.
    #[must_use]
    pub fn apply(&self, mut light: LightState) -> LightState {
        match self.command {
            ButtonCommand::Inc => {
                let current = light.brightness.unwrap_or(0);
                let step = self.amount.unwrap_or(10);
                light.brightness = Some(current.saturating_add(step).min(100));
                light.is_on = true;
            }
            ButtonCommand::Dec => {
                let current = light.brightness.unwrap_or(100);
                let new_brightness = current.saturating_sub(self.amount.unwrap_or(10));
                light.brightness = Some(new_brightness);
                if new_brightness == 0 {
                    light.is_on = false;
                }
            }
            ButtonCommand::Set => {
                let brightness = self.amount.unwrap_or(50);
                light.brightness = Some(brightness);
                light.is_on = brightness > 0;
            }
            ButtonCommand::On => {
                light.is_on = true;
                if light.brightness.unwrap_or(0) == 0 {
                    light.brightness = Some(100);
                }
            }
            ButtonCommand::Off => {
                light.is_on = false;
            }
        }
        light
    }
}

/// The `is_pressed` flag in a switch state `value`: a state echo's
/// `{"Switch": {...}}`, or a bare state object with `is_pressed`.
fn is_pressed_in(value: &serde_json::Value) -> Option<bool> {
    match serde_json::from_value::<DeviceStateValue>(value.clone()) {
        Ok(DeviceStateValue::Switch(switch)) => Some(switch.is_pressed),
        _ => value.get("is_pressed").and_then(serde_json::Value::as_bool),
    }
}

/// Whether a switch that went from `old` to `new` was pressed (`Some(true)`)
/// or released (`Some(false)`). Only a change of `is_pressed` is either. A
/// switch echo that keeps it is something else changing: a battery tick
/// (85 → 84), or a pull confirming what the store has. The sync engine
/// echoes those too, and treating one as a release ran the release action:
/// with the shipped controller, every battery tick turned the lights off.
///
/// With no `old` (`None`: the echo had `Null` there, or something that
/// isn't a switch) there's nothing to compare against, so only
/// `is_pressed: true` counts, as a press. A switch at rest reports `false`,
/// and so does every echo that isn't a press. Dirigera's mapping even
/// defaults the flag to `false`. So a `false` with nothing before it can't
/// be told from a battery tick, and firing on it would bring that bug back.
/// A `true` can only be a press. At worst a release goes unrun. (The only
/// echo that sends `Null` is the engine's optimistic one, and nothing
/// writes a switch optimistically.)
fn transition(old: Option<bool>, new: bool) -> Option<bool> {
    match old {
        Some(old) if old == new => None,
        Some(_) => Some(new),
        None => new.then_some(true),
    }
}

/// Whether `event` is a press (`Some(true)`) or a release (`Some(false)`)
/// of its switch, in any shape that carries one (see [`transition`]: only a
/// change of `is_pressed` is either). The main one is a state echo of a
/// switch (`AttributeChanged{attribute: "state"}`, as the sync engine
/// publishes it when the hub reports the switch has changed), which has the
/// state before in `old_value`. The others are the shapes the controller
/// has always read: a bare state object with `is_pressed`, an
/// `isOn`/`buttonState` flag, and a `StateChanged` switch state.
fn pressed(event: &DeviceEvent) -> Option<bool> {
    let (old, new) = match &event.event_type {
        EventType::AttributeChanged {
            attribute,
            old_value,
            new_value,
        } if attribute == "state" => (is_pressed_in(old_value), is_pressed_in(new_value)?),
        EventType::AttributeChanged {
            attribute,
            old_value,
            new_value,
        } if attribute == "isOn" || attribute == "buttonState" => {
            (old_value.as_bool(), new_value.as_bool()?)
        }
        EventType::StateChanged {
            old_state,
            new_state: Some(DeviceStateValue::Switch(switch)),
        } => {
            let old = match old_state {
                Some(DeviceStateValue::Switch(old)) => Some(old.is_pressed),
                _ => None,
            };
            (old, switch.is_pressed)
        }
        _ => return None,
    };
    transition(old, new)
}

/// Button Controller: runs its actions when its button is pressed.
///
/// It has an action for each edge of a press, and one for a long press:
/// `press_on` runs when the button goes down (a press), `press_off` when it
/// comes back up (a release), and `press_on_long`, in place of `press_on`,
/// for a press the hub reports as held (a long press). `press_off_long` is
/// accepted and checked, but nothing runs it yet: no event source reports
/// the end of a long press. A missing or empty action does nothing.
///
/// A press reaches it in one of two shapes, and each maps onto those
/// actions like this (#35):
///
/// | Event from its button | What happened | Actions run, in order |
/// |---|---|---|
/// | switch echo, `is_pressed` `false` → `true` | the button went down | `press_on` |
/// | switch echo, `is_pressed` `true` → `false` | the button came up | `press_off` |
/// | switch echo that keeps `is_pressed` (a battery tick) | nothing | nothing |
/// | `ButtonPressed { SinglePress }` | a click: down, then up | `press_on`, `press_off` |
/// | `ButtonPressed { DoublePress }` | two clicks | `press_on`, `press_off`, `press_on`, `press_off` |
/// | `ButtonPressed { LongPress }` | a press, still held | `press_on_long` (`press_on` if there's none) |
///
/// The switch echo is the sync engine's `{"Switch": …}` state echo, and
/// only a change of `is_pressed` is a press or a release (#34). A
/// `ButtonPressed` is a gesture a hub recognised on its own and reports
/// whole, over its event stream: a Dirigera remote's `clickPattern`, or the
/// dummy's. Why each gesture maps the way it does:
///
/// - **A `SinglePress` is a press and a release.** A hub reports a click
///   once the button is back up. It can't tell a click from a long press
///   before the release, or from a double press before the double-click
///   time is up. So a click has both edges, and runs both actions.
/// - **A `DoublePress` is two clicks.** The hub folds them into one event,
///   and a controller has no action of its own for that, so it runs what
///   two clicks run. That's also what it would have run had the hub
///   reported two `SinglePress`es, so the result doesn't hang on the hub's
///   double-click timing. Ignoring it would drop two real presses.
/// - **A `LongPress` is a press that's still held.** A hub reports it while
///   the button is still down, once it has been held long enough, and
///   never reports the release. So only the press edge runs, with the long
///   action. Running a release action would make one up. With the shipped
///   binding (`inc` on the long press, `dec` on its release) it would also
///   undo the long press.
///
/// The event's `button_id` (which of the remote's buttons; `main` when the
/// hub doesn't say) isn't matched: a controller binds a whole device.
///
/// A pair like the shipped `on`/`off` is hold-to-light: the lights are on
/// while the button is down. So a click turns them on and off again.
///
/// **One press fires once.** A remote can report one press both ways: as a
/// `ButtonPressed` over the hub's event stream, and as an `is_pressed`
/// change that the sync engine's pull picks up and echoes. So once its
/// button has reported a gesture, a controller goes by those alone, and its
/// button's switch echoes run nothing (#35). A button that never reports
/// one (a switch that only has the flag, or a hub without an event stream)
/// keeps working off its echoes, as in #34. It's not a time window because
/// the echo comes from a pull: up to a pull interval (2 s) and the pull's
/// own time after the press, or later for a long hold. No window both
/// catches every echo and lets the next real press through. The gap: an
/// echo that comes before the button's first `ButtonPressed` still runs.
/// So the first press after a server start can run twice, if a pull lands
/// while the button is down.
///
/// It makes no writes of its own. Once it's registered, the
/// [`VirtualDeviceManager`](crate::VirtualDeviceManager) hands it its
/// button's events ([`VirtualDevice::reactions`]) and makes each write its
/// actions ask for, the way it makes an API write (#16). So a press that
/// targets a light group reaches the group's members and the hub, and is
/// echoed. It used to write the group's state straight into the store,
/// which reached neither.
pub struct ButtonController {
    config: VirtualDeviceConfig,
    button_id: String, // Button device to listen to
    press_on_action: Option<ButtonAction>,
    press_off_action: Option<ButtonAction>,
    press_on_long_action: Option<ButtonAction>,
    press_off_long_action: Option<ButtonAction>,
    /// Set once its button has reported a gesture (`ButtonPressed`). From
    /// then on, only those run anything, and its switch echoes don't (see
    /// the type's docs: one press fires once).
    reports_gestures: AtomicBool,
}

impl ButtonController {
    /// A controller for `button_id` that runs `press_on` on a press,
    /// `press_off` on a release, and `press_on_long` on a long press (see
    /// the type's docs for what runs when). Each is `[command, target,
    /// amount]` (see [`ButtonAction::parse`]); a missing or empty one does
    /// nothing, and a malformed one fails here, the long ones too.
    ///
    /// `state_store` and `event_bus` aren't used any more (the manager
    /// delivers the events and makes the writes). They stay so callers
    /// don't have to change.
    // kept: constructor takes the full set of button actions and dependencies;
    // grouping them into a struct would change the public API.
    #[allow(clippy::too_many_arguments)]
    // kept by value: callers outside this PR's lane (v1bectl_server) pass an
    // owned Vec (some via `cfg.press_on.clone()`); v1bectl_server joins the
    // ratchet separately, so its call sites aren't touched here.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "v1bectl_server passes owned Vecs here and is outside this PR's lane"
    )]
    pub fn new(
        config: VirtualDeviceConfig,
        button_id: String,
        press_on: Vec<serde_json::Value>,
        press_off: Vec<serde_json::Value>,
        press_on_long: Option<Vec<serde_json::Value>>,
        press_off_long: Option<Vec<serde_json::Value>>,
        _state_store: Arc<StateStore>,
        _event_bus: Arc<EventBus>,
    ) -> Result<Self, VirtualDeviceError> {
        let parse = |action: &[serde_json::Value]| {
            if action.is_empty() {
                Ok(None)
            } else {
                ButtonAction::parse(action).map(Some)
            }
        };

        Ok(Self {
            config,
            button_id,
            press_on_action: parse(&press_on)?,
            press_off_action: parse(&press_off)?,
            press_on_long_action: parse(press_on_long.as_deref().unwrap_or_default())?,
            press_off_long_action: parse(press_off_long.as_deref().unwrap_or_default())?,
            reports_gestures: AtomicBool::new(false),
        })
    }

    /// The action for one edge of a press: `press_on` when the button goes
    /// down, `press_off` when it comes back up.
    fn edge(&self, is_pressed: bool) -> Option<&ButtonAction> {
        if is_pressed {
            self.press_on_action.as_ref()
        } else {
            self.press_off_action.as_ref()
        }
    }

    /// The actions for a gesture the hub reports whole, in the order they
    /// run. The type's docs have the table, and why.
    fn gesture(&self, press_type: &ButtonPressType) -> Vec<&ButtonAction> {
        let click = [self.edge(true), self.edge(false)];
        match press_type {
            ButtonPressType::SinglePress => click.into_iter().flatten().collect(),
            ButtonPressType::DoublePress => click.into_iter().chain(click).flatten().collect(),
            ButtonPressType::LongPress => self
                .press_on_long_action
                .as_ref()
                .or_else(|| self.edge(true))
                .into_iter()
                .collect(),
        }
    }
}

#[async_trait]
impl VirtualDevice for ButtonController {
    fn device_id(&self) -> &DeviceId {
        &self.config.device_id
    }

    fn device_type(&self) -> VirtualDeviceType {
        VirtualDeviceType::ButtonController
    }

    fn config(&self) -> &VirtualDeviceConfig {
        &self.config
    }

    async fn set_state(&mut self, _new_state: DeviceStateValue) -> Result<(), VirtualDeviceError> {
        // ButtonController doesn't have its own state, it just reacts
        Ok(())
    }

    fn reactions(&self, event: &DeviceEvent) -> Vec<ButtonAction> {
        if event.device_id != self.button_id {
            return Vec::new();
        }
        if let EventType::ButtonPressed { press_type, .. } = &event.event_type {
            if !self.reports_gestures.swap(true, Ordering::Relaxed) {
                tracing::info!(
                    "🔘 Button {} reports its presses as ButtonPressed: {} ignores its switch echoes from now on",
                    self.button_id,
                    self.config.device_id
                );
            }
            tracing::debug!("🔘 Button {} reported a {:?}", self.button_id, press_type);
            return self.gesture(press_type).into_iter().cloned().collect();
        }
        let Some(is_pressed) = pressed(event) else {
            return Vec::new();
        };
        if self.reports_gestures.load(Ordering::Relaxed) {
            // The same press as a `ButtonPressed` it has run, or will.
            tracing::debug!(
                "🔘 Button {} pressed: {} (skipped: it reports its presses as ButtonPressed)",
                self.button_id,
                is_pressed
            );
            return Vec::new();
        }
        tracing::debug!("🔘 Button {} pressed: {}", self.button_id, is_pressed);
        self.edge(is_pressed).into_iter().cloned().collect()
    }

    fn current_state(&self) -> DeviceStateValue {
        // ButtonController has no state, return empty
        DeviceStateValue::Empty
    }

    fn input_devices(&self) -> Vec<DeviceId> {
        vec![self.button_id.clone()]
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        // The targets of its actions
        let mut devices = Vec::new();
        for action in [
            &self.press_on_action,
            &self.press_off_action,
            &self.press_on_long_action,
            &self.press_off_long_action,
        ]
        .into_iter()
        .flatten()
        {
            if !devices.contains(&action.target) {
                devices.push(action.target.clone());
            }
        }
        devices
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use v1bectl_sync::SwitchState;

    fn switch(is_pressed: bool, battery_level: u8) -> Value {
        serde_json::to_value(DeviceStateValue::Switch(SwitchState {
            is_pressed,
            last_pressed: None,
            battery_level: Some(battery_level),
        }))
        .unwrap()
    }

    fn attribute(attribute: &str, old_value: Value, new_value: Value) -> DeviceEvent {
        DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: "btn".to_string(),
            event_type: EventType::AttributeChanged {
                attribute: attribute.to_string(),
                old_value,
                new_value,
            },
        }
    }

    /// #34 review, finding 1: only a change of `is_pressed` is a press or a
    /// release, in every shape `pressed` reads. Without an old value, only
    /// `is_pressed: true` counts (see [`transition`]).
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "flat table of (case, event, want) triples covering every event shape `pressed` reads; splitting it up would just scatter the table"
    )]
    fn only_an_is_pressed_transition_is_a_press_or_a_release() {
        let switch_state = |is_pressed| {
            DeviceStateValue::Switch(SwitchState {
                is_pressed,
                last_pressed: None,
                battery_level: Some(85),
            })
        };
        let state_changed = |old: Option<bool>, new: bool| DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: "btn".to_string(),
            event_type: EventType::StateChanged {
                old_state: old.map(switch_state),
                new_state: Some(switch_state(new)),
            },
        };
        let cases = [
            // The sync engine's switch echo.
            (
                "press",
                attribute("state", switch(false, 85), switch(true, 85)),
                Some(true),
            ),
            (
                "release",
                attribute("state", switch(true, 85), switch(false, 85)),
                Some(false),
            ),
            (
                "battery tick, released",
                attribute("state", switch(false, 85), switch(false, 84)),
                None,
            ),
            (
                "battery tick, held",
                attribute("state", switch(true, 85), switch(true, 84)),
                None,
            ),
            (
                "confirmation, nothing moved",
                attribute("state", switch(false, 85), switch(false, 85)),
                None,
            ),
            (
                "no old value, pressed",
                attribute("state", Value::Null, switch(true, 85)),
                Some(true),
            ),
            (
                "no old value, released",
                attribute("state", Value::Null, switch(false, 84)),
                None,
            ),
            (
                "old value not a switch, released",
                attribute("state", json!({"Empty": null}), switch(false, 84)),
                None,
            ),
            // The bare state object.
            (
                "bare press",
                attribute(
                    "state",
                    json!({"is_pressed": false}),
                    json!({"is_pressed": true}),
                ),
                Some(true),
            ),
            (
                "bare release",
                attribute(
                    "state",
                    json!({"is_pressed": true}),
                    json!({"is_pressed": false}),
                ),
                Some(false),
            ),
            (
                "bare, no old value, released",
                attribute("state", Value::Null, json!({"is_pressed": false})),
                None,
            ),
            // A flag attribute.
            (
                "isOn press",
                attribute("isOn", json!(false), json!(true)),
                Some(true),
            ),
            (
                "buttonState release",
                attribute("buttonState", json!(true), json!(false)),
                Some(false),
            ),
            (
                "isOn unchanged",
                attribute("isOn", json!(false), json!(false)),
                None,
            ),
            (
                "isOn, no old value, released",
                attribute("isOn", Value::Null, json!(false)),
                None,
            ),
            // `StateChanged`.
            (
                "StateChanged press",
                state_changed(Some(false), true),
                Some(true),
            ),
            (
                "StateChanged release",
                state_changed(Some(true), false),
                Some(false),
            ),
            (
                "StateChanged unchanged",
                state_changed(Some(false), false),
                None,
            ),
            (
                "StateChanged, no old state, pressed",
                state_changed(None, true),
                Some(true),
            ),
            (
                "StateChanged, no old state, released",
                state_changed(None, false),
                None,
            ),
            // Not a switch at all.
            (
                "a light",
                attribute("state", Value::Null, json!({"Light": {"is_on": true}})),
                None,
            ),
            (
                "another attribute",
                attribute("battery", json!(85), json!(84)),
                None,
            ),
        ];
        for (case, event, want) in cases {
            assert_eq!(pressed(&event), want, "{case}");
        }
    }
}
