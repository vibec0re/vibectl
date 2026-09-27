// 🔥 BUTTON CONTROLLER - REACTS TO BUTTON EVENTS! 💖

use crate::virtual_device::*;
use async_trait::async_trait;
use std::sync::Arc;
use v1bectl_sync::*;

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
            .ok_or_else(|| VirtualDeviceError::Config(format!("Unknown command: {}", cmd_str)))?;
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

/// Button Controller: runs an action when its button is pressed, and
/// another when it's released.
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
    // kept: long-press actions are accepted and stored now; the long-press
    // detection path is not wired up yet, so these aren't read.
    #[allow(dead_code)]
    press_on_long_action: Option<Vec<serde_json::Value>>,
    #[allow(dead_code)]
    press_off_long_action: Option<Vec<serde_json::Value>>,
}

impl ButtonController {
    /// A controller for `button_id` that runs `press_on` on a press and
    /// `press_off` on a release. Each is `[command, target, amount]` (see
    /// [`ButtonAction::parse`]); an empty one does nothing, and a malformed
    /// one fails here.
    ///
    /// `state_store` and `event_bus` aren't used any more (the manager
    /// delivers the events and makes the writes). They stay so callers
    /// don't have to change.
    // kept: constructor takes the full set of button actions and dependencies;
    // grouping them into a struct would change the public API.
    #[allow(clippy::too_many_arguments)]
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
            press_on_long_action: press_on_long,
            press_off_long_action: press_off_long,
        })
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
        let Some(is_pressed) = pressed(event) else {
            return Vec::new();
        };
        tracing::debug!("🔘 Button {} pressed: {}", self.button_id, is_pressed);

        let action = if is_pressed {
            &self.press_on_action
        } else {
            &self.press_off_action
        };
        action.iter().cloned().collect()
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
        for action in [&self.press_on_action, &self.press_off_action]
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
