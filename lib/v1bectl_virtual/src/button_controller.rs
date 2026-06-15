// 🔥 BUTTON CONTROLLER - REACTS TO BUTTON EVENTS! 💖

use crate::virtual_device::*;
use async_trait::async_trait;
use std::sync::Arc;
use v1bectl_sync::*;

#[derive(Debug, Clone)]
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

/// Button Controller - listens to button events and controls devices
pub struct ButtonController {
    config: VirtualDeviceConfig,
    button_id: String, // Button device to listen to
    press_on_action: Vec<serde_json::Value>,
    press_off_action: Vec<serde_json::Value>,
    // kept: long-press actions are accepted and stored now; the long-press
    // detection path is not wired up yet, so these aren't read.
    #[allow(dead_code)]
    press_on_long_action: Option<Vec<serde_json::Value>>,
    #[allow(dead_code)]
    press_off_long_action: Option<Vec<serde_json::Value>>,
    state_store: Arc<StateStore>,
    // kept: retained handle to the event bus; the listener task is spawned with
    // its own clone at construction, so this field isn't read afterwards.
    #[allow(dead_code)]
    event_bus: Arc<EventBus>,
    event_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ButtonController {
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
        state_store: Arc<StateStore>,
        event_bus: Arc<EventBus>,
    ) -> Result<Self, VirtualDeviceError> {
        let mut controller = Self {
            config,
            button_id,
            press_on_action: press_on,
            press_off_action: press_off,
            press_on_long_action: press_on_long,
            press_off_long_action: press_off_long,
            state_store,
            event_bus: event_bus.clone(),
            event_handle: None,
        };

        // Start event listener
        controller.start_event_listener(event_bus);

        Ok(controller)
    }

    fn start_event_listener(&mut self, event_bus: Arc<EventBus>) {
        let button_id = self.button_id.clone();
        let state_store = self.state_store.clone();
        let press_on = self.press_on_action.clone();
        let press_off = self.press_off_action.clone();

        // 🔥 SPAWN EVENT LISTENER TASK! 💖
        let handle = tokio::spawn(async move {
            let mut event_rx = event_bus.subscribe();
            tracing::info!(
                "🎮 ButtonController listening for button {} events!",
                button_id
            );

            while let Ok(event) = event_rx.recv().await {
                // Check if event is for our button
                if event.device_id == button_id {
                    match &event.event_type {
                        EventType::AttributeChanged {
                            attribute,
                            old_value: _,
                            new_value,
                        } => {
                            if attribute == "state" {
                                // The sync engine publishes state changes with attribute "state" containing the full state
                                if let Some(state_obj) = new_value.as_object() {
                                    if let Some(is_pressed) =
                                        state_obj.get("is_pressed").and_then(|v| v.as_bool())
                                    {
                                        tracing::debug!(
                                            "🔘 Button {} pressed: {} (from AttributeChanged)",
                                            button_id,
                                            is_pressed
                                        );

                                        let action =
                                            if is_pressed { &press_on } else { &press_off };

                                        if let Err(e) =
                                            Self::execute_action(action, &state_store).await
                                        {
                                            tracing::error!(
                                                "❌ Failed to execute button action: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                            } else if attribute == "isOn" || attribute == "buttonState" {
                                tracing::debug!(
                                    "🔘 Button {} event: {} -> {:?}",
                                    button_id,
                                    attribute,
                                    new_value
                                );

                                // Determine action based on state change
                                let action = if let Some(bool_val) = new_value.as_bool() {
                                    if bool_val {
                                        &press_on
                                    } else {
                                        &press_off
                                    }
                                } else {
                                    continue;
                                };

                                // Execute action
                                if let Err(e) = Self::execute_action(action, &state_store).await {
                                    tracing::error!("❌ Failed to execute button action: {}", e);
                                }
                            }
                        }
                        EventType::StateChanged {
                            new_state: Some(DeviceStateValue::Switch(switch_state)),
                            ..
                        } => {
                            tracing::debug!(
                                "🔘 Button {} pressed: {}",
                                button_id,
                                switch_state.is_pressed
                            );

                            let action = if switch_state.is_pressed {
                                &press_on
                            } else {
                                &press_off
                            };

                            if let Err(e) = Self::execute_action(action, &state_store).await {
                                tracing::error!("❌ Failed to execute button action: {}", e);
                            }
                        }
                        _ => {}
                    }
                }
            }

            tracing::warn!("⚠️ ButtonController event listener ended for {}", button_id);
        });

        self.event_handle = Some(handle);
    }

    async fn execute_action(
        action: &[serde_json::Value],
        state_store: &Arc<StateStore>,
    ) -> Result<(), VirtualDeviceError> {
        if action.len() < 2 {
            return Err(VirtualDeviceError::Config(
                "Invalid action format".to_string(),
            ));
        }

        let cmd_str = action[0]
            .as_str()
            .ok_or_else(|| VirtualDeviceError::Config("Command must be string".to_string()))?;
        let device_id = action[1]
            .as_str()
            .ok_or_else(|| VirtualDeviceError::Config("Device ID must be string".to_string()))?;

        let cmd = ButtonCommand::from_str(cmd_str)
            .ok_or_else(|| VirtualDeviceError::Config(format!("Unknown command: {}", cmd_str)))?;

        // Get current device state
        let device_state = state_store
            .get_device(&device_id.to_string())
            .await
            .ok_or_else(|| VirtualDeviceError::DeviceNotFound(device_id.to_string()))?;

        match device_state.state {
            DeviceStateValue::Light(mut light_state) => {
                match cmd {
                    ButtonCommand::Inc => {
                        // Increment brightness by value (default 10)
                        let increment = action.get(2).and_then(|v| v.as_u64()).unwrap_or(10) as u8;
                        let current = light_state.brightness.unwrap_or(0);
                        light_state.brightness = Some((current + increment).min(100));
                        light_state.is_on = true;
                    }
                    ButtonCommand::Dec => {
                        // Decrement brightness by value (default 10)
                        let decrement = action.get(2).and_then(|v| v.as_u64()).unwrap_or(10) as u8;
                        let current = light_state.brightness.unwrap_or(100);
                        let new_brightness = current.saturating_sub(decrement);
                        light_state.brightness = Some(new_brightness);
                        if new_brightness == 0 {
                            light_state.is_on = false;
                        }
                    }
                    ButtonCommand::Set => {
                        // Set specific brightness
                        let brightness = action.get(2).and_then(|v| v.as_u64()).unwrap_or(50) as u8;
                        light_state.brightness = Some(brightness);
                        light_state.is_on = brightness > 0;
                    }
                    ButtonCommand::On => {
                        light_state.is_on = true;
                        if light_state.brightness.unwrap_or(0) == 0 {
                            light_state.brightness = Some(100);
                        }
                    }
                    ButtonCommand::Off => {
                        light_state.is_on = false;
                    }
                }

                // Update device state
                state_store
                    .update_device_state(
                        &device_id.to_string(),
                        DeviceStateValue::Light(light_state),
                    )
                    .await?;
                tracing::info!("✅ ButtonController executed {:?} on {}", cmd, device_id);
            }
            _ => {
                tracing::warn!(
                    "⚠️ ButtonController can only control lights, got {:?}",
                    device_state.state
                );
            }
        }

        Ok(())
    }
}

impl Drop for ButtonController {
    fn drop(&mut self) {
        // Cancel event listener when dropped
        if let Some(handle) = self.event_handle.take() {
            handle.abort();
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

    async fn on_input_changed(
        &mut self,
        _device_id: &DeviceId,
        _new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        // Handled by event listener
        Ok(())
    }

    fn current_state(&self) -> DeviceStateValue {
        // ButtonController has no state, return empty
        DeviceStateValue::Empty
    }

    fn input_devices(&self) -> Vec<DeviceId> {
        vec![self.button_id.clone()]
    }

    fn output_devices(&self) -> Vec<DeviceId> {
        // Extract device IDs from actions
        let mut devices = Vec::new();

        if self.press_on_action.len() > 1 {
            if let Some(device_id) = self.press_on_action[1].as_str() {
                devices.push(device_id.to_string());
            }
        }

        if self.press_off_action.len() > 1 {
            if let Some(device_id) = self.press_off_action[1].as_str() {
                if !devices.contains(&device_id.to_string()) {
                    devices.push(device_id.to_string());
                }
            }
        }

        devices
    }
}
