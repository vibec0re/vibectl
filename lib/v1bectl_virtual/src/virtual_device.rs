use crate::button_controller::ButtonAction;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use v1bectl_sync::{
    Capability, DeviceEvent, DeviceId, DeviceInfo, DeviceState, DeviceStateValue, DeviceType,
    StateError,
};

#[derive(Debug, thiserror::Error)]
pub enum VirtualDeviceError {
    #[error("Invalid state type for this virtual device")]
    InvalidStateType,
    #[error("Device not found: {0}")]
    DeviceNotFound(String),
    #[error("State store error: {0}")]
    StateStore(#[from] StateError),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Timer error: {0}")]
    Timer(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VirtualDeviceType {
    LightGroup,
    LightGroupLinear, // 🔥 NEW LINEAR BRIGHTNESS MAPPING TYPE! 💖
    ButtonController, // 🔥 BUTTON EVENT CONTROLLER! 💖
    SceneController,
    ConditionalDevice,
    TimerDevice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualDeviceConfig {
    pub device_id: DeviceId,
    pub device_type: VirtualDeviceType,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub config: serde_json::Value, // Type-specific configuration
}

/// Core trait for all virtual devices - VIBEC0RE MAGIC! 🔥
#[async_trait]
pub trait VirtualDevice: Send + Sync {
    /// Get the device ID
    fn device_id(&self) -> &DeviceId;

    /// Get the virtual device type
    fn device_type(&self) -> VirtualDeviceType;

    /// Get the configuration
    fn config(&self) -> &VirtualDeviceConfig;

    /// Called when virtual device state should change (API request)
    async fn set_state(&mut self, new_state: DeviceStateValue) -> Result<(), VirtualDeviceError>;

    /// Take the state this device's inputs give it, as the store holds them
    /// now. The manager calls this once, when it registers the device. So a
    /// group starts out showing its members, and with a level to light them
    /// at (#16), instead of a made-up "off at 0". The default does nothing.
    async fn seed_from_inputs(&mut self) -> Result<(), VirtualDeviceError> {
        Ok(())
    }

    /// Called when input device states change. `new_state` is the input as
    /// the store holds it when the manager gets to the change, which can be
    /// newer than the event that announced it.
    async fn on_input_changed(
        &mut self,
        device_id: &DeviceId,
        new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        // Default implementation does nothing
        let _ = (device_id, new_state);
        Ok(())
    }

    /// Whether this device's current state already accounts for `state`,
    /// which `input` (one of [`Self::input_devices`]) holds now. It does if
    /// fanning the current state out again would leave `input` as it is.
    /// The manager then skips [`Self::on_input_changed`] for it.
    ///
    /// This is how a group tells the echo of its own write from a real
    /// outside change. It matters because re-deriving a group from its
    /// members can be lossy (a linear group set to 50 reads back as 60).
    ///
    /// After a re-derive, a group's state is usually one its members don't
    /// hold (an average), so it accounts for none of them. Every later
    /// event of a member then re-derives it again, until a write fans a
    /// state out to them. That's harmless: the members haven't moved, so
    /// the re-derive lands on the same state, and the manager echoes nothing
    /// for an unchanged one (#22 re-review: a 50-event storm, no echo).
    ///
    /// The default, `false`, re-derives on every input change.
    fn accounts_for(&self, input: &DeviceId, state: &DeviceStateValue) -> bool {
        let _ = (input, state);
        false
    }

    /// The writes this device asks for in reaction to `event`, an event of
    /// one of its [`Self::input_devices`]: a button controller's action for
    /// a press. Unlike [`Self::on_input_changed`], this sees the event
    /// itself, not the input as the store holds it: a press is an event, and
    /// by the time the manager gets to it the store may hold the release.
    ///
    /// The manager makes each write after it has tracked the event, the way
    /// it makes an API write: through `set_virtual_device_state` for a
    /// virtual target (fan-out, echoes, gateway), and like a direct write
    /// for a physical one. The default asks for nothing.
    fn reactions(&self, event: &DeviceEvent) -> Vec<ButtonAction> {
        let _ = event;
        Vec::new()
    }

    /// Get current virtual device state
    fn current_state(&self) -> DeviceStateValue;

    /// Which physical devices this virtual device depends on (for input)
    fn input_devices(&self) -> Vec<DeviceId> {
        Vec::new()
    }

    /// Which physical devices this virtual device controls (for output)
    fn output_devices(&self) -> Vec<DeviceId> {
        Vec::new()
    }

    /// Get device info that will be exposed via API
    fn device_info(&self) -> DeviceInfo {
        let config = self.config();
        DeviceInfo {
            device_id: config.device_id.clone(),
            name: config.name.clone(),
            device_type: match self.device_type() {
                // 🔥 Same device type, different impl!
                VirtualDeviceType::LightGroup | VirtualDeviceType::LightGroupLinear => {
                    DeviceType::VirtualLightGroup
                }
                // 🔥 Controller is like conditional!
                VirtualDeviceType::ButtonController | VirtualDeviceType::ConditionalDevice => {
                    DeviceType::VirtualConditional
                }
                VirtualDeviceType::SceneController => DeviceType::VirtualScene,
                VirtualDeviceType::TimerDevice => DeviceType::VirtualTimer,
            },
            capabilities: self.get_capabilities(),
            device_groups: vec!["virtual".to_string()],
            manufacturer: Some("v1bectl".to_string()),
            model: Some("Virtual Device".to_string()),
            firmware_version: Some("1.0.0".to_string()),
            battery_powered: false,
            reachable: config.enabled,
            last_seen: chrono::Utc::now().timestamp_millis().cast_unsigned(),
            custom_attributes: HashMap::new(),
        }
    }

    /// Get capabilities for this virtual device
    fn get_capabilities(&self) -> Vec<Capability> {
        match self.device_type() {
            VirtualDeviceType::LightGroup | VirtualDeviceType::LightGroupLinear => vec![
                Capability::OnOff,
                Capability::Brightness,
                Capability::ColorTemperature,
                Capability::RgbColor,
            ],
            // 🔥 Controllers have no capabilities, they just react!
            VirtualDeviceType::ButtonController
            | VirtualDeviceType::SceneController
            | VirtualDeviceType::ConditionalDevice
            | VirtualDeviceType::TimerDevice => vec![],
        }
    }
}
