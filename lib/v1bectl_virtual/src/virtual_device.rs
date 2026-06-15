use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use v1bectl_sync::*;

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

    /// Called when input device states change
    async fn on_input_changed(
        &mut self,
        device_id: &DeviceId,
        new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        // Default implementation does nothing
        let _ = (device_id, new_state);
        Ok(())
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
                VirtualDeviceType::LightGroup => DeviceType::VirtualLightGroup,
                VirtualDeviceType::LightGroupLinear => DeviceType::VirtualLightGroup, // 🔥 Same device type, different impl!
                VirtualDeviceType::ButtonController => DeviceType::VirtualConditional, // 🔥 Controller is like conditional!
                VirtualDeviceType::SceneController => DeviceType::VirtualScene,
                VirtualDeviceType::ConditionalDevice => DeviceType::VirtualConditional,
                VirtualDeviceType::TimerDevice => DeviceType::VirtualTimer,
            },
            capabilities: self.get_capabilities(),
            device_groups: vec!["virtual".to_string()],
            manufacturer: Some("v1bectl".to_string()),
            model: Some("Virtual Device".to_string()),
            firmware_version: Some("1.0.0".to_string()),
            battery_powered: false,
            reachable: config.enabled,
            last_seen: chrono::Utc::now().timestamp_millis() as u64,
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
            VirtualDeviceType::ButtonController => vec![], // 🔥 Controllers have no capabilities, they just react!
            VirtualDeviceType::SceneController => vec![],
            VirtualDeviceType::ConditionalDevice => vec![],
            VirtualDeviceType::TimerDevice => vec![],
        }
    }
}
