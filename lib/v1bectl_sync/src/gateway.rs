use async_trait::async_trait;
use std::time::Duration;
use v1bectl_state::*;

pub type EventStream = tokio::sync::broadcast::Receiver<DeviceEvent>;

#[async_trait]
pub trait Gateway: Send + Sync {
    async fn discover_devices(&self) -> Result<Vec<DeviceInfo>, GatewayError>;
    async fn get_device_state(
        &self,
        device_id: &DeviceId,
    ) -> Result<DeviceStateValue, GatewayError>;
    async fn set_device_state(
        &self,
        device_id: &DeviceId,
        state: DeviceStateValue,
    ) -> Result<(), GatewayError>;
    async fn health_check(&self) -> Result<GatewayHealth, GatewayError>;

    async fn event_stream(&self) -> Result<EventStream, GatewayError> {
        Err(GatewayError::InternalError(
            "Event stream not implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct GatewayHealth {
    pub reachable: bool,
    pub response_time_ms: u64,
    pub connected_devices: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Device not found: {0}")]
    DeviceNotFound(DeviceId),
    #[error("Device unreachable: {0}")]
    DeviceUnreachable(DeviceId),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Invalid state for device type")]
    InvalidStateType,
    #[error("Gateway timeout")]
    Timeout,
    #[error("Authentication failed")]
    AuthenticationFailed,
    #[error("Gateway internal error: {0}")]
    InternalError(String),
}

pub struct GatewayConfig {
    pub gateway_type: GatewayType,
    pub dirigera_host: Option<String>,
    pub access_token: Option<String>,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum GatewayType {
    Dummy { scenario: String },
    Dirigera { host: String, token: String },
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            gateway_type: GatewayType::Dummy {
                scenario: "basic_home".to_string(),
            },
            dirigera_host: None,
            access_token: None,
            timeout: Duration::from_secs(10),
        }
    }
}
