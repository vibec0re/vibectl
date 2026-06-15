// API handlers - placeholder
use v1bectl_sync::*;

pub async fn handle_discover_devices() -> Result<DiscoverDevicesResponse, ApiError> {
    // Placeholder
    Ok(DiscoverDevicesResponse {
        devices: vec![],
        total_count: 0,
        discovery_timestamp: 0,
        gateway_scan_duration_ms: 0,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Internal error")]
    Internal,
}
