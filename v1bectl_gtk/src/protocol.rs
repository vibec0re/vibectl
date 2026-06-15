// 🔥 PROTOCOL TYPES - MIRROR API WIRE FORMAT!!! 🚀

use serde::{Deserialize, Serialize};
use v1bectl_sync::*;

/// API message envelope — wraps requests, responses, and events
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApiMessage {
    pub correlation_id: String,
    pub message_type: ApiMessageType,
    pub payload: Vec<u8>, // CBOR-encoded
}

/// Message type classifier for routing
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

/// Client requests to the v1bectl server
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ApiRequest {
    DiscoverDevices,
    GetDeviceState {
        device_id: String,
    },
    SetLightState {
        device_id: String,
        is_on: Option<bool>,
        brightness: Option<u8>,
        color_temp: Option<u16>,
        rgb_color: Option<RgbColor>,
    },
    SetOutletState {
        device_id: String,
        is_on: bool,
    },
    Subscribe {
        device_ids: Vec<String>,
    },
}

/// Server responses to client requests
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    },
    DeviceState {
        state: DeviceStateValue,
    },
    LightUpdated {
        new_state: LightState,
    },
    SubscriptionStarted {
        subscriber_id: String,
    },
    Error {
        code: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_message_serialization() {
        let msg = ApiMessage {
            correlation_id: "test-123".to_string(),
            message_type: ApiMessageType::Request,
            payload: vec![1, 2, 3],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ApiMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.correlation_id, "test-123");
    }
}
