use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;
use v1bectl_sync::*;

// Mirror the API types from the server
#[derive(Serialize, Deserialize, Debug)]
struct ApiMessage {
    correlation_id: String,
    message_type: ApiMessageType,
    payload: Vec<u8>, // CBOR-encoded payload
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiRequest {
    DiscoverDevices,
    ListDevices {
        device_type: Option<String>,
        groups: Option<Vec<String>>,
        reachable_only: Option<bool>,
    },
    GetDevice {
        device_id: String,
    },
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
    Subscribe {
        device_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    }, // 🔥 Fixed to match server sending DeviceState!
    DeviceInfo {
        device: Box<DeviceInfo>,
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

pub struct WebSocketClient {
    server_url: String,
}

impl WebSocketClient {
    pub fn new(server_addr: String) -> Self {
        let server_url = if server_addr.starts_with("ws://") || server_addr.starts_with("wss://") {
            server_addr
        } else {
            format!("ws://{}", server_addr)
        };

        Self { server_url }
    }

    pub async fn discover_devices(&self) -> anyhow::Result<(Vec<DeviceState>, u32)> {
        let response = self.send_request(ApiRequest::DiscoverDevices).await?;

        match response {
            ApiResponse::DeviceList {
                devices,
                total_count,
            } => Ok((devices, total_count)),
            ApiResponse::Error { code, message } => {
                anyhow::bail!("Server error {}: {}", code, message);
            }
            _ => anyhow::bail!("Unexpected response type"),
        }
    }

    pub async fn get_device_state(&self, device_id: &str) -> anyhow::Result<DeviceStateValue> {
        let response = self
            .send_request(ApiRequest::GetDeviceState {
                device_id: device_id.to_string(),
            })
            .await?;

        match response {
            ApiResponse::DeviceState { state } => Ok(state),
            ApiResponse::Error { code, message } => {
                anyhow::bail!("Server error {}: {}", code, message);
            }
            _ => anyhow::bail!("Unexpected response type"),
        }
    }

    pub async fn set_light_state(
        &self,
        device_id: &str,
        light_state: LightState,
    ) -> anyhow::Result<LightState> {
        let response = self
            .send_request(ApiRequest::SetLightState {
                device_id: device_id.to_string(),
                is_on: Some(light_state.is_on),
                brightness: light_state.brightness,
                color_temp: light_state.color_temp,
                rgb_color: light_state.rgb_color,
            })
            .await?;

        match response {
            ApiResponse::LightUpdated { new_state } => Ok(new_state),
            ApiResponse::Error { code, message } => {
                anyhow::bail!("Server error {}: {}", code, message);
            }
            _ => anyhow::bail!("Unexpected response type"),
        }
    }

    pub async fn subscribe_to_events(
        &self,
        device_ids: Vec<String>,
    ) -> anyhow::Result<impl futures_util::Stream<Item = anyhow::Result<DeviceEvent>>> {
        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.server_url).await?;
        let (mut ws_sender, ws_receiver) = ws_stream.split();

        // Generate correlation ID
        let correlation_id = Uuid::new_v4().to_string();

        // Send subscription request
        let request = ApiRequest::Subscribe { device_ids };
        let request_payload = {
            let mut buf = Vec::new();
            ciborium::into_writer(&request, &mut buf)?;
            buf
        };

        let api_message = ApiMessage {
            correlation_id: correlation_id.clone(),
            message_type: ApiMessageType::Request,
            payload: request_payload,
        };

        let message_bytes = {
            let mut buf = Vec::new();
            ciborium::into_writer(&api_message, &mut buf)?;
            buf
        };

        ws_sender.send(Message::Binary(message_bytes)).await?;

        // Return a stream that processes incoming events
        Ok(ws_receiver.filter_map(|msg| async move {
            match msg {
                Ok(Message::Binary(data)) => {
                    // Decode API message
                    match ciborium::from_reader::<ApiMessage, _>(data.as_slice()) {
                        Ok(api_message)
                            if matches!(api_message.message_type, ApiMessageType::Event) =>
                        {
                            // Decode event payload
                            match ciborium::from_reader::<DeviceEvent, _>(
                                api_message.payload.as_slice(),
                            ) {
                                Ok(event) => Some(Ok(event)),
                                Err(e) => {
                                    Some(Err(anyhow::anyhow!("Failed to decode event: {}", e)))
                                }
                            }
                        }
                        Ok(api_message)
                            if matches!(api_message.message_type, ApiMessageType::Response) =>
                        {
                            // Skip response messages (subscription confirmation)
                            None
                        }
                        Err(e) => Some(Err(anyhow::anyhow!("Failed to decode API message: {}", e))),
                        _ => None,
                    }
                }
                Ok(Message::Close(_)) => Some(Err(anyhow::anyhow!("WebSocket connection closed"))),
                Err(e) => Some(Err(anyhow::anyhow!("WebSocket error: {}", e))),
                _ => None,
            }
        }))
    }

    async fn send_request(&self, request: ApiRequest) -> anyhow::Result<ApiResponse> {
        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.server_url).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Generate correlation ID
        let correlation_id = Uuid::new_v4().to_string();

        // Encode request payload
        let mut request_payload = Vec::new();
        ciborium::into_writer(&request, &mut request_payload)?;

        // Create API message
        let api_message = ApiMessage {
            correlation_id: correlation_id.clone(),
            message_type: ApiMessageType::Request,
            payload: request_payload,
        };

        // Encode and send the message
        let mut message_bytes = Vec::new();
        ciborium::into_writer(&api_message, &mut message_bytes)?;
        ws_sender.send(Message::Binary(message_bytes)).await?;

        // Wait for response
        while let Some(msg) = ws_receiver.next().await {
            match msg? {
                Message::Binary(data) => {
                    // Decode API message
                    let api_message: ApiMessage = ciborium::from_reader(data.as_slice())?;

                    // Check if this is our response
                    if api_message.correlation_id == correlation_id
                        && matches!(api_message.message_type, ApiMessageType::Response)
                    {
                        // Decode response payload
                        let response: ApiResponse =
                            ciborium::from_reader(api_message.payload.as_slice())?;
                        return Ok(response);
                    }
                }
                Message::Close(_) => {
                    anyhow::bail!("WebSocket connection closed unexpectedly");
                }
                _ => {
                    // Ignore other message types
                }
            }
        }

        anyhow::bail!("No response received")
    }
}
