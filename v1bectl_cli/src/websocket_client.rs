use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;
use v1bectl_sync::{
    ButtonPressType, DeviceEvent, DeviceId, DeviceInfo, DeviceState, DeviceStateValue, LightState,
    RgbColor,
};

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
    PressButton {
        device_id: String,
        press_type: ButtonPressType,
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
    /// `device_id` is whose state it is (#10). A server from before it
    /// doesn't send one.
    DeviceState {
        #[serde(default)]
        device_id: Option<DeviceId>,
        state: DeviceStateValue,
    },
    LightUpdated {
        new_state: LightState,
    },
    SubscriptionStarted {
        subscriber_id: String,
    },
    ButtonPressed {
        device_id: String,
        press_type: ButtonPressType,
    },
    Error {
        code: String,
        message: String,
    },
}

/// The answer to `GetDeviceState`, once we know it's about `requested` (#10,
/// #54). A server that doesn't send a `device_id` at all (from before #10)
/// is trusted as-is: there's nothing to check it against. Pulled out of
/// [`WebSocketClient::get_device_state`] so the guard is unit-testable
/// without a live server.
fn check_device_state_answer(
    response: ApiResponse,
    requested: &str,
) -> anyhow::Result<DeviceStateValue> {
    match response {
        ApiResponse::DeviceState {
            device_id: Some(answered),
            ..
        } if answered != requested => {
            anyhow::bail!("Server answered with the state of {answered}, not {requested}");
        }
        ApiResponse::DeviceState { state, .. } => Ok(state),
        ApiResponse::Error { code, message } => {
            anyhow::bail!("Server error {code}: {message}");
        }
        _ => anyhow::bail!("Unexpected response type"),
    }
}

/// How long [`WebSocketClient::press_button`] waits for the server's answer.
const PRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub struct WebSocketClient {
    server_url: String,
}

impl WebSocketClient {
    pub fn new(server_addr: String) -> Self {
        let server_url = if server_addr.starts_with("ws://") || server_addr.starts_with("wss://") {
            server_addr
        } else {
            format!("ws://{server_addr}")
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
                anyhow::bail!("Server error {code}: {message}");
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

        check_device_state_answer(response, device_id)
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
                anyhow::bail!("Server error {code}: {message}");
            }
            _ => anyhow::bail!("Unexpected response type"),
        }
    }

    /// Ask the server to press `device_id` with `press_type`, as if by hand
    /// (#35). It publishes the press, so the button controllers bound to
    /// the switch run. Only the dummy gateway's switches can be pressed:
    /// the server refuses any other device. Returns the press the server
    /// published.
    pub async fn press_button(
        &self,
        device_id: &str,
        press_type: ButtonPressType,
    ) -> anyhow::Result<(DeviceId, ButtonPressType)> {
        let request = ApiRequest::PressButton {
            device_id: device_id.to_string(),
            press_type,
        };
        // A server from before #35 can't decode the request, and never
        // answers it.
        let response = tokio::time::timeout(PRESS_TIMEOUT, self.send_request(request))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "No answer from the server within {}s: it may be older than the `button` command",
                    PRESS_TIMEOUT.as_secs()
                )
            })??;

        match response {
            ApiResponse::ButtonPressed {
                device_id,
                press_type,
            } => Ok((device_id, press_type)),
            ApiResponse::Error { code, message } => {
                anyhow::bail!("Server error {code}: {message}");
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
                                Err(e) => Some(Err(anyhow::anyhow!("Failed to decode event: {e}"))),
                            }
                        }
                        Ok(api_message)
                            if matches!(api_message.message_type, ApiMessageType::Response) =>
                        {
                            // Skip response messages (subscription confirmation)
                            None
                        }
                        Err(e) => Some(Err(anyhow::anyhow!("Failed to decode API message: {e}"))),
                        _ => None,
                    }
                }
                Ok(Message::Close(_)) => Some(Err(anyhow::anyhow!("WebSocket connection closed"))),
                Err(e) => Some(Err(anyhow::anyhow!("WebSocket error: {e}"))),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn light_state(is_on: bool) -> DeviceStateValue {
        DeviceStateValue::Light(LightState {
            is_on,
            brightness: Some(50),
            color_temp: None,
            rgb_color: None,
        })
    }

    // #54: the `device_id` guard in `check_device_state_answer` (used to be
    // inline in `get_device_state`). Every arm, so an inverted guard (`==`
    // for `!=`, or vice versa) shows up as a red test rather than a CLI that
    // silently accepts, or silently refuses, every answer.

    #[test]
    fn a_matching_device_id_is_accepted() {
        let response = ApiResponse::DeviceState {
            device_id: Some("light-1".to_string()),
            state: light_state(true),
        };

        let state = check_device_state_answer(response, "light-1").expect("matching id");

        assert_eq!(state, light_state(true));
    }

    #[test]
    fn a_different_device_id_is_refused() {
        let response = ApiResponse::DeviceState {
            device_id: Some("light-2".to_string()),
            state: light_state(true),
        };

        let err = check_device_state_answer(response, "light-1").expect_err("mismatched id");

        assert_eq!(
            err.to_string(),
            "Server answered with the state of light-2, not light-1"
        );
    }

    #[test]
    fn no_device_id_from_an_older_server_is_accepted() {
        // A server from before #10 doesn't send one: there's nothing to
        // check the answer against, so it's trusted as-is.
        let response = ApiResponse::DeviceState {
            device_id: None,
            state: light_state(false),
        };

        let state = check_device_state_answer(response, "light-1").expect("no id: trust it");

        assert_eq!(state, light_state(false));
    }

    #[test]
    fn a_non_device_state_response_is_the_existing_error() {
        let response = ApiResponse::Error {
            code: "NOT_FOUND".to_string(),
            message: "no such device".to_string(),
        };

        let err = check_device_state_answer(response, "light-1").expect_err("server error");

        assert_eq!(err.to_string(), "Server error NOT_FOUND: no such device");
    }

    /// #54: the CLI's `ApiResponse::DeviceState` decodes `device_id` as
    /// `#[serde(default)] Option<DeviceId>`, so a typo in that field name
    /// would silently decode every answer as `None` — the guard above would
    /// simply never fire, and every other test stays green. This mirrors the
    /// server's *current* `ApiResponse::DeviceState` shape exactly (a plain,
    /// non-`Option` `device_id: String`; see
    /// `lib/v1bectl_api/src/axum_server.rs`), encodes it through the CLI's
    /// own `ApiMessage` envelope the way `send_request` receives one over
    /// the wire, and checks that it comes out the other side as `Some`.
    #[test]
    fn a_server_shaped_device_state_answer_decodes_with_its_device_id() {
        #[derive(Serialize)]
        enum ServerShapedResponse {
            DeviceState {
                device_id: String,
                state: DeviceStateValue,
            },
        }

        let payload = {
            let mut buf = Vec::new();
            ciborium::into_writer(
                &ServerShapedResponse::DeviceState {
                    device_id: "light-1".to_string(),
                    state: light_state(true),
                },
                &mut buf,
            )
            .unwrap();
            buf
        };

        let mut bytes = Vec::new();
        ciborium::into_writer(
            &ApiMessage {
                correlation_id: "corr-1".to_string(),
                message_type: ApiMessageType::Response,
                payload,
            },
            &mut bytes,
        )
        .unwrap();

        let envelope: ApiMessage =
            ciborium::from_reader(bytes.as_slice()).expect("envelope decodes");
        let response: ApiResponse =
            ciborium::from_reader(envelope.payload.as_slice()).expect("payload decodes");

        assert!(
            matches!(
                &response,
                ApiResponse::DeviceState { device_id: Some(id), .. } if id == "light-1"
            ),
            "{response:?}"
        );
    }
}
