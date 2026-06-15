// 🔥 SIMPLE WEBSOCKET FOR V1BECTL APP - NO TOKIO! 🔥

use futures::{channel::mpsc, SinkExt, StreamExt};
use gloo_net::http::Request;
use gloo_net::websocket::{futures::WebSocket, Message};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

// Import types from v1bectl_models - no tokio dependency!
use v1bectl_state::RgbColor;

// Config structure
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Config {
    socket: String,
}

// Load config from static file
async fn load_config() -> Option<String> {
    match Request::get("/static/config.json").send().await {
        Ok(resp) => {
            if resp.ok() {
                match resp.json::<Config>().await {
                    Ok(config) => {
                        log::info!("✅ Loaded config: {}", config.socket);
                        Some(config.socket)
                    }
                    Err(e) => {
                        log::error!("❌ Failed to parse config: {}", e);
                        None
                    }
                }
            } else {
                log::warn!("⚠️ Config not found (status: {})", resp.status());
                None
            }
        }
        Err(e) => {
            log::error!("❌ Failed to fetch config: {}", e);
            None
        }
    }
}

// API Message wrapper for CBOR encoding
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApiMessage {
    pub correlation_id: String,
    pub message_type: ApiMessageType,
    pub payload: Vec<u8>, // CBOR-encoded payload
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    }, // Full device states with values!
    DeviceInfo {
        device: DeviceInfo,
    },
    DeviceState {
        state: serde_json::Value,
    }, // Keep as JSON for web simplicity
    LightUpdated {
        new_state: serde_json::Value,
    }, // Keep as JSON for web simplicity
    VirtualDeviceCreated {
        device_id: String,
    },
    VirtualDeviceRemoved {
        device_id: String,
    },
    SceneActivated {
        device_id: String,
        scene_name: String,
    },
    SubscriptionStarted {
        subscriber_id: String,
    },
    Error {
        code: String,
        message: String,
    },
}

// Re-export types from v1bectl_state for convenience
pub use v1bectl_state::{Capability, DeviceInfo, DeviceState, DeviceStateValue, DeviceType};

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
    Error(String),
}

struct WsState {
    sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

// 🔥 WEBSOCKET HOOK - CONNECTS TO ELITE PORT 31337! 🔥
#[hook]
pub fn use_websocket() -> UseWebSocketHandle {
    let status = use_state(|| ConnectionStatus::Connecting);
    let last_response = use_state(|| None::<ApiResponse>);
    let last_event = use_state(|| None::<serde_json::Value>); // 🔥 TRACK EVENTS TOO! 💖
    let ws_state = use_state(|| Rc::new(RefCell::new(WsState { sender: None })));

    // Connect on mount
    {
        let status = status.clone();
        let ws_state = ws_state.clone();
        let last_response = last_response.clone();
        let last_event = last_event.clone(); // 🔥 CLONE FOR USE IN CLOSURE! 💖

        use_effect_with((), move |_| {
            spawn_local(async move {
                log::info!("🔥 Loading config and connecting to VIBEC0RE server!");

                status.set(ConnectionStatus::Connecting);

                // Try to load config first, fallback to default
                let ws_url = load_config().await.unwrap_or_else(|| {
                    log::warn!("⚠️ Failed to load config, using default ws://localhost:31337");
                    "ws://localhost:31337".to_string()
                });

                log::info!("🚀 Connecting to: {}", ws_url);

                match WebSocket::open(&ws_url) {
                    Ok(ws) => {
                        log::info!("🚀 WebSocket connection opened!");
                        status.set(ConnectionStatus::Connected);

                        let (mut write, mut read) = ws.split();

                        // Create channel for sending messages
                        let (tx, mut rx) = mpsc::unbounded::<Vec<u8>>();
                        ws_state.borrow_mut().sender = Some(tx);

                        // Spawn write task
                        spawn_local(async move {
                            while let Some(data) = rx.next().await {
                                if let Err(e) = write.send(Message::Bytes(data)).await {
                                    log::error!("❌ Failed to send: {:?}", e);
                                    break;
                                }
                            }
                        });

                        // Spawn read loop
                        let status_clone = status.clone();
                        let last_response_clone = last_response.clone();
                        let last_event_clone = last_event.clone(); // 🔥 CLONE FOR EVENTS! 💖

                        spawn_local(async move {
                            while let Some(msg) = read.next().await {
                                match msg {
                                    Ok(Message::Bytes(data)) => {
                                        // Decode CBOR message
                                        match ciborium::from_reader::<ApiMessage, _>(
                                            data.as_slice(),
                                        ) {
                                            Ok(api_msg) => {
                                                if api_msg.message_type == ApiMessageType::Response
                                                {
                                                    // Decode response payload
                                                    match ciborium::from_reader::<ApiResponse, _>(
                                                        api_msg.payload.as_slice(),
                                                    ) {
                                                        Ok(response) => {
                                                            log::info!(
                                                                "📥 Received response: {:?}",
                                                                response
                                                            );
                                                            last_response_clone.set(Some(response));
                                                        }
                                                        Err(e) => {
                                                            log::error!(
                                                                "❌ Failed to decode response: {}",
                                                                e
                                                            );
                                                        }
                                                    }
                                                } else if api_msg.message_type
                                                    == ApiMessageType::Event
                                                {
                                                    // 🔥 HANDLE EVENTS - DEVICE STATE CHANGES! 💖
                                                    log::info!(
                                                        "📢 Got Event message type! Decoding..."
                                                    );
                                                    match ciborium::from_reader::<
                                                        serde_json::Value,
                                                        _,
                                                    >(
                                                        api_msg.payload.as_slice()
                                                    ) {
                                                        Ok(event) => {
                                                            log::info!("🔥🔥🔥 EVENT DECODED SUCCESSFULLY!");
                                                            log::info!(
                                                                "📊 Event content: {}",
                                                                serde_json::to_string(&event)
                                                                    .unwrap_or_default()
                                                            );
                                                            last_event_clone.set(Some(event));
                                                        }
                                                        Err(e) => {
                                                            log::error!(
                                                                "❌ Failed to decode event: {}",
                                                                e
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    log::info!(
                                                        "📦 Got message type: {:?}",
                                                        api_msg.message_type
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "❌ Failed to decode CBOR message: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("❌ WebSocket error: {:?}", e);
                                        status_clone
                                            .set(ConnectionStatus::Error(format!("{:?}", e)));
                                        break;
                                    }
                                    _ => {}
                                }
                            }

                            log::warn!("💔 WebSocket connection closed");
                            status_clone.set(ConnectionStatus::Disconnected);
                        });
                    }
                    Err(e) => {
                        log::error!("❌ Failed to connect: {:?}", e);
                        status.set(ConnectionStatus::Error(format!("{:?}", e)));
                    }
                }
            });

            || ()
        });
    }

    let send_request = {
        let ws_state = ws_state.clone();

        Callback::from(move |request: ApiRequest| {
            let ws_state = ws_state.clone();

            spawn_local(async move {
                let correlation_id = Uuid::new_v4().to_string();

                // Encode request
                let mut request_payload = Vec::new();
                if let Err(e) = ciborium::into_writer(&request, &mut request_payload) {
                    log::error!("❌ Failed to encode request: {}", e);
                    return;
                }

                // Create API message
                let api_message = ApiMessage {
                    correlation_id: correlation_id.clone(),
                    message_type: ApiMessageType::Request,
                    payload: request_payload,
                };

                // Encode API message
                let mut message_bytes = Vec::new();
                if let Err(e) = ciborium::into_writer(&api_message, &mut message_bytes) {
                    log::error!("❌ Failed to encode API message: {}", e);
                    return;
                }

                log::info!("📤 Sending request: {:?}", request);

                // Send via channel
                let state = ws_state.borrow();
                if let Some(sender) = &state.sender {
                    if let Err(e) = sender.unbounded_send(message_bytes) {
                        log::error!("❌ Failed to queue message: {:?}", e);
                    }
                } else {
                    log::error!("❌ No WebSocket connection!");
                }
            });
        })
    };

    UseWebSocketHandle {
        status: (*status).clone(),
        last_response: (*last_response).clone(),
        last_event: (*last_event).clone(), // 🔥 INCLUDE EVENTS! 💖
        send_request,
    }
}

#[derive(Clone)]
pub struct UseWebSocketHandle {
    pub status: ConnectionStatus,
    pub last_response: Option<ApiResponse>,
    pub last_event: Option<serde_json::Value>, // 🔥 EVENTS FOR REAL-TIME UPDATES! 💖
    pub send_request: Callback<ApiRequest>,
}
