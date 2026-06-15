// 🔥 WEBSOCKET WITH AUTO-RECONNECT - NEVER GIVE UP! CHOOOM FIX! 💖

use futures::{channel::mpsc, SinkExt, StreamExt};
use gloo_events::EventListener;
use gloo_net::http::Request;
use gloo_net::websocket::{futures::WebSocket, Message};
use gloo_timers::future::TimeoutFuture;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

// Import types from v1bectl_models
use v1bectl_state::{DeviceEvent, RgbColor};

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
    Ping, // 🔥 PING FOR KEEPALIVE! CHOOOM FIX! 💖
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    },
    DeviceInfo {
        device: DeviceInfo,
    },
    DeviceState {
        state: serde_json::Value,
    },
    LightUpdated {
        new_state: serde_json::Value,
    },
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
    Pong, // 🔥 PONG RESPONSE! 💖
    Error {
        code: String,
        message: String,
    },
}

// Re-export types from v1bectl_state
pub use v1bectl_state::{
    Capability, DeviceInfo, DeviceState, DeviceStateValue, DeviceType, EventType,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
    Reconnecting(u32), // 🔥 Show reconnect attempt count! 💖
    Error(String),
}

struct WsState {
    sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
    reconnect_count: u32,
    ping_handle: Option<gloo_timers::callback::Interval>, // 🔥 PING TIMER! 💖
}

// 🔥 WEBSOCKET HOOK WITH AUTO-RECONNECT AND PING! CHOOOM FIX! 💖
#[hook]
pub fn use_websocket() -> UseWebSocketHandle {
    let status = use_state(|| ConnectionStatus::Connecting);
    let last_response = use_state(|| None::<ApiResponse>);
    let last_event = use_state(|| None::<serde_json::Value>);
    let ws_state = use_state(|| {
        Rc::new(RefCell::new(WsState {
            sender: None,
            reconnect_count: 0,
            ping_handle: None,
        }))
    });

    // 🔥 RECONNECT CALLBACK - USE REF FOR ALWAYS-FRESH VALUE! 💖
    let reconnect_callback = use_state(|| None::<Rc<dyn Fn()>>);
    let reconnect_ref: UseStateHandle<Rc<RefCell<Option<Rc<dyn Fn()>>>>> =
        use_state(|| Rc::new(RefCell::new(None)));

    // Create the connect function
    {
        let status = status.clone();
        let ws_state = ws_state.clone();
        let last_response = last_response.clone();
        let last_event = last_event.clone();
        let reconnect_callback = reconnect_callback.clone();
        let reconnect_ref = reconnect_ref.clone();

        use_effect_with((), move |_| {
            // 🔥 CREATE CONNECT FUNCTION THAT CAN CALL ITSELF! 💖
            let connect_fn = {
                let status = status.clone();
                let ws_state = ws_state.clone();
                let last_response = last_response.clone();
                let last_event = last_event.clone();
                let reconnect_callback_ref = reconnect_callback.clone();

                Rc::new(move || {
                    let status = status.clone();
                    let ws_state = ws_state.clone();
                    let last_response = last_response.clone();
                    let last_event = last_event.clone();
                    let reconnect_callback_ref = reconnect_callback_ref.clone();

                    spawn_local(async move {
                        let reconnect_count = ws_state.borrow().reconnect_count;

                        if reconnect_count > 0 {
                            log::info!("🔄 Reconnection attempt #{}", reconnect_count);
                            status.set(ConnectionStatus::Reconnecting(reconnect_count));
                        } else {
                            log::info!("🔥 Initial connection to VIBEC0RE server!");
                            status.set(ConnectionStatus::Connecting);
                        }

                        // Try to load config first, fallback to default
                        let ws_url = load_config().await.unwrap_or_else(|| {
                            log::warn!(
                                "⚠️ Failed to load config, using default ws://localhost:31337"
                            );
                            "ws://localhost:31337".to_string()
                        });

                        log::info!("🚀 Connecting to: {}", ws_url);

                        match WebSocket::open(&ws_url) {
                            Ok(ws) => {
                                log::info!("✅ WebSocket connection opened!");
                                status.set(ConnectionStatus::Connected);

                                // Reset reconnect count on successful connection
                                ws_state.borrow_mut().reconnect_count = 0;

                                let (mut write, mut read) = ws.split();

                                // Create channel for sending messages
                                let (tx, mut rx) = mpsc::unbounded::<Vec<u8>>();

                                // 🔥 START PING TIMER - EVERY 30 SECONDS! 💖
                                let ping_tx = tx.clone();
                                let ping_handle =
                                    gloo_timers::callback::Interval::new(30_000, move || {
                                        log::debug!("🏓 Sending PING to keep connection alive!");

                                        let correlation_id = Uuid::new_v4().to_string();
                                        let mut payload = Vec::new();
                                        if let Ok(()) =
                                            ciborium::into_writer(&ApiRequest::Ping, &mut payload)
                                        {
                                            let api_message = ApiMessage {
                                                correlation_id,
                                                message_type: ApiMessageType::Request,
                                                payload,
                                            };

                                            let mut data = Vec::new();
                                            if let Ok(()) =
                                                ciborium::into_writer(&api_message, &mut data)
                                            {
                                                let _ = ping_tx.unbounded_send(data);
                                            }
                                        }
                                    });

                                // Store sender and ping handle
                                {
                                    let mut state = ws_state.borrow_mut();
                                    state.sender = Some(tx);
                                    state.ping_handle = Some(ping_handle);
                                }

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
                                let last_event_clone = last_event.clone();
                                let ws_state_clone = ws_state.clone();
                                let reconnect_ref = reconnect_callback_ref.clone();

                                spawn_local(async move {
                                    while let Some(msg) = read.next().await {
                                        match msg {
                                            Ok(Message::Bytes(data)) => {
                                                // Decode CBOR message
                                                match ciborium::from_reader::<ApiMessage, _>(
                                                    data.as_slice(),
                                                ) {
                                                    Ok(api_msg) => {
                                                        if api_msg.message_type
                                                            == ApiMessageType::Response
                                                        {
                                                            // Decode response payload
                                                            match ciborium::from_reader::<
                                                                ApiResponse,
                                                                _,
                                                            >(
                                                                api_msg.payload.as_slice()
                                                            ) {
                                                                Ok(ApiResponse::Pong) => {
                                                                    log::debug!("🏓 PONG received - connection alive!");
                                                                }
                                                                Ok(response) => {
                                                                    log::info!("📥 Received response: {:?}", response);
                                                                    last_response_clone
                                                                        .set(Some(response));
                                                                }
                                                                Err(e) => {
                                                                    log::error!("❌ Failed to decode response: {}", e);
                                                                }
                                                            }
                                                        } else if api_msg.message_type
                                                            == ApiMessageType::Event
                                                        {
                                                            // Handle events - CBOR to JSON for now (TODO: pure CBOR) 🔥
                                                            log::info!("📢 Got Event message!");
                                                            match ciborium::from_reader::<
                                                                DeviceEvent,
                                                                _,
                                                            >(
                                                                api_msg.payload.as_slice()
                                                            ) {
                                                                Ok(device_event) => {
                                                                    log::info!("🔥 DeviceEvent: device_id={}, event_type={:?}", device_event.device_id, device_event.event_type);

                                                                    // Convert to JSON for UI compatibility (temporary)
                                                                    match serde_json::to_value(
                                                                        &device_event,
                                                                    ) {
                                                                        Ok(event_json) => {
                                                                            last_event_clone.set(
                                                                                Some(event_json),
                                                                            );
                                                                        }
                                                                        Err(e) => {
                                                                            log::error!("❌ Failed to convert DeviceEvent to JSON: {}", e);
                                                                        }
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    log::error!("❌ Failed to decode DeviceEvent: {}", e);
                                                                }
                                                            }
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
                                                status_clone.set(ConnectionStatus::Error(format!(
                                                    "{:?}",
                                                    e
                                                )));
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }

                                    log::warn!("💔 WebSocket connection closed!");
                                    status_clone.set(ConnectionStatus::Disconnected);

                                    // 🔥 CLEANUP AND RECONNECT! 💖
                                    {
                                        let mut state = ws_state_clone.borrow_mut();
                                        state.reconnect_count += 1;
                                        state.sender = None;
                                        if let Some(handle) = state.ping_handle.take() {
                                            drop(handle); // Stop ping timer
                                        }
                                    }

                                    // Calculate backoff delay
                                    let delay = std::cmp::min(
                                        1000 * (2_u32
                                            .pow(ws_state_clone.borrow().reconnect_count - 1)),
                                        30000,
                                    );
                                    log::info!("⏰ Will reconnect in {}ms", delay);

                                    // Schedule reconnection
                                    if let Some(reconnect_fn) = &*reconnect_ref {
                                        let reconnect_fn = reconnect_fn.clone();
                                        spawn_local(async move {
                                            TimeoutFuture::new(delay).await;
                                            reconnect_fn();
                                        });
                                    }
                                });
                            }
                            Err(e) => {
                                log::error!("❌ Failed to connect: {:?}", e);
                                status.set(ConnectionStatus::Error(format!("{:?}", e)));

                                // 🔥 RETRY CONNECTION ON ERROR! 💖
                                {
                                    let mut state = ws_state.borrow_mut();
                                    state.reconnect_count += 1;
                                }

                                let delay = std::cmp::min(
                                    1000 * (2_u32.pow(ws_state.borrow().reconnect_count - 1)),
                                    30000,
                                );
                                log::info!("⏰ Will retry in {}ms", delay);

                                if let Some(reconnect_fn) = &*reconnect_callback_ref {
                                    let reconnect_fn = reconnect_fn.clone();
                                    spawn_local(async move {
                                        TimeoutFuture::new(delay).await;
                                        reconnect_fn();
                                    });
                                }
                            }
                        }
                    });
                })
            };

            // Store the connect function for self-reference
            reconnect_callback.set(Some(connect_fn.clone()));
            // 🔥 ALSO STORE IN REF FOR VISIBILITY HANDLERS! 💖
            *reconnect_ref.borrow_mut() = Some(connect_fn.clone());

            // Initial connection
            connect_fn();

            || ()
        });
    }

    // 🔥 iOS WAKE DETECTION - FORCE RECONNECT! 💖
    // iOS webapps need both visibilitychange AND pageshow
    {
        let reconnect_ref = reconnect_ref.clone();
        let ws_state = ws_state.clone();

        use_effect_with((), move |_| {
            let window = web_sys::window().expect("window");
            let document = window.document().expect("document");

            // 🔥 USE REF TO GET FRESH CALLBACK VALUE! 💖
            let reconnect_ref_clone = (*reconnect_ref).clone();
            let ws_state_ref = ws_state.clone();

            let visibility_listener = EventListener::new(&document, "visibilitychange", {
                let reconnect_ref = reconnect_ref_clone.clone();
                let ws_state_ref = ws_state_ref.clone();
                move |_| {
                    let document = web_sys::window()
                        .and_then(|w| w.document())
                        .expect("document");
                    if document.visibility_state() == web_sys::VisibilityState::Visible {
                        log::info!("👁️ visibilitychange: visible - forcing reconnect!");
                        // Clear old sender to force fresh connection
                        ws_state_ref.borrow_mut().sender = None;
                        // 🔥 GET FRESH VALUE FROM REF! 💖
                        if let Some(reconnect_fn) = reconnect_ref.borrow().as_ref() {
                            reconnect_fn();
                        } else {
                            log::warn!("⚠️ No reconnect callback available yet!");
                        }
                    }
                }
            });

            // 🔥 iOS PAGESHOW - MORE RELIABLE FOR WEBAPPS! 💖
            let pageshow_listener = EventListener::new(&window, "pageshow", {
                let reconnect_ref = reconnect_ref_clone.clone();
                let ws_state_ref = ws_state_ref.clone();
                move |_| {
                    log::info!("📱 pageshow event - forcing reconnect!");
                    // Clear old sender to force fresh connection
                    ws_state_ref.borrow_mut().sender = None;
                    // 🔥 GET FRESH VALUE FROM REF! 💖
                    if let Some(reconnect_fn) = reconnect_ref.borrow().as_ref() {
                        reconnect_fn();
                    } else {
                        log::warn!("⚠️ No reconnect callback available yet!");
                    }
                }
            });

            // Store listeners to keep alive (leak intentionally - lives forever)
            std::mem::forget(visibility_listener);
            std::mem::forget(pageshow_listener);

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

                // Encode to CBOR
                let mut data = Vec::new();
                if let Err(e) = ciborium::into_writer(&api_message, &mut data) {
                    log::error!("❌ Failed to encode API message: {}", e);
                    return;
                }

                log::info!("📤 Sending request: {:?}", request);

                // Send via channel
                if let Some(sender) = &ws_state.borrow().sender {
                    if let Err(e) = sender.unbounded_send(data) {
                        log::error!("❌ Failed to queue message: {:?}", e);
                    }
                } else {
                    log::warn!("⚠️ WebSocket not connected, can't send request");
                }
            });
        })
    };

    UseWebSocketHandle {
        status: (*status).clone(),
        last_response: (*last_response).clone(),
        last_event: (*last_event).clone(),
        send_request,
    }
}

#[derive(Clone)]
pub struct UseWebSocketHandle {
    pub status: ConnectionStatus,
    pub last_response: Option<ApiResponse>,
    pub last_event: Option<serde_json::Value>,
    pub send_request: Callback<ApiRequest>,
}
