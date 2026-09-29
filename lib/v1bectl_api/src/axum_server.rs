use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use v1bectl_sync::{
    ButtonPressType, DeviceEvent, DeviceInfo, DeviceState, DeviceStateValue, EventBus, EventType,
    Gateway, LagAwareReceiver, LightState, OutletState, Recv, RgbColor, SceneState, StateStore,
    SyncEngine,
};
use v1bectl_virtual::{
    is_simulated, LightGroup, SceneController, VirtualDeviceConfig, VirtualDeviceManager,
    VirtualDeviceType,
};

/// Each connected WebSocket client's outbox, by subscriber id.
type Subscribers = Arc<RwLock<HashMap<String, tokio::sync::mpsc::UnboundedSender<Outbound>>>>;

#[derive(Clone)]
pub struct AxumServer {
    port: u16,
    state_store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
    gateway: Arc<dyn Gateway>,
    virtual_device_manager: Arc<VirtualDeviceManager>,
    subscribers: Subscribers,
    sync_engine: Option<Arc<SyncEngine>>, // 🔥 OPTIONAL SYNC ENGINE FOR OPTIMISTIC UPDATES!
}

/// What goes out to a WebSocket client unasked, next to the answers to its
/// requests.
#[derive(Clone)]
enum Outbound {
    /// A bus event, sent as an `Event` frame.
    Event(DeviceEvent),
    /// Every device in the store, after the forwarder fell behind the bus
    /// and the client missed events (#15). It goes out as a `DeviceList`
    /// response nobody asked for (see [`spawn_event_forwarder`]). This is
    /// that response, encoded once for every client (see [`Self::resync`]).
    Resync(Arc<[u8]>),
}

impl std::fmt::Debug for Outbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Event(event) => f.debug_tuple("Event").field(event).finish(),
            Self::Resync(response) => write!(f, "Resync({} bytes)", response.len()),
        }
    }
}

impl Outbound {
    /// A resync with `devices`, the whole store.
    fn resync(devices: Vec<DeviceState>) -> Result<Self, ciborium::ser::Error<std::io::Error>> {
        Ok(Self::Resync(
            cbor(&ApiResponse::device_list(devices))?.into(),
        ))
    }

    /// The binary WebSocket frame the client gets for this, with a
    /// correlation id of its own. A resync is a `DeviceList` `Response`,
    /// the answer to `DiscoverDevices`: every client already takes one,
    /// whenever it comes, as its new device list. None of them matches it
    /// to a request it made (the CLI only waits for its own ids, and its
    /// `subscribe` skips responses).
    fn into_frame(self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let (message_type, payload) = match self {
            Self::Event(event) => (ApiMessageType::Event, cbor(&event)?),
            Self::Resync(response) => (ApiMessageType::Response, response.to_vec()),
        };
        cbor(&ApiMessage {
            correlation_id: Uuid::new_v4().to_string(),
            message_type,
            payload,
        })
    }
}

/// `value` as CBOR.
fn cbor(value: &impl Serialize) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)?;
    Ok(bytes)
}

// WebSocket API Messages - CBOR encoded! 🔥
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
    SetOutletState {
        device_id: String,
        is_on: bool,
    },
    CreateVirtualDevice {
        config: VirtualDeviceConfig,
    },
    RemoveVirtualDevice {
        device_id: String,
    },
    ActivateScene {
        device_id: String,
        scene_name: String,
    },
    Subscribe {
        device_ids: Vec<String>,
    },
    Ping, // 🔥 KEEPALIVE PING! CHOOOM FIX! 💖
    /// Press `device_id`, a switch the dummy gateway simulates, as if by
    /// hand (#35): the server publishes the `ButtonPressed` a hub would,
    /// so the button controllers bound to it run. The answer is
    /// `ButtonPressed`, or an `Error`: `PRESS_UNSUPPORTED` for a device the
    /// dummy doesn't simulate (every device of a real hub, whose remotes are
    /// pressed by hand; a virtual device), `WRONG_TYPE` for a simulated
    /// device that isn't a switch, and `NOT_FOUND`. It's a new variant
    /// only, so clients that don't know it are unaffected.
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
    }, // 🔥 Return FULL DeviceState with values!
    DeviceInfo {
        device: Box<DeviceInfo>,
    },
    /// The answer to `GetDeviceState`: the state of `device_id` (#10). A
    /// client can tell by the id which device the state belongs to, not
    /// only by which request it answers. A client that doesn't know the
    /// field (the web UI's copy, or one built before it) still decodes the
    /// answer: serde skips fields it doesn't know.
    DeviceState {
        device_id: String,
        state: DeviceStateValue,
    },
    LightUpdated {
        new_state: LightState,
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
    Pong, // 🔥 KEEPALIVE PONG! CHOOOM FIX! 💖
    /// `PressButton` published its press.
    ButtonPressed {
        device_id: String,
        press_type: ButtonPressType,
    },
    Error {
        code: String,
        message: String,
    },
}

impl ApiResponse {
    /// A `DeviceList` of `devices`: the answer to `DiscoverDevices` and
    /// `ListDevices`, and a resync (see [`Outbound::Resync`]).
    fn device_list(devices: Vec<DeviceState>) -> Self {
        // A device count, always far below u32::MAX.
        let total_count = u32::try_from(devices.len()).unwrap_or(u32::MAX);
        Self::DeviceList {
            devices,
            total_count,
        }
    }
}

impl AxumServer {
    pub fn new(
        port: u16,
        state_store: Arc<StateStore>,
        event_bus: Arc<EventBus>,
        gateway: Arc<dyn Gateway>,
    ) -> Self {
        let virtual_device_manager = Arc::new(VirtualDeviceManager::new(
            Arc::clone(&state_store),
            Arc::clone(&event_bus),
        ));

        Self {
            port,
            state_store,
            event_bus,
            gateway,
            virtual_device_manager,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            sync_engine: None,
        }
    }

    // 🔥 Set sync engine for OPTIMISTIC UPDATES!
    #[must_use]
    pub fn with_sync_engine(mut self, sync_engine: Arc<SyncEngine>) -> Self {
        // Virtual writes fan out to physical members, and those take the same
        // path as a direct write: store, echo, gateway push.
        self.virtual_device_manager
            .attach_sync_engine(Arc::clone(&sync_engine));
        self.sync_engine = Some(sync_engine);
        self
    }

    /// 🔥 GET VIRTUAL DEVICE MANAGER FOR EXTERNAL REGISTRATION! 💖
    #[must_use]
    pub fn virtual_device_manager(&self) -> Arc<VirtualDeviceManager> {
        Arc::clone(&self.virtual_device_manager)
    }

    pub async fn start(self) -> anyhow::Result<()> {
        info!(
            "Starting WebSocket-ONLY server on VIBEC0RE port {}",
            self.port
        );

        let app = self.start_app().await?;
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        info!(
            "WebSocket-ONLY server listening on 0.0.0.0:{} - NO BOOMER REST! 🚀",
            self.port
        );

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// [`Self::start`], on `listener` rather than on the port given to
    /// [`Self::new`] (which this ignores). The API round-trip tests bind
    /// `127.0.0.1:0` and connect to the port the OS picked (#8).
    pub async fn serve(self, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
        let app = self.start_app().await?;
        info!(
            "WebSocket-ONLY server listening on {} - NO BOOMER REST! 🚀",
            listener.local_addr()?
        );

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start what runs next to the socket (the virtual devices' input
    /// tracking, and the forwarder that hands bus events to the clients),
    /// and return the router for the WebSocket API.
    async fn start_app(&self) -> anyhow::Result<Router> {
        // Start virtual device manager
        self.virtual_device_manager
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start virtual device manager: {e}"))?;

        // 🔥 SUBSCRIBE TO EVENTBUS AND FORWARD TO WEBSOCKET CLIENTS! 💖
        spawn_event_forwarder(
            self.event_bus.subscribe(),
            Arc::clone(&self.state_store),
            Arc::clone(&self.subscribers),
        );

        Ok(Router::new()
            // WebSocket ONLY - pure async real-time vibes!! 🔥
            .route("/", get(websocket_handler))
            .layer(ServiceBuilder::new().layer(CorsLayer::permissive()))
            .with_state(self.clone()))
    }

    async fn broadcast_event(&self, event: DeviceEvent) {
        let subscribers = self.subscribers.read().await;
        let mut failed_subscribers = Vec::new();

        for (subscriber_id, sender) in subscribers.iter() {
            if sender.send(Outbound::Event(event.clone())).is_err() {
                failed_subscribers.push(subscriber_id.clone());
            }
        }

        // Clean up failed subscribers
        if !failed_subscribers.is_empty() {
            drop(subscribers);
            let mut subscribers = self.subscribers.write().await;
            for failed_id in failed_subscribers {
                subscribers.remove(&failed_id);
                debug!("Removed disconnected subscriber: {}", failed_id);
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one match arm per ApiRequest variant; splitting each arm into its own function is a real restructure, out of scope for a lint gate"
    )]
    async fn handle_api_request(
        &self,
        request: ApiRequest,
        _correlation_id: String,
    ) -> ApiResponse {
        match request {
            ApiRequest::DiscoverDevices => {
                debug!("Handling device discovery request - WITH STATES! 🔥");
                // 🔥 Return FULL DeviceState with values!
                ApiResponse::device_list(self.state_store.list_devices().await)
            }
            ApiRequest::ListDevices {
                device_type,
                groups,
                reachable_only,
            } => {
                debug!("Handling device list request with filters");
                let all_device_states = self.state_store.list_devices().await;
                let mut filtered_devices: Vec<DeviceState> = all_device_states;

                // Filter by device type if specified
                if let Some(device_type_str) = device_type {
                    filtered_devices.retain(|d| {
                        format!("{:?}", d.device_info.device_type)
                            .to_lowercase()
                            .contains(&device_type_str.to_lowercase())
                    });
                }

                // Filter by groups if specified
                if let Some(target_groups) = groups {
                    filtered_devices.retain(|d| {
                        d.device_info
                            .device_groups
                            .iter()
                            .any(|g| target_groups.contains(g))
                    });
                }

                // Filter by reachability if specified
                if let Some(true) = reachable_only {
                    filtered_devices.retain(|d| d.device_info.reachable);
                }

                ApiResponse::device_list(filtered_devices) // 🔥 Return FULL DeviceState!
            }
            ApiRequest::GetDevice { device_id } => {
                match self.state_store.get_device(&device_id).await {
                    Some(device_state) => ApiResponse::DeviceInfo {
                        device: Box::new(device_state.device_info),
                    },
                    None => ApiResponse::Error {
                        code: "NOT_FOUND".to_string(),
                        message: "Device not found".to_string(),
                    },
                }
            }
            ApiRequest::GetDeviceState { device_id } => {
                match self.state_store.get_device(&device_id).await {
                    Some(device_state) => ApiResponse::DeviceState {
                        device_id,
                        state: device_state.state,
                    },
                    None => ApiResponse::Error {
                        code: "NOT_FOUND".to_string(),
                        message: "Device not found".to_string(),
                    },
                }
            }
            ApiRequest::SetLightState {
                device_id,
                is_on,
                brightness,
                color_temp,
                rgb_color,
            } => {
                match self.state_store.get_device(&device_id).await {
                    Some(device_state)
                        if matches!(device_state.state, DeviceStateValue::Light(_)) =>
                    {
                        if let DeviceStateValue::Light(mut current_light) = device_state.state {
                            // Update only specified fields
                            if let Some(is_on) = is_on {
                                current_light.is_on = is_on;
                            }
                            if let Some(brightness) = brightness {
                                current_light.brightness = Some(brightness);
                            }
                            if let Some(color_temp) = color_temp {
                                current_light.color_temp = Some(color_temp);
                            }
                            if let Some(rgb_color) = rgb_color {
                                current_light.rgb_color = Some(rgb_color);
                            }

                            let new_state = DeviceStateValue::Light(current_light.clone());

                            // 🔥 CHECK IF THIS IS A VIRTUAL DEVICE FIRST! 💖
                            if device_state
                                .device_info
                                .device_groups
                                .contains(&"virtual".to_string())
                            {
                                debug!("🌟 Virtual device detected - routing through VirtualDeviceManager!");

                                match self
                                    .virtual_device_manager
                                    .set_virtual_device_state(&device_id, new_state.clone())
                                    .await
                                {
                                    Ok(()) => {
                                        debug!(
                                            "✅ Virtual device {} updated successfully!",
                                            device_id
                                        );

                                        // 🔥 GET ACTUAL STATE FROM VIRTUAL DEVICE AFTER UPDATE! 💖
                                        match self
                                            .virtual_device_manager
                                            .get_virtual_device_state(&device_id)
                                            .await
                                        {
                                            Ok(DeviceStateValue::Light(actual_state)) => {
                                                debug!("🌟 Virtual device actual state: on={}, brightness={:?}", actual_state.is_on, actual_state.brightness);
                                                ApiResponse::LightUpdated {
                                                    new_state: actual_state,
                                                }
                                            }
                                            Ok(_) => {
                                                error!(
                                                    "Virtual device {} returned non-light state!",
                                                    device_id
                                                );
                                                ApiResponse::LightUpdated {
                                                    new_state: current_light,
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to get virtual device state after update: {}", e);
                                                ApiResponse::LightUpdated {
                                                    new_state: current_light,
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to update virtual device {}: {}",
                                            device_id, e
                                        );
                                        ApiResponse::Error {
                                            code: "VIRTUAL_UPDATE_FAILED".to_string(),
                                            message: format!("Virtual device update failed: {e}"),
                                        }
                                    }
                                }
                            } else {
                                // 🔥 USE OPTIMISTIC UPDATES IF SYNC ENGINE AVAILABLE!
                                if let Some(sync_engine) = &self.sync_engine {
                                    debug!("🔥 USING OPTIMISTIC UPDATE - INSTANT FEEDBACK!");

                                    // Apply optimistic update - UI updates IMMEDIATELY!
                                    match sync_engine
                                        .apply_optimistic_update(&device_id, new_state.clone())
                                        .await
                                    {
                                        Ok(()) => {
                                            debug!("✅ Optimistic update applied for {} - USER SEES CHANGE NOW!", device_id);
                                            ApiResponse::LightUpdated {
                                                new_state: current_light,
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to apply optimistic update: {}", e);
                                            ApiResponse::Error {
                                                code: "OPTIMISTIC_UPDATE_FAILED".to_string(),
                                                message: format!("Update failed: {e}"),
                                            }
                                        }
                                    }
                                } else {
                                    // Fallback to old synchronous approach
                                    match self
                                        .gateway
                                        .set_device_state(&device_id, new_state.clone())
                                        .await
                                    {
                                        Ok(()) => {
                                            // If gateway update succeeds, update local state
                                            match self
                                                .state_store
                                                .update_device_state(&device_id, new_state.clone())
                                                .await
                                            {
                                                Ok(()) => {
                                                    debug!("Updated light state for device: {} - VIBEC0RE CONTROL! 🔥", device_id);

                                                    // 🔥 Broadcast state change event - PURE CBOR!
                                                    let event = DeviceEvent {
                                                        timestamp: std::time::SystemTime::now(),
                                                        device_id: device_id.clone(),
                                                        event_type: EventType::StateChanged {
                                                            old_state: None,
                                                            new_state: Some(new_state.clone()),
                                                        },
                                                    };

                                                    self.broadcast_event(event).await;

                                                    ApiResponse::LightUpdated {
                                                        new_state: current_light,
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to update local state after gateway success: {}", e);
                                                    ApiResponse::Error {
                                                        code: "STORE_UPDATE_FAILED".to_string(),
                                                        message: format!(
                                                            "Local state update failed: {e}"
                                                        ),
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to update device {} via gateway: {}",
                                                device_id, e
                                            );
                                            ApiResponse::Error {
                                                code: "GATEWAY_FAILED".to_string(),
                                                message: format!("Gateway update failed: {e}"),
                                            }
                                        }
                                    }
                                } // 🔥 Close the if-else for sync engine
                            } // 🔥 Close the else for virtual device check
                        } else {
                            ApiResponse::Error {
                                code: "STATE_MISMATCH".to_string(),
                                message: "Unexpected state mismatch".to_string(),
                            }
                        }
                    }
                    Some(_) => ApiResponse::Error {
                        code: "WRONG_TYPE".to_string(),
                        message: "Device is not a light".to_string(),
                    },
                    None => ApiResponse::Error {
                        code: "NOT_FOUND".to_string(),
                        message: "Device not found".to_string(),
                    },
                }
            }
            ApiRequest::SetOutletState { device_id, is_on } => {
                match self.state_store.get_device(&device_id).await {
                    Some(device_state)
                        if matches!(device_state.state, DeviceStateValue::Outlet(_)) =>
                    {
                        // 🔥 CREATE NEW OUTLET STATE WITH UPDATED VALUE! 💖
                        let new_state = DeviceStateValue::Outlet(OutletState {
                            is_on,
                            power_consumption: None, // Keep existing power data
                            total_energy: None,      // Keep existing energy data
                        });

                        // 🔥 USE OPTIMISTIC UPDATES IF SYNC ENGINE AVAILABLE!
                        if let Some(sync_engine) = &self.sync_engine {
                            debug!("🔌 OUTLET OPTIMISTIC UPDATE - INSTANT POWER CONTROL!");

                            // Apply optimistic update - UI updates IMMEDIATELY!
                            match sync_engine
                                .apply_optimistic_update(&device_id, new_state.clone())
                                .await
                            {
                                Ok(()) => {
                                    debug!(
                                        "✅ Outlet {} set to {} - INSTANT UPDATE!",
                                        device_id,
                                        if is_on { "ON" } else { "OFF" }
                                    );
                                    ApiResponse::LightUpdated {
                                        new_state: LightState {
                                            is_on,
                                            brightness: None,
                                            color_temp: None,
                                            rgb_color: None,
                                        },
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to apply outlet update: {}", e);
                                    ApiResponse::Error {
                                        code: "OUTLET_UPDATE_FAILED".to_string(),
                                        message: format!("Update failed: {e}"),
                                    }
                                }
                            }
                        } else {
                            // Fallback to old synchronous approach
                            match self
                                .gateway
                                .set_device_state(&device_id, new_state.clone())
                                .await
                            {
                                Ok(()) => {
                                    // If gateway update succeeds, update local state
                                    match self
                                        .state_store
                                        .update_device_state(&device_id, new_state.clone())
                                        .await
                                    {
                                        Ok(()) => {
                                            debug!(
                                                "🔌 Updated outlet state for device: {} to {}",
                                                device_id,
                                                if is_on { "ON" } else { "OFF" }
                                            );

                                            // 🔥 Broadcast state change event - PURE CBOR!
                                            let event = DeviceEvent {
                                                timestamp: std::time::SystemTime::now(),
                                                device_id: device_id.clone(),
                                                event_type: EventType::StateChanged {
                                                    old_state: None,
                                                    new_state: Some(new_state.clone()),
                                                },
                                            };

                                            self.broadcast_event(event).await;

                                            ApiResponse::LightUpdated {
                                                new_state: LightState {
                                                    is_on,
                                                    brightness: None,
                                                    color_temp: None,
                                                    rgb_color: None,
                                                },
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to update local state after gateway success: {}", e);
                                            ApiResponse::Error {
                                                code: "STORE_UPDATE_FAILED".to_string(),
                                                message: format!("Local state update failed: {e}"),
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to update outlet {} via gateway: {}",
                                        device_id, e
                                    );
                                    ApiResponse::Error {
                                        code: "GATEWAY_FAILED".to_string(),
                                        message: format!("Gateway update failed: {e}"),
                                    }
                                }
                            }
                        }
                    }
                    Some(_) => ApiResponse::Error {
                        code: "WRONG_TYPE".to_string(),
                        message: "Device is not an outlet".to_string(),
                    },
                    None => ApiResponse::Error {
                        code: "NOT_FOUND".to_string(),
                        message: "Device not found".to_string(),
                    },
                }
            }
            ApiRequest::CreateVirtualDevice { config } => {
                // Create virtual device based on type
                let device_result = match config.device_type {
                    VirtualDeviceType::LightGroup => {
                        match LightGroup::new(config.clone(), Arc::clone(&self.state_store)) {
                            Ok(light_group) => self
                                .virtual_device_manager
                                .add_virtual_device(Box::new(light_group))
                                .await
                                .map_err(|e| format!("Failed to add virtual device: {e}")),
                            Err(e) => Err(format!("Failed to create light group: {e}")),
                        }
                    }
                    VirtualDeviceType::SceneController => {
                        match SceneController::new(config.clone(), Arc::clone(&self.state_store)) {
                            Ok(scene_controller) => self
                                .virtual_device_manager
                                .add_virtual_device(Box::new(scene_controller))
                                .await
                                .map_err(|e| format!("Failed to add virtual device: {e}")),
                            Err(e) => Err(format!("Failed to create scene controller: {e}")),
                        }
                    }
                    _ => Err("Virtual device type not yet implemented".to_string()),
                };

                match device_result {
                    Ok(()) => {
                        debug!("Created virtual device: {}", config.device_id);
                        ApiResponse::VirtualDeviceCreated {
                            device_id: config.device_id,
                        }
                    }
                    Err(e) => ApiResponse::Error {
                        code: "CREATE_FAILED".to_string(),
                        message: e,
                    },
                }
            }
            ApiRequest::RemoveVirtualDevice { device_id } => {
                match self
                    .virtual_device_manager
                    .remove_virtual_device(&device_id)
                    .await
                {
                    Ok(()) => {
                        debug!("Removed virtual device: {}", device_id);
                        ApiResponse::VirtualDeviceRemoved { device_id }
                    }
                    Err(e) => ApiResponse::Error {
                        code: "REMOVE_FAILED".to_string(),
                        message: format!("Failed to remove virtual device: {e}"),
                    },
                }
            }
            ApiRequest::ActivateScene {
                device_id,
                scene_name,
            } => {
                // Create scene activation request
                let scene_state = DeviceStateValue::Scene(SceneState {
                    scene_name: scene_name.clone(),
                    is_active: true,
                });

                match self
                    .virtual_device_manager
                    .set_virtual_device_state(&device_id, scene_state)
                    .await
                {
                    Ok(()) => {
                        debug!("Activated scene '{}' on device '{}'", scene_name, device_id);
                        ApiResponse::SceneActivated {
                            device_id,
                            scene_name,
                        }
                    }
                    Err(e) => ApiResponse::Error {
                        code: "SCENE_FAILED".to_string(),
                        message: format!("Failed to activate scene: {e}"),
                    },
                }
            }
            ApiRequest::Subscribe { device_ids: _ } => {
                let subscriber_id = Uuid::new_v4().to_string();
                debug!("Created subscription: {}", subscriber_id);
                ApiResponse::SubscriptionStarted { subscriber_id }
            }
            ApiRequest::Ping => {
                debug!("🏓 Received PING, sending PONG!");
                ApiResponse::Pong
            }
            ApiRequest::PressButton {
                device_id,
                press_type,
            } => self.press_button(device_id, press_type).await,
        }
    }

    /// `ApiRequest::PressButton`: press `device_id` with `press_type`, the
    /// way a hub reports a press over its event stream. The `ButtonPressed`
    /// goes on the bus, and input tracking runs the controllers bound to
    /// the switch from there, as for a real remote (#35).
    ///
    /// Only a switch the dummy gateway simulates can be pressed like this
    /// (see [`is_simulated`]). `v1bectl_server` hands this server a
    /// `dyn Gateway`, which can't say what it is, so it goes by the device.
    /// A real hub's devices are never simulated, so against one every press
    /// is refused: its remotes are pressed by hand, and the server mustn't
    /// make up a press the hub never saw.
    async fn press_button(&self, device_id: String, press_type: ButtonPressType) -> ApiResponse {
        let Some(device) = self.state_store.get_device(&device_id).await else {
            return ApiResponse::Error {
                code: "NOT_FOUND".to_string(),
                message: "Device not found".to_string(),
            };
        };
        if !is_simulated(&device.device_info) {
            return ApiResponse::Error {
                code: "PRESS_UNSUPPORTED".to_string(),
                message: format!(
                    "{device_id} isn't a simulated switch: only the dummy gateway's switches \
                     can be pressed from here (v1bectl_server dummy). Press a real remote by hand."
                ),
            };
        }
        if !matches!(device.state, DeviceStateValue::Switch(_)) {
            return ApiResponse::Error {
                code: "WRONG_TYPE".to_string(),
                message: "Device is not a switch".to_string(),
            };
        }

        info!("🔘 Simulating a {:?} of {}", press_type, device_id);
        self.event_bus
            .publish(DeviceEvent {
                timestamp: std::time::SystemTime::now(),
                device_id: device_id.clone(),
                event_type: EventType::ButtonPressed {
                    button_id: "main".to_string(),
                    press_type: press_type.clone(),
                },
            })
            .await;
        ApiResponse::ButtonPressed {
            device_id,
            press_type,
        }
    }
}

/// Forward every event on `event_rx`, a subscription to the bus, to each
/// WebSocket subscriber, and drop the subscribers whose client has gone.
///
/// When the forwarder falls behind the bus, the events it missed never
/// reach the clients, and their devices would show stale state until they
/// changed again (#15). So it first forwards the events still buffered,
/// and then sends each client every device in the store (see
/// [`Outbound::Resync`]), once, taken at the bus's edge (see
/// [`LagAwareReceiver`]). No event older than that snapshot follows it, and
/// a burst that goes on meanwhile doesn't make it resync again (#47
/// review).
fn spawn_event_forwarder(
    event_rx: broadcast::Receiver<DeviceEvent>,
    state_store: Arc<StateStore>,
    subscribers: Subscribers,
) {
    debug!("📢 API Server subscribed to EventBus - will forward events to WebSocket clients!");

    tokio::spawn(async move {
        let mut events = LagAwareReceiver::new(event_rx, "WebSocket forwarder");
        loop {
            let outbound = match events.recv().await {
                Recv::Event(event) => {
                    debug!("🔥 Forwarding event to WebSocket clients: {:?}", event);
                    Outbound::Event(event)
                }
                Recv::Lagged(_) => {
                    // Nobody missed anything (a client that connects later
                    // asks for its device list anyway).
                    if subscribers.read().await.is_empty() {
                        continue;
                    }
                    info!(
                        "🔧 Resyncing every WebSocket client with the store after missing events"
                    );
                    match Outbound::resync(state_store.list_devices().await) {
                        Ok(resync) => resync,
                        Err(e) => {
                            error!("Failed to encode the resync as CBOR: {}", e);
                            continue;
                        }
                    }
                }
                Recv::Closed => break,
            };
            let subscribers_read = subscribers.read().await;
            let mut failed = Vec::new();

            for (id, tx) in subscribers_read.iter() {
                if tx.send(outbound.clone()).is_err() {
                    failed.push(id.clone());
                }
            }
            drop(subscribers_read);

            // Clean up failed subscribers
            if !failed.is_empty() {
                let mut subscribers_write = subscribers.write().await;
                for id in failed {
                    subscribers_write.remove(&id);
                }
            }
        }
        warn!("EventBus subscription ended!");
    });
}

async fn websocket_handler(ws: WebSocketUpgrade, State(server): State<AxumServer>) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, server))
}

#[expect(
    clippy::too_many_lines,
    reason = "one flat loop wiring the socket's send/recv halves to the subscriber channel and request dispatch; splitting it apart would scatter shared connection state across functions"
)]
async fn handle_websocket(socket: WebSocket, server: AxumServer) {
    let subscriber_id = Uuid::new_v4().to_string();
    debug!(
        "🔥 NEW WEBSOCKET CONNECTION: {} - PURE ASYNC VIBES!",
        subscriber_id
    );

    let (ws_sender, mut ws_receiver) = socket.split();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    // Add subscriber to the map for events
    {
        let mut subscribers = server.subscribers.write().await;
        subscribers.insert(subscriber_id.clone(), event_tx);
    }

    // Clone server for async tasks
    let server_for_receiver = server.clone();

    // Spawn task to handle outgoing messages (responses + events to client)
    let sender_task = {
        let mut ws_sender = ws_sender;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle events (and resyncs)
                    Some(outbound) = event_rx.recv() => {
                        let message_bytes = match outbound.into_frame() {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                error!("Failed to encode a message for the client as CBOR: {}", e);
                                continue;
                            }
                        };

                        if ws_sender.send(Message::Binary(message_bytes)).await.is_err() {
                            break;
                        }
                    }
                    // Handle responses
                    Some(response_bytes) = response_rx.recv() => {
                        if ws_sender.send(Message::Binary(response_bytes)).await.is_err() {
                            break;
                        }
                    }
                    else => break,
                }
            }
        })
    };

    // Handle incoming API requests via WebSocket
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    // Decode CBOR API message
                    match ciborium::from_reader::<ApiMessage, _>(data.as_slice()) {
                        Ok(api_message) => {
                            debug!("🚀 Received API request: {:?}", api_message.message_type);

                            // Decode the request payload
                            match ciborium::from_reader::<ApiRequest, _>(
                                api_message.payload.as_slice(),
                            ) {
                                Ok(request) => {
                                    debug!("🎯 Processing: {:?}", request);

                                    // Handle the API request
                                    let response = server_for_receiver
                                        .handle_api_request(
                                            request,
                                            api_message.correlation_id.clone(),
                                        )
                                        .await;

                                    // Encode response
                                    let mut response_payload = Vec::new();
                                    if let Err(e) =
                                        ciborium::into_writer(&response, &mut response_payload)
                                    {
                                        error!("Failed to encode response: {}", e);
                                        continue;
                                    }

                                    let correlation_id = api_message.correlation_id.clone();
                                    let response_message = ApiMessage {
                                        correlation_id: api_message.correlation_id,
                                        message_type: ApiMessageType::Response,
                                        payload: response_payload,
                                    };

                                    let mut response_bytes = Vec::new();
                                    if let Err(e) = ciborium::into_writer(
                                        &response_message,
                                        &mut response_bytes,
                                    ) {
                                        error!("Failed to encode response message: {}", e);
                                        continue;
                                    }

                                    // Send response via channel
                                    debug!(
                                        "📤 Sending response for correlation_id: {}",
                                        correlation_id
                                    );
                                    if response_tx.send(response_bytes).is_err() {
                                        error!("Failed to send response through channel!");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode API request: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to decode API message: {}", e);
                        }
                    }
                }
                Ok(Message::Text(text)) => {
                    info!("⚡ Text message (legacy): {}", text);
                }
                Ok(Message::Close(_)) => {
                    info!("🔌 WebSocket connection closed");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = sender_task => {},
        _ = receiver_task => {},
    }

    // Clean up subscriber
    {
        let mut subscribers = server.subscribers.write().await;
        subscribers.remove(&subscriber_id);
    }

    info!(
        "🔥 WebSocket connection {} closed - ASYNC VIBES ENDED!",
        subscriber_id
    );
}

#[cfg(test)]
mod tests {
    //! Write → echo round trips through `handle_api_request`, against the
    //! dummy `basic_home` hub, wired the way `v1bectl_server` wires them.
    //!
    //! Input tracking doesn't run on its own here (except in
    //! `input_tracking_follows_the_bus`): [`pump`] feeds it the bus. So each
    //! test decides exactly when tracking catches up, after every write or
    //! only after several, and nothing waits on a clock.
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use tokio::sync::broadcast::{self, error::TryRecvError};
    use v1bectl_sync::{
        Capability, DeviceId, DeviceType, GatewayError, GatewayHealth, SwitchState, SyncStatus,
    };
    use v1bectl_virtual::{
        ButtonController, DummyGateway, LightGroupLinear, VirtualDeviceTomlConfig,
    };

    const GROUP: &str = "virtual_bedroom_lights";
    /// (member, its brightness when the group is on at 50%): the linear
    /// ranges of the shipped `virtual_devices/bedroom_lights.toml`.
    const MEMBERS_AT_50: [(&str, u8); 3] = [
        ("light_bedroom", 90),
        ("light_living_room", 65),
        ("light_kitchen", 25),
    ];
    /// The same at 80%.
    const MEMBERS_AT_80: [(&str, u8); 3] = [
        ("light_bedroom", 96),
        ("light_living_room", 80),
        ("light_kitchen", 40),
    ];

    struct Home {
        server: AxumServer,
        store: Arc<StateStore>,
        bus: Arc<EventBus>,
        engine: Arc<SyncEngine>,
        manager: Arc<VirtualDeviceManager>,
    }

    /// Dummy devices seeded into the store as the server does at startup,
    /// a sync engine attached, and the Bedroom Lights linear group registered.
    async fn home() -> Home {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(1000));
        let gateway: Arc<dyn Gateway> = Arc::new(DummyGateway::new("basic_home"));
        for info in gateway.discover_devices().await.expect("discover") {
            let state = gateway
                .get_device_state(&info.device_id)
                .await
                .expect("initial state");
            store.add_device(info, state).await;
        }
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            bus.clone(),
            gateway.clone(),
            None,
        ));
        let server = AxumServer::new(0, store.clone(), bus.clone(), gateway)
            .with_sync_engine(engine.clone());

        let members = HashMap::from([
            ("top".to_string(), "light_bedroom".to_string()),
            ("main".to_string(), "light_living_room".to_string()),
            ("bed".to_string(), "light_kitchen".to_string()),
        ]);
        let ranges = HashMap::from([
            ("top".to_string(), (80, 100)),
            ("main".to_string(), (40, 90)),
            ("bed".to_string(), (0, 50)),
        ]);
        let config = VirtualDeviceConfig {
            device_id: GROUP.to_string(),
            device_type: VirtualDeviceType::LightGroupLinear,
            name: "Bedroom Lights".to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let group = LightGroupLinear::new(config, members, ranges, store.clone()).expect("group");
        let manager = server.virtual_device_manager();
        manager
            .add_virtual_device(Box::new(group))
            .await
            .expect("register group");

        Home {
            server,
            store,
            bus,
            engine,
            manager,
        }
    }

    async fn set_light(
        server: &AxumServer,
        device_id: &str,
        is_on: Option<bool>,
        brightness: Option<u8>,
    ) -> ApiResponse {
        let request = ApiRequest::SetLightState {
            device_id: device_id.to_string(),
            is_on,
            brightness,
            color_temp: None,
            rgb_color: None,
        };
        server.handle_api_request(request, String::new()).await
    }

    /// Input tracking, caught up: hands the manager every event published so
    /// far, in order, including the ones that handling publishes in turn.
    /// Returns them all. Every write here publishes before it returns (the
    /// sync engine's workers aren't running), so this sees all of them.
    async fn pump(home: &Home, rx: &mut broadcast::Receiver<DeviceEvent>) -> Vec<DeviceEvent> {
        let mut events = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => {
                    home.manager
                        .handle_event(&event)
                        .await
                        .expect("input tracking");
                    events.push(event);
                }
                Err(TryRecvError::Empty) => return events,
                Err(e) => panic!("event bus: {e}"),
            }
        }
    }

    /// The states echoed for `device_id`, in either shape clients decode.
    fn echoes(events: &[DeviceEvent], device_id: &str) -> Vec<DeviceStateValue> {
        events
            .iter()
            .filter(|e| e.device_id == device_id)
            .filter_map(|e| match &e.event_type {
                EventType::StateChanged { new_state, .. } => new_state.clone(),
                EventType::AttributeChanged {
                    attribute,
                    new_value,
                    ..
                } if attribute == "state" => serde_json::from_value(new_value.clone()).ok(),
                _ => None,
            })
            .collect()
    }

    /// `(is_on, brightness)` of each light echoed for `device_id`.
    fn levels(events: &[DeviceEvent], device_id: &str) -> Vec<(bool, Option<u8>)> {
        echoes(events, device_id)
            .into_iter()
            .map(|state| match state {
                DeviceStateValue::Light(light) => (light.is_on, light.brightness),
                other => panic!("{device_id} echoed a non-light: {other:?}"),
            })
            .collect()
    }

    async fn light(store: &StateStore, device_id: &str) -> LightState {
        match store.get_device(&device_id.to_string()).await {
            Some(DeviceState {
                state: DeviceStateValue::Light(light),
                ..
            }) => light,
            other => panic!("{device_id} is not a light in the store: {other:?}"),
        }
    }

    async fn assert_members(store: &StateStore, want: [(&str, u8); 3]) {
        for (member, brightness) in want {
            let state = light(store, member).await;
            assert!(state.is_on, "{member} should be on: {state:?}");
            assert_eq!(state.brightness, Some(brightness), "{member} brightness");
        }
    }

    async fn assert_members_off(store: &StateStore) {
        for (member, _) in MEMBERS_AT_50 {
            let state = light(store, member).await;
            assert!(!state.is_on, "{member} should be off: {state:?}");
        }
    }

    /// #1: a virtual group write must echo the group and every member it
    /// changed, exactly once each, with the state the store now holds.
    #[tokio::test]
    async fn virtual_group_write_echoes_group_and_members() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        let response = set_light(&home.server, GROUP, Some(true), Some(50)).await;
        assert!(
            matches!(
                response,
                ApiResponse::LightUpdated {
                    new_state: LightState {
                        is_on: true,
                        brightness: Some(50),
                        ..
                    }
                }
            ),
            "unexpected response: {response:?}"
        );
        let events = pump(&home, &mut rx).await;

        let group = home.store.get_device(&GROUP.to_string()).await.unwrap();
        assert_eq!(
            echoes(&events, GROUP),
            vec![group.state],
            "group {GROUP} must be echoed exactly once, with its stored state"
        );

        for (member, brightness) in MEMBERS_AT_50 {
            let state = light(&home.store, member).await;
            assert!(state.is_on, "{member} should be on: {state:?}");
            assert_eq!(state.brightness, Some(brightness), "{member} brightness");
            assert_eq!(
                echoes(&events, member),
                vec![DeviceStateValue::Light(state)],
                "member {member} must be echoed exactly once, with its stored state"
            );
            // Handed to the gateway sync, like a direct write to the member.
            let status = home.engine.get_sync_status(&member.to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "member {member} write never queued for the gateway: {status:?}"
            );
        }
    }

    /// The members' echoes of our own fan-out must not re-derive the group.
    /// If they did, an off would store brightness 0 and the next plain `on`
    /// would light nothing. Here tracking keeps up with every write.
    #[tokio::test]
    async fn group_off_then_on_restores_members() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, GROUP, Some(true), Some(50)).await;
        pump(&home, &mut rx).await;
        set_light(&home.server, GROUP, Some(false), None).await;
        pump(&home, &mut rx).await;

        let group = light(&home.store, GROUP).await;
        assert!(!group.is_on, "group should be off: {group:?}");
        assert_eq!(group.brightness, Some(50), "group forgot its level");
        assert_members_off(&home.store).await;

        set_light(&home.server, GROUP, Some(true), None).await;
        pump(&home, &mut rx).await;
        assert_members(&home.store, MEMBERS_AT_50).await;
    }

    /// #14 review, finding 1: two writes before tracking sees the first
    /// one's echoes (a double toggle, two frames in one WS read, a busy
    /// tracker). By the time tracking reads the first write's member echoes,
    /// they are stale, and they must not re-derive the group.
    #[tokio::test]
    async fn back_to_back_on_then_off_keeps_the_level() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, GROUP, Some(true), Some(50)).await;
        set_light(&home.server, GROUP, Some(false), None).await;
        let events = pump(&home, &mut rx).await;

        let group = light(&home.store, GROUP).await;
        assert!(!group.is_on, "group should be off: {group:?}");
        assert_eq!(group.brightness, Some(50), "group forgot its level");
        assert_eq!(
            levels(&events, GROUP),
            vec![(true, Some(50)), (false, Some(50))],
            "one group echo per write, and no re-derived one"
        );
        assert_members_off(&home.store).await;

        // The next plain `on` (the widget's toggle) lights them again.
        set_light(&home.server, GROUP, Some(true), None).await;
        pump(&home, &mut rx).await;
        assert_members(&home.store, MEMBERS_AT_50).await;
    }

    /// #14 review, finding 1: two level changes in a row (a slider drag)
    /// end at the last one, not at a re-derived average of the members.
    #[tokio::test]
    async fn back_to_back_level_changes_end_at_the_last_level() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, GROUP, Some(true), Some(50)).await;
        set_light(&home.server, GROUP, Some(true), Some(80)).await;
        let events = pump(&home, &mut rx).await;

        let group = light(&home.store, GROUP).await;
        assert!(group.is_on, "group should be on: {group:?}");
        assert_eq!(
            group.brightness,
            Some(80),
            "group must end at the last level"
        );
        assert_eq!(
            levels(&events, GROUP),
            vec![(true, Some(50)), (true, Some(80))],
            "one group echo per write, and no re-derived one"
        );
        assert_members(&home.store, MEMBERS_AT_80).await;
    }

    /// A direct write to a group member still echoes once (no double
    /// publish on the physical path), and the group it feeds re-derives its
    /// state and echoes that.
    #[tokio::test]
    async fn member_write_echoes_once_and_updates_group() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, "light_kitchen", Some(true), Some(40)).await;
        let events = pump(&home, &mut rx).await;

        let kitchen = light(&home.store, "light_kitchen").await;
        assert_eq!(kitchen.brightness, Some(40));
        assert_eq!(
            echoes(&events, "light_kitchen"),
            vec![DeviceStateValue::Light(kitchen)],
            "a physical write must be echoed exactly once"
        );

        let group = light(&home.store, GROUP).await;
        assert!(group.is_on, "group should follow its lit member: {group:?}");
        assert_eq!(
            echoes(&events, GROUP),
            vec![DeviceStateValue::Light(group)],
            "the re-derived group state must be echoed once"
        );
    }

    /// #14 review, finding 2: after a group write, a real outside change to
    /// one of its members must still re-derive the group, and echo it.
    #[tokio::test]
    async fn outside_member_change_after_a_group_write_re_derives_the_group() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, GROUP, Some(true), Some(50)).await;
        pump(&home, &mut rx).await;
        set_light(&home.server, "light_kitchen", Some(true), Some(40)).await;
        let events = pump(&home, &mut rx).await;

        let kitchen = light(&home.store, "light_kitchen").await;
        assert_eq!(
            echoes(&events, "light_kitchen"),
            vec![DeviceStateValue::Light(kitchen)],
            "a physical write must be echoed exactly once"
        );
        // Re-derived from the members, each at the group level it inverts
        // to (#10): 90 and 65 are still where 50 put them, and 40 is where
        // 79 and 80 put the kitchen light (0-50), 79 being nearer 50. So
        // (50 + 50 + 79) / 3.
        let group = light(&home.store, GROUP).await;
        assert_eq!(
            (group.is_on, group.brightness),
            (true, Some(59)),
            "group must follow the outside change: {group:?}"
        );
        assert_eq!(
            echoes(&events, GROUP),
            vec![DeviceStateValue::Light(group)],
            "the re-derived group state must be echoed once"
        );
    }

    /// #14 review, finding 4b: a hub reports a member's colour its own way
    /// (a bulb without colour temperature has none, the RGB bulb its hue).
    /// When that view lands in the store (`GatewayWins`, after the protection
    /// window), the member is still where the group put it. So the group
    /// must not be re-derived, which would be lossy: Bedroom Lights at 50
    /// would read back as 60.
    #[tokio::test]
    async fn hub_normalised_member_colour_does_not_re_derive_the_group() {
        let home = home().await;
        let mut rx = home.bus.subscribe();

        set_light(&home.server, GROUP, Some(true), Some(50)).await;
        pump(&home, &mut rx).await;

        // What the sync engine's GatewayWins does with the hub's view.
        let member = "light_bedroom".to_string();
        let ours = home.store.get_device(&member).await.unwrap().state;
        let hub_view = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(90),
            color_temp: None,
            rgb_color: Some(RgbColor { r: 255, g: 0, b: 0 }),
        });
        home.store
            .update_device_state(&member, hub_view.clone())
            .await
            .unwrap();
        home.bus
            .publish(DeviceEvent {
                timestamp: std::time::SystemTime::now(),
                device_id: member.clone(),
                event_type: EventType::AttributeChanged {
                    attribute: "state".to_string(),
                    old_value: serde_json::to_value(&ours).unwrap(),
                    new_value: serde_json::to_value(&hub_view).unwrap(),
                },
            })
            .await;
        let events = pump(&home, &mut rx).await;

        let group = light(&home.store, GROUP).await;
        assert_eq!(
            (group.is_on, group.brightness),
            (true, Some(50)),
            "group was re-derived from a colour-only difference"
        );
        assert!(echoes(&events, GROUP).is_empty(), "group re-echoed");
    }

    /// The shipped `virtual_devices/button_ctrl.toml`, read from the file
    /// and registered the way `v1bectl_server` registers it: it binds the
    /// dummy's switch to Bedroom Lights. `toggle` on a click, `set 100` on
    /// a double press, `inc 10` on a long press.
    async fn add_shipped_controller(home: &Home) {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../virtual_devices");
        let configs = v1bectl_virtual::load_virtual_devices_from_dir(&dir)
            .await
            .expect("load virtual_devices/");
        let Some(c) = configs.into_iter().find_map(|config| match config {
            VirtualDeviceTomlConfig::ButtonController(c)
                if c.device_id == "ctrl_lightgroup_bed" =>
            {
                Some(c)
            }
            _ => None,
        }) else {
            panic!("button_ctrl.toml no longer ships ctrl_lightgroup_bed");
        };
        assert_eq!(c.button, SWITCH, "the button the shipped controller binds");
        let config = VirtualDeviceConfig {
            device_id: c.device_id.clone(),
            device_type: VirtualDeviceType::ButtonController,
            name: c.name.clone(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let controller = ButtonController::new(
            config,
            c.button.clone(),
            &c.press_on,
            &c.press_off,
            c.press_on_long.as_deref(),
            c.press_off_long.as_deref(),
            c.press_double.as_deref(),
        )
        .expect("controller");
        home.manager
            .add_virtual_device(Box::new(controller))
            .await
            .expect("register controller");
    }

    /// The dummy `basic_home` scenario's switch.
    const SWITCH: &str = "switch_hallway";

    async fn press(
        server: &AxumServer,
        device_id: &str,
        press_type: ButtonPressType,
    ) -> ApiResponse {
        let request = ApiRequest::PressButton {
            device_id: device_id.to_string(),
            press_type,
        };
        server.handle_api_request(request, String::new()).await
    }

    /// #35: `PressButton` on the dummy's switch, through the API handler.
    /// It's answered, its `ButtonPressed` goes on the bus, and the
    /// controller bound to the switch runs from there, as for a real
    /// remote. Bedroom Lights starts on at 100: the dummy's kitchen light,
    /// its only lit member, is on at 75, past the top of its range (0-50),
    /// which the group puts it nearest to at 100 (#10). It's set to 75
    /// first, so a long press has room to go brighter. A click toggles it
    /// off, keeping 75, and its members with it (#35 review, finding 1: it
    /// used to run `on` then `off`, so a click always ended off). The next
    /// click lights it again at 75. A long press takes it to 85 and a
    /// double press to 100, and each member write is queued for the
    /// gateway.
    #[tokio::test]
    async fn press_button_runs_the_controller_bound_to_the_dummy_switch() {
        let home = home().await;
        add_shipped_controller(&home).await;
        let group = light(&home.store, GROUP).await;
        assert_eq!(
            (group.is_on, group.brightness),
            (true, Some(100)),
            "at start"
        );
        let mut rx = home.bus.subscribe();
        set_light(&home.server, GROUP, Some(true), Some(75)).await;
        pump(&home, &mut rx).await;

        let response = press(&home.server, SWITCH, ButtonPressType::SinglePress).await;
        assert!(
            matches!(
                &response,
                ApiResponse::ButtonPressed {
                    device_id,
                    press_type: ButtonPressType::SinglePress,
                } if device_id == SWITCH
            ),
            "unexpected response: {response:?}"
        );
        let events = pump(&home, &mut rx).await;
        assert!(
            matches!(
                events.first(),
                Some(DeviceEvent {
                    device_id,
                    event_type: EventType::ButtonPressed {
                        press_type: ButtonPressType::SinglePress,
                        ..
                    },
                    ..
                }) if device_id == SWITCH
            ),
            "the press must go on the bus first: {:?}",
            events.first()
        );
        assert_eq!(
            levels(&events, GROUP),
            vec![(false, Some(75))],
            "a click: toggled off, keeping the level"
        );
        assert_members_off(&home.store).await;

        for (press_type, want, case) in [
            (
                ButtonPressType::SinglePress,
                (true, Some(75)),
                "the next click: back on at 75",
            ),
            (
                ButtonPressType::LongPress,
                (true, Some(85)),
                "a long press: inc 10",
            ),
            (
                ButtonPressType::DoublePress,
                (true, Some(100)),
                "a double press: set 100",
            ),
        ] {
            let response = press(&home.server, SWITCH, press_type).await;
            assert!(
                matches!(response, ApiResponse::ButtonPressed { .. }),
                "unexpected response: {response:?}"
            );
            let events = pump(&home, &mut rx).await;
            assert_eq!(levels(&events, GROUP), vec![want], "{case}");
        }
        for (member, _) in MEMBERS_AT_50 {
            let state = light(&home.store, member).await;
            assert!(state.is_on, "{member} should be on: {state:?}");
            let status = home.engine.get_sync_status(&member.to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "member {member} write never queued for the gateway: {status:?}"
            );
        }
    }

    /// A stand-in for a real hub: one remote, shaped the way
    /// `DirigeraGateway` maps a controller (made by IKEA, a `Switch`, no
    /// custom attributes), so not simulated. The methods are written out
    /// the way `#[async_trait]` expands them: `v1bectl_api` has no
    /// `async-trait` dependency of its own.
    struct RealHub;

    const REMOTE: &str = "44444444-4444-4444-4444-444444444444_1";

    type HubFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, GatewayError>> + Send + 'a>>;

    impl Gateway for RealHub {
        fn discover_devices<'life0, 'async_trait>(
            &'life0 self,
        ) -> HubFuture<'async_trait, Vec<DeviceInfo>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async {
                Ok(vec![DeviceInfo {
                    device_id: REMOTE.to_string(),
                    name: "Remote".to_string(),
                    device_type: DeviceType::Switch,
                    capabilities: vec![Capability::BatteryLevel],
                    device_groups: vec![],
                    manufacturer: Some("IKEA".to_string()),
                    model: Some("controller".to_string()),
                    firmware_version: None,
                    battery_powered: true,
                    reachable: true,
                    last_seen: 0,
                    custom_attributes: HashMap::new(),
                }])
            })
        }

        fn get_device_state<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _device_id: &'life1 DeviceId,
        ) -> HubFuture<'async_trait, DeviceStateValue>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async {
                Ok(DeviceStateValue::Switch(SwitchState {
                    is_pressed: false,
                    last_pressed: None,
                    battery_level: Some(85),
                }))
            })
        }

        fn set_device_state<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _device_id: &'life1 DeviceId,
            _state: DeviceStateValue,
        ) -> HubFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(()) })
        }

        fn health_check<'life0, 'async_trait>(
            &'life0 self,
        ) -> HubFuture<'async_trait, GatewayHealth>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async {
                Ok(GatewayHealth {
                    reachable: true,
                    response_time_ms: 0,
                    connected_devices: 1,
                    last_error: None,
                })
            })
        }
    }

    /// #35: against a real hub, `PressButton` is refused with
    /// `PRESS_UNSUPPORTED`, even for its remote, and nothing goes on the
    /// bus: a real remote is pressed by hand.
    #[tokio::test]
    async fn press_button_is_refused_on_a_real_hub() {
        let hub: Arc<dyn Gateway> = Arc::new(RealHub);
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        for info in hub.discover_devices().await.expect("discover") {
            let state = hub
                .get_device_state(&info.device_id)
                .await
                .expect("initial state");
            store.add_device(info, state).await;
        }
        let server = AxumServer::new(0, store, Arc::clone(&bus), hub);
        let mut rx = bus.subscribe();

        let response = press(&server, REMOTE, ButtonPressType::SinglePress).await;
        assert!(
            matches!(
                &response,
                ApiResponse::Error { code, message }
                    if code == "PRESS_UNSUPPORTED" && message.contains(REMOTE)
            ),
            "unexpected response: {response:?}"
        );
        assert!(
            matches!(rx.try_recv(), Err(TryRecvError::Empty)),
            "a refused press went on the bus"
        );
    }

    /// #35: on the dummy, only a switch can be pressed. A light is
    /// `WRONG_TYPE`, a virtual device `PRESS_UNSUPPORTED` (the dummy doesn't
    /// simulate it) and an unknown id `NOT_FOUND`. None of them publishes
    /// anything.
    #[tokio::test]
    async fn press_button_refuses_what_isnt_a_dummy_switch() {
        let home = home().await;
        let mut rx = home.bus.subscribe();
        for (device_id, want) in [
            ("light_kitchen", "WRONG_TYPE"),
            (GROUP, "PRESS_UNSUPPORTED"),
            ("no_such_device", "NOT_FOUND"),
        ] {
            let response = press(&home.server, device_id, ButtonPressType::SinglePress).await;
            assert!(
                matches!(&response, ApiResponse::Error { code, .. } if code == want),
                "{device_id}: want {want}, got {response:?}"
            );
            assert!(
                matches!(rx.try_recv(), Err(TryRecvError::Empty)),
                "{device_id}: a refused press went on the bus"
            );
        }
    }

    /// #35: `PressButton` and its answer go over CBOR by variant name, as
    /// the CLI's own copy of these types (`v1bectl_cli`'s
    /// `websocket_client.rs`) encodes and decodes them. It lists fewer
    /// variants, in another order, which is why only the names may matter.
    /// A request from a client that doesn't know the new variant still
    /// decodes.
    #[test]
    fn press_button_goes_over_cbor_as_the_cli_sends_it() {
        #[derive(Serialize)]
        enum CliRequest {
            GetDeviceState {
                device_id: String,
            },
            PressButton {
                device_id: String,
                press_type: ButtonPressType,
            },
        }
        #[derive(Deserialize, Debug)]
        enum CliResponse {
            Pong,
            ButtonPressed {
                device_id: String,
                press_type: ButtonPressType,
            },
        }
        fn cbor(value: &impl Serialize) -> Vec<u8> {
            let mut bytes = Vec::new();
            ciborium::into_writer(value, &mut bytes).expect("encode");
            bytes
        }

        let request: ApiRequest = ciborium::from_reader(
            cbor(&CliRequest::PressButton {
                device_id: SWITCH.to_string(),
                press_type: ButtonPressType::DoublePress,
            })
            .as_slice(),
        )
        .expect("the server decodes the CLI's PressButton");
        assert!(
            matches!(
                &request,
                ApiRequest::PressButton {
                    device_id,
                    press_type: ButtonPressType::DoublePress,
                } if device_id == SWITCH
            ),
            "{request:?}"
        );
        let older: ApiRequest = ciborium::from_reader(
            cbor(&CliRequest::GetDeviceState {
                device_id: SWITCH.to_string(),
            })
            .as_slice(),
        )
        .expect("an older request still decodes");
        assert!(
            matches!(older, ApiRequest::GetDeviceState { .. }),
            "{older:?}"
        );

        let response: CliResponse = ciborium::from_reader(
            cbor(&ApiResponse::ButtonPressed {
                device_id: SWITCH.to_string(),
                press_type: ButtonPressType::LongPress,
            })
            .as_slice(),
        )
        .expect("the CLI decodes the server's ButtonPressed");
        assert!(
            matches!(
                &response,
                CliResponse::ButtonPressed {
                    device_id,
                    press_type: ButtonPressType::LongPress,
                } if device_id == SWITCH
            ),
            "{response:?}"
        );
    }

    /// #15: the WebSocket forwarder must outlive falling behind the bus. It
    /// is held at the subscriber lock on its first event while more events
    /// pile up behind it than the bus keeps (1000). An event published after
    /// that must still reach the client.
    #[tokio::test]
    async fn event_forwarder_survives_falling_behind_the_bus() {
        let bus = EventBus::new(10);
        let subscribers: Subscribers = Arc::new(RwLock::new(HashMap::new()));
        let (tx, mut client) = tokio::sync::mpsc::unbounded_channel();
        subscribers.write().await.insert("client".to_string(), tx);
        spawn_event_forwarder(bus.subscribe(), StateStore::new(), Arc::clone(&subscribers));

        {
            // The forwarder blocks here at its first event.
            let _held = subscribers.write().await;
            for _ in 0..1500 {
                bus.publish(removed("flood")).await;
            }
        }
        bus.publish(removed("after")).await;

        // The timeout only bounds a failure; the event ends the wait.
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(forwarded) = client.recv().await {
                if matches!(&forwarded, Outbound::Event(e) if e.device_id == "after") {
                    return;
                }
            }
            panic!("the client's channel closed");
        })
        .await
        .expect("forwarding died behind the bus: `after` never reached the client");
    }

    /// A `DeviceRemoved` of `device_id`: an event no test here reads the
    /// state of.
    fn removed(device_id: &str) -> DeviceEvent {
        DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: device_id.to_string(),
            event_type: EventType::DeviceRemoved,
        }
    }

    /// `devices`, sorted by id: the store lists them in no order.
    fn by_id(mut devices: Vec<DeviceState>) -> Vec<DeviceState> {
        devices.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        devices
    }

    /// The devices in `resync`'s frame, decoded the way a client decodes a
    /// response: with its own copy of the types (the widget's, the TUI's,
    /// the web UI's), which by CBOR's external tagging only need the
    /// variant's name.
    fn decoded_by_a_client(resync: Outbound) -> Vec<DeviceState> {
        #[derive(Deserialize, Debug)]
        enum ClientResponse {
            Pong,
            DeviceList {
                devices: Vec<DeviceState>,
                total_count: u32,
            },
        }
        let frame = resync.into_frame().expect("encode");
        let message: ApiMessage = ciborium::from_reader(frame.as_slice()).expect("envelope");
        assert!(
            matches!(message.message_type, ApiMessageType::Response),
            "a resync goes out as a response: {message:?}"
        );
        match ciborium::from_reader(message.payload.as_slice()).expect("a client's DeviceList") {
            ClientResponse::DeviceList {
                devices,
                total_count,
            } => {
                assert_eq!(u32::try_from(devices.len()).ok(), Some(total_count));
                devices
            }
            ClientResponse::Pong => panic!("a resync must be a DeviceList"),
        }
    }

    /// What `client` gets, up to and including the first message `last`
    /// matches. The timeout only bounds a failure.
    async fn receive_until(
        client: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
        last: impl Fn(&Outbound) -> bool,
    ) -> Vec<Outbound> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut received = Vec::new();
            while let Some(outbound) = client.recv().await {
                let done = last(&outbound);
                received.push(outbound);
                if done {
                    return received;
                }
            }
            panic!("the client's channel closed");
        })
        .await
        .expect("the forwarder never sent it")
    }

    fn is_event(device_id: &str) -> impl Fn(&Outbound) -> bool + '_ {
        move |outbound| matches!(outbound, Outbound::Event(e) if e.device_id == device_id)
    }

    fn is_resync(outbound: &Outbound) -> bool {
        matches!(outbound, Outbound::Resync(_))
    }

    /// A forwarder on a 4-slot bus, with one client. The client has a first
    /// event once this returns, so the forwarder has run, and waits for the
    /// next one (on the `current_thread` test runtime nothing else runs it).
    async fn warm_forwarder(
        store: Arc<StateStore>,
    ) -> (
        broadcast::Sender<DeviceEvent>,
        Subscribers,
        tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    ) {
        let (bus, rx) = broadcast::channel(4);
        let subscribers: Subscribers = Arc::new(RwLock::new(HashMap::new()));
        let (tx, mut client) = tokio::sync::mpsc::unbounded_channel();
        subscribers.write().await.insert("client".to_string(), tx);
        spawn_event_forwarder(rx, store, Arc::clone(&subscribers));
        bus.send(removed("warmup")).expect("forwarder subscribed");
        receive_until(&mut client, is_event("warmup")).await;
        (bus, subscribers, client)
    }

    /// #15: once the forwarder has fallen behind the bus, the client gets
    /// the whole store, then the live events again. A 4-slot bus overflows
    /// while the forwarder is held at the subscriber lock, and the kitchen
    /// light moves meanwhile. Its echo is lost, so only the resync can tell
    /// the client.
    ///
    /// The resync must come once, carry exactly what the store holds, and
    /// go out as the unsolicited `DeviceList` response every client already
    /// takes as its new device list. The forwarder has run before the burst
    /// (see [`warm_forwarder`]), so a snapshot it took before it knew of the
    /// lag would miss the kitchen light's move (#47 review, finding 2). And
    /// no event from before the resync may follow it (finding 1): the
    /// forwarder hands on what's still buffered first.
    #[tokio::test]
    async fn event_forwarder_resyncs_the_clients_after_falling_behind() {
        let home = home().await;
        let (bus, subscribers, mut client) = warm_forwarder(Arc::clone(&home.store)).await;

        let kitchen = "light_kitchen".to_string();
        let moved = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(40),
            color_temp: None,
            rgb_color: None,
        });
        let before = home.store.get_device(&kitchen).await.unwrap().state;
        assert_ne!(before, moved, "the kitchen light must move");
        {
            // The forwarder gets no further than its first event before we
            // let go.
            let _held = subscribers.write().await;
            bus.send(removed("first")).expect("forwarder subscribed");
            home.store
                .update_device_state(&kitchen, moved.clone())
                .await
                .unwrap();
            let echo = EventType::AttributeChanged {
                attribute: "state".to_string(),
                old_value: serde_json::to_value(&before).unwrap(),
                new_value: serde_json::to_value(&moved).unwrap(),
            };
            bus.send(DeviceEvent {
                event_type: echo,
                ..removed(&kitchen)
            })
            .expect("forwarder subscribed");
            for _ in 0..16 {
                bus.send(removed("flood")).expect("forwarder subscribed");
            }
        }

        let received = receive_until(&mut client, is_resync).await;
        assert!(
            !received.iter().any(is_event(&kitchen)),
            "the kitchen echo should have been lost to the lag: {received:?}"
        );
        let resynced = by_id(decoded_by_a_client(received.last().unwrap().clone()));
        assert!(
            resynced
                .iter()
                .any(|d| d.device_id == kitchen && d.state == moved),
            "the resync must carry the kitchen light's move"
        );
        assert_eq!(
            resynced,
            by_id(home.store.list_devices().await),
            "the resync must be the store"
        );

        // And then live events, with nothing from before the resync and no
        // more resyncs.
        bus.send(removed("live")).expect("forwarder subscribed");
        let after = receive_until(&mut client, is_event("live")).await;
        assert_eq!(after.len(), 1, "only `live` after the resync: {after:?}");
    }

    /// #47 review, finding 1: a burst that goes on while the forwarder
    /// catches up must not make it resync again and again. The forwarder
    /// falls behind a 4-slot bus, and while it's held at the subscriber lock
    /// in the middle of catching up, another ring's worth of events comes
    /// in. Resyncing right at the lag left it a whole ring behind the bus,
    /// so those events lagged it again: a second resync (and, as long as
    /// the burst went on, one after another, with no event forwarded in
    /// between). It must be one resync for the lag, once what's buffered is
    /// handed on, and then live events.
    #[tokio::test]
    async fn event_forwarder_resyncs_once_under_a_burst_that_goes_on() {
        let (bus, subscribers, mut client) = warm_forwarder(StateStore::new()).await;
        {
            let _held = subscribers.write().await;
            for _ in 0..8 {
                bus.send(removed("flood")).expect("forwarder subscribed");
            }
            // The forwarder runs until it waits here, with its next message.
            tokio::task::yield_now().await;
            for _ in 0..4 {
                bus.send(removed("burst")).expect("forwarder subscribed");
            }
        }

        let mut received = receive_until(&mut client, is_resync).await;
        bus.send(removed("live")).expect("forwarder subscribed");
        let after = receive_until(&mut client, is_event("live")).await;
        let resyncs = received.iter().chain(&after).filter(|o| is_resync(o));
        assert_eq!(
            resyncs.count(),
            1,
            "one resync for the lag: {received:?}, then {after:?}"
        );
        assert_eq!(after.len(), 1, "only `live` after the resync: {after:?}");
        received.pop();
        assert!(
            received.first().is_some_and(is_event("flood")),
            "the burst must come in while the forwarder catches up: {received:?}"
        );
        assert!(
            received.iter().skip(1).all(is_event("burst")),
            "the burst is handed on before the resync: {received:?}"
        );
    }

    /// The same tracking, run the way the server runs it
    /// (`VirtualDeviceManager::start`) rather than pumped by the test.
    #[tokio::test]
    async fn input_tracking_follows_the_bus() {
        let home = home().await;
        home.manager.start().await.expect("start input tracking");
        let mut rx = home.bus.subscribe();

        set_light(&home.server, "light_kitchen", Some(true), Some(40)).await;

        // The timeout only bounds a failure; the echo ends the wait.
        let group_echo = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = rx.recv().await.expect("event bus");
                if event.device_id == GROUP {
                    return event;
                }
            }
        })
        .await
        .expect("the group was never re-derived");
        let group = light(&home.store, GROUP).await;
        assert!(group.is_on, "group should follow its lit member: {group:?}");
        assert_eq!(
            echoes(&[group_echo], GROUP),
            vec![DeviceStateValue::Light(group)],
            "the group echo must carry its stored state"
        );
    }
}
