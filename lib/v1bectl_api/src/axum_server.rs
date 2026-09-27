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
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use v1bectl_sync::sync::*;
use v1bectl_sync::*;
use v1bectl_virtual::*;

#[derive(Clone)]
pub struct AxumServer {
    port: u16,
    state_store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
    gateway: Arc<dyn Gateway>,
    virtual_device_manager: Arc<VirtualDeviceManager>,
    subscribers: Arc<RwLock<HashMap<String, tokio::sync::mpsc::UnboundedSender<DeviceEvent>>>>,
    sync_engine: Option<Arc<SyncEngine>>, // 🔥 OPTIONAL SYNC ENGINE FOR OPTIMISTIC UPDATES!
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
    DeviceState {
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
    Error {
        code: String,
        message: String,
    },
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
    pub fn with_sync_engine(mut self, sync_engine: Arc<SyncEngine>) -> Self {
        // Virtual writes fan out to physical members, and those take the same
        // path as a direct write: store, echo, gateway push.
        self.virtual_device_manager
            .attach_sync_engine(Arc::clone(&sync_engine));
        self.sync_engine = Some(sync_engine);
        self
    }

    /// 🔥 GET VIRTUAL DEVICE MANAGER FOR EXTERNAL REGISTRATION! 💖
    pub fn virtual_device_manager(&self) -> Arc<VirtualDeviceManager> {
        Arc::clone(&self.virtual_device_manager)
    }

    pub async fn start(self) -> anyhow::Result<()> {
        info!(
            "Starting WebSocket-ONLY server on VIBEC0RE port {}",
            self.port
        );

        // Start virtual device manager
        self.virtual_device_manager
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start virtual device manager: {}", e))?;

        // 🔥 SUBSCRIBE TO EVENTBUS AND FORWARD TO WEBSOCKET CLIENTS! 💖
        spawn_event_forwarder(&self.event_bus, Arc::clone(&self.subscribers));

        let app = Router::new()
            // WebSocket ONLY - pure async real-time vibes!! 🔥
            .route("/", get(websocket_handler))
            .layer(ServiceBuilder::new().layer(CorsLayer::permissive()))
            .with_state(self.clone());

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        info!(
            "WebSocket-ONLY server listening on 0.0.0.0:{} - NO BOOMER REST! 🚀",
            self.port
        );

        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn broadcast_event(&self, event: DeviceEvent) {
        let subscribers = self.subscribers.read().await;
        let mut failed_subscribers = Vec::new();

        for (subscriber_id, sender) in subscribers.iter() {
            if sender.send(event.clone()).is_err() {
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

    async fn handle_api_request(
        &self,
        request: ApiRequest,
        _correlation_id: String,
    ) -> ApiResponse {
        match request {
            ApiRequest::DiscoverDevices => {
                debug!("Handling device discovery request - WITH STATES! 🔥");
                let all_device_states = self.state_store.list_devices().await;
                let total = all_device_states.len() as u32;

                ApiResponse::DeviceList {
                    total_count: total,
                    devices: all_device_states, // 🔥 Return FULL DeviceState with values!
                }
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

                ApiResponse::DeviceList {
                    total_count: filtered_devices.len() as u32,
                    devices: filtered_devices, // 🔥 Return FULL DeviceState!
                }
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
                                    Ok(_) => {
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
                                            message: format!("Virtual device update failed: {}", e),
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
                                        Ok(_) => {
                                            debug!("✅ Optimistic update applied for {} - USER SEES CHANGE NOW!", device_id);
                                            ApiResponse::LightUpdated {
                                                new_state: current_light,
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to apply optimistic update: {}", e);
                                            ApiResponse::Error {
                                                code: "OPTIMISTIC_UPDATE_FAILED".to_string(),
                                                message: format!("Update failed: {}", e),
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
                                        Ok(_) => {
                                            // If gateway update succeeds, update local state
                                            match self
                                                .state_store
                                                .update_device_state(&device_id, new_state.clone())
                                                .await
                                            {
                                                Ok(_) => {
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
                                                            "Local state update failed: {}",
                                                            e
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
                                                message: format!("Gateway update failed: {}", e),
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
                                Ok(_) => {
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
                                        message: format!("Update failed: {}", e),
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
                                Ok(_) => {
                                    // If gateway update succeeds, update local state
                                    match self
                                        .state_store
                                        .update_device_state(&device_id, new_state.clone())
                                        .await
                                    {
                                        Ok(_) => {
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
                                                message: format!(
                                                    "Local state update failed: {}",
                                                    e
                                                ),
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
                                        message: format!("Gateway update failed: {}", e),
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
                                .map_err(|e| format!("Failed to add virtual device: {}", e)),
                            Err(e) => Err(format!("Failed to create light group: {}", e)),
                        }
                    }
                    VirtualDeviceType::SceneController => {
                        match SceneController::new(config.clone(), Arc::clone(&self.state_store)) {
                            Ok(scene_controller) => self
                                .virtual_device_manager
                                .add_virtual_device(Box::new(scene_controller))
                                .await
                                .map_err(|e| format!("Failed to add virtual device: {}", e)),
                            Err(e) => Err(format!("Failed to create scene controller: {}", e)),
                        }
                    }
                    _ => Err("Virtual device type not yet implemented".to_string()),
                };

                match device_result {
                    Ok(_) => {
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
                    Ok(_) => {
                        debug!("Removed virtual device: {}", device_id);
                        ApiResponse::VirtualDeviceRemoved { device_id }
                    }
                    Err(e) => ApiResponse::Error {
                        code: "REMOVE_FAILED".to_string(),
                        message: format!("Failed to remove virtual device: {}", e),
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
                    Ok(_) => {
                        debug!("Activated scene '{}' on device '{}'", scene_name, device_id);
                        ApiResponse::SceneActivated {
                            device_id,
                            scene_name,
                        }
                    }
                    Err(e) => ApiResponse::Error {
                        code: "SCENE_FAILED".to_string(),
                        message: format!("Failed to activate scene: {}", e),
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
        }
    }
}

/// Forward every event on `event_bus` to each WebSocket subscriber, and drop
/// the subscribers whose client has gone. Subscribes before it returns, so
/// nothing published after this call is missed.
fn spawn_event_forwarder(
    event_bus: &EventBus,
    subscribers: Arc<RwLock<HashMap<String, tokio::sync::mpsc::UnboundedSender<DeviceEvent>>>>,
) {
    let mut event_rx = event_bus.subscribe();
    debug!("📢 API Server subscribed to EventBus - will forward events to WebSocket clients!");

    tokio::spawn(async move {
        // Lagging behind the bus skips events instead of ending the
        // forwarding for every client (#15).
        while let Some(event) = recv_lossy(&mut event_rx, "WebSocket forwarder").await {
            debug!("🔥 Forwarding event to WebSocket clients: {:?}", event);
            let subscribers_read = subscribers.read().await;
            let mut failed = Vec::new();

            for (id, tx) in subscribers_read.iter() {
                if tx.send(event.clone()).is_err() {
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
                    // Handle events
                    Some(event) = event_rx.recv() => {
                        // Encode event as CBOR in ApiMessage wrapper
                        let mut event_payload = Vec::new();
                        if let Err(e) = ciborium::into_writer(&event, &mut event_payload) {
                            error!("Failed to encode event as CBOR: {}", e);
                            continue;
                        }

                        let api_message = ApiMessage {
                            correlation_id: Uuid::new_v4().to_string(),
                            message_type: ApiMessageType::Event,
                            payload: event_payload,
                        };

                        let mut message_bytes = Vec::new();
                        if let Err(e) = ciborium::into_writer(&api_message, &mut message_bytes) {
                            error!("Failed to encode API message: {}", e);
                            continue;
                        }

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
    use std::time::Duration;
    use tokio::sync::broadcast::{self, error::TryRecvError};

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
        // Re-derived from the members: the average of 90, 65 and 40.
        let group = light(&home.store, GROUP).await;
        assert_eq!(
            (group.is_on, group.brightness),
            (true, Some(65)),
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
    /// When that view lands in the store (GatewayWins, after the protection
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

    /// #15: the WebSocket forwarder must outlive falling behind the bus. It
    /// is held at the subscriber lock on its first event while more events
    /// pile up behind it than the bus keeps (1000). An event published after
    /// that must still reach the client.
    #[tokio::test]
    async fn event_forwarder_survives_falling_behind_the_bus() {
        let bus = EventBus::new(10);
        let subscribers = Arc::new(RwLock::new(HashMap::new()));
        let (tx, mut client) = tokio::sync::mpsc::unbounded_channel();
        subscribers.write().await.insert("client".to_string(), tx);
        spawn_event_forwarder(&bus, Arc::clone(&subscribers));

        let event = |device_id: &str| DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: device_id.to_string(),
            event_type: EventType::DeviceRemoved,
        };
        {
            // The forwarder blocks here at its first event.
            let _held = subscribers.write().await;
            for _ in 0..1500 {
                bus.publish(event("flood")).await;
            }
        }
        bus.publish(event("after")).await;

        // The timeout only bounds a failure; the event ends the wait.
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(forwarded) = client.recv().await {
                if forwarded.device_id == "after" {
                    return;
                }
            }
            panic!("the client's channel closed");
        })
        .await
        .expect("forwarding died behind the bus: `after` never reached the client");
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
