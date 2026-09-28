//! The legacy TCP/CBOR API server.
//!
//! Nothing in the workspace starts it: the server binary serves the
//! WebSocket API ([`crate::AxumServer`]) only, and the CLI's TCP client is
//! not wired up. It's kept as an alternative transport, and its light
//! writes go through the sync engine like the WebSocket API's (#45).

use ciborium::{de, ser};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};
use v1bectl_sync::{
    DeviceInfo, DeviceStateValue, DiscoverDevicesResponse, LightState, Message, MessageType,
    StateStore, SyncEngine,
};

pub struct TcpServer {
    port: u16,
    state_store: Arc<StateStore>,
    /// The write path: a light write is a user write, queued for the
    /// gateway and protected like the WebSocket API's (#45).
    sync_engine: Arc<SyncEngine>,
}

impl TcpServer {
    pub fn new(port: u16, state_store: Arc<StateStore>, sync_engine: Arc<SyncEngine>) -> Self {
        Self {
            port,
            state_store,
            sync_engine,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("TCP API server listening on {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("New TCP connection from {}", addr);

            let state_store = self.state_store.clone();
            let sync_engine = self.sync_engine.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state_store, sync_engine).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state_store: Arc<StateStore>,
    sync_engine: Arc<SyncEngine>,
) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; 4096];

    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            debug!("Connection closed");
            break;
        }

        // Decode CBOR message
        let message: Message = de::from_reader(&buffer[..n])?;
        debug!("Received message: {:?}", message.message_type);

        // Process based on payload type
        let response = if let MessageType::Request = &message.message_type {
            process_request(message, &state_store, &sync_engine).await
        } else {
            warn!("Unexpected message type: {:?}", message.message_type);
            create_error_response(message.correlation_id, "Invalid message type")
        };

        // Send response
        let mut response_bytes = Vec::new();
        ser::into_writer(&response, &mut response_bytes)?;
        stream.write_all(&response_bytes).await?;
        stream.flush().await?;
    }

    Ok(())
}

async fn process_request(
    message: Message,
    state_store: &Arc<StateStore>,
    sync_engine: &SyncEngine,
) -> Message {
    // For now, let's handle a simple device discovery request
    // We'll check the first byte of the payload to determine request type
    if message.payload.is_empty() {
        return create_error_response(message.correlation_id, "Empty payload");
    }

    match message.payload[0] {
        1 => {
            // Discover devices request
            handle_discover_devices(message.correlation_id, state_store).await
        }
        2 => {
            // Get device state request
            if message.payload.len() < 2 {
                return create_error_response(message.correlation_id, "Missing device ID");
            }

            // Simple: rest of payload is device ID string
            if let Ok(device_id) = String::from_utf8(message.payload[1..].to_vec()) {
                handle_get_device_state(message.correlation_id, &device_id, state_store).await
            } else {
                create_error_response(message.correlation_id, "Invalid device ID")
            }
        }
        3 => {
            // Set light state request
            handle_set_light_state(message, state_store, sync_engine).await
        }
        _ => create_error_response(message.correlation_id, "Unknown request type"),
    }
}

async fn handle_discover_devices(correlation_id: String, state_store: &Arc<StateStore>) -> Message {
    let devices = state_store.list_devices().await;

    let device_infos: Vec<DeviceInfo> = devices.into_iter().map(|d| d.device_info).collect();

    // Discovered-device count, always far below u32::MAX.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "discovered-device count, always far below u32::MAX"
    )]
    let total_count = device_infos.len() as u32;
    let response = DiscoverDevicesResponse {
        devices: device_infos.clone(),
        total_count,
        discovery_timestamp: chrono::Utc::now().timestamp_millis().cast_unsigned(),
        gateway_scan_duration_ms: 0,
    };

    let mut payload = Vec::new();
    let _ = ser::into_writer(&response, &mut payload);

    Message {
        correlation_id,
        message_type: MessageType::Response,
        payload,
        timestamp: chrono::Utc::now().timestamp_millis().cast_unsigned(),
    }
}

async fn handle_get_device_state(
    correlation_id: String,
    device_id: &str,
    state_store: &Arc<StateStore>,
) -> Message {
    match state_store.get_device(&device_id.to_string()).await {
        Some(device) => {
            let mut payload = Vec::new();
            let _ = ser::into_writer(&device.state, &mut payload);
            Message {
                correlation_id,
                message_type: MessageType::Response,
                payload,
                timestamp: chrono::Utc::now().timestamp_millis().cast_unsigned(),
            }
        }
        None => create_error_response(correlation_id, "Device not found"),
    }
}

/// A light write, through [`SyncEngine::apply_optimistic_update`]: the
/// WebSocket API's write path for a physical device (#45). It used to write
/// the store only: no push to the gateway, and no protection window, so the
/// next pull took the hub's unchanged value for an outside change and
/// reverted it.
///
/// A virtual device (a light group) is refused: its writes fan out to its
/// members through the virtual device manager, which this server doesn't
/// have, and the gateway doesn't know it. So is a device that isn't a
/// light.
async fn handle_set_light_state(
    message: Message,
    state_store: &Arc<StateStore>,
    sync_engine: &SyncEngine,
) -> Message {
    // Parse device ID and state from payload
    // Format: [3, device_id_len, ...device_id, ...cbor_light_state]
    if message.payload.len() < 3 {
        return create_error_response(message.correlation_id, "Invalid payload");
    }

    let device_id_len = message.payload[1] as usize;
    if message.payload.len() < 2 + device_id_len {
        return create_error_response(message.correlation_id, "Invalid device ID length");
    }

    let Ok(device_id) = String::from_utf8(message.payload[2..2 + device_id_len].to_vec()) else {
        return create_error_response(message.correlation_id, "Invalid device ID");
    };

    let state_bytes = &message.payload[2 + device_id_len..];
    let light_state: LightState = match de::from_reader(state_bytes) {
        Ok(state) => state,
        Err(_) => return create_error_response(message.correlation_id, "Invalid light state"),
    };

    let Some(device) = state_store.get_device(&device_id).await else {
        return create_error_response(message.correlation_id, "Device not found");
    };
    if device
        .device_info
        .device_groups
        .iter()
        .any(|group| group == "virtual")
    {
        return create_error_response(
            message.correlation_id,
            "Virtual devices can't be written over the TCP API",
        );
    }
    // As the WebSocket API: only a light takes a light state (#50 review).
    // An outlet or a sensor would get a `Light` state in the store, and the
    // hub a PATCH of light attributes.
    if !matches!(device.state, DeviceStateValue::Light(_)) {
        return create_error_response(message.correlation_id, "Device is not a light");
    }

    // Update state: queued for the gateway, and protected until it lands.
    let new_state = DeviceStateValue::Light(light_state);
    match sync_engine
        .apply_optimistic_update(&device_id, new_state)
        .await
    {
        Ok(()) => {
            let mut response = Vec::new();
            let _ = ser::into_writer(&true, &mut response);
            Message {
                correlation_id: message.correlation_id,
                message_type: MessageType::Response,
                payload: response,
                timestamp: chrono::Utc::now().timestamp_millis().cast_unsigned(),
            }
        }
        Err(e) => create_error_response(message.correlation_id, &format!("Update failed: {e}")),
    }
}

fn create_error_response(correlation_id: String, error: &str) -> Message {
    let error_bytes = error.as_bytes().to_vec();
    Message {
        correlation_id,
        message_type: MessageType::Error,
        payload: error_bytes,
        timestamp: chrono::Utc::now().timestamp_millis().cast_unsigned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use v1bectl_sync::{EventBus, Gateway, SyncStatus};
    use v1bectl_virtual::DummyGateway;

    /// A set-light request for `device_id`, framed the way the CLI's TCP
    /// client frames it.
    fn set_light_request(device_id: &str, state: &LightState) -> Message {
        let mut payload = vec![3, u8::try_from(device_id.len()).expect("a short id")];
        payload.extend_from_slice(device_id.as_bytes());
        ser::into_writer(state, &mut payload).expect("CBOR");
        Message {
            correlation_id: "tcp-test".to_string(),
            message_type: MessageType::Request,
            payload,
            timestamp: 0,
        }
    }

    /// The dummy gateway's devices, seeded into a store as the server seeds
    /// them at startup, and a sync engine (not started) over both.
    async fn home() -> (Arc<DummyGateway>, Arc<StateStore>, Arc<SyncEngine>) {
        let gateway = Arc::new(DummyGateway::new("basic_home"));
        let store = StateStore::new();
        for info in gateway.discover_devices().await.expect("discover") {
            let state = gateway
                .get_device_state(&info.device_id)
                .await
                .expect("initial state");
            store.add_device(info, state).await;
        }
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            Arc::new(EventBus::new(100)),
            gateway.clone(),
            None,
        ));
        (gateway, store, engine)
    }

    /// #45: a light write over the TCP API goes through the sync engine. It
    /// used to write the store only, so the engine never heard of it: no
    /// push to the gateway, and nothing to shield it from the next pull,
    /// which reverted it (`GatewayWins`).
    ///
    /// The engine starts after the write. Its first pull runs at once,
    /// racing the push, and must leave the write alone either way; the push
    /// must reach the gateway.
    #[tokio::test]
    async fn a_light_write_goes_through_the_sync_engine() {
        let (gateway, store, engine) = home().await;
        let id = "light_kitchen".to_string();
        let DeviceStateValue::Light(current) = store.get_device(&id).await.expect("kitchen").state
        else {
            panic!("the kitchen light isn't a light");
        };
        let written = LightState {
            is_on: !current.is_on,
            brightness: Some(42),
            ..current
        };
        let want = DeviceStateValue::Light(written.clone());

        let response = process_request(set_light_request(&id, &written), &store, &engine).await;
        assert!(
            matches!(response.message_type, MessageType::Response),
            "{:?}: {}",
            response.message_type,
            String::from_utf8_lossy(&response.payload)
        );
        assert_eq!(store.get_device(&id).await.expect("kitchen").state, want);
        assert!(
            matches!(
                engine.get_sync_status(&id).await,
                Some(SyncStatus::PendingSync { .. })
            ),
            "the write never reached the sync engine"
        );

        let runner = tokio::spawn({
            let engine = engine.clone();
            async move { engine.start().await }
        });
        tokio::time::timeout(Duration::from_secs(20), async {
            while gateway.get_device_state(&id).await.ok().as_ref() != Some(&want) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the write was never pushed to the gateway");
        assert_eq!(
            store.get_device(&id).await.expect("kitchen").state,
            want,
            "the write was reverted"
        );

        engine.stop().await;
        runner.await.expect("engine task").expect("engine");
    }

    /// A device that isn't a light is refused, as the WebSocket API refuses
    /// it (#50 review): an outlet and a sensor keep their state, and nothing
    /// is queued for the gateway.
    #[tokio::test]
    async fn a_light_write_to_something_else_is_refused() {
        let (_gateway, store, engine) = home().await;
        let written = LightState {
            is_on: true,
            brightness: Some(42),
            color_temp: None,
            rgb_color: None,
        };
        for id in ["outlet_tv", "temperature_living_room"] {
            let id = id.to_string();
            let before = store.get_device(&id).await.expect("in the store").state;
            assert!(!matches!(before, DeviceStateValue::Light(_)), "{id}");

            let response = process_request(set_light_request(&id, &written), &store, &engine).await;
            assert!(
                matches!(response.message_type, MessageType::Error),
                "{id}: {:?}",
                response.message_type
            );
            assert_eq!(
                String::from_utf8_lossy(&response.payload),
                "Device is not a light",
                "{id}"
            );
            assert_eq!(
                store.get_device(&id).await.expect("in the store").state,
                before,
                "{id} in the store"
            );
            assert!(engine.get_sync_status(&id).await.is_none(), "{id} queued");
        }
    }

    /// A virtual device is refused, not pushed to a gateway that doesn't
    /// know it.
    #[tokio::test]
    async fn a_virtual_device_is_refused() {
        let (_gateway, store, engine) = home().await;
        let mut info = store
            .get_device(&"light_kitchen".to_string())
            .await
            .expect("kitchen")
            .device_info;
        info.device_id = "group".to_string();
        info.device_groups = vec!["virtual".to_string()];
        let before = LightState {
            is_on: false,
            brightness: Some(10),
            color_temp: None,
            rgb_color: None,
        };
        store
            .add_device(info, DeviceStateValue::Light(before.clone()))
            .await;

        let written = LightState {
            is_on: true,
            ..before.clone()
        };
        let group = "group".to_string();
        let response = process_request(set_light_request(&group, &written), &store, &engine).await;
        assert!(matches!(response.message_type, MessageType::Error));
        assert_eq!(
            store.get_device(&group).await.expect("group").state,
            DeviceStateValue::Light(before)
        );
        assert!(engine.get_sync_status(&group).await.is_none());
    }
}
