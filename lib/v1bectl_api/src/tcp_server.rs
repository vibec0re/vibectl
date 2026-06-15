use ciborium::{de, ser};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};
use v1bectl_sync::*;

pub struct TcpServer {
    port: u16,
    state_store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
}

impl TcpServer {
    pub fn new(port: u16, state_store: Arc<StateStore>, event_bus: Arc<EventBus>) -> Self {
        Self {
            port,
            state_store,
            event_bus,
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
            let event_bus = self.event_bus.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state_store, event_bus).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state_store: Arc<StateStore>,
    event_bus: Arc<EventBus>,
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
        let response = match &message.message_type {
            MessageType::Request => process_request(message, &state_store, &event_bus).await,
            _ => {
                warn!("Unexpected message type: {:?}", message.message_type);
                create_error_response(message.correlation_id, "Invalid message type")
            }
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
    _event_bus: &Arc<EventBus>,
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
            handle_set_light_state(message, state_store).await
        }
        _ => create_error_response(message.correlation_id, "Unknown request type"),
    }
}

async fn handle_discover_devices(correlation_id: String, state_store: &Arc<StateStore>) -> Message {
    let devices = state_store.list_devices().await;

    let device_infos: Vec<DeviceInfo> = devices.into_iter().map(|d| d.device_info).collect();

    let response = DiscoverDevicesResponse {
        devices: device_infos.clone(),
        total_count: device_infos.len() as u32,
        discovery_timestamp: chrono::Utc::now().timestamp_millis() as u64,
        gateway_scan_duration_ms: 0,
    };

    let mut payload = Vec::new();
    let _ = ser::into_writer(&response, &mut payload);

    Message {
        correlation_id,
        message_type: MessageType::Response,
        payload,
        timestamp: chrono::Utc::now().timestamp_millis() as u64,
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
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }
        }
        None => create_error_response(correlation_id, "Device not found"),
    }
}

async fn handle_set_light_state(message: Message, state_store: &Arc<StateStore>) -> Message {
    // Parse device ID and state from payload
    // Format: [3, device_id_len, ...device_id, ...cbor_light_state]
    if message.payload.len() < 3 {
        return create_error_response(message.correlation_id, "Invalid payload");
    }

    let device_id_len = message.payload[1] as usize;
    if message.payload.len() < 2 + device_id_len {
        return create_error_response(message.correlation_id, "Invalid device ID length");
    }

    let device_id = match String::from_utf8(message.payload[2..2 + device_id_len].to_vec()) {
        Ok(id) => id,
        Err(_) => return create_error_response(message.correlation_id, "Invalid device ID"),
    };

    let state_bytes = &message.payload[2 + device_id_len..];
    let light_state: LightState = match de::from_reader(state_bytes) {
        Ok(state) => state,
        Err(_) => return create_error_response(message.correlation_id, "Invalid light state"),
    };

    // Update state
    let new_state = DeviceStateValue::Light(light_state);
    match state_store.update_device_state(&device_id, new_state).await {
        Ok(()) => {
            let mut response = Vec::new();
            let _ = ser::into_writer(&true, &mut response);
            Message {
                correlation_id: message.correlation_id,
                message_type: MessageType::Response,
                payload: response,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }
        }
        Err(e) => create_error_response(message.correlation_id, &format!("Update failed: {}", e)),
    }
}

fn create_error_response(correlation_id: String, error: &str) -> Message {
    let error_bytes = error.as_bytes().to_vec();
    Message {
        correlation_id,
        message_type: MessageType::Error,
        payload: error_bytes,
        timestamp: chrono::Utc::now().timestamp_millis() as u64,
    }
}
