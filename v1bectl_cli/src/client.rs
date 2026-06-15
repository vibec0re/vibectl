use anyhow::Result;
use ciborium::{de, ser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;
use uuid::Uuid;
use v1bectl_sync::*;

pub struct ApiClient {
    server_addr: String,
}

impl ApiClient {
    pub fn new(server_addr: String) -> Self {
        Self { server_addr }
    }

    async fn send_request(&self, request_type: u8, data: Vec<u8>) -> Result<Message> {
        let mut stream = TcpStream::connect(&self.server_addr).await?;

        // Create request payload
        let mut payload = vec![request_type];
        payload.extend_from_slice(&data);

        // Create message
        let message = Message {
            correlation_id: Uuid::new_v4().to_string(),
            message_type: MessageType::Request,
            payload,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };

        // Send request
        let mut request_bytes = Vec::new();
        ser::into_writer(&message, &mut request_bytes)?;
        stream.write_all(&request_bytes).await?;
        stream.flush().await?;

        // Read response
        let mut buffer = vec![0u8; 8192];
        let n = stream.read(&mut buffer).await?;

        let response: Message = de::from_reader(&buffer[..n])?;

        if matches!(response.message_type, MessageType::Error) {
            let error_msg = String::from_utf8_lossy(&response.payload);
            anyhow::bail!("Server error: {}", error_msg);
        }

        Ok(response)
    }

    pub async fn discover_devices(&self) -> Result<DiscoverDevicesResponse> {
        debug!("Discovering devices...");
        let response = self.send_request(1, vec![]).await?;
        let discovery_response: DiscoverDevicesResponse = de::from_reader(&response.payload[..])?;
        Ok(discovery_response)
    }

    pub async fn get_device_state(&self, device_id: &str) -> Result<DeviceStateValue> {
        debug!("Getting device state for: {}", device_id);
        let response = self.send_request(2, device_id.as_bytes().to_vec()).await?;
        let state: DeviceStateValue = de::from_reader(&response.payload[..])?;
        Ok(state)
    }

    pub async fn set_light_state(&self, device_id: &str, state: LightState) -> Result<()> {
        debug!("Setting light state for: {}", device_id);

        // Encode the light state
        let mut state_bytes = Vec::new();
        ser::into_writer(&state, &mut state_bytes)?;

        // Build payload: [device_id_len, ...device_id, ...state]
        let mut data = vec![device_id.len() as u8];
        data.extend_from_slice(device_id.as_bytes());
        data.extend_from_slice(&state_bytes);

        let response = self.send_request(3, data).await?;
        let success: bool = de::from_reader(&response.payload[..])?;

        if !success {
            anyhow::bail!("Failed to set light state");
        }

        Ok(())
    }
}
