use serde::{Deserialize, Serialize};
use v1bectl_sync::*;

pub struct HttpClient {
    base_url: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
pub struct ListDevicesResponse {
    pub devices: Vec<DeviceInfo>,
    pub total_count: u32,
}

#[derive(Serialize)]
struct SetLightRequest {
    is_on: Option<bool>,
    brightness: Option<u8>,
    color_temp: Option<u16>,
    rgb_color: Option<RgbColor>,
}

impl HttpClient {
    pub fn new(server_addr: String) -> Self {
        let base_url = if server_addr.starts_with("http") {
            server_addr
        } else {
            format!("http://{}", server_addr)
        };

        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn discover_devices(&self) -> anyhow::Result<ListDevicesResponse> {
        let url = format!("{}/api/devices", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<ListDevicesResponse>()
            .await?;

        Ok(response)
    }

    pub async fn get_device_state(&self, device_id: &str) -> anyhow::Result<DeviceStateValue> {
        let url = format!("{}/api/devices/{}/state", self.base_url, device_id);
        let response = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<DeviceStateValue>()
            .await?;

        Ok(response)
    }

    pub async fn set_light_state(
        &self,
        device_id: &str,
        light_state: LightState,
    ) -> anyhow::Result<LightState> {
        let url = format!("{}/api/devices/{}/light", self.base_url, device_id);
        let request = SetLightRequest {
            is_on: Some(light_state.is_on),
            brightness: light_state.brightness,
            color_temp: light_state.color_temp,
            rgb_color: light_state.rgb_color,
        };

        let response = self
            .client
            .put(&url)
            .json(&request)
            .send()
            .await?
            .json::<LightState>()
            .await?;

        Ok(response)
    }

    pub async fn health_check(&self) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}/health", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        Ok(response)
    }
}
