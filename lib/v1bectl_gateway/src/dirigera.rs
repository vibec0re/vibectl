use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite};
use tracing::{debug, error, info, warn};
use v1bectl_sync::*;

// 🔥 mDNS COLLISION FALLBACK TUNING 💖
// Highest numeric collision suffix we probe: base + `-2`..=`-N`. IKEA's
// responder rarely climbs past `-3`, so a small ceiling keeps startup snappy.
const MAX_MDNS_COLLISION_SUFFIX: u32 = 5;
// Per-candidate probe timeout — short so a list of dead hosts fails fast
// instead of stacking the full request timeout N times.
const HOST_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Dirigera Gateway - connects to actual IKEA Dirigera hub! 🔥
pub struct DirigeraGateway {
    client: Client,
    base_url: String,
    ws_url: String,
    access_token: String,
    // kept: configured request timeout, retained for introspection; it is already
    // applied to the reqwest `Client` built in `new`.
    #[allow(dead_code)]
    timeout: Duration,
    event_sender: broadcast::Sender<DeviceEvent>,
}

// Dirigera API response structures
#[derive(Debug, Deserialize, Serialize)]
struct DirigeraDevice {
    id: String,
    #[serde(rename = "type")]
    device_type: String,
    #[serde(rename = "deviceType")]
    device_category: String,
    attributes: DirigeraAttributes,
    #[serde(rename = "isReachable")]
    is_reachable: bool,
    room: Option<DirigeraRoom>,
    capabilities: Option<DirigeraCapabilities>, // 🔥 REAL CAPABILITIES FROM DIRIGERA! CHOOOM FIX! 💖
}

#[derive(Debug, Deserialize, Serialize)]
struct DirigeraCapabilities {
    #[serde(rename = "canReceive")]
    can_receive: Vec<String>,
    #[serde(rename = "canSend")]
    can_send: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DirigeraAttributes {
    #[serde(rename = "customName", default)]
    custom_name: Option<String>,
    #[serde(rename = "isOn", default)]
    is_on: Option<bool>,
    #[serde(rename = "lightLevel", default)]
    light_level: Option<u8>,
    #[serde(rename = "colorTemperature", default)]
    color_temperature: Option<u16>,
    #[serde(rename = "colorHue", default)]
    color_hue: Option<f32>,
    #[serde(rename = "colorSaturation", default)]
    color_saturation: Option<f32>,
    #[serde(rename = "batteryPercentage", default)]
    battery_percentage: Option<u8>,
    #[serde(rename = "isPressed", default)]
    is_pressed: Option<bool>,
    #[serde(rename = "currentTemperature", default)]
    current_temperature: Option<f32>,
    #[serde(rename = "currentRH", default)]
    current_humidity: Option<f32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DirigeraRoom {
    id: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct DirigeraStateUpdate {
    attributes: DirigeraUpdateAttributes,
}

#[derive(Debug, Serialize)]
struct DirigeraUpdateAttributes {
    #[serde(rename = "isOn", skip_serializing_if = "Option::is_none")]
    is_on: Option<bool>,
    #[serde(rename = "lightLevel", skip_serializing_if = "Option::is_none")]
    light_level: Option<u8>,
    #[serde(rename = "colorTemperature", skip_serializing_if = "Option::is_none")]
    color_temperature: Option<u16>,
    #[serde(rename = "transitionTime", skip_serializing_if = "Option::is_none")]
    transition_time: Option<u32>,
}

// 🔥 WebSocket event structures from Dirigera!
#[derive(Debug, Deserialize)]
struct DirigeraWsEvent {
    #[serde(rename = "type")]
    event_type: String,
    // kept: present in the Dirigera WS payload; deserialized to document the
    // protocol shape even though we don't currently act on them.
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    time: String,
    #[serde(rename = "deviceId")]
    device_id: Option<String>,
    data: Option<serde_json::Value>,
}

impl DirigeraGateway {
    pub fn new(host: &str, access_token: &str, timeout: Duration) -> Self {
        let base_url = format!("https://{}:8443/v1", host);
        let ws_url = format!("wss://{}:8443/v1", host);
        let client = Client::builder()
            .timeout(timeout)
            .danger_accept_invalid_certs(true) // Dirigera uses self-signed certs
            .build()
            .expect("Failed to create HTTP client");

        let (event_sender, _) = broadcast::channel(1000);

        info!(
            "🔥 Initialized DirigeraGateway for host: {} - VIBEC0RE LIVE DATA! 🚀",
            host
        );

        Self {
            client,
            base_url,
            ws_url,
            access_token: access_token.to_string(),
            timeout,
            event_sender,
        }
    }

    /// Create from access token (env var or file)
    ///
    /// Token resolution order:
    /// 1. V1BECTL_ACCESS_TOKEN environment variable (for NixOS/systemd)
    /// 2. $HOME/.local/state/v1bectl/access.token file
    ///
    /// 🔁 The configured host is resolved against mDNS collision variants
    /// (see [`Self::resolve_reachable_host`]) before the gateway is built, so a
    /// hub that re-advertised itself as `gw2-xxxx-2.local` is still found.
    pub async fn from_token_file(host: &str, timeout: Duration) -> Result<Self, GatewayError> {
        let access_token = Self::load_access_token().await?;

        // 🔥 mDNS COLLISION FALLBACK — find a live hub among the candidate hosts.
        // Falls back to the configured host as-is if nothing answers (preserves
        // the previous behaviour: build anyway, surface the failure on first use).
        let resolved = Self::resolve_reachable_host(host, &access_token, timeout).await;
        let host = resolved.as_deref().unwrap_or(host);

        Ok(Self::new(host, &access_token, timeout))
    }

    /// Load the Dirigera access token from env var (preferred) or the state file.
    async fn load_access_token() -> Result<String, GatewayError> {
        // 🔥 CHECK ENV VAR FIRST - WORKS WITH NIXOS LOADCREDENTIAL! 💖
        if let Ok(token) = std::env::var("V1BECTL_ACCESS_TOKEN") {
            let access_token = token.trim().to_string();
            info!(
                "🔥 Loaded access token from V1BECTL_ACCESS_TOKEN env var - {} chars",
                access_token.len()
            );
            return Ok(access_token);
        }

        // 🔥 FALLBACK TO FILE 💖
        let token_path = format!(
            "{}/.local/state/v1bectl/access.token",
            std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
        );

        let access_token = tokio::fs::read_to_string(&token_path)
            .await
            .map_err(|e| {
                GatewayError::InternalError(format!(
                    "Failed to read access token from {}: {}",
                    token_path, e
                ))
            })?
            .trim()
            .to_string();

        info!(
            "🔥 Loaded access token from {} - {} chars",
            token_path,
            access_token.len()
        );
        Ok(access_token)
    }

    /// Build the ordered list of hostnames to probe for a Dirigera hub. 🔁
    ///
    /// IKEA gateways advertise over mDNS as `gw2-xxxx.local`. When the name
    /// collides on the network (a second hub, or a stale Avahi entry) the
    /// responder hands out a numeric suffix instead: `gw2-xxxx-2.local`,
    /// `gw2-xxxx-3.local`, … So we strip `.local`, re-derive the base name, and
    /// enumerate the variants. Ordering, deduplicated:
    ///   1. the configured host (always probed first)
    ///   2. the canonical base name (in case the configured host was a stale `-N`)
    ///   3. `-2`..=`-max_suffix` collision variants
    ///
    /// Hosts without a `.local` suffix (raw IPs, regular FQDNs) are returned
    /// as-is with no variants — collisions only happen on mDNS names.
    fn host_candidates(host: &str, max_suffix: u32) -> Vec<String> {
        let Some(stem) = host.strip_suffix(".local") else {
            return vec![host.to_string()];
        };

        // Re-derive the base name: if the configured host already carries a
        // numeric collision suffix (`gw2-xxxx-2`), drop it so we probe from the
        // canonical `gw2-xxxx` upward.
        let base = match stem.rsplit_once('-') {
            Some((prefix, n)) if !prefix.is_empty() && n.parse::<u32>().is_ok() => prefix,
            _ => stem,
        };

        let mut candidates = Vec::new();
        // Configured host always comes first — try exactly what was asked for.
        candidates.push(host.to_string());

        let push_unique = |candidates: &mut Vec<String>, h: String| {
            if !candidates.contains(&h) {
                candidates.push(h);
            }
        };

        push_unique(&mut candidates, format!("{}.local", base));
        for n in 2..=max_suffix {
            push_unique(&mut candidates, format!("{}-{}.local", base, n));
        }

        candidates
    }

    /// Probe the candidate hosts and return the first that responds. 🔁
    ///
    /// Any HTTP response (even `401`/`403`) proves the host is up — we only
    /// reject connection/DNS/timeout errors. Returns `None` when nothing answers
    /// so the caller can fall back to the configured host unchanged.
    async fn resolve_reachable_host(
        host: &str,
        access_token: &str,
        timeout: Duration,
    ) -> Option<String> {
        let candidates = Self::host_candidates(host, MAX_MDNS_COLLISION_SUFFIX);

        // No fallback work to do for a single, non-mDNS candidate (raw IP/FQDN):
        // skip the probe entirely and let the gateway connect lazily as before.
        if candidates.len() == 1 {
            return None;
        }

        let probe_timeout = timeout.min(HOST_PROBE_TIMEOUT);
        let client = Client::builder()
            .timeout(probe_timeout)
            .danger_accept_invalid_certs(true) // Dirigera uses self-signed certs
            .build()
            .ok()?;

        for (idx, candidate) in candidates.iter().enumerate() {
            let url = format!("https://{}:8443/v1/status", candidate);
            match client
                .get(&url)
                .header("Authorization", format!("Bearer {}", access_token))
                .send()
                .await
            {
                Ok(_) => {
                    if idx == 0 {
                        debug!("💚 Dirigera host {} reachable on first try", candidate);
                    } else {
                        info!(
                            "🔁 mDNS COLLISION FALLBACK — '{}' was silent, hub answered at '{}' instead! 🔥",
                            host, candidate
                        );
                    }
                    return Some(candidate.clone());
                }
                Err(e) => {
                    debug!(
                        "🔧 Dirigera host candidate {} not responding: {}",
                        candidate, e
                    );
                }
            }
        }

        warn!(
            "💔 No Dirigera hub responded among {} candidate(s) for '{}' — using configured host as-is",
            candidates.len(), host
        );
        None
    }

    fn convert_dirigera_device(&self, device: DirigeraDevice) -> Result<DeviceInfo, GatewayError> {
        let device_type = match device.device_type.as_str() {
            "light" => DeviceType::Light,
            "outlet" => DeviceType::Light, // Outlets are controllable like lights but simpler
            "blinds" => DeviceType::Switch, // Map blinds to switch for now
            "sensor" => DeviceType::Sensor,
            "controller" => DeviceType::Switch,
            _ => {
                warn!(
                    "Unknown Dirigera device type: {}, mapping to Light",
                    device.device_type
                );
                DeviceType::Light
            }
        };

        // 🔥 USE REAL CAPABILITIES FROM DIRIGERA! NO MORE HARDCODING! CHOOOM FIX! 💖
        let capabilities = if let Some(caps) = &device.capabilities {
            let mut cap_list = Vec::new();

            // Parse canReceive capabilities
            for cap in &caps.can_receive {
                match cap.as_str() {
                    "isOn" => cap_list.push(Capability::OnOff),
                    "lightLevel" => {
                        // 🍺 IKEA GONKS HAD TOO MUCH SCHNAPS! OUTLETS DON'T HAVE BRIGHTNESS! CHOOOM FIX! 💖
                        if device.device_type != "outlet" {
                            cap_list.push(Capability::Brightness);
                        } else {
                            warn!(
                                "🍺 Ignoring bullshit lightLevel for outlet {} - IKEA gonks drunk!",
                                device.id
                            );
                        }
                    }
                    "colorTemperature" => cap_list.push(Capability::ColorTemperature),
                    "colorHue" | "colorSaturation" if !cap_list.contains(&Capability::RgbColor) => {
                        cap_list.push(Capability::RgbColor);
                    }
                    _ => {} // Ignore unknown capabilities like customName
                }
            }

            // Parse canSend for buttons/controllers
            for cap in &caps.can_send {
                match cap.as_str() {
                    "isOn" | "lightLevel" | "singlePress" | "doublePress" | "longPress"
                        if !cap_list.contains(&Capability::OnOff) =>
                    {
                        cap_list.push(Capability::OnOff);
                    }
                    _ => {}
                }
            }

            // Fallback for sensors (they don't report temp/humidity in capabilities)
            if cap_list.is_empty() && device.device_type == "sensor" {
                if device.attributes.current_temperature.is_some() {
                    cap_list.push(Capability::Temperature);
                }
                if device.attributes.current_humidity.is_some() {
                    cap_list.push(Capability::Humidity);
                }
            }

            cap_list
        } else {
            // Fallback for devices without capabilities field (shouldn't happen)
            warn!(
                "⚠️ Device {} has no capabilities field, using defaults",
                device.id
            );
            match device.device_type.as_str() {
                "light" => vec![Capability::OnOff],
                "outlet" => vec![Capability::OnOff],
                "sensor" => vec![Capability::Temperature, Capability::Humidity],
                "controller" => vec![Capability::OnOff],
                _ => vec![Capability::OnOff],
            }
        };

        let device_groups = if let Some(room) = &device.room {
            vec![room.name.clone()]
        } else {
            vec!["ungrouped".to_string()]
        };

        let device_name = device
            .attributes
            .custom_name
            .unwrap_or_else(|| format!("{} {}", device.device_category, device.id));

        Ok(DeviceInfo {
            device_id: device.id,
            name: device_name,
            device_type,
            capabilities,
            device_groups,
            manufacturer: Some("IKEA".to_string()),
            model: Some(device.device_category),
            firmware_version: None,
            battery_powered: device.attributes.battery_percentage.is_some(),
            reachable: device.is_reachable,
            last_seen: chrono::Utc::now().timestamp_millis() as u64,
            custom_attributes: HashMap::new(),
        })
    }

    fn convert_dirigera_state(&self, device: &DirigeraDevice) -> DeviceStateValue {
        match device.device_type.as_str() {
            "outlet" => {
                // 🍺 OUTLETS ARE JUST ON/OFF - NO BRIGHTNESS! IKEA GONKS DRUNK! CHOOOM FIX! 💖
                DeviceStateValue::Light(LightState {
                    is_on: device.attributes.is_on.unwrap_or(false),
                    brightness: None, // OUTLETS DON'T DIM, SILLY!
                    color_temp: None,
                    rgb_color: None,
                })
            }
            "light" => {
                DeviceStateValue::Light(LightState {
                    is_on: device.attributes.is_on.unwrap_or(false),
                    brightness: device.attributes.light_level,
                    color_temp: device.attributes.color_temperature,
                    rgb_color: None, // TODO: Convert from HSV if available
                })
            }
            "sensor" => DeviceStateValue::Sensor(SensorState {
                temperature: device.attributes.current_temperature,
                humidity: device.attributes.current_humidity,
                last_updated: chrono::Utc::now().timestamp_millis() as u64,
            }),
            "controller" => DeviceStateValue::Switch(SwitchState {
                is_pressed: device.attributes.is_pressed.unwrap_or(false),
                last_pressed: None,
                battery_level: device.attributes.battery_percentage,
            }),
            _ => {
                // Default to light state
                DeviceStateValue::Light(LightState {
                    is_on: device.attributes.is_on.unwrap_or(false),
                    brightness: device.attributes.light_level,
                    color_temp: device.attributes.color_temperature,
                    rgb_color: None,
                })
            }
        }
    }
}

#[async_trait]
impl Gateway for DirigeraGateway {
    async fn discover_devices(&self) -> Result<Vec<DeviceInfo>, GatewayError> {
        info!("🔍 Discovering devices from Dirigera hub - VIBEC0RE LIVE DATA! 🔥");

        let response = self
            .client
            .get(format!("{}/devices", self.base_url))
            .header("Authorization", &format!("Bearer {}", self.access_token))
            .send()
            .await
            .map_err(|e| GatewayError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!("Dirigera API error: HTTP {} - {}", status, error_text);
            return Err(GatewayError::InternalError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let devices: Vec<DirigeraDevice> = response.json().await.map_err(|e| {
            GatewayError::InternalError(format!("Failed to parse Dirigera response: {}", e))
        })?;

        info!("🚀 Found {} devices from Dirigera hub!", devices.len());

        let mut device_infos = Vec::new();
        for device in devices {
            debug!(
                "🔍 Raw Dirigera device: id={}, type={}, category={}, custom_name={:?}",
                device.id,
                device.device_type,
                device.device_category,
                device.attributes.custom_name
            );
            match self.convert_dirigera_device(device) {
                Ok(device_info) => {
                    debug!(
                        "✅ Converted device: {} ({:?})",
                        device_info.name, device_info.device_type
                    );
                    device_infos.push(device_info);
                }
                Err(e) => {
                    warn!("❌ Failed to convert device: {}", e);
                }
            }
        }

        info!(
            "🔥 Successfully converted {} devices from Dirigera! 🔥",
            device_infos.len()
        );
        Ok(device_infos)
    }

    async fn get_device_state(
        &self,
        device_id: &DeviceId,
    ) -> Result<DeviceStateValue, GatewayError> {
        debug!("📡 Getting device state for: {}", device_id);

        let response = self
            .client
            .get(format!("{}/devices/{}", self.base_url, device_id))
            .header("Authorization", &format!("Bearer {}", self.access_token))
            .send()
            .await
            .map_err(|e| GatewayError::NetworkError(e.to_string()))?;

        if response.status() == 404 {
            return Err(GatewayError::DeviceNotFound(device_id.clone()));
        }

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(GatewayError::InternalError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let device: DirigeraDevice = response.json().await.map_err(|e| {
            GatewayError::InternalError(format!("Failed to parse device response: {}", e))
        })?;

        let state = self.convert_dirigera_state(&device);
        debug!("🔧 Got state for device {}: {:?}", device_id, state);
        Ok(state)
    }

    async fn set_device_state(
        &self,
        device_id: &DeviceId,
        state: DeviceStateValue,
    ) -> Result<(), GatewayError> {
        info!(
            "🔥 Setting device state for: {} - VIBEC0RE CONTROL! 🚀",
            device_id
        );

        // First get device info to check if it's an outlet
        let device_response = self
            .client
            .get(format!("{}/devices/{}", self.base_url, device_id))
            .header("Authorization", &format!("Bearer {}", self.access_token))
            .send()
            .await
            .map_err(|e| GatewayError::NetworkError(e.to_string()))?;

        if !device_response.status().is_success() {
            return Err(GatewayError::DeviceNotFound(device_id.clone()));
        }

        let device: DirigeraDevice = device_response.json().await.map_err(|e| {
            GatewayError::InternalError(format!("Failed to parse device response: {}", e))
        })?;

        let dirigera_payload = match state {
            DeviceStateValue::Light(mut light_state) => {
                // 🔥 SMART BRIGHTNESS HANDLING - CHOOOM REQUESTED! 💖
                // Only auto-turn-on when brightness > 0 AND we're not explicitly turning OFF!
                // When turning OFF, preserve brightness but keep light OFF!
                if !light_state.is_on {
                    // Explicitly turning OFF - respect that! Keep brightness for later!
                    debug!("💡 Turning light OFF - preserving brightness value for later use!");
                } else if light_state.brightness.is_some() && light_state.brightness.unwrap() > 0 {
                    // Setting brightness while ON or turning ON - ensure light is ON!
                    debug!("🚀 Auto-ensuring light is ON when setting brightness > 0!");
                    light_state.is_on = true;
                }

                // Get current device state to see if it's already on
                let current_state = self.get_device_state(device_id).await.ok();
                let is_currently_on = match current_state {
                    Some(DeviceStateValue::Light(ls)) => ls.is_on,
                    _ => false,
                };

                let attributes = if device.device_type == "outlet" {
                    debug!(
                        "🔌 Updating outlet {} - ON/OFF ONLY! NO BRIGHTNESS!",
                        device_id
                    );
                    DirigeraUpdateAttributes {
                        // 🍺 OUTLETS ARE JUST ON/OFF - IKEA GONKS DRUNK! CHOOOM FIX! 💖
                        is_on: Some(light_state.is_on),
                        light_level: None, // NEVER send brightness to outlets!
                        color_temperature: None, // Don't send color temp for outlets
                        transition_time: None, // No transitions for outlets
                    }
                } else {
                    debug!(
                        "💡 Updating light {} - smart field selection with auto-on!",
                        device_id
                    );
                    DirigeraUpdateAttributes {
                        // Smart isOn handling:
                        // - If brightness is set and light is currently OFF → send isOn: true
                        // - If brightness is set and light is already ON → don't send isOn
                        // - Otherwise send the requested on/off state
                        is_on: if light_state.brightness.is_some() {
                            if !is_currently_on && light_state.is_on {
                                Some(true) // Turn on if currently off and setting brightness
                            } else if is_currently_on && light_state.is_on {
                                None // Don't send isOn if already on and just changing brightness
                            } else {
                                Some(light_state.is_on) // Normal on/off control
                            }
                        } else {
                            Some(light_state.is_on) // Normal on/off without brightness
                        },
                        light_level: light_state.brightness,
                        color_temperature: light_state.color_temp,
                        transition_time: if light_state.brightness.is_some() {
                            Some(500)
                        } else {
                            None
                        },
                    }
                };

                DirigeraStateUpdate { attributes }
            }
            _ => {
                return Err(GatewayError::InvalidStateType);
            }
        };

        // Dirigera expects an array of updates
        let payload_array = vec![dirigera_payload];

        // Debug log the payload being sent
        let payload_json = serde_json::to_string_pretty(&payload_array).unwrap_or_default();
        debug!(
            "🔧 Sending payload to Dirigera hub for device {}: {}",
            device_id, payload_json
        );

        let response = self
            .client
            .patch(format!("{}/devices/{}", self.base_url, device_id))
            .header("Authorization", &format!("Bearer {}", self.access_token))
            .header("Content-Type", "application/json")
            .json(&payload_array)
            .send()
            .await
            .map_err(|e| GatewayError::NetworkError(e.to_string()))?;

        if response.status() == 404 {
            return Err(GatewayError::DeviceNotFound(device_id.clone()));
        }

        // 🔥 CHECK RESPONSE MORE CAREFULLY! <3
        let status = response.status();

        if status == 202 || status == 200 {
            // 202 Accepted or 200 OK means success!
            debug!(
                "✅ Device {} update accepted by Dirigera (HTTP {})",
                device_id, status
            );
        } else if status == 204 {
            // 204 No Content also means success (nothing changed)
            debug!(
                "💫 Device {} already in requested state (HTTP 204)",
                device_id
            );
        } else if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!(
                "❌ Failed to update device {}: HTTP {} - {}",
                device_id, status, error_text
            );
            return Err(GatewayError::InternalError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        } else {
            // Log response body for debugging even on success
            let response_text = response.text().await.unwrap_or_default();
            debug!("📝 Dirigera response for {}: {}", device_id, response_text);
        }

        info!(
            "🔥 Successfully updated device: {} - VIBEC0RE POWER! 🔥",
            device_id
        );
        Ok(())
    }

    async fn health_check(&self) -> Result<GatewayHealth, GatewayError> {
        let start = Instant::now();

        let response = self
            .client
            .get(format!("{}/status", self.base_url))
            .header("Authorization", &format!("Bearer {}", self.access_token))
            .send()
            .await;

        let response_time = start.elapsed().as_millis() as u64;

        match response {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    "🔥 Dirigera hub is ALIVE! Response time: {}ms",
                    response_time
                );
                Ok(GatewayHealth {
                    reachable: true,
                    response_time_ms: response_time,
                    connected_devices: 0, // We'd need to query devices separately
                    last_error: None,
                })
            }
            Ok(resp) => {
                let error_msg = format!("HTTP {}", resp.status());
                warn!("Dirigera hub responded with error: {}", error_msg);
                Ok(GatewayHealth {
                    reachable: false,
                    response_time_ms: response_time,
                    connected_devices: 0,
                    last_error: Some(error_msg),
                })
            }
            Err(e) => {
                error!("Failed to reach Dirigera hub: {}", e);
                Ok(GatewayHealth {
                    reachable: false,
                    response_time_ms: response_time,
                    connected_devices: 0,
                    last_error: Some(e.to_string()),
                })
            }
        }
    }

    async fn event_stream(&self) -> Result<EventStream, GatewayError> {
        info!("🔥 STARTING DIRIGERA WEBSOCKET EVENT STREAM! 🚀");

        // Create WebSocket request with auth header
        let request = tungstenite::http::Request::builder()
            .uri(&self.ws_url)
            .header("Authorization", format!("Bearer {}", self.access_token))
            .header("Sec-WebSocket-Protocol", "v1.user")
            .body(())
            .map_err(|e| {
                GatewayError::InternalError(format!("Failed to build WS request: {}", e))
            })?;

        // Clone values for the spawned task
        let ws_url = self.ws_url.clone();
        let event_sender = self.event_sender.clone();

        // Spawn WebSocket listener task
        tokio::spawn(async move {
            info!("🚀 Connecting to Dirigera WebSocket at: {}", ws_url);

            match connect_async(request).await {
                Ok((ws_stream, _)) => {
                    info!("✅ WebSocket connected to Dirigera hub!");
                    let (_, mut read) = ws_stream.split();

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(tungstenite::Message::Text(text)) => {
                                // 🔥 LOG ALL WEBSOCKET MESSAGES TO SEE WHAT DIRIGERA SENDS! 💖
                                info!("📡 DIRIGERA WEBSOCKET RAW: {}", text);

                                // Parse Dirigera event
                                if let Ok(ws_event) = serde_json::from_str::<DirigeraWsEvent>(&text)
                                {
                                    if let Some(device_id) = ws_event.device_id {
                                        // Convert to our event format
                                        let event_type = match ws_event.event_type.as_str() {
                                            "deviceStateChanged" => {
                                                // 🔥 CHECK FOR BUTTON/SWITCH PRESS EVENTS! 💖
                                                if let Some(data) = &ws_event.data {
                                                    // Check if this is a button press event
                                                    if let Some(is_pressed) = data
                                                        .get("isPressed")
                                                        .and_then(|v| v.as_bool())
                                                    {
                                                        info!("🔘 DIRIGERA BUTTON PRESS DETECTED: Device {} - Pressed: {}", device_id, is_pressed);
                                                    }

                                                    // Check for button specific fields
                                                    if data.get("buttonEvent").is_some()
                                                        || data.get("clickPattern").is_some()
                                                    {
                                                        let button_id = data
                                                            .get("buttonId")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("main")
                                                            .to_string();

                                                        let press_type = if let Some(pattern) = data
                                                            .get("clickPattern")
                                                            .and_then(|v| v.as_str())
                                                        {
                                                            match pattern {
                                                                "singlePress" => {
                                                                    ButtonPressType::SinglePress
                                                                }
                                                                "doublePress" => {
                                                                    ButtonPressType::DoublePress
                                                                }
                                                                "longPress" => {
                                                                    ButtonPressType::LongPress
                                                                }
                                                                _ => ButtonPressType::SinglePress,
                                                            }
                                                        } else {
                                                            ButtonPressType::SinglePress
                                                        };

                                                        info!("🎯 DIRIGERA BUTTON EVENT: Device {} - Button {} - Type: {:?}", 
                                                            device_id, button_id, press_type);

                                                        EventType::ButtonPressed {
                                                            button_id,
                                                            press_type,
                                                        }
                                                    } else {
                                                        // Regular state change
                                                        EventType::AttributeChanged {
                                                            attribute: "state".to_string(),
                                                            old_value: serde_json::Value::Null,
                                                            new_value: data.clone(),
                                                        }
                                                    }
                                                } else {
                                                    EventType::AttributeChanged {
                                                        attribute: "state".to_string(),
                                                        old_value: serde_json::Value::Null,
                                                        new_value: serde_json::Value::Null,
                                                    }
                                                }
                                            }
                                            "deviceDiscovered" | "deviceAdded" => {
                                                EventType::DeviceAdded {
                                                    device_type: "unknown".to_string(),
                                                }
                                            }
                                            "deviceRemoved" => EventType::DeviceRemoved,
                                            "deviceReachabilityChanged" => {
                                                let reachable = ws_event
                                                    .data
                                                    .as_ref()
                                                    .and_then(|d| d.get("isReachable"))
                                                    .and_then(|v| v.as_bool())
                                                    .unwrap_or(true);
                                                EventType::DeviceReachabilityChanged { reachable }
                                            }
                                            "sceneUpdated" | "sceneTriggered" => {
                                                // 🔥 SCENE TRIGGERED - WORKAROUND FOR BUTTON EVENTS! 💖
                                                // Since Dirigera doesn't expose button events, we use scene triggers
                                                let scene_id = ws_event
                                                    .data
                                                    .as_ref()
                                                    .and_then(|d| d.get("sceneId").or(d.get("id")))
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("unknown")
                                                    .to_string();

                                                info!("🎬 SCENE TRIGGERED: {} - This might be from a button press!", scene_id);

                                                EventType::SceneActivated { scene_id }
                                            }
                                            _ => {
                                                // For other events, treat as attribute change
                                                EventType::AttributeChanged {
                                                    attribute: ws_event.event_type.clone(),
                                                    old_value: serde_json::Value::Null,
                                                    new_value: ws_event
                                                        .data
                                                        .clone()
                                                        .unwrap_or(serde_json::Value::Null),
                                                }
                                            }
                                        };

                                        let device_event = DeviceEvent {
                                            timestamp: SystemTime::now(),
                                            device_id,
                                            event_type,
                                        };

                                        debug!("🔧 DIRIGERA EVENT: {:?}", device_event);
                                        let _ = event_sender.send(device_event);
                                    }
                                }
                            }
                            Ok(tungstenite::Message::Close(_)) => {
                                warn!("WebSocket closed by Dirigera hub");
                                break;
                            }
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Failed to connect WebSocket: {}", e);
                }
            }
        });

        // Return a receiver for events
        Ok(self.event_sender.subscribe())
    }
}

// 🔥 VIBEC0RE TESTS — mDNS COLLISION FALLBACK LOGIC, NO HARDWARE NEEDED! 💖
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_for_plain_local_host() {
        let c = DirigeraGateway::host_candidates("gw2-b1223333.local", 5);
        // Configured (== base) host first, then numeric variants in order.
        assert_eq!(c[0], "gw2-b1223333.local");
        assert_eq!(c[1], "gw2-b1223333-2.local");
        assert_eq!(c[2], "gw2-b1223333-3.local");
        assert_eq!(c.last().unwrap(), "gw2-b1223333-5.local");
        // Base name must not be duplicated (configured host == base name here).
        assert_eq!(
            c.iter()
                .filter(|h| h.as_str() == "gw2-b1223333.local")
                .count(),
            1
        );
    }

    #[test]
    fn candidates_when_configured_with_collision_suffix() {
        let c = DirigeraGateway::host_candidates("gw2-b1223333-2.local", 5);
        // Configured host probed first...
        assert_eq!(c[0], "gw2-b1223333-2.local");
        // ...then the canonical base name...
        assert_eq!(c[1], "gw2-b1223333.local");
        // ...and the configured collision host is not enumerated a second time.
        assert_eq!(
            c.iter()
                .filter(|h| h.as_str() == "gw2-b1223333-2.local")
                .count(),
            1
        );
        assert!(c.contains(&"gw2-b1223333-3.local".to_string()));
    }

    #[test]
    fn non_local_host_is_used_as_is() {
        // Raw IPs and regular FQDNs get no collision fallback.
        assert_eq!(
            DirigeraGateway::host_candidates("192.168.1.1", 5),
            vec!["192.168.1.1".to_string()]
        );
        assert_eq!(
            DirigeraGateway::host_candidates("hub.example.com", 5),
            vec!["hub.example.com".to_string()]
        );
    }

    #[test]
    fn hyphenated_base_without_numeric_suffix_is_preserved() {
        // Trailing segment isn't numeric → whole stem is the base.
        let c = DirigeraGateway::host_candidates("gw2-b1223333.local", 3);
        assert_eq!(c[0], "gw2-b1223333.local");
        assert_eq!(c.len(), 3); // base + -2 + -3
    }
}
