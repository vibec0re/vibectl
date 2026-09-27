use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest, ClientRequestBuilder};
use tokio_tungstenite::Connector;
use tracing::{debug, error, info, warn};
use v1bectl_sync::{
    ButtonPressType, Capability, DeviceEvent, DeviceId, DeviceInfo, DeviceStateValue, DeviceType,
    EventStream, EventType, Gateway, GatewayError, GatewayHealth, LightState, SensorState,
    SwitchState,
};

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
    /// # Panics
    ///
    /// Panics if the underlying `reqwest` HTTP client fails to build (e.g. the
    /// TLS backend can't initialize) — this is an environment failure, not a
    /// runtime condition callers are expected to recover from.
    pub fn new(host: &str, access_token: &str, timeout: Duration) -> Self {
        let base_url = format!("https://{host}:8443/v1");
        let ws_url = format!("wss://{host}:8443/v1");
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
    /// 1. `V1BECTL_ACCESS_TOKEN` environment variable (for NixOS/systemd)
    /// 2. `$HOME/.local/state/v1bectl/access.token` file
    ///
    /// 🔁 The configured host is resolved against mDNS collision variants
    /// (see `resolve_reachable_host`, private to this module) before the
    /// gateway is built, so a hub that re-advertised itself as
    /// `gw2-xxxx-2.local` is still found.
    ///
    /// # Errors
    ///
    /// Returns an error if no access token can be found in the environment
    /// variable or the state file.
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
                    "Failed to read access token from {token_path}: {e}"
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

        push_unique(&mut candidates, format!("{base}.local"));
        for n in 2..=max_suffix {
            push_unique(&mut candidates, format!("{base}-{n}.local"));
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
            let url = format!("https://{candidate}:8443/v1/status");
            match client
                .get(&url)
                .header("Authorization", format!("Bearer {access_token}"))
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

    fn convert_dirigera_device(device: DirigeraDevice) -> DeviceInfo {
        let device_type = match device.device_type.as_str() {
            "light" | "outlet" => DeviceType::Light, // Outlets are controllable like lights but simpler
            "blinds" | "controller" => DeviceType::Switch, // Map blinds to switch for now
            "sensor" => DeviceType::Sensor,
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
                        if device.device_type == "outlet" {
                            warn!(
                                "🍺 Ignoring bullshit lightLevel for outlet {} - IKEA gonks drunk!",
                                device.id
                            );
                        } else {
                            cap_list.push(Capability::Brightness);
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
                "sensor" => vec![Capability::Temperature, Capability::Humidity],
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

        DeviceInfo {
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
            last_seen: chrono::Utc::now().timestamp_millis().cast_unsigned(),
            custom_attributes: HashMap::new(),
        }
    }

    fn convert_dirigera_state(device: &DirigeraDevice) -> DeviceStateValue {
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
                last_updated: chrono::Utc::now().timestamp_millis().cast_unsigned(),
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
#[expect(
    clippy::too_many_lines,
    reason = "async-trait's macro expansion collapses each long async fn's too_many_lines diagnostic onto this attribute; splitting the trait methods is a real refactor, not this gate PR's job"
)]
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
                "HTTP {status}: {error_text}"
            )));
        }

        let devices: Vec<DirigeraDevice> = response.json().await.map_err(|e| {
            GatewayError::InternalError(format!("Failed to parse Dirigera response: {e}"))
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
            let device_info = Self::convert_dirigera_device(device);
            debug!(
                "✅ Converted device: {} ({:?})",
                device_info.name, device_info.device_type
            );
            device_infos.push(device_info);
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
                "HTTP {status}: {error_text}"
            )));
        }

        let device: DirigeraDevice = response.json().await.map_err(|e| {
            GatewayError::InternalError(format!("Failed to parse device response: {e}"))
        })?;

        let state = Self::convert_dirigera_state(&device);
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
            GatewayError::InternalError(format!("Failed to parse device response: {e}"))
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
                "HTTP {status}: {error_text}"
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

        let response_time = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

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

    /// Stream live events from the hub's `wss://{host}:8443/v1` endpoint. 🔥
    ///
    /// # TLS trust 🔓
    ///
    /// The Dirigera hub serves a **self-signed** certificate, so the event
    /// stream makes the same trust decision as the REST client built in
    /// [`DirigeraGateway::new`]: `danger_accept_invalid_certs(true)`. The link
    /// is still encrypted, but the hub's identity is not verified, so anything
    /// that can impersonate the hub on the local network can feed us events.
    /// That matches the project's "local network only" stance; pinning the
    /// hub's certificate fingerprint on first use would be a later hardening.
    ///
    /// # Reconnects 🔁
    ///
    /// A background task keeps the stream alive: after a failed connect (or a
    /// connect/handshake that hangs for more than 10 s), a close from the hub,
    /// or a read error, it reconnects with capped exponential backoff — 1 s,
    /// doubling up to 60 s, and back to 1 s once a connection has stayed up for
    /// 30 s. It runs for the lifetime of the process, and stops only once every
    /// receiver of the returned stream has been dropped. Events the hub sends
    /// while we're disconnected are lost: the sync engine's periodic pull
    /// catches up on device state, but button presses in that gap are not
    /// replayed.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError::InternalError`] if the WebSocket request (host
    /// or token not valid in a URI / header) or the TLS connector can't be
    /// built. Connection problems are not errors here; they are retried in the
    /// background.
    async fn event_stream(&self) -> Result<EventStream, GatewayError> {
        info!("🔥 STARTING DIRIGERA WEBSOCKET EVENT STREAM! 🚀");

        let request = dirigera_ws_request(&self.ws_url, &self.access_token)?;
        let connector = dirigera_ws_connector().map_err(|e| {
            GatewayError::InternalError(format!("Failed to build WS TLS connector: {e}"))
        })?;

        // Subscribe *before* spawning, so the listener can never see zero
        // receivers on its first check and mistake a fresh stream for an
        // abandoned one.
        let events = self.event_sender.subscribe();

        tokio::spawn(run_dirigera_ws(
            self.ws_url.clone(),
            request,
            connector,
            ReconnectPolicy::DIRIGERA,
            self.event_sender.clone(),
        ));

        Ok(events)
    }
}

// 🔥 DIRIGERA WEBSOCKET EVENT STREAM — TLS, RECONNECT LOOP, FRAME PARSING 💖

/// How the event-stream listener paces its reconnects. 🔁
#[derive(Debug, Clone, Copy)]
struct ReconnectPolicy {
    /// Delay before the first retry; doubles after every failed or
    /// short-lived connection.
    base_delay: Duration,
    /// Ceiling for the doubling.
    max_delay: Duration,
    /// A connection that stayed up at least this long resets the delay to
    /// `base_delay`.
    stable_after: Duration,
    /// Upper bound for TCP connect + TLS + WebSocket handshake, so a hub that
    /// accepts the socket but never answers can't stall the loop forever.
    connect_timeout: Duration,
}

impl ReconnectPolicy {
    /// Production pacing: 1 s doubling to 60 s, reset after 30 s of uptime.
    const DIRIGERA: Self = Self {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_mins(1),
        stable_after: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(10),
    };

    /// The delay to use after waiting `delay` once more without success.
    fn next_delay(self, delay: Duration) -> Duration {
        delay.saturating_mul(2).min(self.max_delay)
    }

    /// The delay to carry on with after a connection that was up for `uptime`.
    fn delay_after_session(self, delay: Duration, uptime: Duration) -> Duration {
        if uptime >= self.stable_after {
            self.base_delay
        } else {
            delay
        }
    }
}

/// Why a connected event-stream session ended.
enum WsSessionEnd {
    /// The hub sent a Close frame, or the stream ended.
    Closed,
    /// Reading from the socket failed.
    Failed(tungstenite::Error),
    /// Every receiver of the event channel is gone; nobody wants events.
    NoReceivers,
}

type DirigeraWsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The handshake request for the hub's event stream. 🔧
///
/// A [`ClientRequestBuilder`] rather than a finished request: every connect
/// turns it into a fresh request, so each attempt gets its own
/// `Sec-WebSocket-Key` plus the `Host` / `Upgrade` / `Connection` /
/// `Sec-WebSocket-Version` headers that tungstenite's handshake insists on.
///
/// Validated once up front, so a malformed host or token makes
/// `event_stream` fail instead of retrying in the background forever.
fn dirigera_ws_request(
    ws_url: &str,
    access_token: &str,
) -> Result<ClientRequestBuilder, GatewayError> {
    let uri: tungstenite::http::Uri = ws_url
        .parse()
        .map_err(|e| GatewayError::InternalError(format!("Failed to build WS request: {e}")))?;
    let request = ClientRequestBuilder::new(uri)
        .with_header("Authorization", format!("Bearer {access_token}"))
        .with_sub_protocol("v1.user");
    request
        .clone()
        .into_client_request()
        .map_err(|e| GatewayError::InternalError(format!("Failed to build WS request: {e}")))?;
    Ok(request)
}

/// TLS for the event stream: the same trust decision as the REST client. 🔓
///
/// The hub's certificate is self-signed, so certificate (and with it,
/// hostname) verification is off, exactly like `danger_accept_invalid_certs`
/// on the `reqwest` clients.
fn dirigera_ws_connector() -> Result<Connector, native_tls::Error> {
    let tls = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true) // Dirigera uses self-signed certs
        .build()?;
    Ok(Connector::NativeTls(tls))
}

/// One connect attempt: TCP, TLS through `connector`, WebSocket handshake.
///
/// The error is boxed: `tungstenite::Error` is 136 bytes, which trips
/// `clippy::result_large_err` on newer toolchains (CI runs latest stable).
async fn connect_dirigera_ws(
    request: &ClientRequestBuilder,
    connector: &Connector,
    connect_timeout: Duration,
) -> Result<DirigeraWsStream, Box<tungstenite::Error>> {
    let handshake = tokio_tungstenite::connect_async_tls_with_config(
        request.clone(),
        None,
        false,
        Some(connector.clone()),
    );
    let (ws_stream, _response) = tokio::time::timeout(connect_timeout, handshake)
        .await
        .map_err(|_| {
            tungstenite::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("no WebSocket handshake within {connect_timeout:?}"),
            ))
        })??;
    Ok(ws_stream)
}

/// Keep the hub's event stream connected until nobody is listening. 🔁
///
/// Connect, pump frames into `event_sender`, and on any failure or close
/// wait out the backoff from `policy` and go again.
async fn run_dirigera_ws(
    ws_url: String,
    request: ClientRequestBuilder,
    connector: Connector,
    policy: ReconnectPolicy,
    event_sender: broadcast::Sender<DeviceEvent>,
) {
    let mut delay = policy.base_delay;

    loop {
        if event_sender.receiver_count() == 0 {
            info!("🛑 Nobody is listening to Dirigera events anymore - stopping the WebSocket listener");
            return;
        }

        info!("🚀 Connecting to Dirigera WebSocket at: {ws_url}");
        match connect_dirigera_ws(&request, &connector, policy.connect_timeout).await {
            Ok(ws_stream) => {
                info!("✅ WebSocket connected to Dirigera hub!");
                let connected_at = Instant::now();

                match pump_dirigera_ws(ws_stream, &event_sender).await {
                    WsSessionEnd::Closed => warn!("💔 WebSocket closed by Dirigera hub"),
                    WsSessionEnd::Failed(e) => error!("❌ WebSocket error: {e}"),
                    WsSessionEnd::NoReceivers => {
                        info!("🛑 Nobody is listening to Dirigera events anymore - stopping the WebSocket listener");
                        return;
                    }
                }

                delay = policy.delay_after_session(delay, connected_at.elapsed());
            }
            Err(e) => error!("❌ Failed to connect WebSocket: {e}"),
        }

        info!("⏰ Retrying Dirigera WebSocket in {delay:?}");
        tokio::time::sleep(delay).await;
        delay = policy.next_delay(delay);
    }
}

/// Read one connected session until it closes, fails, or loses its audience.
async fn pump_dirigera_ws(
    mut ws_stream: DirigeraWsStream,
    event_sender: &broadcast::Sender<DeviceEvent>,
) -> WsSessionEnd {
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(tungstenite::Message::Text(text)) => {
                // 🔥 LOG ALL WEBSOCKET MESSAGES TO SEE WHAT DIRIGERA SENDS! 💖
                info!("📡 DIRIGERA WEBSOCKET RAW: {}", text);

                if let Some(device_event) = parse_dirigera_ws_event(&text) {
                    debug!("🔧 DIRIGERA EVENT: {:?}", device_event);
                    if event_sender.send(device_event).is_err() {
                        return WsSessionEnd::NoReceivers;
                    }
                }
            }
            Ok(tungstenite::Message::Close(_)) => return WsSessionEnd::Closed,
            Err(e) => return WsSessionEnd::Failed(e),
            Ok(_) => {}
        }
    }
    WsSessionEnd::Closed
}

/// Turn one Dirigera WebSocket text frame into a [`DeviceEvent`].
///
/// Frames that don't parse, or that carry no `deviceId`, yield `None`.
fn parse_dirigera_ws_event(text: &str) -> Option<DeviceEvent> {
    // Parse Dirigera event
    let ws_event = serde_json::from_str::<DirigeraWsEvent>(text).ok()?;
    let device_id = ws_event.device_id?;

    // Convert to our event format
    let event_type = match ws_event.event_type.as_str() {
        "deviceStateChanged" => device_state_changed_event_type(&device_id, ws_event.data.as_ref()),
        "deviceDiscovered" | "deviceAdded" => EventType::DeviceAdded {
            device_type: "unknown".to_string(),
        },
        "deviceRemoved" => EventType::DeviceRemoved,
        "deviceReachabilityChanged" => {
            let reachable = ws_event
                .data
                .as_ref()
                .and_then(|d| d.get("isReachable"))
                .and_then(serde_json::Value::as_bool)
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

            info!(
                "🎬 SCENE TRIGGERED: {} - This might be from a button press!",
                scene_id
            );

            EventType::SceneActivated { scene_id }
        }
        _ => {
            // For other events, treat as attribute change
            EventType::AttributeChanged {
                attribute: ws_event.event_type.clone(),
                old_value: serde_json::Value::Null,
                new_value: ws_event.data.clone().unwrap_or(serde_json::Value::Null),
            }
        }
    };

    Some(DeviceEvent {
        timestamp: SystemTime::now(),
        device_id,
        event_type,
    })
}

/// The [`EventType`] for a `deviceStateChanged` frame: a button press when the
/// payload says so, otherwise a plain state change.
fn device_state_changed_event_type(device_id: &str, data: Option<&serde_json::Value>) -> EventType {
    // 🔥 CHECK FOR BUTTON/SWITCH PRESS EVENTS! 💖
    if let Some(data) = data {
        // Check if this is a button press event
        if let Some(is_pressed) = data.get("isPressed").and_then(serde_json::Value::as_bool) {
            info!(
                "🔘 DIRIGERA BUTTON PRESS DETECTED: Device {} - Pressed: {}",
                device_id, is_pressed
            );
        }

        // Check for button specific fields
        if data.get("buttonEvent").is_some() || data.get("clickPattern").is_some() {
            let button_id = data
                .get("buttonId")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();

            let press_type =
                if let Some(pattern) = data.get("clickPattern").and_then(|v| v.as_str()) {
                    match pattern {
                        "doublePress" => ButtonPressType::DoublePress,
                        "longPress" => ButtonPressType::LongPress,
                        _ => ButtonPressType::SinglePress, // "singlePress" and anything unknown
                    }
                } else {
                    ButtonPressType::SinglePress
                };

            info!(
                "🎯 DIRIGERA BUTTON EVENT: Device {} - Button {} - Type: {:?}",
                device_id, button_id, press_type
            );

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

// 🔥 VIBEC0RE TESTS — DIRIGERA EVENT STREAM VS A FAKE SELF-SIGNED HUB 💖
//
// The fake hub is a blocking std TLS WebSocket server on 127.0.0.1 with a
// checked-in, TEST-ONLY self-signed `localhost` certificate, generated with:
//
//   openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 36500 \
//     -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
//     -keyout selfsigned-localhost.key.pem -out selfsigned-localhost.crt.pem
#[cfg(test)]
mod event_stream_tests {
    use super::*;
    use std::net::{SocketAddr, TcpListener};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tungstenite::handshake::server::{ErrorResponse, Request, Response};
    use tungstenite::http::{HeaderValue, StatusCode};

    const CERT_PEM: &[u8] = include_bytes!("../tests/fixtures/selfsigned-localhost.crt.pem");
    const KEY_PEM: &[u8] = include_bytes!("../tests/fixtures/selfsigned-localhost.key.pem");
    const TEST_TOKEN: &str = "test-token";

    /// Millisecond backoff so reconnect tests finish fast.
    const FAST: ReconnectPolicy = ReconnectPolicy {
        base_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(80),
        stable_after: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(2),
    };
    /// Generous upper bound for anything the tests wait on.
    const WAIT: Duration = Duration::from_secs(10);

    /// What the fake hub does with one accepted WebSocket session.
    enum Session {
        /// Send the frames, then close the connection from the hub's side.
        SendThenClose(Vec<String>),
        /// Send the frames, then keep the connection open until the client leaves.
        SendThenHold(Vec<String>),
    }

    struct FakeHub {
        addr: SocketAddr,
        /// Sessions that completed their TLS + WebSocket handshake.
        served: Arc<AtomicUsize>,
        /// Handshake failures the hub saw, so a RED test says *why*.
        errors: Arc<Mutex<Vec<String>>>,
    }

    impl FakeHub {
        fn url(&self) -> String {
            format!("wss://{}/v1", self.addr)
        }

        fn errors(&self) -> Vec<String> {
            self.errors.lock().unwrap().clone()
        }
    }

    fn bind_localhost() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").expect("bind fake hub")
    }

    /// Serve `sessions` in order on `listener`, one connection each. Failed
    /// handshakes are recorded and don't use up a session.
    fn spawn_fake_hub(listener: TcpListener, sessions: Vec<Session>) -> FakeHub {
        let addr = listener.local_addr().unwrap();
        let identity =
            native_tls::Identity::from_pkcs8(CERT_PEM, KEY_PEM).expect("fixture identity");
        let acceptor = native_tls::TlsAcceptor::new(identity).expect("TLS acceptor");
        let served = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(Mutex::new(Vec::new()));

        let (served_in, errors_in) = (Arc::clone(&served), Arc::clone(&errors));
        std::thread::spawn(move || {
            let mut sessions = sessions.into_iter().peekable();
            for tcp in listener.incoming() {
                let Some(session) = sessions.peek() else {
                    break;
                };
                let result = tcp
                    .map_err(|e| format!("accept: {e}"))
                    .and_then(|tcp| serve_session(&acceptor, tcp, session, &served_in));
                match result {
                    Ok(()) => {
                        sessions.next();
                        if sessions.peek().is_none() {
                            break; // drop the listener: later connects are refused
                        }
                    }
                    Err(e) => errors_in.lock().unwrap().push(e),
                }
            }
        });

        FakeHub {
            addr,
            served,
            errors,
        }
    }

    fn serve_session(
        acceptor: &native_tls::TlsAcceptor,
        tcp: std::net::TcpStream,
        session: &Session,
        served: &AtomicUsize,
    ) -> Result<(), String> {
        let tls = acceptor
            .accept(tcp)
            .map_err(|e| format!("TLS handshake: {e}"))?;
        let mut ws = tungstenite::accept_hdr(tls, check_dirigera_handshake)
            .map_err(|e| format!("WebSocket handshake: {e}"))?;
        served.fetch_add(1, Ordering::SeqCst);

        let (frames, close) = match session {
            Session::SendThenClose(frames) => (frames, true),
            Session::SendThenHold(frames) => (frames, false),
        };
        for frame in frames {
            ws.send(tungstenite::Message::Text(frame.clone()))
                .map_err(|e| format!("send: {e}"))?;
        }
        if close {
            let _ = ws.close(None);
        }
        // Drain until the client answers the close or goes away.
        while ws.read().is_ok() {}
        Ok(())
    }

    /// Accept only what the hub accepts: our bearer token and the `v1.user`
    /// subprotocol, which a compliant server must echo back.
    #[expect(
        clippy::result_large_err,
        reason = "signature fixed by tungstenite's server handshake `Callback`"
    )]
    fn check_dirigera_handshake(
        request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
        };
        let expected_auth = format!("Bearer {TEST_TOKEN}");
        if header("Authorization") != Some(expected_auth.as_str())
            || header("Sec-WebSocket-Protocol") != Some("v1.user")
        {
            let mut reject = ErrorResponse::new(Some("bad token or subprotocol".into()));
            *reject.status_mut() = StatusCode::UNAUTHORIZED;
            return Err(reject);
        }
        response.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("v1.user"),
        );
        Ok(response)
    }

    /// A `deviceStateChanged` frame in the shape `parse_dirigera_ws_event` reads.
    fn state_changed_frame(device_id: &str, light_level: u8) -> String {
        serde_json::json!({
            "id": format!("evt-{device_id}-{light_level}"),
            "time": "2026-09-27T12:00:00.000Z",
            "specversion": "1.1.0",
            "source": "urn:com:ikea:homesmart:iotc:zigbee",
            "type": "deviceStateChanged",
            "deviceId": device_id,
            "data": state_data(device_id, light_level),
        })
        .to_string()
    }

    fn state_data(device_id: &str, light_level: u8) -> serde_json::Value {
        serde_json::json!({
            "id": device_id,
            "attributes": { "isOn": true, "lightLevel": light_level },
        })
    }

    /// Run the real reconnect loop against `url` with the `FAST` policy.
    fn start_listener(url: String) -> (EventStream, tokio::task::JoinHandle<()>) {
        let request = dirigera_ws_request(&url, TEST_TOKEN).expect("WS request");
        let connector = dirigera_ws_connector().expect("TLS connector");
        let (event_sender, events) = broadcast::channel(16);
        let task = tokio::spawn(run_dirigera_ws(url, request, connector, FAST, event_sender));
        (events, task)
    }

    async fn next_event(events: &mut EventStream, hub: &FakeHub) -> DeviceEvent {
        match tokio::time::timeout(WAIT, events.recv()).await {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => panic!("event channel failed: {e}"),
            Err(elapsed) => panic!(
                "no DeviceEvent ({elapsed} after {WAIT:?}); fake hub saw: {:?}",
                hub.errors()
            ),
        }
    }

    // (a) The self-signed cert is accepted and a frame becomes a DeviceEvent.
    #[tokio::test]
    async fn event_stream_trusts_the_hubs_self_signed_cert() {
        let hub = spawn_fake_hub(
            bind_localhost(),
            vec![Session::SendThenHold(vec![state_changed_frame(
                "light_1", 42,
            )])],
        );

        let request = dirigera_ws_request(&hub.url(), TEST_TOKEN).expect("WS request");
        let connector = dirigera_ws_connector().expect("TLS connector");
        let ws_stream = connect_dirigera_ws(&request, &connector, FAST.connect_timeout)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "handshake with the self-signed fake hub failed: {e}; hub saw: {:?}",
                    hub.errors()
                )
            });

        let (event_sender, mut events) = broadcast::channel(16);
        tokio::spawn(async move { pump_dirigera_ws(ws_stream, &event_sender).await });

        let event = next_event(&mut events, &hub).await;
        assert_eq!(event.device_id, "light_1");
        assert_eq!(
            event.event_type,
            EventType::AttributeChanged {
                attribute: "state".to_string(),
                old_value: serde_json::Value::Null,
                new_value: state_data("light_1", 42),
            }
        );
    }

    // (b) The hub closes the stream; the listener reconnects for the next frame.
    #[tokio::test]
    async fn event_stream_reconnects_after_the_hub_closes_it() {
        let hub = spawn_fake_hub(
            bind_localhost(),
            vec![
                Session::SendThenClose(vec![state_changed_frame("light_1", 10)]),
                Session::SendThenHold(vec![state_changed_frame("light_2", 20)]),
            ],
        );
        let (mut events, _task) = start_listener(hub.url());

        assert_eq!(next_event(&mut events, &hub).await.device_id, "light_1");
        assert_eq!(next_event(&mut events, &hub).await.device_id, "light_2");
        assert_eq!(hub.served.load(Ordering::SeqCst), 2);
    }

    // (c) Nothing listens yet, so connects are refused; once the hub is up, it connects.
    #[tokio::test]
    async fn event_stream_retries_until_the_hub_is_up() {
        // Reserve a free port, then release it so nothing is listening there.
        let addr = bind_localhost().local_addr().unwrap();
        let (mut events, _task) = start_listener(format!("wss://{addr}/v1"));

        // Refused for sure right now; this await also lets the listener
        // make its first (refused) attempts.
        assert!(tokio::net::TcpStream::connect(addr).await.is_err());
        tokio::time::sleep(FAST.base_delay * 8).await;

        let listener = TcpListener::bind(addr).expect("re-bind the reserved port");
        let hub = spawn_fake_hub(
            listener,
            vec![Session::SendThenHold(vec![state_changed_frame(
                "light_1", 42,
            )])],
        );

        assert_eq!(next_event(&mut events, &hub).await.device_id, "light_1");
    }

    // Once every receiver is gone, the listener stops instead of reconnecting.
    #[tokio::test]
    async fn event_stream_stops_once_every_receiver_is_gone() {
        let hub = spawn_fake_hub(
            bind_localhost(),
            vec![
                Session::SendThenClose(vec![state_changed_frame("light_1", 10)]),
                Session::SendThenHold(vec![state_changed_frame("light_2", 20)]),
            ],
        );
        let (mut events, task) = start_listener(hub.url());

        assert_eq!(next_event(&mut events, &hub).await.device_id, "light_1");
        drop(events);

        tokio::time::timeout(WAIT, task)
            .await
            .expect("listener should stop without receivers")
            .expect("listener task should not panic");
        assert_eq!(hub.served.load(Ordering::SeqCst), 1, "must not reconnect");
    }

    #[test]
    fn reconnect_backoff_doubles_from_1s_to_a_60s_cap() {
        let policy = ReconnectPolicy::DIRIGERA;
        let mut delay = policy.base_delay;
        let mut seen = Vec::new();
        for _ in 0..8 {
            seen.push(delay.as_secs());
            delay = policy.next_delay(delay);
        }
        assert_eq!(seen, [1, 2, 4, 8, 16, 32, 60, 60]);
    }

    #[test]
    fn reconnect_backoff_resets_only_after_a_stable_connection() {
        let policy = ReconnectPolicy::DIRIGERA;
        let backed_off = Duration::from_secs(32);
        assert_eq!(
            policy.delay_after_session(backed_off, Duration::from_secs(5)),
            backed_off
        );
        assert_eq!(
            policy.delay_after_session(backed_off, Duration::from_secs(30)),
            policy.base_delay
        );
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
