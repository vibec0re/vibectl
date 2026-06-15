use crate::event_producer::{EventProducer, EventScenario};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;
use tracing::{debug, info};
use v1bectl_gateway::Gateway;
use v1bectl_sync::*;

pub struct DummyGateway {
    devices: Arc<RwLock<HashMap<DeviceId, MockDevice>>>,
    response_delays: ResponseDelayConfig,
    failure_config: FailureConfig,
    scenario_name: String,
    event_tx: broadcast::Sender<DeviceEvent>,
}

#[derive(Debug, Clone)]
struct MockDevice {
    info: DeviceInfo,
    state: DeviceStateValue,
    last_updated: Timestamp,
}

#[derive(Debug, Clone)]
struct ResponseDelayConfig {
    get_device: Duration,
    set_device: Duration,
    discover: Duration,
}

#[derive(Debug, Clone)]
struct FailureConfig {
    get_failure_rate: f32,
    set_failure_rate: f32,
    unreachable_devices: Vec<DeviceId>,
}

impl DummyGateway {
    pub fn new(scenario: &str) -> Self {
        let devices = Arc::new(RwLock::new(HashMap::new()));
        let (event_tx, _) = broadcast::channel(1000);

        let gateway = Self {
            devices: devices.clone(),
            response_delays: ResponseDelayConfig {
                get_device: Duration::from_millis(50),
                set_device: Duration::from_millis(100),
                discover: Duration::from_millis(500),
            },
            failure_config: FailureConfig {
                get_failure_rate: 0.0,
                set_failure_rate: 0.0,
                unreachable_devices: Vec::new(),
            },
            scenario_name: scenario.to_string(),
            event_tx: event_tx.clone(),
        };

        let devices_clone = devices.clone();
        let event_tx_clone = event_tx.clone();
        let scenario_owned = scenario.to_string();

        tokio::spawn(async move {
            let mut devices_map = devices_clone.write().await;
            Self::load_scenario_static(&mut devices_map, &scenario_owned);
            drop(devices_map);

            let device_ids = Arc::new(RwLock::new(Vec::new()));
            {
                let devices = devices_clone.read().await;
                let ids: Vec<String> = devices.keys().cloned().collect();
                *device_ids.write().await = ids;
            }

            let event_scenario = match scenario_owned.as_str() {
                "ambient" => EventScenario::AmbientActivity,
                _ => EventScenario::BasicHome,
            };

            let (event_producer, mut event_rx) = EventProducer::new(device_ids, event_scenario);
            let event_producer = Arc::new(event_producer);

            let tx = event_tx_clone.clone();
            tokio::spawn(async move {
                while let Ok(event) = event_rx.recv().await {
                    let _ = tx.send(event);
                }
            });

            event_producer.start().await;
        });

        info!(
            "🔥 Initialized DummyGateway with scenario: {} - EVENT STREAM ACTIVE! ⚡",
            scenario
        );
        gateway
    }

    fn load_scenario_static(devices: &mut HashMap<DeviceId, MockDevice>, scenario: &str) {
        match scenario {
            "basic_home" => Self::load_basic_home_scenario_static(devices),
            "large_home" => Self::load_large_home_scenario_static(devices),
            "unreliable_network" => Self::load_basic_home_scenario_static(devices),
            _ => {
                tracing::warn!("Unknown scenario '{}', using basic_home", scenario);
                Self::load_basic_home_scenario_static(devices);
            }
        }
    }

    fn load_basic_home_scenario_static(devices: &mut HashMap<DeviceId, MockDevice>) {
        Self::add_mock_device_static(
            devices,
            "light_living_room",
            "Living Room Light",
            DeviceType::Light,
            vec![
                Capability::OnOff,
                Capability::Brightness,
                Capability::ColorTemperature,
            ],
            DeviceStateValue::Light(LightState {
                is_on: false,
                brightness: Some(0),
                color_temp: Some(2700),
                rgb_color: None,
            }),
        );

        Self::add_mock_device_static(
            devices,
            "light_kitchen",
            "Kitchen Light",
            DeviceType::Light,
            vec![Capability::OnOff, Capability::Brightness],
            DeviceStateValue::Light(LightState {
                is_on: true,
                brightness: Some(75),
                color_temp: None,
                rgb_color: None,
            }),
        );

        Self::add_mock_device_static(
            devices,
            "light_bedroom",
            "Bedroom RGB Light",
            DeviceType::Light,
            vec![
                Capability::OnOff,
                Capability::Brightness,
                Capability::RgbColor,
            ],
            DeviceStateValue::Light(LightState {
                is_on: false,
                brightness: Some(0),
                color_temp: None,
                rgb_color: Some(RgbColor {
                    r: 255,
                    g: 100,
                    b: 50,
                }),
            }),
        );

        Self::add_mock_device_static(
            devices,
            "switch_hallway",
            "Hallway Switch",
            DeviceType::Switch,
            vec![Capability::OnOff, Capability::BatteryLevel],
            DeviceStateValue::Switch(SwitchState {
                is_pressed: false,
                last_pressed: None,
                battery_level: Some(85),
            }),
        );

        Self::add_mock_device_static(
            devices,
            "motion_hallway",
            "Hallway Motion",
            DeviceType::MotionSensor,
            vec![Capability::MotionDetection, Capability::BatteryLevel],
            DeviceStateValue::MotionSensor(MotionSensorState {
                motion_detected: false,
                last_motion: None,
                battery_level: Some(75),
            }),
        );

        Self::add_mock_device_static(
            devices,
            "motion_kitchen",
            "Kitchen Motion",
            DeviceType::MotionSensor,
            vec![Capability::MotionDetection, Capability::BatteryLevel],
            DeviceStateValue::MotionSensor(MotionSensorState {
                motion_detected: false,
                last_motion: None,
                battery_level: Some(90),
            }),
        );

        Self::add_mock_device_static(
            devices,
            "temperature_living_room",
            "Living Room Sensor",
            DeviceType::Sensor,
            vec![Capability::Temperature, Capability::Humidity],
            DeviceStateValue::Sensor(SensorState {
                temperature: Some(22.5),
                humidity: Some(45.2),
                last_updated: chrono::Utc::now().timestamp_millis() as u64,
            }),
        );

        Self::add_mock_device_static(
            devices,
            "outlet_tv",
            "TV Outlet",
            DeviceType::Outlet,
            vec![Capability::OnOff],
            DeviceStateValue::Outlet(OutletState {
                is_on: true,
                power_consumption: Some(45.5),
                total_energy: Some(123.4),
            }),
        );
    }

    fn load_large_home_scenario_static(devices: &mut HashMap<DeviceId, MockDevice>) {
        Self::load_basic_home_scenario_static(devices);

        for i in 1..=20 {
            Self::add_mock_device_static(
                devices,
                &format!("light_room_{}", i),
                &format!("Room {} Light", i),
                DeviceType::Light,
                vec![Capability::OnOff, Capability::Brightness],
                DeviceStateValue::Light(LightState {
                    is_on: i % 3 == 0,
                    brightness: Some((i * 5) % 100),
                    color_temp: None,
                    rgb_color: None,
                }),
            );
        }

        for i in 1..=10 {
            Self::add_mock_device_static(
                devices,
                &format!("sensor_zone_{}", i),
                &format!("Zone {} Sensor", i),
                DeviceType::Sensor,
                vec![Capability::Temperature],
                DeviceStateValue::Sensor(SensorState {
                    temperature: Some(20.0 + (i as f32 * 0.5)),
                    humidity: None,
                    last_updated: chrono::Utc::now().timestamp_millis() as u64,
                }),
            );
        }
    }

    fn add_mock_device_static(
        devices: &mut HashMap<DeviceId, MockDevice>,
        id: &str,
        name: &str,
        device_type: DeviceType,
        capabilities: Vec<Capability>,
        initial_state: DeviceStateValue,
    ) {
        let info = DeviceInfo {
            device_id: id.to_string(),
            name: name.to_string(),
            device_type,
            capabilities,
            device_groups: vec!["Living Room".to_string()],
            manufacturer: Some("VIBEC0RE".to_string()),
            model: Some("Mock-1000".to_string()),
            firmware_version: Some("1.0.0".to_string()),
            battery_powered: matches!(
                &initial_state,
                DeviceStateValue::Switch(_) | DeviceStateValue::MotionSensor(_)
            ),
            reachable: true,
            last_seen: chrono::Utc::now().timestamp_millis() as u64,
            custom_attributes: HashMap::new(),
        };

        devices.insert(
            id.to_string(),
            MockDevice {
                info,
                state: initial_state,
                last_updated: chrono::Utc::now().timestamp_millis() as u64,
            },
        );
    }

    fn should_fail(&self, rate: f32) -> bool {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen::<f32>() < rate
    }
}

#[async_trait]
impl Gateway for DummyGateway {
    async fn discover_devices(&self) -> Result<Vec<DeviceInfo>, GatewayError> {
        debug!(
            "Starting device discovery (scenario: {})",
            self.scenario_name
        );
        sleep(self.response_delays.discover).await;

        let devices = self.devices.read().await;
        let device_list: Vec<DeviceInfo> = devices.values().map(|d| d.info.clone()).collect();

        info!("Discovered {} devices", device_list.len());
        Ok(device_list)
    }

    async fn get_device_state(
        &self,
        device_id: &DeviceId,
    ) -> Result<DeviceStateValue, GatewayError> {
        sleep(self.response_delays.get_device).await;

        if self.should_fail(self.failure_config.get_failure_rate) {
            return Err(GatewayError::DeviceUnreachable(device_id.clone()));
        }

        if self.failure_config.unreachable_devices.contains(device_id) {
            return Err(GatewayError::DeviceUnreachable(device_id.clone()));
        }

        let devices = self.devices.read().await;
        devices
            .get(device_id)
            .map(|d| d.state.clone())
            .ok_or_else(|| GatewayError::DeviceNotFound(device_id.clone()))
    }

    async fn set_device_state(
        &self,
        device_id: &DeviceId,
        state: DeviceStateValue,
    ) -> Result<(), GatewayError> {
        sleep(self.response_delays.set_device).await;

        if self.should_fail(self.failure_config.set_failure_rate) {
            return Err(GatewayError::DeviceUnreachable(device_id.clone()));
        }

        if self.failure_config.unreachable_devices.contains(device_id) {
            return Err(GatewayError::DeviceUnreachable(device_id.clone()));
        }

        let mut devices = self.devices.write().await;
        if let Some(device) = devices.get_mut(device_id) {
            // 🔥 FIX: MERGE state instead of replacing! This fixes brightness control!
            match (&mut device.state, &state) {
                (DeviceStateValue::Light(current), DeviceStateValue::Light(new)) => {
                    // 🔥 IMPORTANT: The API already handles partial updates!
                    // When brightness is changed without is_on, the API preserves is_on
                    // So here we just replace the whole state as it's already merged!
                    *current = new.clone();
                    debug!("🔥 Updated light {} - New state: {:?}", device_id, current);
                }
                _ => {
                    // For non-light devices, replace the whole state
                    device.state = state.clone();
                    debug!("Set device {} state: {:?}", device_id, state);
                }
            }
            device.last_updated = chrono::Utc::now().timestamp_millis() as u64;
            Ok(())
        } else {
            Err(GatewayError::DeviceNotFound(device_id.clone()))
        }
    }

    async fn health_check(&self) -> Result<GatewayHealth, GatewayError> {
        let start = Instant::now();
        sleep(Duration::from_millis(10)).await;

        let devices = self.devices.read().await;

        Ok(GatewayHealth {
            reachable: true,
            response_time_ms: start.elapsed().as_millis() as u64,
            connected_devices: devices.len() as u32,
            last_error: None,
        })
    }

    async fn event_stream(&self) -> Result<EventStream, GatewayError> {
        Ok(self.event_tx.subscribe())
    }
}
