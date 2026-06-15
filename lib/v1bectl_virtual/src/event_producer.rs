use rand::Rng;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;
use v1bectl_sync::{ButtonPressType, DeviceEvent, EventType};

pub struct EventProducer {
    devices: Arc<RwLock<Vec<String>>>,
    event_tx: broadcast::Sender<DeviceEvent>,
    scenario: EventScenario,
}

#[derive(Debug, Clone)]
pub enum EventScenario {
    BasicHome,
    AmbientActivity,
}

impl EventProducer {
    pub fn new(
        devices: Arc<RwLock<Vec<String>>>,
        scenario: EventScenario,
    ) -> (Self, broadcast::Receiver<DeviceEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1000);

        (
            Self {
                devices,
                event_tx,
                scenario,
            },
            event_rx,
        )
    }

    pub async fn start(self: Arc<Self>) {
        let scenario = self.scenario.clone();
        match scenario {
            EventScenario::BasicHome => {
                self.run_basic_home().await;
            }
            EventScenario::AmbientActivity => {
                self.run_ambient_activity().await;
            }
        }
    }

    async fn run_basic_home(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(500));
            use rand::SeedableRng;
            let mut rng = rand::rngs::StdRng::from_entropy();

            loop {
                interval.tick().await;

                let devices = self.devices.read().await;
                if devices.is_empty() {
                    continue;
                }

                if rng.gen_bool(0.1) {
                    let device_idx = rng.gen_range(0..devices.len());
                    let device_id = devices[device_idx].clone();

                    let event = match rng.gen_range(0..6) {
                        0 => DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id: device_id.clone(),
                            event_type: EventType::AttributeChanged {
                                attribute: "on".to_string(),
                                old_value: json!(rng.gen_bool(0.5)),
                                new_value: json!(!rng.gen_bool(0.5)),
                            },
                        },
                        1 => {
                            let old_brightness = rng.gen_range(0..=100);
                            let new_brightness = rng.gen_range(0..=100);
                            DeviceEvent {
                                timestamp: SystemTime::now(),
                                device_id: device_id.clone(),
                                event_type: EventType::AttributeChanged {
                                    attribute: "brightness".to_string(),
                                    old_value: json!(old_brightness),
                                    new_value: json!(new_brightness),
                                },
                            }
                        }
                        2 if device_id.contains("switch") => DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id: device_id.clone(),
                            event_type: EventType::ButtonPressed {
                                button_id: "main".to_string(),
                                press_type: match rng.gen_range(0..3) {
                                    0 => ButtonPressType::SinglePress,
                                    1 => ButtonPressType::DoublePress,
                                    _ => ButtonPressType::LongPress,
                                },
                            },
                        },
                        3 if device_id.contains("motion") => DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id: device_id.clone(),
                            event_type: EventType::AttributeChanged {
                                attribute: "is_detected".to_string(),
                                old_value: json!(false),
                                new_value: json!(true),
                            },
                        },
                        4 if device_id.contains("temperature") => {
                            let old_temp = 20.0 + rng.gen::<f32>() * 5.0;
                            let new_temp = 20.0 + rng.gen::<f32>() * 5.0;
                            DeviceEvent {
                                timestamp: SystemTime::now(),
                                device_id: device_id.clone(),
                                event_type: EventType::AttributeChanged {
                                    attribute: "temperature".to_string(),
                                    old_value: json!(old_temp),
                                    new_value: json!(new_temp),
                                },
                            }
                        }
                        _ => DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id: device_id.clone(),
                            event_type: EventType::DeviceReachabilityChanged {
                                reachable: rng.gen_bool(0.95),
                            },
                        },
                    };

                    let _ = self.event_tx.send(event);
                }
            }
        });
    }

    async fn run_ambient_activity(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            use rand::SeedableRng;
            let mut rng = rand::rngs::StdRng::from_entropy();

            loop {
                interval.tick().await;

                let devices = self.devices.read().await;
                if devices.is_empty() {
                    continue;
                }

                if rng.gen_bool(0.3) {
                    let device_idx = rng.gen_range(0..devices.len());
                    let device_id = devices[device_idx].clone();

                    let event = if device_id.contains("light") {
                        if rng.gen_bool(0.5) {
                            DeviceEvent {
                                timestamp: SystemTime::now(),
                                device_id,
                                event_type: EventType::AttributeChanged {
                                    attribute: "on".to_string(),
                                    old_value: json!(rng.gen_bool(0.3)),
                                    new_value: json!(rng.gen_bool(0.7)),
                                },
                            }
                        } else {
                            let old_brightness = rng.gen_range(20..=100);
                            let new_brightness = rng.gen_range(20..=100);
                            DeviceEvent {
                                timestamp: SystemTime::now(),
                                device_id,
                                event_type: EventType::AttributeChanged {
                                    attribute: "brightness".to_string(),
                                    old_value: json!(old_brightness),
                                    new_value: json!(new_brightness),
                                },
                            }
                        }
                    } else if device_id.contains("motion") {
                        DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id,
                            event_type: EventType::AttributeChanged {
                                attribute: "is_detected".to_string(),
                                old_value: json!(false),
                                new_value: json!(rng.gen_bool(0.2)),
                            },
                        }
                    } else if device_id.contains("switch") {
                        DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id,
                            event_type: EventType::ButtonPressed {
                                button_id: "main".to_string(),
                                press_type: ButtonPressType::SinglePress,
                            },
                        }
                    } else if device_id.contains("temperature") {
                        let old_temp = 18.0 + rng.gen::<f32>() * 8.0;
                        let new_temp = 18.0 + rng.gen::<f32>() * 8.0;
                        DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id,
                            event_type: EventType::AttributeChanged {
                                attribute: "temperature".to_string(),
                                old_value: json!(old_temp),
                                new_value: json!(new_temp),
                            },
                        }
                    } else if device_id.contains("outlet") {
                        DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id,
                            event_type: EventType::AttributeChanged {
                                attribute: "on".to_string(),
                                old_value: json!(rng.gen_bool(0.2)),
                                new_value: json!(rng.gen_bool(0.8)),
                            },
                        }
                    } else {
                        continue;
                    };

                    let _ = self.event_tx.send(event);
                }

                if rng.gen_bool(0.01) {
                    let device_idx = rng.gen_range(0..devices.len());
                    let device_id = devices[device_idx].clone();

                    if device_id.contains("switch") || device_id.contains("motion") {
                        let old_level = rng.gen_range(50..=100);
                        let new_level = old_level - rng.gen_range(1..=5);
                        let event = DeviceEvent {
                            timestamp: SystemTime::now(),
                            device_id,
                            event_type: EventType::BatteryLevelChanged {
                                old_level,
                                new_level,
                            },
                        };
                        let _ = self.event_tx.send(event);
                    }
                }
            }
        });
    }
}
