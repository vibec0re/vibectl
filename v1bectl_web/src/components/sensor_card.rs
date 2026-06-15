// 🔥 SENSOR CARD COMPONENT - VIBEC0RE SENSOR WIDGETS! 🔥

use crate::context::use_favorites_context;
use crate::websocket_reconnect::{ApiRequest, DeviceState}; // 🔥 USE RECONNECTING MODULE! 💖
use gloo_timers::callback::Interval;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SensorCardProps {
    pub device: DeviceState,
    pub on_request: Callback<ApiRequest>,
}

#[function_component(SensorCard)]
pub fn sensor_card(props: &SensorCardProps) -> Html {
    let device = &props.device.device_info;
    let _state_value = &props.device.state; // Will use once sensor data comes through
    let favorites = use_favorites_context();
    let is_fav = favorites.favorites.is_favorite(&device.device_id);

    // Set up 5s timer for periodic refresh
    {
        let device_id = device.device_id.clone();
        let on_request = props.on_request.clone();

        use_effect_with(device_id.clone(), move |_| {
            log::info!("🔥 Setting up 5s timer for sensor: {}", device_id);

            let interval = Interval::new(5000, move || {
                log::debug!("⏰ Auto-refreshing sensor: {}", device_id);
                on_request.emit(ApiRequest::GetDeviceState {
                    device_id: device_id.clone(),
                });
            });

            // Return cleanup function
            move || {
                drop(interval);
            }
        });
    }

    // Check capabilities
    let has_temp = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::Temperature);
    let has_humidity = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::Humidity);
    let has_motion = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::Motion);
    let has_contact = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::ContactSensor);
    let has_battery = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::BatteryLevel);

    let room = device
        .device_groups
        .first()
        .cloned()
        .unwrap_or_else(|| "No Room".to_string());
    let card_border = if device.reachable {
        "border-success"
    } else {
        "border-danger"
    };

    // 🔥 GET REAL SENSOR STATE FROM DEVICE! NO MORE MOCK DATA! CHOOOM FIX! 💖
    let (temperature, humidity, battery) = match &props.device.state {
        crate::websocket_reconnect::DeviceStateValue::Sensor(sensor_state) => {
            (sensor_state.temperature, sensor_state.humidity, None)
        }
        crate::websocket_reconnect::DeviceStateValue::Switch(switch_state) => {
            (None, None, switch_state.battery_level)
        }
        _ => (None, None, None),
    };

    html! {
        <div class="col-md-6 col-lg-4 mb-4">
            <div class={format!("card bg-dark text-white h-100 {}", card_border)}>
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start mb-3">
                        <div>
                            <h5 class="card-title mb-1">
                                {"📡 "}{&device.name}
                                <button
                                    class={format!("btn btn-sm ms-2 {}", if is_fav { "btn-warning" } else { "btn-outline-warning" })}
                                    onclick={
                                        let device_id = device.device_id.clone();
                                        let toggle = favorites.toggle_favorite.clone();
                                        move |_| { toggle.emit(device_id.clone()); }
                                    }
                                    title={if is_fav { "Remove from favorites" } else { "Add to favorites" }}
                                >
                                    {if is_fav { "⭐" } else { "☆" }}
                                </button>
                            </h5>
                            <small class="text-muted">{room}</small>
                        </div>
                        <div class="text-end">
                            {if device.reachable {
                                html! { <span class="badge bg-success">{"ONLINE"}</span> }
                            } else {
                                html! { <span class="badge bg-danger">{"OFFLINE"}</span> }
                            }}
                            {if let Some(battery_level) = battery {
                                let battery_color = if battery_level > 50 {
                                    "success"
                                } else if battery_level > 20 {
                                    "warning"
                                } else {
                                    "danger"
                                };
                                html! {
                                    <div class="mt-1">
                                        <small class={format!("text-{}", battery_color)}>
                                            {"🔋 "}{battery_level}{"%"}
                                        </small>
                                    </div>
                                }
                            } else {
                                html! {}
                            }}
                        </div>
                    </div>

                    <div class="sensor-data">
                        {if let Some(temp) = temperature {
                            html! {
                                <div class="mb-3">
                                    <div class="d-flex justify-content-between align-items-center mb-1">
                                        <span class="text-warning">{"🌡️ Temperature"}</span>
                                        <span class="h4 mb-0">{format!("{:.1}°C", temp)}</span>
                                    </div>
                                    <div class="progress" style="height: 8px;">
                                        <div
                                            class="progress-bar bg-warning"
                                            role="progressbar"
                                            style={format!("width: {}%", (temp.max(0.0).min(40.0) / 40.0 * 100.0))}
                                        />
                                    </div>
                                </div>
                            }
                        } else {
                            html! {}
                        }}

                        {if let Some(hum) = humidity {
                            html! {
                                <div class="mb-3">
                                    <div class="d-flex justify-content-between align-items-center mb-1">
                                        <span class="text-info">{"💧 Humidity"}</span>
                                        <span class="h4 mb-0">{format!("{:.0}%", hum)}</span>
                                    </div>
                                    <div class="progress" style="height: 8px;">
                                        <div
                                            class="progress-bar bg-info"
                                            role="progressbar"
                                            style={format!("width: {}%", hum)}
                                        />
                                    </div>
                                </div>
                            }
                        } else {
                            html! {}
                        }}

                        {if has_motion {
                            html! {
                                <div class="mb-3">
                                    <div class="d-flex justify-content-between align-items-center">
                                        <span class="text-primary">{"🏃 Motion"}</span>
                                        <span class="badge bg-primary">{"NO MOTION"}</span>
                                    </div>
                                </div>
                            }
                        } else {
                            html! {}
                        }}

                        {if has_contact {
                            html! {
                                <div class="mb-3">
                                    <div class="d-flex justify-content-between align-items-center">
                                        <span class="text-secondary">{"🚪 Contact"}</span>
                                        <span class="badge bg-secondary">{"CLOSED"}</span>
                                    </div>
                                </div>
                            }
                        } else {
                            html! {}
                        }}
                    </div>

                    <div class="mt-3">
                        <small class="text-muted font-monospace d-block text-truncate" style="opacity: 0.5;">
                            {&device.device_id}
                        </small>
                    </div>
                </div>
            </div>
        </div>
    }
}
