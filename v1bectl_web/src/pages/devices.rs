use crate::components::light_card::LightCard;
use crate::components::outlet_card::OutletCard;
use crate::components::sensor_card::SensorCard;
use crate::hooks::use_favorites::use_favorites; // 🔥 Import the hook!
use crate::router::Route;
use crate::websocket_reconnect::{
    use_websocket, ApiRequest, ApiResponse, ConnectionStatus, DeviceState,
}; // 🔥 USE RECONNECTING WEBSOCKET! 💖
use std::collections::HashMap;
use yew::prelude::*;
use yew_router::prelude::*;

// 🔥 DEVICE COMPONENT 🔥
#[derive(Properties, PartialEq)]
struct DeviceCardProps {
    device: DeviceState,
}

#[function_component(DeviceCard)]
fn device_card(props: &DeviceCardProps) -> Html {
    let device = &props.device.device_info;
    let state_value = &props.device.state;

    let icon = match &device.device_type {
        crate::websocket_reconnect::DeviceType::Light => "💡",
        crate::websocket_reconnect::DeviceType::Outlet => "🔌",
        crate::websocket_reconnect::DeviceType::Sensor => "📡",
        crate::websocket_reconnect::DeviceType::Blinds => "🪟",
        crate::websocket_reconnect::DeviceType::Speaker => "🔊",
        crate::websocket_reconnect::DeviceType::Gateway => "🌐",
        _ => "❓",
    };

    // Extract actual device state values from enum
    let (is_on, brightness) = match state_value {
        crate::websocket_reconnect::DeviceStateValue::Light(light) => {
            (light.is_on, light.brightness)
        }
        crate::websocket_reconnect::DeviceStateValue::Outlet(outlet) => (outlet.is_on, None),
        _ => (false, None),
    };

    let status_color = if is_on { "text-success" } else { "text-danger" };
    let card_border = if device.reachable {
        "border-success"
    } else {
        "border-secondary"
    };
    let room = device
        .device_groups
        .first()
        .cloned()
        .unwrap_or_else(|| "No Room".to_string());

    html! {
        <div class={format!("col-md-4 col-lg-3 mb-4")}>
            <div class={format!("card bg-dark text-white h-100 {}", card_border)}>
                <div class="card-body">
                    <h5 class="card-title d-flex justify-content-between align-items-center">
                        <span>{icon}{" "}{&device.name}</span>
                        <span class={format!("badge {}", status_color)}>
                            {if is_on { "ON" } else { "OFF" }}
                        </span>
                    </h5>
                    <p class="card-text">
                        <small class="text-muted d-block">{"Type: "}{format!("{:?}", &device.device_type)}</small>
                        <small class="text-muted d-block">{"Room: "}{room}</small>
                        <small class="text-muted d-block">
                            {"Reachable: "}
                            <span class={if device.reachable { "text-success" } else { "text-danger" }}>
                                {if device.reachable { "YES" } else { "NO" }}
                            </span>
                        </small>
                        {if let Some(brightness) = brightness {
                            html! {
                                <small class="text-muted d-block">{"Brightness: "}{brightness}{"%"}</small>
                            }
                        } else {
                            html! {}
                        }}
                    </p>
                    <div class="mt-3">
                        <small class="text-muted font-monospace">{&device.device_id}</small>
                    </div>
                </div>
            </div>
        </div>
    }
}

// 🔥 DEVICES PAGE COMPONENT 🔥
#[function_component(Devices)]
pub fn devices() -> Html {
    let ws = use_websocket();
    let devices = use_state(|| Vec::<DeviceState>::new());
    let loading = use_state(|| false);
    let favorites_handle = use_favorites(); // 🔥 Get favorites & offline filter!

    // 💖 FILTER DEVICES BASED ON OFFLINE TOGGLE!
    let filtered_devices: Vec<DeviceState> = devices
        .iter()
        .filter(|device| favorites_handle.show_offline || device.device_info.reachable)
        .cloned()
        .collect();

    // Group filtered devices by type
    let mut grouped: HashMap<String, Vec<DeviceState>> = HashMap::new();
    for device in filtered_devices.iter() {
        grouped
            .entry(format!("{:?}", device.device_info.device_type))
            .or_insert_with(Vec::new)
            .push(device.clone());
    }

    // Sort groups by type name
    let mut sorted_groups: Vec<(String, Vec<DeviceState>)> = grouped.into_iter().collect();
    sorted_groups.sort_by(|a, b| a.0.cmp(&b.0));

    // Load devices on mount or when connection status changes
    {
        let ws = ws.clone();
        let loading = loading.clone();
        let _devices = devices.clone();

        use_effect_with(ws.status.clone(), move |status| {
            if *status == ConnectionStatus::Connected && !*loading {
                log::info!("🔥 AUTO-LOADING DEVICES! NO WAIT!");
                loading.set(true);
                ws.send_request.emit(ApiRequest::DiscoverDevices);

                // 🔥 SUBSCRIBE TO ALL DEVICE EVENTS! 💖
                ws.send_request.emit(ApiRequest::Subscribe {
                    device_ids: vec!["*".to_string()], // Subscribe to all devices
                });
                log::info!("📢 Subscribed to device events!");
            }
            || ()
        });
    }

    // Handle WebSocket responses
    {
        let last_response = ws.last_response.clone();
        let devices = devices.clone();
        let loading = loading.clone();

        use_effect_with(last_response, move |response| {
            if let Some(resp) = response {
                match resp {
                    ApiResponse::DeviceList {
                        devices: device_list,
                        total_count,
                    } => {
                        log::info!("🔥 Received {} devices!", total_count);
                        devices.set(device_list.clone());
                        loading.set(false);
                    }
                    _ => {
                        log::debug!("Received other response: {:?}", resp);
                    }
                }
            }
            || ()
        });
    }

    // 🔥 HANDLE WEBSOCKET EVENTS - REAL-TIME UPDATES! 💖
    {
        let devices = devices.clone();

        use_effect_with(ws.last_event.clone(), move |event| {
            log::info!("📢 use_effect_with triggered for event!");
            if let Some(evt) = event.as_ref() {
                log::info!("🔥 DEVICE EVENT RECEIVED - UPDATING UI!");
                log::info!(
                    "📊 Full event JSON: {}",
                    serde_json::to_string(evt).unwrap_or_default()
                );

                // Parse the event to get device_id and new state
                if let Some(device_id) = evt.get("device_id").and_then(|v| v.as_str()) {
                    log::info!("📍 Found device_id: {}", device_id);
                    if let Some(event_type) = evt.get("event_type").and_then(|v| v.as_object()) {
                        // Check event type
                        if let Some(event_type_name) =
                            event_type.get("type").and_then(|v| v.as_str())
                        {
                            log::info!("🔍 Event type: {}", event_type_name);

                            // Handle both attribute_changed AND state_changed! 🔥
                            if event_type_name == "attribute_changed" {
                                if let Some(new_value) = event_type.get("new_value") {
                                    // Update the device in our list!
                                    let mut updated_devices = (*devices).clone();
                                    for device in &mut updated_devices {
                                        if device.device_id == device_id {
                                            // Parse the new state value
                                            if let Ok(new_state) = serde_json::from_value::<
                                                v1bectl_state::DeviceStateValue,
                                            >(
                                                new_value.clone()
                                            ) {
                                                device.state = new_state;
                                                log::info!(
                                                    "✅ Updated device {} from attribute_changed!",
                                                    device_id
                                                );
                                            }
                                            break;
                                        }
                                    }
                                    devices.set(updated_devices);
                                }
                            } else if event_type_name == "state_changed" {
                                // 🔥 NEW STATE_CHANGED EVENT HANDLING! 💖
                                if let Some(new_state_value) = event_type.get("new_state") {
                                    let mut updated_devices = (*devices).clone();
                                    for device in &mut updated_devices {
                                        if device.device_id == device_id {
                                            // Parse the new state value directly!
                                            if let Ok(new_state) = serde_json::from_value::<
                                                v1bectl_state::DeviceStateValue,
                                            >(
                                                new_state_value.clone()
                                            ) {
                                                device.state = new_state;
                                                log::info!("🔥 Updated device {} from state_changed! PURE CBOR EVENT! 💖", device_id);
                                            }
                                            break;
                                        }
                                    }
                                    devices.set(updated_devices);
                                }
                            }
                        }
                    }
                }
            }
            || ()
        });
    }

    html! {
        <div class="container-fluid min-vh-100 bg-dark">
            // Navigation bar
            <nav class="navbar navbar-dark bg-dark border-bottom border-secondary">
                <div class="container-fluid">
                    <span class="navbar-brand mb-0 h1 text-danger">{"🔥 V1BECTL"}</span>
                    <div class="d-flex align-items-center">
                        <Link<Route> to={Route::Home} classes="btn btn-outline-warning btn-sm me-2">
                            {"⭐ FAVORITES"}
                        </Link<Route>>
                        <Link<Route> to={Route::About} classes="btn btn-outline-danger btn-sm">
                            {"🔥 ABOUT"}
                        </Link<Route>>
                    </div>
                </div>
            </nav>

            <div class="container py-4">
                <div class="mb-4 d-flex justify-content-between align-items-center">
                    <h1 class="text-danger fw-bold glow">{"🔥 ALL DEVICES 🔥"}</h1>
                    <div class="form-check form-switch">
                        <input
                            class="form-check-input"
                            type="checkbox"
                            id="offlineToggle"
                            checked={favorites_handle.show_offline}
                            onchange={
                                let toggle = favorites_handle.toggle_show_offline.clone();
                                move |_| toggle.emit(())
                            }
                        />
                        <label class="form-check-label text-white" for="offlineToggle">
                            {"💔 Show Offline Devices"}
                        </label>
                    </div>
                </div>

                if ws.status != ConnectionStatus::Connected {
                    <div class="alert alert-danger" role="alert">
                        {"⚠️ Not connected to V1BECTL server!"}
                    </div>
                } else if *loading {
                    <div class="text-center py-5">
                        <div class="spinner-border text-danger" role="status">
                            <span class="visually-hidden">{"Loading..."}</span>
                        </div>
                        <p class="text-white mt-3">{"🔥 DISCOVERING DEVICES... 🔥"}</p>
                    </div>
                } else if filtered_devices.is_empty() {
                    <div class="text-center py-5">
                        <p class="text-white">
                            {if devices.is_empty() {
                                "No devices found!"
                            } else {
                                "💔 All devices are offline! Toggle filter to see them!"
                            }}
                        </p>
                        <button
                            class="btn btn-vibec0re"
                            onclick={
                                let ws = ws.clone();
                                let loading = loading.clone();
                                move |_| {
                                    loading.set(true);
                                    ws.send_request.emit(ApiRequest::DiscoverDevices);
                                }
                            }
                        >
                            {"🔄 REFRESH"}
                        </button>
                    </div>
                } else {
                    <div>
                        {for sorted_groups.into_iter().map(|(device_type, type_devices)| {
                            let type_icon = match device_type.as_str() {
                                "Light" => "💡",
                                "Outlet" => "🔌",
                                "Sensor" => "📡",
                                "Controller" => "🎮",
                                "Blinds" => "🪟",
                                "Speaker" => "🔊",
                                "Gateway" => "🌐",
                                _ => "❓",
                            };

                            html! {
                                <div key={device_type.clone()} class="mb-5">
                                    <h3 class="text-warning mb-3">
                                        {type_icon}{" "}{&device_type}{"s"}
                                        <span class="badge bg-danger ms-2">{type_devices.len()}</span>
                                    </h3>
                                    <div class="row">
                                        {for type_devices.into_iter().map(|device| {
                                            match device_type.as_str() {
                                                "Sensor" => html! {
                                                    <SensorCard
                                                        device={device}
                                                        on_request={ws.send_request.clone()}
                                                    />
                                                },
                                                "Light" => html! {
                                                    <LightCard
                                                        device={device}
                                                        on_request={ws.send_request.clone()}
                                                    />
                                                },
                                                "Outlet" => html! {
                                                    <OutletCard
                                                        device={device}
                                                        on_request={ws.send_request.clone()}
                                                    />
                                                },
                                                _ => html! { <DeviceCard device={device} /> }
                                            }
                                        })}
                                    </div>
                                </div>
                            }
                        })}
                    </div>
                }
            </div>
        </div>
    }
}
