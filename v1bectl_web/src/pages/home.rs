use crate::components::{LightCard, SensorCard};
use crate::context::use_favorites_context;
use crate::router::Route;
use crate::websocket_reconnect::{
    use_websocket, ApiRequest, ApiResponse, ConnectionStatus, DeviceState,
}; // 🔥 USE RECONNECTING WEBSOCKET! 💖
use yew::prelude::*;
use yew_router::prelude::*;

// 🔥 HOME PAGE COMPONENT 🔥
#[function_component(Home)]
pub fn home() -> Html {
    let ws = use_websocket();
    let favorites = use_favorites_context();
    let devices = use_state(Vec::<DeviceState>::new);

    // Refresh devices on page load
    {
        let send_request = ws.send_request.clone();
        use_effect_with(ws.status.clone(), move |status| {
            if *status == ConnectionStatus::Connected {
                log::info!("🔥 Home page loaded - refreshing devices");
                send_request.emit(ApiRequest::DiscoverDevices);
            }
            || ()
        });
    }

    // Handle WebSocket responses
    {
        let last_response = ws.last_response.clone();
        let devices = devices.clone();

        use_effect_with(last_response, move |response| {
            if let Some(resp) = response {
                match resp {
                    ApiResponse::DeviceList {
                        devices: device_list,
                        total_count,
                    } => {
                        log::info!("🔥 Received {} devices for favorites!", total_count);
                        devices.set(device_list.clone());
                    }
                    _ => {}
                }
            }
            || ()
        });
    }

    let status_badge = match &ws.status {
        ConnectionStatus::Connecting => html! {
            <span class="badge bg-warning text-dark badge-pulse">{"CONNECTING..."}</span>
        },
        ConnectionStatus::Connected => html! {
            <span class="badge bg-success">{"💚 CONNECTED"}</span>
        },
        ConnectionStatus::Disconnected => html! {
            <span class="badge bg-danger">{"💔 DISCONNECTED"}</span>
        },
        ConnectionStatus::Reconnecting(count) => html! {
            <span class="badge bg-warning text-dark badge-pulse">{format!("🔄 RECONNECTING #{}", count)}</span>
        },
        ConnectionStatus::Error(err) => html! {
            <span class="badge bg-danger" title={err.clone()}>{"❌ ERROR"}</span>
        },
    };

    html! {
        <div class="container-fluid min-vh-100 bg-dark">
            // Navigation bar
            <nav class="navbar navbar-dark bg-dark border-bottom border-secondary">
                <div class="container-fluid">
                    <span class="navbar-brand mb-0 h1 text-danger">{"🔥 V1BECTL"}</span>
                    <div class="d-flex align-items-center">
                        <Link<Route> to={Route::Devices} classes="btn btn-outline-warning btn-sm me-2">
                            {"💡 ALL DEVICES"}
                        </Link<Route>>
                        <Link<Route> to={Route::About} classes="btn btn-outline-danger btn-sm me-3">
                            {"🔥 ABOUT"}
                        </Link<Route>>
                        {status_badge}
                    </div>
                </div>
            </nav>

            // Favorites content
            <div class="container py-4">
                {if !favorites.favorites.device_ids.is_empty() {
                    let favorite_devices = devices.iter()
                        .filter(|d| favorites.favorites.is_favorite(&d.device_info.device_id))
                        .cloned()
                        .collect::<Vec<_>>();

                    if !favorite_devices.is_empty() {
                        html! {
                            <div class="row">
                                {for favorite_devices.iter().map(|device| {
                                    match &device.device_info.device_type {
                                        crate::websocket_reconnect::DeviceType::Sensor |
                                        crate::websocket_reconnect::DeviceType::MotionSensor => {
                                            html! {
                                                <SensorCard
                                                    device={device.clone()}
                                                    on_request={ws.send_request.clone()}
                                                />
                                            }
                                        },
                                        crate::websocket_reconnect::DeviceType::Light => {
                                            html! {
                                                <LightCard
                                                    device={device.clone()}
                                                    on_request={ws.send_request.clone()}
                                                />
                                            }
                                        },
                                        _ => html! {}
                                    }
                                })}
                            </div>
                        }
                    } else {
                        html! {
                            <div class="text-center py-5">
                                <h3 class="text-warning">{"No favorite devices yet! 😢"}</h3>
                                <p class="text-muted">{"Click the ⭐ button on any device to add it to favorites!"}</p>
                                <Link<Route> to={Route::Devices} classes="btn btn-vibec0re mt-3">
                                    {"💡 BROWSE DEVICES"}
                                </Link<Route>>
                            </div>
                        }
                    }
                } else {
                    html! {
                        <div class="text-center py-5">
                            <h3 class="text-warning">{"No favorite devices yet! 😢"}</h3>
                            <p class="text-muted">{"Click the ⭐ button on any device to add it to favorites!"}</p>
                            <Link<Route> to={Route::Devices} classes="btn btn-vibec0re mt-3">
                                {"💡 BROWSE DEVICES"}
                            </Link<Route>>
                        </div>
                    }
                }}
            </div>
        </div>
    }
}
