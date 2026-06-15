// 🔥 LIGHT CARD COMPONENT - VIBEC0RE LIGHT CONTROLS! 🔥

use crate::context::use_favorites_context;
use crate::websocket_reconnect::{ApiRequest, DeviceState}; // 🔥 USE RECONNECTING MODULE! 💖
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct LightCardProps {
    pub device: DeviceState,
    pub on_request: Callback<ApiRequest>,
}

#[function_component(LightCard)]
pub fn light_card(props: &LightCardProps) -> Html {
    let device = &props.device.device_info;
    let state_value = &props.device.state;
    let favorites = use_favorites_context();
    let is_fav = favorites.favorites.is_favorite(&device.device_id);

    // Check capabilities
    let has_brightness = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::Brightness);
    let has_color_temp = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::ColorTemperature);
    let has_rgb = device
        .capabilities
        .contains(&crate::websocket_reconnect::Capability::RgbColor);

    let room = device
        .device_groups
        .first()
        .cloned()
        .unwrap_or_else(|| "No Room".to_string());
    let card_border = if device.reachable {
        "border-warning"
    } else {
        "border-secondary"
    };

    // Extract actual state from enum!
    let (initial_on, initial_brightness, initial_color_temp) = match state_value {
        crate::websocket_reconnect::DeviceStateValue::Light(light) => (
            light.is_on,
            light.brightness.unwrap_or(50),
            light.color_temp.unwrap_or(2700),
        ),
        _ => (false, 50, 2700),
    };

    let is_on = use_state(|| initial_on);
    let brightness = use_state(|| initial_brightness);
    let color_temp = use_state(|| initial_color_temp);

    // 🔥 UPDATE LOCAL STATE WHEN PROPS CHANGE - REAL-TIME SYNC! 💖
    {
        let is_on = is_on.clone();
        let brightness = brightness.clone();
        let color_temp = color_temp.clone();
        let device_id = device.device_id.clone();

        use_effect_with(
            (
                initial_on,
                initial_brightness,
                initial_color_temp,
                device_id,
            ),
            move |(new_on, new_brightness, new_color_temp, _)| {
                is_on.set(*new_on);
                brightness.set(*new_brightness);
                color_temp.set(*new_color_temp);
                || ()
            },
        );
    }

    let toggle_light = {
        let device_id = device.device_id.clone();
        let on_request = props.on_request.clone();
        let is_on = is_on.clone();
        Callback::from(move |_| {
            let new_state = !*is_on;
            is_on.set(new_state);
            on_request.emit(ApiRequest::SetLightState {
                device_id: device_id.clone(),
                is_on: Some(new_state),
                brightness: None,
                color_temp: None,
                rgb_color: None,
            });
        })
    };

    let on_brightness_change = {
        let device_id = device.device_id.clone();
        let on_request = props.on_request.clone();
        let brightness = brightness.clone();
        let is_on = is_on.clone();
        Callback::from(move |e: InputEvent| {
            if let Some(input) = e.target_dyn_into::<web_sys::HtmlInputElement>() {
                let value = input.value().parse::<u8>().unwrap_or(0);
                brightness.set(value);
                // Turn on if adjusting brightness from 0
                if value > 0 && !*is_on {
                    is_on.set(true);
                }
                on_request.emit(ApiRequest::SetLightState {
                    device_id: device_id.clone(),
                    is_on: Some(value > 0),
                    brightness: Some(value),
                    color_temp: None,
                    rgb_color: None,
                });
            }
        })
    };

    let on_color_temp_change = {
        let device_id = device.device_id.clone();
        let on_request = props.on_request.clone();
        let color_temp = color_temp.clone();
        Callback::from(move |e: InputEvent| {
            if let Some(input) = e.target_dyn_into::<web_sys::HtmlInputElement>() {
                let value = input.value().parse::<u16>().unwrap_or(2700);
                color_temp.set(value);
                on_request.emit(ApiRequest::SetLightState {
                    device_id: device_id.clone(),
                    is_on: None,
                    brightness: None,
                    color_temp: Some(value),
                    rgb_color: None,
                });
            }
        })
    };

    let light_color = if *is_on {
        format!("rgba(255, 255, 0, {})", (*brightness as f32 / 100.0))
    } else {
        "rgba(255, 255, 255, 0.1)".to_string()
    };

    html! {
        <div class="col-md-6 col-lg-4 mb-4">
            <div class={format!("card bg-dark text-white h-100 {}", card_border)}>
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start mb-3">
                        <div>
                            <h5 class="card-title mb-1">
                                {"💡 "}{&device.name}
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
                                html! {
                                    <div class="form-check form-switch">
                                        <input
                                            class="form-check-input"
                                            type="checkbox"
                                            role="switch"
                                            checked={*is_on}
                                            onclick={toggle_light}
                                            style="cursor: pointer;"
                                        />
                                    </div>
                                }
                            } else {
                                html! { <span class="badge bg-danger">{"OFFLINE"}</span> }
                            }}
                        </div>
                    </div>

                    <div class="light-controls">
                        // Light icon with glow effect
                        <div class="text-center mb-4">
                            <div
                                class="light-icon"
                                style={format!(
                                    "font-size: 4rem; color: {}; text-shadow: 0 0 20px {}, 0 0 40px {};",
                                    light_color, light_color, light_color
                                )}
                            >
                                {"💡"}
                            </div>
                        </div>

                        {if has_brightness && device.reachable {
                            html! {
                                <div class="mb-3">
                                    <label class="form-label text-warning">
                                        {"☀️ Brightness: "}{*brightness}{"%"}
                                    </label>
                                    <input
                                        type="range"
                                        class="form-range"
                                        min="0"
                                        max="100"
                                        value={brightness.to_string()}
                                        oninput={on_brightness_change}
                                        disabled={!device.reachable}
                                    />
                                </div>
                            }
                        } else {
                            html! {}
                        }}

                        {if has_color_temp && device.reachable {
                            let temp_percent = (((*color_temp - 2200) as f32 / (6500 - 2200) as f32) * 100.0) as u16;
                            html! {
                                <div class="mb-3">
                                    <label class="form-label text-info">
                                        {"🌡️ Color Temperature: "}{*color_temp}{"K"}
                                    </label>
                                    <div class="d-flex align-items-center">
                                        <span class="text-warning me-2">{"🕯️"}</span>
                                        <input
                                            type="range"
                                            class="form-range color-temp-slider"
                                            min="2200"
                                            max="6500"
                                            value={color_temp.to_string()}
                                            oninput={on_color_temp_change}
                                            disabled={!device.reachable}
                                            style={format!(
                                                "background: linear-gradient(to right, #ffcc66 0%, #ffffff {}%, #99ccff 100%)",
                                                temp_percent
                                            )}
                                        />
                                        <span class="text-info ms-2">{"❄️"}</span>
                                    </div>
                                </div>
                            }
                        } else {
                            html! {}
                        }}

                        {if has_rgb && device.reachable {
                            html! {
                                <div class="mb-3">
                                    <label class="form-label text-danger">{"🎨 RGB Color"}</label>
                                    <div class="text-muted small">{"Coming soon!"}</div>
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
