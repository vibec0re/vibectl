// 🔥 OUTLET CARD COMPONENT - POWER CONTROL! 🔌

use crate::context::use_favorites_context;
use crate::websocket_reconnect::{ApiRequest, DeviceState}; // 🔥 USE RECONNECTING MODULE! 💖
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct OutletCardProps {
    pub device: DeviceState,
    pub on_request: Callback<ApiRequest>,
}

#[function_component(OutletCard)]
pub fn outlet_card(props: &OutletCardProps) -> Html {
    let device = &props.device.device_info;
    let state_value = &props.device.state;
    let favorites = use_favorites_context();
    let is_fav = favorites.favorites.is_favorite(&device.device_id);

    let room = device
        .device_groups
        .first()
        .cloned()
        .unwrap_or_else(|| "No Room".to_string());
    let card_border = if device.reachable {
        "border-primary"
    } else {
        "border-secondary"
    };

    // Extract outlet state (outlets are stored as Light with no brightness!)
    let initial_on = match state_value {
        crate::websocket_reconnect::DeviceStateValue::Light(light) => light.is_on,
        crate::websocket_reconnect::DeviceStateValue::Outlet(outlet) => outlet.is_on,
        _ => false,
    };

    let is_on = use_state(|| initial_on);

    // 🔥 UPDATE LOCAL STATE WHEN PROPS CHANGE - REAL-TIME SYNC! 💖
    {
        let is_on = is_on.clone();
        let device_id = device.device_id.clone();

        use_effect_with((initial_on, device_id), move |(new_on, _)| {
            is_on.set(*new_on);
            || ()
        });
    }

    let toggle_outlet = {
        let device_id = device.device_id.clone();
        let on_request = props.on_request.clone();
        let is_on = is_on.clone();
        Callback::from(move |_| {
            let new_state = !*is_on;
            is_on.set(new_state);
            // 🔥 USE SetOutletState REQUEST! 💖
            on_request.emit(ApiRequest::SetOutletState {
                device_id: device_id.clone(),
                is_on: new_state,
            });
        })
    };

    let power_color = if *is_on {
        "text-success"
    } else {
        "text-danger"
    };

    html! {
        <div class="col-md-6 col-lg-4 mb-4">
            <div class={format!("card bg-dark text-white h-100 {}", card_border)}>
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start mb-3">
                        <div>
                            <h5 class="card-title mb-1">
                                {"🔌 "}{&device.name}
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
                                    <span class={format!("badge bg-pill {}", if *is_on { "bg-success" } else { "bg-secondary" })}>
                                        {if *is_on { "ON" } else { "OFF" }}
                                    </span>
                                }
                            } else {
                                html! { <span class="badge bg-danger">{"OFFLINE"}</span> }
                            }}
                        </div>
                    </div>

                    // 🔥 BIG POWER BUTTON! 💖
                    <div class="text-center my-4">
                        <button
                            class={format!("btn btn-lg rounded-circle p-4 {}",
                                if *is_on { "btn-success" } else { "btn-outline-danger" }
                            )}
                            onclick={toggle_outlet}
                            disabled={!device.reachable}
                            style="width: 120px; height: 120px; font-size: 3rem;"
                        >
                            <span class={power_color}>
                                {if *is_on { "⚡" } else { "⭕" }}
                            </span>
                        </button>
                    </div>

                    <div class="text-center mb-3">
                        <p class="mb-1">
                            {"Power: "}
                            <strong class={power_color}>
                                {if *is_on { "ON" } else { "OFF" }}
                            </strong>
                        </p>
                        <small class="text-muted">
                            {"Click button to toggle power"}
                        </small>
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
