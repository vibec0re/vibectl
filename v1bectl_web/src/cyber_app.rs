// 🔥 CYBER APP - VIBEC0RE DYNAMIC UI! 💖

use gloo_net::http::Request;
use web_sys::TouchEvent;
use yew::prelude::*;

use crate::screen_renderer::{ScreenDots, ScreenRenderer};
use crate::screens::{default_config, parse_screens, Screen};
use crate::websocket_reconnect::{
    use_websocket, ApiRequest, ApiResponse, ConnectionStatus, DeviceState,
};

// 🔥 SWIPE DETECTION CONFIG 💖
const SWIPE_THRESHOLD: f64 = 50.0;

#[function_component(CyberApp)]
pub fn cyber_app() -> Html {
    let ws = use_websocket();
    let devices = use_state(Vec::<DeviceState>::new);
    let screens = use_state(Vec::<Screen>::new);
    let current_screen = use_state(|| 0usize);

    // Touch tracking for swipe
    let touch_start = use_state(|| None::<f64>);

    // 🔥 LOAD SCREEN CONFIG! 💖
    {
        let screens = screens.clone();
        use_effect_with((), move |_| {
            wasm_bindgen_futures::spawn_local(async move {
                // Try to load from /static/screens.kdl first
                let kdl_content = match Request::get("/static/screens.kdl").send().await {
                    Ok(resp) if resp.ok() => match resp.text().await {
                        Ok(text) => {
                            log::info!("🔥 Loaded screens.kdl from server!");
                            text
                        }
                        Err(_) => default_config().to_string(),
                    },
                    _ => {
                        log::info!("📝 Using default screen config");
                        default_config().to_string()
                    }
                };

                match parse_screens(&kdl_content) {
                    Ok(parsed) => {
                        log::info!("✅ Parsed {} screens!", parsed.len());
                        screens.set(parsed);
                    }
                    Err(e) => {
                        log::error!("❌ Failed to parse screens: {}", e);
                    }
                }
            });
            || ()
        });
    }

    // 🔥 REQUEST DEVICES ON CONNECT! 💖
    {
        let send_request = ws.send_request.clone();
        use_effect_with(ws.status.clone(), move |status| {
            if *status == ConnectionStatus::Connected {
                log::info!("🔥 Connected! Requesting devices...");
                send_request.emit(ApiRequest::DiscoverDevices);
            }
            || ()
        });
    }

    // 🔥 HANDLE WEBSOCKET RESPONSES! 💖
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
                        log::info!("🔥 Got {} devices!", total_count);
                        devices.set(device_list.clone());
                    }
                    _ => {}
                }
            }
            || ()
        });
    }

    // 🔥 HANDLE DEVICE EVENTS FOR REAL-TIME UPDATES! 💖
    {
        let last_event = ws.last_event.clone();
        let devices = devices.clone();
        let send_request = ws.send_request.clone();

        use_effect_with(last_event, move |event| {
            if event.is_some() {
                // Refresh device list on any event
                send_request.emit(ApiRequest::DiscoverDevices);
            }
            || ()
        });
    }

    // 🔥 SWIPE HANDLERS! 💖
    let on_touch_start = {
        let touch_start = touch_start.clone();
        Callback::from(move |e: TouchEvent| {
            if let Some(touch) = e.touches().get(0) {
                touch_start.set(Some(touch.client_x() as f64));
            }
        })
    };

    let on_touch_end = {
        let touch_start = touch_start.clone();
        let current_screen = current_screen.clone();
        let screens_len = screens.len();
        Callback::from(move |e: TouchEvent| {
            if let (Some(start), Some(touch)) = (*touch_start, e.changed_touches().get(0)) {
                let end = touch.client_x() as f64;
                let diff = end - start;

                if diff.abs() > SWIPE_THRESHOLD {
                    let current = *current_screen;
                    if diff > 0.0 && current > 0 {
                        // Swipe right - previous screen
                        current_screen.set(current - 1);
                    } else if diff < 0.0 && current < screens_len.saturating_sub(1) {
                        // Swipe left - next screen
                        current_screen.set(current + 1);
                    }
                }

                touch_start.set(None);
            }
        })
    };

    let on_screen_select = {
        let current_screen = current_screen.clone();
        Callback::from(move |index: usize| {
            current_screen.set(index);
        })
    };

    // 🔥 CONNECTION STATUS DOT! 💖
    let status_class = match &ws.status {
        ConnectionStatus::Connected => "dot connected",
        ConnectionStatus::Connecting | ConnectionStatus::Reconnecting(_) => "dot connecting",
        _ => "dot disconnected",
    };

    // Get current screen
    let active_screen = screens.get(*current_screen).cloned();
    let screen_title = active_screen
        .as_ref()
        .map(|s| s.title.clone())
        .unwrap_or_else(|| "VIBEC0RE".to_string());

    html! {
        <div class="app"
            ontouchstart={on_touch_start}
            ontouchend={on_touch_end}
        >
            // 🔥 DECORATIVE CORNER CIRCLES! 💖
            <div class="cyber-corner-decor top-left"></div>
            <div class="cyber-corner-decor top-right"></div>
            <div class="cyber-corner-decor bottom-left"></div>

            // 🔥 HEADER 💖
            <header class="header">
                <div class="title">{screen_title}</div>
                <div class="status">
                    <div class={status_class}></div>
                </div>
            </header>

            // 🔥 SCREEN CONTENT 💖
            <main class="screen-container">
                {if let Some(screen) = active_screen {
                    html! {
                        <ScreenRenderer
                            screen={screen}
                            devices={(*devices).clone()}
                            on_request={ws.send_request.clone()}
                        />
                    }
                } else {
                    html! {
                        <div class="loading">
                            <p class="text-neon-pink">{"LOADING CONFIG..."}</p>
                        </div>
                    }
                }}
            </main>

            // 🔥 SCREEN DOTS (only if multiple screens) 💖
            {if screens.len() > 1 {
                html! {
                    <ScreenDots
                        total={screens.len()}
                        current={*current_screen}
                        on_select={on_screen_select}
                    />
                }
            } else {
                html! {}
            }}
        </div>
    }
}
