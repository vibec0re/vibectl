// 🔥 VIBEC0RE WEBUI - CYBER EDITION! 💖

use yew::prelude::*;

mod components;
mod cyber_app;
mod screen_renderer;
mod screens;
mod websocket_reconnect;
mod websocket_simple;

// Legacy modules (keep for now)
mod context;
mod favorites;
mod hooks;
mod pages;
mod router;

use cyber_app::CyberApp;

fn main() {
    // 🔥 PANIC HOOK FIRST - CATCH ALL PANICS! 💖
    console_error_panic_hook::set_once();

    // 🔥 IMMEDIATE CONSOLE OUTPUT - BEFORE ANYTHING ELSE! 💖
    web_sys::console::log_1(&"🔥 VIBEC0RE WASM LOADED!".into());

    // Initialize WASM logger with DEBUG level
    wasm_logger::init(wasm_logger::Config::new(log::Level::Debug));

    log::info!("🔥 VIBEC0RE CYBER APP STARTING! 💖");

    // 🚀 LAUNCH THE APP! 🚀
    yew::Renderer::<CyberApp>::new().render();

    log::info!("🚀 Yew renderer started!");
}
