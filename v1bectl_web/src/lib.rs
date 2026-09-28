//! 🔥 VIBEC0RE WEBUI - CYBER EDITION! 💖
//!
//! The whole web UI. `main.rs` (the wasm entry point trunk builds) only calls
//! [`run`], so every module and test is compiled once, here.

pub mod components;
pub mod cyber_app;
pub mod screen_renderer;
pub mod screens;
pub mod websocket_reconnect; // 🔥 THE RECONNECTING WEBSOCKET! CHOOOM FIX! 💖

/// 🚀 Start the app: panic hook, logger, then mount `CyberApp` on the page.
pub fn run() {
    // 🔥 PANIC HOOK FIRST - CATCH ALL PANICS! 💖
    console_error_panic_hook::set_once();

    // 🔥 IMMEDIATE CONSOLE OUTPUT - BEFORE ANYTHING ELSE! 💖
    web_sys::console::log_1(&"🔥 VIBEC0RE WASM LOADED!".into());

    // Initialize WASM logger with DEBUG level
    wasm_logger::init(wasm_logger::Config::new(log::Level::Debug));

    log::info!("🔥 VIBEC0RE CYBER APP STARTING! 💖");

    // 🚀 LAUNCH THE APP! 🚀
    yew::Renderer::<cyber_app::CyberApp>::new().render();

    log::info!("🚀 Yew renderer started!");
}
