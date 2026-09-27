// 🔥 VIBEC0RE WEBUI - CYBER EDITION! 💖
//
// This file owns the crate's whole module tree, so `cargo test -p v1bectl_web
// --lib` runs every test. The wasm entry point in `main.rs` compiles this same
// file with `include!` (the lib is `cdylib`-only, which a bin can't link
// against), so there's exactly one list of modules. Because of the
// `include!`, keep this file to plain items: no inner attributes or `//!`
// docs.

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
