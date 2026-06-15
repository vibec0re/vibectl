// 🔥 VIBEC0RE GTK4 CONTROL PANEL — MAIN ENTRY POINT!!! 🚀

mod bridge;
mod config;
mod connection;
mod protocol;
mod screen;
mod tray;
mod widgets;
mod window;

use adw::prelude::*;
use libadwaita as adw;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc;

use bridge::GtkMsg;
use window::AppWindow;

const APP_ID: &str = "dev.v1bectl.gtk";

fn main() {
    tracing_subscriber::fmt::init();

    let app = adw::Application::builder().application_id(APP_ID).build();

    app.connect_activate(build_ui);
    app.run();
}

fn build_ui(app: &adw::Application) {
    // 🔥 Load CSS
    let css_provider = gtk4::CssProvider::new();
    css_provider.load_from_data(include_str!("style.css"));
    gtk4::style_context_add_provider_for_display(
        &gtk4::gdk::Display::default().expect("display"),
        &css_provider,
        gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );

    // 📋 Load config
    let config_path = config::config_path();
    let config_content = match std::fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(e) => {
            show_error(
                app,
                &format!(
                    "Could not read config file\n{}\n\n{}",
                    config_path.display(),
                    e
                ),
            );
            return;
        }
    };

    let app_config = match config::parse_config(&config_content) {
        Ok(c) => c,
        Err(e) => {
            show_error(app, &format!("Config parse error:\n{}", e));
            return;
        }
    };

    // 🚀 Create cross-thread channels
    let (cmd_tx, cmd_rx) = mpsc::channel::<bridge::ServerCmd>(64);
    let (gtk_tx, gtk_rx) = mpsc::unbounded_channel::<GtkMsg>();

    // 🏗️ Build window — wrapped in Rc<RefCell> for the poll closure
    let app_window = Rc::new(RefCell::new(AppWindow::new(
        app,
        app_config.clone(),
        cmd_tx,
    )));
    let window = app_window.borrow().window.clone();

    // ⚡ Poll GTK messages from the Tokio thread at ~60fps (every 16ms)
    // Uses try_recv so it never blocks the GTK main loop
    let app_window_poll = app_window.clone();
    let gtk_rx = Rc::new(RefCell::new(gtk_rx));
    glib::timeout_add_local(std::time::Duration::from_millis(16), move || {
        let mut rx = gtk_rx.borrow_mut();
        while let Ok(msg) = rx.try_recv() {
            let mut win = app_window_poll.borrow_mut();
            match msg {
                GtkMsg::DevicesDiscovered(devices) => {
                    win.on_devices_discovered(devices);
                }
                GtkMsg::DeviceStateChanged {
                    device_id,
                    new_state,
                } => {
                    win.on_device_state_changed(&device_id, &new_state);
                }
                GtkMsg::ConnectionStatus(status) => {
                    win.on_connection_status(&status);
                }
            }
        }
        glib::ControlFlow::Continue
    });

    // 🔥 Spawn Tokio runtime on a background thread — keep GTK thread clean!
    let server_config = app_config.server.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(connection::run(server_config, gtk_tx, cmd_rx));
    });

    window.present();

    // 🔥 System tray — close-to-tray behavior
    let tray_handle = tray::spawn_tray();

    // Close window → hide to tray instead of quit
    window.connect_close_request(move |win| {
        win.set_visible(false);
        glib::Propagation::Stop
    });

    // Poll tray visibility state to sync with window
    // ksni::Handle::update() returns R directly (infallible), no Result wrapping
    let window_for_tray = window.clone();
    glib::timeout_add_local(std::time::Duration::from_millis(250), move || {
        let visible = tray_handle.update(|tray| tray.visible);
        if visible != window_for_tray.is_visible() {
            window_for_tray.set_visible(visible);
        }
        glib::ControlFlow::Continue
    });
}

// ============================================================
// ❌ ERROR DISPLAY — SHOW CONFIG ERRORS GRACEFULLY! ⚡
// ============================================================

fn show_error(app: &adw::Application, message: &str) {
    let window = adw::ApplicationWindow::builder()
        .application(app)
        .default_width(480)
        .default_height(300)
        .build();

    let header = adw::HeaderBar::new();
    let content = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
    content.append(&header);

    let status = adw::StatusPage::builder()
        .icon_name("dialog-error-symbolic")
        .title("Configuration Error")
        .description(message)
        .build();
    content.append(&status);

    window.set_content(Some(&content));
    window.present();
}
