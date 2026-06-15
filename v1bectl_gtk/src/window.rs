// 🔥 APP WINDOW — THE HEART OF THE VIBEC0RE CONTROL PANEL!!! 🚀

use adw::prelude::*;
use gtk::prelude::*;
use gtk4 as gtk;
use libadwaita as adw;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc;
use v1bectl_sync::*;

use crate::bridge::{ConnectionState, ServerCmd};
use crate::config::AppConfig;
use crate::screen::{self, DeviceWidget};
use crate::widgets::screen_dots::ScreenDots;

// ============================================================
// 🏗️ APP WINDOW STRUCT
// ============================================================

pub struct AppWindow {
    pub window: adw::ApplicationWindow,
    device_widgets: Rc<RefCell<Vec<DeviceWidget>>>,
    connection_dot: gtk::Box,
    screen_title: gtk::Label,
    stack: gtk::Stack,
    header: adw::HeaderBar,
    config: AppConfig,
    cmd_tx: mpsc::Sender<ServerCmd>,
}

impl AppWindow {
    // ============================================================
    // 🚀 CONSTRUCTOR — BUILD THE UI SKELETON! ⚡
    // ============================================================

    pub fn new(app: &adw::Application, config: AppConfig, cmd_tx: mpsc::Sender<ServerCmd>) -> Self {
        // 🔥 Main application window — layer shell popup near tray!
        let is_layer_shell = gtk4_layer_shell::is_supported();

        let window = adw::ApplicationWindow::builder()
            .application(app)
            .title("v1bectl")
            .default_width(if is_layer_shell {
                config.window.width
            } else {
                480
            })
            .default_height(if is_layer_shell {
                config.window.height
            } else {
                720
            })
            .build();

        // 🔥 Layer shell — anchor to top-right like a notification panel!
        if is_layer_shell {
            use gtk4_layer_shell::LayerShell;
            window.init_layer_shell();
            window.set_layer(gtk4_layer_shell::Layer::Top);
            window.set_anchor(gtk4_layer_shell::Edge::Top, true);
            window.set_anchor(gtk4_layer_shell::Edge::Right, true);
            // Outer margin — keep clear of screen edges so shadow isn't clipped
            window.set_margin(gtk4_layer_shell::Edge::Top, -15);
            window.set_margin(gtk4_layer_shell::Edge::Right, 5);
            window.set_margin(gtk4_layer_shell::Edge::Bottom, 0);
            window.set_keyboard_mode(gtk4_layer_shell::KeyboardMode::OnDemand);
            window.set_namespace("v1bectl");
            window.add_css_class("layer-shell");
        }

        // 🚀 Header bar — no decorations in layer shell mode
        let header = adw::HeaderBar::new();
        if is_layer_shell {
            header.set_show_start_title_buttons(false);
            header.set_show_end_title_buttons(false);
            header.add_css_class("flat");
        }

        // 📋 Screen title — left-aligned in the header title slot
        let screen_title = gtk::Label::new(None);
        screen_title.add_css_class("screen-title");
        screen_title.set_halign(gtk::Align::Start);
        screen_title.set_hexpand(true);
        // header.set_title_widget(Some(&screen_title));

        // 💚 Connection dot wrapped in an indicator box so CSS can give it end-margin
        let connection_indicator = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        connection_indicator.add_css_class("connection-indicator");
        connection_indicator.set_valign(gtk::Align::Center);

        let connection_dot = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        connection_dot.add_css_class("connection-dot");
        connection_dot.add_css_class("connecting");
        connection_dot.set_size_request(10, 10);
        connection_dot.set_valign(gtk::Align::Center);
        connection_indicator.append(&connection_dot);

        // header.pack_end(&connection_indicator);

        // 📦 Stack for screen pages — vexpand so it fills remaining space
        let stack = gtk::Stack::new();
        stack.set_transition_type(gtk::StackTransitionType::SlideLeftRight);
        stack.set_vexpand(true);

        // 🔧 Loading page — shown while connecting
        let server_addr = format!("{}:{}", config.server.host, config.server.port);
        let loading = adw::StatusPage::builder()
            .icon_name("network-server-symbolic")
            .title("Connecting...")
            .description(&server_addr)
            .build();
        stack.add_named(&loading, Some("loading"));

        // 🏗️ Main layout: header + stack, wrapped in panel container
        let content = gtk::Box::new(gtk::Orientation::Vertical, 0);
        // content.append(&header);
        content.append(&stack);

        if is_layer_shell {
            // Panel container for rounded corners + shadow
            // set_overflow clips children so content doesn't bleed past rounded corners
            content.add_css_class("panel-content");
            content.set_overflow(gtk::Overflow::Hidden);

            let outer = gtk::Box::new(gtk::Orientation::Vertical, 0);
            outer.add_css_class("panel-outer");
            outer.append(&content);
            window.set_content(Some(&outer));
        } else {
            window.set_content(Some(&content));
        }

        // 📋 Set initial title from first screen in config (or "v1bectl" if no screens)
        let initial_title = config
            .screens
            .first()
            .map(|s| s.title.clone())
            .unwrap_or_else(|| "v1bectl".to_string());
        screen_title.set_text(&initial_title);

        AppWindow {
            window,
            device_widgets: Rc::new(RefCell::new(Vec::new())),
            connection_dot,
            screen_title,
            stack,
            header,
            config,
            cmd_tx,
        }
    }

    // ============================================================
    // 🔥 ON_DEVICES_DISCOVERED — BUILD REAL UI FROM DEVICE LIST! ⚡
    // ============================================================

    pub fn on_devices_discovered(&mut self, devices: Vec<DeviceState>) {
        // ✅ Remove loading placeholder
        if let Some(loading_page) = self.stack.child_by_name("loading") {
            self.stack.remove(&loading_page);
        }

        let mut all_widgets: Vec<DeviceWidget> = Vec::new();

        // 🚀 Build each screen as a scrolled window inside the stack
        for (i, screen_cfg) in self.config.screens.iter().enumerate() {
            let (scrolled, device_widgets) =
                screen::build_screen(screen_cfg, &devices, &self.cmd_tx);
            let page_name = format!("screen-{}", i);
            self.stack.add_named(&scrolled, Some(&page_name));
            all_widgets.extend(device_widgets);
        }

        // ✅ Show first screen
        self.stack.set_visible_child_name("screen-0");

        // 🔥 Navigation dots for multiple screens
        if self.config.screens.len() > 1 {
            let screen_count = self.config.screens.len();
            let stack_clone = self.stack.clone();
            let screen_title_clone = self.screen_title.clone();
            let titles: Vec<String> = self
                .config
                .screens
                .iter()
                .map(|s| s.title.clone())
                .collect();

            let dots = ScreenDots::new(screen_count, move |index| {
                // 🚀 Switch visible page
                stack_clone.set_visible_child_name(&format!("screen-{}", index));
                // 📋 Update title
                if let Some(title) = titles.get(index) {
                    screen_title_clone.set_text(title);
                }
            });

            self.header.pack_start(&dots.container);
        }

        // 💾 Store all device widgets for future state updates
        *self.device_widgets.borrow_mut() = all_widgets;
    }

    // ============================================================
    // ⚡ ON_DEVICE_STATE_CHANGED — LIVE STATE UPDATE! 🔥
    // ============================================================

    pub fn on_device_state_changed(&self, device_id: &str, new_state: &DeviceStateValue) {
        let widgets = self.device_widgets.borrow();
        for widget in widgets.iter() {
            if widget.device_id() == device_id {
                widget.update(new_state);
            }
        }
    }

    // ============================================================
    // 💚 ON_CONNECTION_STATUS — UPDATE THE DOT! ⚡
    // ============================================================

    pub fn on_connection_status(&self, status: &ConnectionState) {
        // 🔧 Remove all state classes first
        self.connection_dot.remove_css_class("connected");
        self.connection_dot.remove_css_class("disconnected");
        self.connection_dot.remove_css_class("connecting");

        // ✅ Apply the correct class
        match status {
            ConnectionState::Connected => {
                self.connection_dot.add_css_class("connected");
            }
            ConnectionState::Disconnected => {
                self.connection_dot.add_css_class("disconnected");
            }
            ConnectionState::Connecting | ConnectionState::Reconnecting(_) => {
                self.connection_dot.add_css_class("connecting");
            }
        }
    }
}
