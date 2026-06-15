// 🔥 SCREEN BUILDER — KDL CONFIG TO GTK WIDGET TREE!!! 🚀

use gtk::prelude::*;
use gtk4 as gtk;
use libadwaita as adw;
use tokio::sync::mpsc;
use v1bectl_sync::*;

use crate::bridge::ServerCmd;
use crate::config::{self, DeviceRef, Element};
use crate::widgets::light_row::LightRow;
use crate::widgets::outlet_row::OutletRow;
use crate::widgets::sensor_row::SensorRow;

// ============================================================
// 🏗️ DEVICE WIDGET ENUM — POLYMORPHIC DEVICE CONTROL! ⚡
// ============================================================

/// A device widget that can be updated with new state
pub enum DeviceWidget {
    Light(LightRow),
    Outlet(OutletRow),
    Sensor(SensorRow),
}

impl DeviceWidget {
    pub fn update(&self, state: &DeviceStateValue) {
        match self {
            DeviceWidget::Light(w) => w.update(state),
            DeviceWidget::Outlet(w) => w.update(state),
            DeviceWidget::Sensor(w) => w.update(state),
        }
    }

    pub fn device_id(&self) -> &str {
        match self {
            DeviceWidget::Light(w) => w.device_id(),
            DeviceWidget::Outlet(w) => w.device_id(),
            DeviceWidget::Sensor(w) => w.device_id(),
        }
    }
}

// ============================================================
// 🔧 DEVICE RESOLUTION — FIND DEVICES BY ID OR NAME! ⚡
// ============================================================

/// Resolve a DeviceRef to a concrete device_id string.
/// - ById: returned directly
/// - ByName: exact match on device_info.name (substring would shadow "Deko" with "Deko Bunt" etc.)
pub fn resolve_device_ref(device_ref: &DeviceRef, devices: &[DeviceState]) -> Option<String> {
    match device_ref {
        DeviceRef::ById(id) => Some(id.clone()),
        DeviceRef::ByName(name) => devices
            .iter()
            .find(|ds| ds.device_info.name == name.as_str())
            .map(|ds| ds.device_id.clone()),
    }
}

// ============================================================
// 🔥 SCREEN BUILDER — BLAZING FAST WIDGET CONSTRUCTION!!! 🚀
// ============================================================

/// Build a GTK widget tree from a parsed Screen config.
/// Returns the scrolled window (ready to embed) and all device widgets (for state updates).
pub fn build_screen(
    screen: &config::Screen,
    devices: &[DeviceState],
    cmd_tx: &mpsc::Sender<ServerCmd>,
) -> (gtk::ScrolledWindow, Vec<DeviceWidget>) {
    // 🚀 Scrolled window — full height, no horizontal scrollbar
    let scrolled = gtk::ScrolledWindow::new();
    scrolled.set_vexpand(true);
    scrolled.set_hscrollbar_policy(gtk::PolicyType::Never);

    // 🏗️ Clamp for comfortable reading width
    let clamp = adw::Clamp::new();
    clamp.set_maximum_size(700);
    clamp.set_tightening_threshold(500);

    // 📦 Vertical box holding all groups
    let vbox = gtk::Box::new(gtk::Orientation::Vertical, 8);
    vbox.set_margin_top(12);
    vbox.set_margin_bottom(20);
    vbox.set_margin_start(14);
    vbox.set_margin_end(14);

    let mut device_widgets: Vec<DeviceWidget> = Vec::new();

    // ✅ Build each group
    for group in &screen.groups {
        let group_box = gtk::Box::new(gtk::Orientation::Vertical, 0);
        group_box.add_css_class("device-group");

        build_elements(
            &group.elements,
            &group_box,
            devices,
            cmd_tx,
            &mut device_widgets,
        );

        vbox.append(&group_box);
    }

    clamp.set_child(Some(&vbox));
    scrolled.set_child(Some(&clamp));

    (scrolled, device_widgets)
}

// ============================================================
// 🔧 ELEMENT BUILDER — RECURSIVE WIDGET CONSTRUCTION! ⚡
// ============================================================

/// Populate `parent` with widgets for each element in `elements`.
/// Appends any created DeviceWidgets to `device_widgets`.
pub fn build_elements(
    elements: &[Element],
    parent: &gtk::Box,
    devices: &[DeviceState],
    cmd_tx: &mpsc::Sender<ServerCmd>,
    device_widgets: &mut Vec<DeviceWidget>,
) {
    for element in elements {
        match element {
            // 💡 Light element — but adapt to actual device type!
            // Users frequently declare `light` for devices that are actually
            // outlets (e.g., IKEA TRADFRI control outlets wired to lamps).
            // Build the widget that matches the device's real state type so
            // server events actually update the UI.
            Element::Light {
                name,
                device_ref,
                show_switch,
                show_slider,
            } => {
                let device_id = resolve_device_ref(device_ref, devices).unwrap_or_default();

                let device = devices.iter().find(|ds| ds.device_id == device_id);

                match device.map(|ds| &ds.state) {
                    // 🔌 Device is actually an outlet — render as outlet, ignore slider
                    Some(DeviceStateValue::Outlet(_)) => {
                        if *show_slider {
                            tracing::warn!(
                                "🔧 '{}' declared as light with slider but device is an outlet — slider disabled",
                                name
                            );
                        }
                        let row = OutletRow::new(name, &device_id, cmd_tx.clone());
                        if let Some(ds) = device {
                            row.update(&ds.state);
                        }
                        parent.append(&row.container);
                        device_widgets.push(DeviceWidget::Outlet(row));
                    }

                    // 💡 Light (or unresolved — assume light by default)
                    _ => {
                        let row = LightRow::new(
                            name,
                            &device_id,
                            *show_switch,
                            *show_slider,
                            cmd_tx.clone(),
                        );
                        if let Some(ds) = device {
                            row.update(&ds.state);
                        }
                        parent.append(&row.container);
                        device_widgets.push(DeviceWidget::Light(row));
                    }
                }
            }

            // 🔌 Outlet element
            Element::Outlet { name, device_ref } => {
                let device_id = resolve_device_ref(device_ref, devices).unwrap_or_default();

                let row = OutletRow::new(name, &device_id, cmd_tx.clone());

                // 🔥 Apply initial state if device is known
                if let Some(ds) = devices.iter().find(|ds| ds.device_id == device_id) {
                    row.update(&ds.state);
                }

                parent.append(&row.container);
                device_widgets.push(DeviceWidget::Outlet(row));
            }

            // 🌡️ Sensor element (read-only, no cmd_tx needed)
            Element::Sensor {
                device_ref,
                show_temp,
                show_humidity,
            } => {
                let device_id = resolve_device_ref(device_ref, devices).unwrap_or_default();

                let row = SensorRow::new(&device_id, *show_temp, *show_humidity);

                // 🔥 Apply initial state if device is known
                if let Some(ds) = devices.iter().find(|ds| ds.device_id == device_id) {
                    row.update(&ds.state);
                }

                parent.append(&row.container);
                device_widgets.push(DeviceWidget::Sensor(row));
            }

            // 📝 Text label element
            Element::Text { template } => {
                let label = gtk::Label::new(Some(template));
                label.set_halign(gtk::Align::Start);
                parent.append(&label);
            }

            // 📐 Block — horizontal layout of child elements
            Element::Block { elements: children } => {
                let hbox = gtk::Box::new(gtk::Orientation::Horizontal, 8);
                hbox.set_homogeneous(true);
                hbox.add_css_class("device-block");

                for el in children {
                    let child_box = gtk::Box::new(gtk::Orientation::Vertical, 0);
                    build_elements(
                        std::slice::from_ref(el),
                        &child_box,
                        devices,
                        cmd_tx,
                        device_widgets,
                    );
                    hbox.append(&child_box);
                }

                parent.append(&hbox);
            }
        }
    }
}
