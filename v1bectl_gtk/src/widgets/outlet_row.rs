// 🔥 OUTLET ROW WIDGET - SMART POWER CONTROL!!! 🚀

use gtk::prelude::*;
use gtk4 as gtk;
use std::cell::Cell;
use std::rc::Rc;
use tokio::sync::mpsc;
use v1bectl_sync::*;

use crate::bridge::ServerCmd;

pub struct OutletRow {
    pub container: gtk::Box,
    switch: gtk::Switch,
    device_id: Rc<String>,
    updating: Rc<Cell<bool>>,
}

impl OutletRow {
    pub fn new(name: &str, resolved_device_id: &str, cmd_tx: mpsc::Sender<ServerCmd>) -> Self {
        // 🔥 Horizontal container
        let container = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        container.add_css_class("device-row");
        container.add_css_class("device-outlet");
        container.add_css_class("device-unknown");

        let device_id = Rc::new(resolved_device_id.to_string());
        let updating = Rc::new(Cell::new(false));

        let name_label = gtk::Label::new(Some(name));
        name_label.add_css_class("device-name");
        name_label.set_halign(gtk::Align::Start);
        name_label.set_hexpand(true);
        container.append(&name_label);

        let switch = gtk::Switch::new();
        switch.set_valign(gtk::Align::Center);
        container.append(&switch);

        // ⚡ Wire up switch signal
        {
            let device_id_clone = Rc::clone(&device_id);
            let updating_clone = Rc::clone(&updating);
            let cmd_tx_clone = cmd_tx.clone();
            switch.connect_state_set(move |_switch, is_on| {
                if !updating_clone.get() {
                    let _ = cmd_tx_clone.try_send(ServerCmd::SetOutletState {
                        device_id: (*device_id_clone).clone(),
                        is_on,
                    });
                }
                glib::Propagation::Proceed
            });
        }

        OutletRow {
            container,
            switch,
            device_id,
            updating,
        }
    }

    pub fn update(&self, state: &DeviceStateValue) {
        // 🔧 Suppress signal feedback while updating
        self.updating.set(true);

        if let DeviceStateValue::Outlet(outlet) = state {
            self.switch.set_active(outlet.is_on);

            // ✅ Toggle CSS state classes
            self.container.remove_css_class("device-on");
            self.container.remove_css_class("device-off");
            self.container.remove_css_class("device-unknown");

            if outlet.is_on {
                self.container.add_css_class("device-on");
            } else {
                self.container.add_css_class("device-off");
            }
        }

        self.updating.set(false);
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }
}
