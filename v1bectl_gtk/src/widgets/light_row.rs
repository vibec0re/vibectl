// 🔥 LIGHT ROW WIDGET - BLAZING FAST LIGHT CONTROL!!! 🚀

use gtk::prelude::*;
use gtk4 as gtk;
use std::cell::Cell;
use std::rc::Rc;
use tokio::sync::mpsc;
use v1bectl_sync::*;

use crate::bridge::ServerCmd;

pub struct LightRow {
    pub container: gtk::Box,
    switch: gtk::Switch,
    slider: Option<gtk::Scale>,
    brightness_label: Option<gtk::Label>,
    device_id: Rc<String>,
    updating: Rc<Cell<bool>>, // suppress signal feedback while updating from server
}

impl LightRow {
    pub fn new(
        name: &str,
        resolved_device_id: &str,
        show_switch: bool,
        show_slider: bool,
        cmd_tx: mpsc::Sender<ServerCmd>,
    ) -> Self {
        // 🔥 Main vertical container
        let container = gtk::Box::new(gtk::Orientation::Vertical, 4);
        container.add_css_class("device-row");
        container.add_css_class("device-light");
        container.add_css_class("device-unknown");

        let device_id = Rc::new(resolved_device_id.to_string());
        let updating = Rc::new(Cell::new(false));

        // 🚀 Top row: name label + switch — vexpand so content centers when
        // the row's min-height exceeds natural content height
        let top_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        top_row.add_css_class("device-top-row");
        top_row.set_vexpand(true);
        top_row.set_valign(gtk::Align::Center);

        let name_label = gtk::Label::new(Some(name));
        name_label.add_css_class("device-name");
        name_label.set_halign(gtk::Align::Start);
        name_label.set_hexpand(true);
        top_row.append(&name_label);

        let switch = gtk::Switch::new();
        switch.set_valign(gtk::Align::Center);

        if show_switch {
            top_row.append(&switch);
        }

        container.append(&top_row);

        // ⚡ Wire up switch signal
        {
            let device_id_clone = Rc::clone(&device_id);
            let updating_clone = Rc::clone(&updating);
            let cmd_tx_clone = cmd_tx.clone();
            switch.connect_state_set(move |_switch, is_on| {
                if !updating_clone.get() {
                    let _ = cmd_tx_clone.try_send(ServerCmd::SetLightState {
                        device_id: (*device_id_clone).clone(),
                        is_on: Some(is_on),
                        brightness: None,
                        color_temp: None,
                    });
                }
                glib::Propagation::Proceed
            });
        }

        // 💡 Optional brightness slider row
        let (slider, brightness_label) = if show_slider {
            let slider_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
            slider_row.add_css_class("brightness-row");

            let bri_label = gtk::Label::new(Some("--"));
            bri_label.add_css_class("brightness-label");
            bri_label.set_width_chars(4);
            slider_row.append(&bri_label);

            let scale = gtk::Scale::with_range(gtk::Orientation::Horizontal, 0.0, 100.0, 1.0);
            scale.set_draw_value(false);
            scale.add_css_class("brightness-slider");
            scale.set_hexpand(true);
            slider_row.append(&scale);

            container.append(&slider_row);

            // ⚡ Wire up scale signal
            {
                let device_id_clone = Rc::clone(&device_id);
                let updating_clone = Rc::clone(&updating);
                let cmd_tx_clone = cmd_tx.clone();
                let bri_label_clone = bri_label.clone();
                scale.connect_value_changed(move |scale| {
                    let val = scale.value() as u8;
                    bri_label_clone.set_text(&format!("{}%", val));
                    if !updating_clone.get() {
                        let _ = cmd_tx_clone.try_send(ServerCmd::SetLightState {
                            device_id: (*device_id_clone).clone(),
                            is_on: if val > 0 { Some(true) } else { None },
                            brightness: Some(val),
                            color_temp: None,
                        });
                    }
                });
            }

            (Some(scale), Some(bri_label))
        } else {
            (None, None)
        };

        LightRow {
            container,
            switch,
            slider,
            brightness_label,
            device_id,
            updating,
        }
    }

    pub fn update(&self, state: &DeviceStateValue) {
        // 🔧 Suppress signal feedback while updating
        self.updating.set(true);

        if let DeviceStateValue::Light(light) = state {
            self.switch.set_active(light.is_on);

            if let Some(slider) = &self.slider {
                let brightness = light.brightness.unwrap_or(0) as f64;
                slider.set_value(brightness);
            }

            if let Some(bri_label) = &self.brightness_label {
                match light.brightness {
                    Some(b) => bri_label.set_text(&format!("{}%", b)),
                    None => bri_label.set_text("--"),
                }
            }

            // ✅ Toggle CSS state classes
            self.container.remove_css_class("device-on");
            self.container.remove_css_class("device-off");
            self.container.remove_css_class("device-unknown");

            if light.is_on {
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
