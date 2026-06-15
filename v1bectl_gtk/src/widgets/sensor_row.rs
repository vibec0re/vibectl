// 🔥 SENSOR ROW WIDGET - REAL-TIME ENVIRONMENTAL DATA!!! 🚀

use gtk::prelude::*;
use gtk4 as gtk;
use v1bectl_sync::*;

pub struct SensorRow {
    pub container: gtk::Box,
    temp_label: Option<gtk::Label>,
    humidity_label: Option<gtk::Label>,
    device_id: String,
}

impl SensorRow {
    pub fn new(resolved_device_id: &str, show_temp: bool, show_humidity: bool) -> Self {
        // 🔥 Horizontal container
        let container = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        container.add_css_class("device-row");
        container.add_css_class("device-sensor");

        // Name label — initially empty, will be filled when state arrives
        let name_label = gtk::Label::new(None);
        name_label.add_css_class("sensor-label");
        name_label.set_halign(gtk::Align::Start);
        name_label.set_hexpand(true);
        container.append(&name_label);

        // 📊 Values box (right-aligned)
        let values_box = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        values_box.set_halign(gtk::Align::End);

        let temp_label = if show_temp {
            let label = gtk::Label::new(Some("--°C"));
            label.add_css_class("sensor-temp");
            values_box.append(&label);
            Some(label)
        } else {
            None
        };

        let humidity_label = if show_humidity {
            let label = gtk::Label::new(Some("💧 --%"));
            label.add_css_class("sensor-humidity");
            values_box.append(&label);
            Some(label)
        } else {
            None
        };

        container.append(&values_box);

        SensorRow {
            container,
            temp_label,
            humidity_label,
            device_id: resolved_device_id.to_string(),
        }
    }

    pub fn update(&self, state: &DeviceStateValue) {
        if let DeviceStateValue::Sensor(sensor) = state {
            // 🌡️ Update temperature
            if let Some(label) = &self.temp_label {
                match sensor.temperature {
                    Some(t) => label.set_text(&format!("🌡️ {:.1}°C", t)),
                    None => label.set_text("--°C"),
                }
            }

            // 💧 Update humidity
            if let Some(label) = &self.humidity_label {
                match sensor.humidity {
                    Some(h) => label.set_text(&format!("💧 {:.0}%", h)),
                    None => label.set_text("💧 --%"),
                }
            }
        }
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }
}
