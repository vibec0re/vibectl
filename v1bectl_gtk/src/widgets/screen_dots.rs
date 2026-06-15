// 🔥 SCREEN DOTS - NAVIGATION PERFECTED!!! 🚀

use gtk::prelude::*;
use gtk4 as gtk;

pub struct ScreenDots {
    pub container: gtk::Box,
    buttons: Vec<gtk::Button>,
}

impl ScreenDots {
    pub fn new(count: usize, on_select: impl Fn(usize) + 'static) -> Self {
        let container = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        container.set_valign(gtk::Align::Center);

        // 🔧 Wrap callback so each button can call it with its index
        let on_select = std::rc::Rc::new(on_select);

        let mut buttons = Vec::with_capacity(count);

        for i in 0..count {
            let btn = gtk::Button::new();
            btn.add_css_class("screen-dot");
            btn.set_size_request(10, 10);

            if i == 0 {
                btn.add_css_class("active");
            }

            let on_select_clone = std::rc::Rc::clone(&on_select);
            btn.connect_clicked(move |_| {
                on_select_clone(i);
            });

            container.append(&btn);
            buttons.push(btn);
        }

        ScreenDots { container, buttons }
    }

    pub fn set_active(&self, index: usize) {
        for btn in &self.buttons {
            btn.remove_css_class("active");
        }
        if let Some(btn) = self.buttons.get(index) {
            btn.add_css_class("active");
        }
    }
}
