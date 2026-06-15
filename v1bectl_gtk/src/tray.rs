// 🔥 VIBEC0RE SYSTEM TRAY — HIDE TO TRAY, BACKGROUND VIBES!!! 🚀

use ksni::{self, menu::StandardItem, MenuItem};

/// 🔥 System tray icon — tracks window visibility state
pub struct V1bectlTray {
    pub visible: bool,
}

impl V1bectlTray {
    pub fn new() -> Self {
        Self { visible: true }
    }
}

impl ksni::Tray for V1bectlTray {
    fn id(&self) -> String {
        "v1bectl".to_string()
    }

    fn icon_name(&self) -> String {
        "network-server-symbolic".to_string()
    }

    fn title(&self) -> String {
        "v1bectl".to_string()
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        vec![
            MenuItem::Standard(StandardItem {
                label: if self.visible { "Hide" } else { "Show" }.to_string(),
                activate: Box::new(|tray: &mut Self| {
                    tray.visible = !tray.visible;
                }),
                ..Default::default()
            }),
            MenuItem::Separator,
            MenuItem::Standard(StandardItem {
                label: "Quit".to_string(),
                activate: Box::new(|_: &mut Self| {
                    std::process::exit(0);
                }),
                ..Default::default()
            }),
        ]
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        // ⚡ Left-click toggles window visibility
        self.visible = !self.visible;
    }
}

/// 🚀 Spawn the tray service in a background thread, return a handle
pub fn spawn_tray() -> ksni::Handle<V1bectlTray> {
    let service = ksni::TrayService::new(V1bectlTray::new());
    let handle = service.handle();
    service.spawn();
    handle
}
