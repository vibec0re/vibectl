// Virtual device system - VIBEC0RE MAGIC! 🔥
pub mod button_controller; // 🔥 BUTTON EVENT CONTROLLER! 💖
pub mod config;
pub mod dummy;
pub mod event_producer;
pub mod light_group;
pub mod light_group_linear; // 🔥 LINEAR BRIGHTNESS MAPPING! 💖
pub mod manager;
pub mod scene_controller;
pub mod virtual_device; // 🔥 TOML CONFIG SUPPORT! 💖

pub use button_controller::*;
pub use config::*;
pub use dummy::*;
pub use event_producer::*;
pub use light_group::*;
pub use light_group_linear::*;
pub use manager::*;
pub use scene_controller::*;
pub use virtual_device::*;
