// 🔥 VIRTUAL DEVICE TOML CONFIG STRUCTURES! 💖

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum VirtualDeviceTomlConfig {
    #[serde(rename = "light_group")]
    LightGroup(LightGroupConfig),
    #[serde(rename = "light_group_linear")]
    LightGroupLinear(LightGroupLinearConfig), // 🔥 NEW LINEAR CONFIG TYPE! 💖
    #[serde(rename = "button_controller")]
    ButtonController(ButtonControllerConfig), // 🔥 BUTTON CONTROLLER CONFIG! 💖
    #[serde(rename = "scene_controller")]
    SceneController(SceneControllerConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LightGroupConfig {
    pub device_id: String,
    pub name: String,
    pub members: Vec<String>,
    #[serde(default)]
    pub brightness_curves: HashMap<String, BrightnessCurveConfig>,
    #[serde(default)]
    pub settings: LightGroupSettings,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BrightnessCurveConfig {
    pub min: u8,
    pub max: u8,
}

// 🔥 NEW LINEAR LIGHT GROUP CONFIG! 💖
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LightGroupLinearConfig {
    pub device_id: String,
    pub name: String,
    pub members: HashMap<String, String>, // name -> device_id mapping
    pub brightness: HashMap<String, [u8; 2]>, // name -> [min, max] range
    #[serde(default)]
    pub settings: LightGroupLinearSettings,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LightGroupLinearSettings {
    #[serde(default = "default_transition_time")]
    pub transition_time: u32,
}

impl Default for LightGroupLinearSettings {
    fn default() -> Self {
        Self {
            transition_time: default_transition_time(),
        }
    }
}

// 🔥 BUTTON CONTROLLER CONFIG! 💖
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ButtonControllerConfig {
    pub device_id: String,
    pub name: String,
    pub button: String,                    // Button device ID to listen to
    pub press_on: Vec<serde_json::Value>,  // [cmd, device_id, ...params]
    pub press_off: Vec<serde_json::Value>, // [cmd, device_id, ...params]
    #[serde(default)]
    pub press_on_long: Option<Vec<serde_json::Value>>, // Optional long press
    #[serde(default)]
    pub press_off_long: Option<Vec<serde_json::Value>>, // Optional long press off
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LightGroupSettings {
    #[serde(default = "default_aggregation")]
    pub aggregation: String, // average, min, max, any
    #[serde(default = "default_transition_time")]
    pub transition_time: u32,
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl Default for LightGroupSettings {
    fn default() -> Self {
        Self {
            aggregation: default_aggregation(),
            transition_time: default_transition_time(),
            exclude: Vec::new(),
        }
    }
}

fn default_aggregation() -> String {
    "average".to_string()
}

fn default_transition_time() -> u32 {
    500
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SceneControllerConfig {
    pub device_id: String,
    pub name: String,
    pub scenes: Vec<SceneConfig>,
    #[serde(default)]
    pub settings: SceneControllerSettings,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SceneConfig {
    pub name: String,
    pub display_name: String,
    pub devices: Vec<SceneDeviceConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SceneDeviceConfig {
    pub device_id: String,
    pub state: SceneDeviceState,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum SceneDeviceState {
    #[serde(rename = "light")]
    Light {
        is_on: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        brightness: Option<u8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        color_temp: Option<u16>,
    },
    #[serde(rename = "outlet")]
    Outlet { is_on: bool },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SceneControllerSettings {
    #[serde(default = "default_transition_duration")]
    pub transition_duration: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_scene: Option<String>,
}

impl Default for SceneControllerSettings {
    fn default() -> Self {
        Self {
            transition_duration: default_transition_duration(),
            default_scene: None,
        }
    }
}

fn default_transition_duration() -> u32 {
    1000
}

// 🔥 HELPER TO LOAD ALL CONFIGS FROM DIRECTORY! 💖
use std::fs;
use std::path::Path;

pub async fn load_virtual_devices_from_dir(
    dir: &Path,
) -> anyhow::Result<Vec<VirtualDeviceTomlConfig>> {
    let mut configs = Vec::new();

    // Create directory if it doesn't exist
    if !dir.exists() {
        fs::create_dir_all(dir)?;
        tracing::info!("🔥 Created virtual_devices directory at {:?}", dir);
        return Ok(configs);
    }

    // Read all .toml files
    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            match load_virtual_device_from_file(&path).await {
                Ok(config) => {
                    tracing::info!("✅ Loaded virtual device from {:?}", path);
                    configs.push(config);
                }
                Err(e) => {
                    tracing::error!("❌ Failed to load {:?}: {}", path, e);
                }
            }
        }
    }

    tracing::info!("🔥 Loaded {} virtual devices from {:?}", configs.len(), dir);
    Ok(configs)
}

async fn load_virtual_device_from_file(path: &Path) -> anyhow::Result<VirtualDeviceTomlConfig> {
    let content = fs::read_to_string(path)?;
    let config: VirtualDeviceTomlConfig = toml::from_str(&content)?;
    Ok(config)
}
