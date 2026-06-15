// 🔥 VIBEC0RE CONFIG PARSER — KDL-POWERED SCREEN DEFINITIONS! ⚡

use kdl::KdlDocument;
use std::path::PathBuf;

// ============================================================
// 🏗️ DATA TYPES
// ============================================================

#[derive(Debug, Clone, PartialEq)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub window: WindowConfig,
    pub screens: Vec<Screen>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowConfig {
    pub width: i32,
    pub height: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Screen {
    pub name: String,
    pub title: String,
    pub groups: Vec<Group>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    pub elements: Vec<Element>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Element {
    Light {
        name: String,
        device_ref: DeviceRef,
        show_switch: bool,
        show_slider: bool,
    },
    Outlet {
        name: String,
        device_ref: DeviceRef,
    },
    Sensor {
        device_ref: DeviceRef,
        show_temp: bool,
        show_humidity: bool,
    },
    Text {
        template: String,
    },
    Block {
        elements: Vec<Element>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeviceRef {
    ById(String),
    ByName(String),
}

// ============================================================
// 📁 CONFIG PATH
// ============================================================

/// Returns `~/.config/v1bectl/screens.kdl`
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("v1bectl")
        .join("screens.kdl")
}

// ============================================================
// 🔧 KDL V1 COMPAT — BARE true/false → #true/#false ⚡
// ============================================================

/// Converts KDL v1-style bare `true`/`false`/`null` to KDL v2 `#true`/`#false`/`#null`.
/// Only rewrites tokens on lines that contain no quoted strings (safe heuristic for
/// our config format where booleans are always the last token on a line).
fn preprocess_kdl_v1_compat(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    for line in content.lines() {
        let trimmed = line.trim_end();
        // Check if line ends with bare true/false (not inside a string)
        // Only replace if the boolean is a standalone value (preceded by whitespace)
        let processed = if !trimmed.contains('"') {
            // No strings on this line - safe to replace
            line.replace(" true", " #true")
                .replace(" false", " #false")
                .replace("\ttrue", "\t#true")
                .replace("\tfalse", "\t#false")
                .replace(" null", " #null")
                .replace("\tnull", "\t#null")
        } else {
            line.to_string()
        };
        result.push_str(&processed);
        result.push('\n');
    }
    result
}

// ============================================================
// 🔧 PARSER — ENTRY POINT
// ============================================================

pub fn parse_config(content: &str) -> Result<AppConfig, String> {
    let content = preprocess_kdl_v1_compat(content);
    let doc: KdlDocument = content
        .parse()
        .map_err(|e| format!("KDL parse error: {e}"))?;

    let server = parse_server(&doc)?;
    let window = parse_window(&doc);
    let screens = parse_screens(&doc)?;

    Ok(AppConfig {
        server,
        window,
        screens,
    })
}

// ============================================================
// 🔧 PARSER HELPERS
// ============================================================

fn parse_server(doc: &KdlDocument) -> Result<ServerConfig, String> {
    let Some(node) = doc.get("server") else {
        // No server node → default to localhost:31337
        return Ok(ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 31337,
        });
    };

    // First positional arg is the host
    let host = node
        .entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .ok_or_else(|| "server node requires a host string argument".to_string())?
        .to_string();

    // Optional `port` property, default 31337
    let port = node
        .get("port")
        .and_then(|v| v.as_integer())
        .map(|i| i as u16)
        .unwrap_or(31337);

    Ok(ServerConfig { host, port })
}

fn parse_window(doc: &KdlDocument) -> WindowConfig {
    let node = doc.get("window");
    let width = node
        .and_then(|n| n.get("width"))
        .and_then(|v| v.as_integer())
        .map(|i| i as i32)
        .unwrap_or(420);
    let height = node
        .and_then(|n| n.get("height"))
        .and_then(|v| v.as_integer())
        .map(|i| i as i32)
        .unwrap_or(900);
    WindowConfig { width, height }
}

fn parse_screens(doc: &KdlDocument) -> Result<Vec<Screen>, String> {
    doc.nodes()
        .iter()
        .filter(|n| n.name().value() == "screen")
        .map(parse_screen)
        .collect()
}

fn parse_screen(node: &kdl::KdlNode) -> Result<Screen, String> {
    // First positional arg is the name
    let name = node
        .entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .ok_or_else(|| "screen node requires a name string argument".to_string())?
        .to_string();

    let children = node.children();

    // Optional `title` child node, defaults to name
    let title = children
        .and_then(|c| c.get("title"))
        .and_then(|n| n.entries().iter().find(|e| e.name().is_none()))
        .and_then(|e| e.value().as_string())
        .map(|s| s.to_string())
        .unwrap_or_else(|| name.clone());

    // Parse `group` child nodes
    let groups = children
        .map(|c| {
            c.nodes()
                .iter()
                .filter(|n| n.name().value() == "group")
                .map(parse_group)
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok(Screen {
        name,
        title,
        groups,
    })
}

fn parse_group(node: &kdl::KdlNode) -> Result<Group, String> {
    let elements = node
        .children()
        .map(|c| {
            c.nodes()
                .iter()
                .map(parse_element)
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok(Group { elements })
}

fn parse_element(node: &kdl::KdlNode) -> Result<Element, String> {
    match node.name().value() {
        "light" => parse_light(node),
        "outlet" => parse_outlet(node),
        "sensor" => parse_sensor(node),
        "text" => parse_text(node),
        "block" => parse_block(node),
        other => Err(format!("Unknown element type: {other}")),
    }
}

fn parse_light(node: &kdl::KdlNode) -> Result<Element, String> {
    // Optional name from first positional arg
    let name = node
        .entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Light".to_string());

    let children = node.children();
    let device_ref = parse_device_ref(children)?;
    let show_switch = get_bool(children, "switch", true);
    let show_slider = get_bool(children, "slider", true);

    Ok(Element::Light {
        name,
        device_ref,
        show_switch,
        show_slider,
    })
}

fn parse_outlet(node: &kdl::KdlNode) -> Result<Element, String> {
    let name = node
        .entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Outlet".to_string());

    let children = node.children();
    let device_ref = parse_device_ref(children)?;

    Ok(Element::Outlet { name, device_ref })
}

fn parse_sensor(node: &kdl::KdlNode) -> Result<Element, String> {
    let children = node.children();
    let device_ref = parse_device_ref(children)?;
    let show_temp = get_bool(children, "show-temp", true);
    let show_humidity = get_bool(children, "show-humidity", true);

    Ok(Element::Sensor {
        device_ref,
        show_temp,
        show_humidity,
    })
}

fn parse_text(node: &kdl::KdlNode) -> Result<Element, String> {
    let template = node
        .entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .ok_or_else(|| "text node requires a template string argument".to_string())?
        .to_string();

    Ok(Element::Text { template })
}

fn parse_block(node: &kdl::KdlNode) -> Result<Element, String> {
    let elements = node
        .children()
        .map(|c| {
            c.nodes()
                .iter()
                .map(parse_element)
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok(Element::Block { elements })
}

/// Check for `device-id` first (ById), then `device` (ByName), error if neither.
fn parse_device_ref(doc: Option<&KdlDocument>) -> Result<DeviceRef, String> {
    let Some(doc) = doc else {
        return Err("Element requires a device or device-id child node".to_string());
    };

    if let Some(node) = doc.get("device-id") {
        let id = node
            .entries()
            .iter()
            .find(|e| e.name().is_none())
            .and_then(|e| e.value().as_string())
            .ok_or_else(|| "device-id requires a string argument".to_string())?
            .to_string();
        return Ok(DeviceRef::ById(id));
    }

    if let Some(node) = doc.get("device") {
        let name = node
            .entries()
            .iter()
            .find(|e| e.name().is_none())
            .and_then(|e| e.value().as_string())
            .ok_or_else(|| "device requires a string argument".to_string())?
            .to_string();
        return Ok(DeviceRef::ByName(name));
    }

    Err("Element requires a device or device-id child node".to_string())
}

/// Helper to fetch a boolean property from a child doc, with a default.
fn get_bool(doc: Option<&KdlDocument>, key: &str, default: bool) -> bool {
    doc.and_then(|d| d.get(key))
        .and_then(|n| n.entries().iter().find(|e| e.name().is_none()))
        .and_then(|e| e.value().as_bool())
        .unwrap_or(default)
}

// ============================================================
// 🧪 TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_config() {
        let kdl = r#"server "myhost.local" port=31337"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.server.host, "myhost.local");
        assert_eq!(config.server.port, 31337);
    }

    #[test]
    fn test_parse_server_default_port() {
        let kdl = r#"server "myhost.local""#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.server.host, "myhost.local");
        assert_eq!(config.server.port, 31337);
    }

    #[test]
    fn test_parse_no_server_defaults_to_localhost() {
        let kdl = r#"// no server node"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 31337);
    }

    #[test]
    fn test_parse_screen_with_title() {
        let kdl = r#"
screen "living-room" {
    title "Living Room"
}
"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.screens.len(), 1);
        assert_eq!(config.screens[0].name, "living-room");
        assert_eq!(config.screens[0].title, "Living Room");
    }

    #[test]
    fn test_parse_screen_title_defaults_to_name() {
        let kdl = r#"
screen "bedroom" {
}
"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.screens[0].title, "bedroom");
    }

    #[test]
    fn test_parse_light_by_name() {
        let kdl = r#"
screen "s" {
    group {
        light "Ceiling" {
            device "MyLight"
            switch #true
            slider #false
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        assert!(matches!(
            el,
            Element::Light {
                name,
                device_ref: DeviceRef::ByName(n),
                show_switch: true,
                show_slider: false,
            } if name == "Ceiling" && n == "MyLight"
        ));
    }

    #[test]
    fn test_parse_light_by_id() {
        let kdl = r#"
screen "s" {
    group {
        light {
            device-id "abc123_1"
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        assert!(matches!(
            el,
            Element::Light {
                name,
                device_ref: DeviceRef::ById(id),
                show_switch: true,
                show_slider: true,
            } if name == "Light" && id == "abc123_1"
        ));
    }

    #[test]
    fn test_parse_sensor() {
        let kdl = r#"
screen "s" {
    group {
        sensor {
            device "TempSensor"
            show-temp #true
            show-humidity #false
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        assert!(matches!(
            el,
            Element::Sensor {
                device_ref: DeviceRef::ByName(n),
                show_temp: true,
                show_humidity: false,
            } if n == "TempSensor"
        ));
    }

    #[test]
    fn test_parse_outlet() {
        let kdl = r#"
screen "s" {
    group {
        outlet "Lamp" {
            device-id "outlet_abc_1"
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        assert!(matches!(
            el,
            Element::Outlet {
                name,
                device_ref: DeviceRef::ById(id),
            } if name == "Lamp" && id == "outlet_abc_1"
        ));
    }

    #[test]
    fn test_parse_block() {
        let kdl = r#"
screen "s" {
    group {
        block {
            light "A" {
                device "LightA"
            }
            light "B" {
                device "LightB"
            }
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        if let Element::Block { elements } = el {
            assert_eq!(elements.len(), 2);
            assert!(matches!(&elements[0], Element::Light { name, .. } if name == "A"));
            assert!(matches!(&elements[1], Element::Light { name, .. } if name == "B"));
        } else {
            panic!("Expected Block element, got: {el:?}");
        }
    }

    #[test]
    fn test_parse_full_config() {
        let kdl = r#"
server "hub.local" port=31337

screen "main" {
    title "Main Floor"
    group {
        sensor {
            device "HallwaySensor"
        }
        block {
            light "Hall" {
                device "HallLight"
                switch #true
                slider #true
            }
        }
        light "Porch" {
            device-id "porch_1"
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.server.host, "hub.local");
        assert_eq!(config.server.port, 31337);
        assert_eq!(config.screens.len(), 1);
        assert_eq!(config.screens[0].title, "Main Floor");
        assert_eq!(config.screens[0].groups[0].elements.len(), 3);
        assert!(matches!(
            &config.screens[0].groups[0].elements[0],
            Element::Sensor { device_ref: DeviceRef::ByName(n), .. } if n == "HallwaySensor"
        ));
        assert!(matches!(
            &config.screens[0].groups[0].elements[1],
            Element::Block { .. }
        ));
        assert!(matches!(
            &config.screens[0].groups[0].elements[2],
            Element::Light { device_ref: DeviceRef::ById(id), .. } if id == "porch_1"
        ));
    }

    #[test]
    fn test_parse_multiple_screens() {
        let kdl = r#"
screen "floor1" {
    title "Ground Floor"
}
screen "floor2" {
    title "Upper Floor"
}
"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.screens.len(), 2);
        assert_eq!(config.screens[0].name, "floor1");
        assert_eq!(config.screens[0].title, "Ground Floor");
        assert_eq!(config.screens[1].name, "floor2");
        assert_eq!(config.screens[1].title, "Upper Floor");
    }

    #[test]
    fn test_parse_v1_style_booleans() {
        let kdl = r#"
screen "s" {
    group {
        light "Accent" {
            device "Accent"
            switch true
            slider false
        }
        sensor {
            device "Living Room"
            show-temp true
            show-humidity true
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        let el = &config.screens[0].groups[0].elements[0];
        match el {
            Element::Light {
                show_switch,
                show_slider,
                ..
            } => {
                assert!(*show_switch);
                assert!(!(*show_slider));
            }
            _ => panic!("expected Light"),
        }
    }

    #[test]
    fn test_parse_full_config() {
        let kdl = r#"
server "localhost" port=31337

screen "living_room" {
    title "Living Room"
    group {
        sensor {
            device "Living Room"
            show-temp true
            show-humidity true
        }
        block {
            light "Accent" {
                device "Accent"
                switch true
                slider false
            }
            light "ACC" {
                device "Accent Light"
                switch true
                slider false
            }
        }
        light "MAIN" {
            device "Shelf Light"
            switch true
            slider true
        }
    }
}
"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.server.host, "localhost");
        assert_eq!(config.screens[0].groups[0].elements.len(), 3);
    }

    #[test]
    fn test_parse_window_config() {
        let kdl = r#"window width=500 height=1000"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.window.width, 500);
        assert_eq!(config.window.height, 1000);
    }

    #[test]
    fn test_parse_window_defaults() {
        let kdl = r#"// no window node"#;
        let config = parse_config(kdl).unwrap();
        assert_eq!(config.window.width, 420);
        assert_eq!(config.window.height, 900);
    }
}
