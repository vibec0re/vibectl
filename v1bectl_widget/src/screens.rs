//! The `screens.kdl` layout: the curated screen / group / block arrangement
//! the GTK and web clients already render, now driving the sidebar widget too.
//!
//! A config file lets you pick *which* devices show, in *what* order and
//! grouping, under *what* labels — instead of the widget's default
//! auto-grouping by room. When no config is found (or it references only
//! devices the server doesn't know), the widget falls back to that auto
//! layout, so the sidebar is never blank.
//!
//! The schema mirrors `v1bectl_gtk`'s `config` module (the canonical
//! `kdl`-crate parser), trimmed to the `screen` blocks: the widget takes its
//! server address from `$V1BECTL_SERVER` (see `ws::server_url`), so any
//! `server` / `window` nodes a shared file carries are simply ignored here.
//! Keeping the shape identical means the *same*
//! `~/.config/v1bectl/screens.kdl` drives all three clients.

use std::path::PathBuf;

use kdl::KdlDocument;

// ── Config model ─────────────────────────────────────────────────────────────

/// A named screen: an *optional* title plus an ordered list of groups. `title`
/// is `None` when the KDL omits the `title` node — so dropping `title "…"`
/// from a screen drops the header from the widget, rather than falling back to
/// the screen name.
#[derive(Debug, Clone, PartialEq)]
pub struct Screen {
    pub name: String,
    pub title: Option<String>,
    pub groups: Vec<Group>,
}

/// A vertical run of elements, rendered as one visual group.
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    pub elements: Vec<Element>,
}

/// One thing in a group. `Block` nests a horizontal row of elements.
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

/// How an element points at a live device: by the gateway id, or by the
/// device's display name (resolved against the server's device list).
#[derive(Debug, Clone, PartialEq)]
pub enum DeviceRef {
    ById(String),
    ByName(String),
}

// ── Loading ──────────────────────────────────────────────────────────────────

/// Where the widget looks for its layout: `$V1BECTL_SCREENS` if set, else
/// `$XDG_CONFIG_HOME/v1bectl/screens.kdl` (falling back to
/// `~/.config/v1bectl/screens.kdl`) — the same file the GTK app reads. `None`
/// only when neither the env override nor a home directory can be found.
pub fn config_path() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("V1BECTL_SCREENS") {
        return Some(PathBuf::from(explicit));
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("v1bectl").join("screens.kdl"))
}

/// Load the configured screens, or an empty vec (→ auto-by-room fallback).
///
/// Thin I/O glue over [`parse_screens`]: a missing file is the common, silent
/// "no config" case; a present-but-malformed file logs and still falls back —
/// a sidebar widget must always render *something*.
pub fn load() -> Vec<Screen> {
    let Some(path) = config_path() else {
        return Vec::new();
    };
    let content = match std::fs::read_to_string(&path) {
        Ok(content) => content,
        // No config on this machine → auto layout, no noise.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            eprintln!("[v1bectl-widget] {}: {e}", path.display());
            return Vec::new();
        }
    };
    match parse_screens(&content) {
        Ok(screens) => screens,
        Err(e) => {
            eprintln!(
                "[v1bectl-widget] {}: {e}; falling back to auto layout",
                path.display()
            );
            Vec::new()
        }
    }
}

// ── KDL v1 compat ────────────────────────────────────────────────────────────

/// Rewrite KDL v1-style bare `true`/`false`/`null` to KDL v2 `#true`/`#false`/
/// `#null`. Only touches lines with no quoted strings — a safe heuristic for
/// this config format, where booleans are always the lone value on a line.
/// (Verbatim from the GTK parser so both accept the same files.)
fn preprocess_kdl_v1_compat(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    for line in content.lines() {
        let trimmed = line.trim_end();
        let processed = if trimmed.contains('"') {
            line.to_string()
        } else {
            line.replace(" true", " #true")
                .replace(" false", " #false")
                .replace("\ttrue", "\t#true")
                .replace("\tfalse", "\t#false")
                .replace(" null", " #null")
                .replace("\tnull", "\t#null")
        };
        result.push_str(&processed);
        result.push('\n');
    }
    result
}

// ── Parser ───────────────────────────────────────────────────────────────────

/// Parse the `screen` blocks out of a `screens.kdl` document. `server` /
/// `window` nodes are ignored — the widget doesn't use them.
pub fn parse_screens(content: &str) -> Result<Vec<Screen>, String> {
    let content = preprocess_kdl_v1_compat(content);
    let doc: KdlDocument = content
        .parse()
        .map_err(|e| format!("KDL parse error: {e}"))?;
    doc.nodes()
        .iter()
        .filter(|n| n.name().value() == "screen")
        .map(parse_screen)
        .collect()
}

fn parse_screen(node: &kdl::KdlNode) -> Result<Screen, String> {
    let name = first_string(node)
        .ok_or_else(|| "screen node requires a name string argument".to_string())?;

    let children = node.children();

    // `title` child, if any — no default: an absent `title` renders no header.
    let title = children.and_then(|c| c.get("title")).and_then(first_string);

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
    let name = first_string(node).unwrap_or_else(|| "Light".to_string());
    let children = node.children();
    Ok(Element::Light {
        name,
        device_ref: parse_device_ref(children)?,
        show_switch: get_bool(children, "switch", true),
        show_slider: get_bool(children, "slider", true),
    })
}

fn parse_outlet(node: &kdl::KdlNode) -> Result<Element, String> {
    let name = first_string(node).unwrap_or_else(|| "Outlet".to_string());
    let children = node.children();
    Ok(Element::Outlet {
        name,
        device_ref: parse_device_ref(children)?,
    })
}

fn parse_sensor(node: &kdl::KdlNode) -> Result<Element, String> {
    let children = node.children();
    Ok(Element::Sensor {
        device_ref: parse_device_ref(children)?,
        show_temp: get_bool(children, "show-temp", true),
        show_humidity: get_bool(children, "show-humidity", true),
    })
}

fn parse_text(node: &kdl::KdlNode) -> Result<Element, String> {
    let template = first_string(node)
        .ok_or_else(|| "text node requires a template string argument".to_string())?;
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

/// `device-id "…"` (→ [`DeviceRef::ById`]) takes precedence over `device "…"`
/// (→ [`DeviceRef::ByName`]); error if neither is present.
fn parse_device_ref(doc: Option<&KdlDocument>) -> Result<DeviceRef, String> {
    let Some(doc) = doc else {
        return Err("Element requires a device or device-id child node".to_string());
    };
    if let Some(node) = doc.get("device-id") {
        let id =
            first_string(node).ok_or_else(|| "device-id requires a string argument".to_string())?;
        return Ok(DeviceRef::ById(id));
    }
    if let Some(node) = doc.get("device") {
        let name =
            first_string(node).ok_or_else(|| "device requires a string argument".to_string())?;
        return Ok(DeviceRef::ByName(name));
    }
    Err("Element requires a device or device-id child node".to_string())
}

/// The node's first positional (unnamed) argument, as a string.
fn first_string(node: &kdl::KdlNode) -> Option<String> {
    node.entries()
        .iter()
        .find(|e| e.name().is_none())
        .and_then(|e| e.value().as_string())
        .map(str::to_string)
}

/// A boolean property on a child doc (e.g. `switch #true`), with a default.
fn get_bool(doc: Option<&KdlDocument>, key: &str, default: bool) -> bool {
    doc.and_then(|d| d.get(key))
        .and_then(|n| n.entries().iter().find(|e| e.name().is_none()))
        .and_then(|e| e.value().as_bool())
        .unwrap_or(default)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn title_is_present_only_when_declared() {
        let with_title = parse_screens(r#"screen "lr" { title "Living Room" }"#).unwrap();
        assert_eq!(with_title[0].name, "lr");
        assert_eq!(with_title[0].title.as_deref(), Some("Living Room"));

        // No `title` node → None (not defaulted to the name), so nothing renders.
        let no_title = parse_screens(r#"screen "bedroom" {}"#).unwrap();
        assert_eq!(no_title[0].title, None);
    }

    #[test]
    fn ignores_server_and_window_nodes() {
        // A file shared with the GTK client carries these; we skip them.
        let screens = parse_screens(
            r#"
server "hub.local" port=31337
window width=420 height=800
screen "s" { title "S" }
"#,
        )
        .unwrap();
        assert_eq!(screens.len(), 1);
        assert_eq!(screens[0].title.as_deref(), Some("S"));
    }

    #[test]
    fn parses_light_by_name_with_v1_booleans() {
        // Bare (v1) booleans, like the repo's own screens.kdl.
        let screens = parse_screens(
            r#"
screen "s" {
    group {
        light "MAIN" {
            device "LR Shelf"
            switch true
            slider false
        }
    }
}
"#,
        )
        .unwrap();
        assert!(matches!(
            &screens[0].groups[0].elements[0],
            Element::Light { name, device_ref: DeviceRef::ByName(n), show_switch: true, show_slider: false }
                if name == "MAIN" && n == "LR Shelf"
        ));
    }

    #[test]
    fn light_by_id_defaults_switch_and_slider_on() {
        let screens =
            parse_screens(r#"screen "s" { group { light { device-id "abc_1" } } }"#).unwrap();
        assert!(matches!(
            &screens[0].groups[0].elements[0],
            Element::Light { name, device_ref: DeviceRef::ById(id), show_switch: true, show_slider: true }
                if name == "Light" && id == "abc_1"
        ));
    }

    #[test]
    fn parses_sensor_flags() {
        let screens = parse_screens(
            r#"
screen "s" {
    group {
        sensor {
            device "Wohnzimmer"
            show-temp true
            show-humidity false
        }
    }
}
"#,
        )
        .unwrap();
        assert!(matches!(
            &screens[0].groups[0].elements[0],
            Element::Sensor { device_ref: DeviceRef::ByName(n), show_temp: true, show_humidity: false }
                if n == "Wohnzimmer"
        ));
    }

    #[test]
    fn parses_block_of_lights() {
        let screens = parse_screens(
            r#"
screen "s" {
    group {
        block {
            light "A" { device "LightA" }
            light "B" { device "LightB" }
        }
    }
}
"#,
        )
        .unwrap();
        let Element::Block { elements } = &screens[0].groups[0].elements[0] else {
            panic!("expected a block");
        };
        assert_eq!(elements.len(), 2);
        assert!(matches!(&elements[0], Element::Light { name, .. } if name == "A"));
    }

    #[test]
    fn missing_device_ref_is_an_error() {
        let err = parse_screens(r#"screen "s" { group { light "X" {} } }"#).unwrap_err();
        assert!(err.contains("device"), "got: {err}");
    }

    #[test]
    fn unknown_element_is_an_error() {
        let err = parse_screens(r#"screen "s" { group { doohickey {} } }"#).unwrap_err();
        assert!(err.contains("Unknown element type"), "got: {err}");
    }

    #[test]
    fn parses_the_repos_own_screens_kdl() {
        // Guard that the layout checked in at the repo root actually parses —
        // without hard-coding its contents (so editing it won't break this).
        let screens = parse_screens(include_str!("../../screens.kdl")).unwrap();
        assert!(
            !screens.is_empty(),
            "the shipped screens.kdl defines a screen"
        );
        assert!(
            screens.iter().all(|s| !s.groups.is_empty()),
            "every shipped screen has at least one group"
        );
    }
}
