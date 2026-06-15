// 🔥 SCREEN CONFIG PARSER - SIMPLE KDL-STYLE PARSER! 💖
// Hand-rolled parser - no external deps needed!

use serde::{Deserialize, Serialize};

// 🔥 SCREEN MODEL 💖
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Screen {
    pub name: String,
    pub title: String,
    pub groups: Vec<Group>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub elements: Vec<Element>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Element {
    Sensor {
        device_ref: DeviceRef,
        show_temp: bool,
        show_humidity: bool,
    },
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
    Text {
        template: String,
    },
    // 🔥 BLOCK FOR HORIZONTAL GROUPING! 💖
    Block {
        elements: Vec<Element>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeviceRef {
    ById(String),
    ByName(String),
}

// 🔥 SIMPLE KDL-STYLE PARSER 💖
// Parses a subset of KDL - just enough for our screen configs

pub fn parse_screens(content: &str) -> Result<Vec<Screen>, String> {
    let mut screens = Vec::new();
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.peek() {
        skip_whitespace_and_comments(&mut chars);

        if chars.peek().is_none() {
            break;
        }

        let word = read_identifier(&mut chars);
        if word == "screen" {
            skip_whitespace(&mut chars);
            let name = read_string(&mut chars)?;
            skip_whitespace(&mut chars);
            expect_char(&mut chars, '{')?;

            let screen = parse_screen_body(&mut chars, &name)?;
            screens.push(screen);
        }
    }

    Ok(screens)
}

fn parse_screen_body(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    name: &str,
) -> Result<Screen, String> {
    let mut title = name.to_string();
    let mut groups = Vec::new();

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "title" => {
                        title = read_string(chars)?;
                    }
                    "group" => {
                        expect_char(chars, '{')?;
                        groups.push(parse_group_body(chars)?);
                    }
                    "" => break,
                    _ => {
                        // Skip unknown node
                        skip_block(chars);
                    }
                }
            }
            None => break,
        }
    }

    Ok(Screen {
        name: name.to_string(),
        title,
        groups,
    })
}

fn parse_group_body(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Group, String> {
    let mut elements = Vec::new();

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "sensor" => {
                        expect_char(chars, '{')?;
                        elements.push(parse_sensor_body(chars)?);
                    }
                    "light" => {
                        let name = read_string(chars).unwrap_or_else(|_| "Light".to_string());
                        skip_whitespace(chars);
                        expect_char(chars, '{')?;
                        elements.push(parse_light_body(chars, &name)?);
                    }
                    "outlet" => {
                        let name = read_string(chars).unwrap_or_else(|_| "Outlet".to_string());
                        skip_whitespace(chars);
                        expect_char(chars, '{')?;
                        elements.push(parse_outlet_body(chars, &name)?);
                    }
                    "text" => {
                        let template = read_string(chars).unwrap_or_default();
                        elements.push(Element::Text { template });
                    }
                    // 🔥 BLOCK FOR HORIZONTAL GROUPING! 💖
                    "block" => {
                        expect_char(chars, '{')?;
                        elements.push(parse_block_body(chars)?);
                    }
                    "" => break,
                    _ => {
                        skip_block(chars);
                    }
                }
            }
            None => break,
        }
    }

    Ok(Group { elements })
}

fn parse_sensor_body(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Element, String> {
    let mut device_ref = None;
    let mut show_temp = true;
    let mut show_humidity = true;

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "device" => {
                        let name = read_string(chars)?;
                        device_ref = Some(DeviceRef::ByName(name));
                    }
                    "device-id" => {
                        let id = read_string(chars)?;
                        device_ref = Some(DeviceRef::ById(id));
                    }
                    "show-temp" => {
                        show_temp = read_bool(chars);
                    }
                    "show-humidity" => {
                        show_humidity = read_bool(chars);
                    }
                    "" => break,
                    _ => {}
                }
            }
            None => break,
        }
    }

    Ok(Element::Sensor {
        device_ref: device_ref.ok_or("sensor requires device or device-id")?,
        show_temp,
        show_humidity,
    })
}

fn parse_light_body(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    name: &str,
) -> Result<Element, String> {
    let mut device_ref = None;
    let mut show_switch = true;
    let mut show_slider = true;

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "device" => {
                        let val = read_string(chars)?;
                        device_ref = Some(DeviceRef::ByName(val));
                    }
                    "device-id" => {
                        let id = read_string(chars)?;
                        device_ref = Some(DeviceRef::ById(id));
                    }
                    "switch" => {
                        show_switch = read_bool(chars);
                    }
                    "slider" => {
                        show_slider = read_bool(chars);
                    }
                    "" => break,
                    _ => {}
                }
            }
            None => break,
        }
    }

    Ok(Element::Light {
        name: name.to_string(),
        device_ref: device_ref.ok_or("light requires device or device-id")?,
        show_switch,
        show_slider,
    })
}

fn parse_outlet_body(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    name: &str,
) -> Result<Element, String> {
    let mut device_ref = None;

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "device" => {
                        let val = read_string(chars)?;
                        device_ref = Some(DeviceRef::ByName(val));
                    }
                    "device-id" => {
                        let id = read_string(chars)?;
                        device_ref = Some(DeviceRef::ById(id));
                    }
                    "" => break,
                    _ => {}
                }
            }
            None => break,
        }
    }

    Ok(Element::Outlet {
        name: name.to_string(),
        device_ref: device_ref.ok_or("outlet requires device or device-id")?,
    })
}

// 🔥 BLOCK PARSER - HORIZONTAL GROUPING! 💖
fn parse_block_body(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Element, String> {
    let mut elements = Vec::new();

    loop {
        skip_whitespace_and_comments(chars);

        match chars.peek() {
            Some('}') => {
                chars.next();
                break;
            }
            Some(_) => {
                let keyword = read_identifier(chars);
                skip_whitespace(chars);

                match keyword.as_str() {
                    "sensor" => {
                        expect_char(chars, '{')?;
                        elements.push(parse_sensor_body(chars)?);
                    }
                    "light" => {
                        let name = read_string(chars).unwrap_or_else(|_| "Light".to_string());
                        skip_whitespace(chars);
                        expect_char(chars, '{')?;
                        elements.push(parse_light_body(chars, &name)?);
                    }
                    "outlet" => {
                        let name = read_string(chars).unwrap_or_else(|_| "Outlet".to_string());
                        skip_whitespace(chars);
                        expect_char(chars, '{')?;
                        elements.push(parse_outlet_body(chars, &name)?);
                    }
                    "" => break,
                    _ => {
                        skip_block(chars);
                    }
                }
            }
            None => break,
        }
    }

    Ok(Element::Block { elements })
}

// 🔥 HELPER FUNCTIONS 💖

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars>) {
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

fn skip_whitespace_and_comments(chars: &mut std::iter::Peekable<std::str::Chars>) {
    loop {
        skip_whitespace(chars);

        // Check for // comment
        if let Some(&'/') = chars.peek() {
            let mut clone = chars.clone();
            clone.next();
            if let Some(&'/') = clone.peek() {
                // Skip until end of line
                chars.next();
                chars.next();
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c == '\n' {
                        break;
                    }
                }
                continue;
            }
        }

        break;
    }
}

fn read_identifier(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut result = String::new();

    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' || c == '-' {
            result.push(c);
            chars.next();
        } else {
            break;
        }
    }

    result
}

fn read_string(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<String, String> {
    skip_whitespace(chars);

    match chars.peek() {
        Some(&'"') => {
            chars.next(); // consume opening quote
            let mut result = String::new();

            while let Some(c) = chars.next() {
                if c == '"' {
                    return Ok(result);
                }
                if c == '\\' {
                    if let Some(escaped) = chars.next() {
                        match escaped {
                            'n' => result.push('\n'),
                            't' => result.push('\t'),
                            '"' => result.push('"'),
                            '\\' => result.push('\\'),
                            _ => result.push(escaped),
                        }
                    }
                } else {
                    result.push(c);
                }
            }

            Err("Unterminated string".to_string())
        }
        _ => Err("Expected string".to_string()),
    }
}

fn read_bool(chars: &mut std::iter::Peekable<std::str::Chars>) -> bool {
    let word = read_identifier(chars);
    word == "true"
}

fn expect_char(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    expected: char,
) -> Result<(), String> {
    skip_whitespace(chars);
    match chars.next() {
        Some(c) if c == expected => Ok(()),
        Some(c) => Err(format!("Expected '{}', got '{}'", expected, c)),
        None => Err(format!("Expected '{}', got EOF", expected)),
    }
}

fn skip_block(chars: &mut std::iter::Peekable<std::str::Chars>) {
    skip_whitespace(chars);

    // Skip any string argument
    if let Some(&'"') = chars.peek() {
        let _ = read_string(chars);
        skip_whitespace(chars);
    }

    // If there's a block, skip it
    if let Some(&'{') = chars.peek() {
        chars.next();
        let mut depth = 1;
        while depth > 0 {
            match chars.next() {
                Some('{') => depth += 1,
                Some('}') => depth -= 1,
                None => break,
                _ => {}
            }
        }
    }
}

// 🔥 DEFAULT CONFIG FOR TESTING! 💖
pub fn default_config() -> &'static str {
    r#"
screen "home" {
    title "Home"

    group {
        sensor {
            device "Living Room"
            show-temp true
            show-humidity true
        }
    }

    group {
        light "Main" {
            device-id "demo-light-1"
            switch true
            slider true
        }
        light "Accent" {
            device-id "demo-light-2"
            switch true
            slider true
        }
    }
}
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_screen() {
        let kdl = r#"
screen "test" {
    title "Test Screen"

    group {
        light "Main" {
            device-id "abc123"
            switch true
            slider true
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens.len(), 1);
        assert_eq!(screens[0].name, "test");
        assert_eq!(screens[0].title, "Test Screen");
        assert_eq!(screens[0].groups.len(), 1);
    }

    #[test]
    fn test_parse_multiple_screens() {
        let kdl = r#"
screen "living" {
    title "Living Room"
    group {
        light "Main" {
            device-id "light-1"
        }
    }
}

screen "bedroom" {
    title "Bedroom"
    group {
        light "Ceiling" {
            device-id "light-2"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens.len(), 2);
        assert_eq!(screens[0].name, "living");
        assert_eq!(screens[1].name, "bedroom");
    }

    #[test]
    fn test_parse_sensor_element() {
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        sensor {
            device "Living Room Sensor"
            show-temp true
            show-humidity false
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens[0].groups[0].elements.len(), 1);

        match &screens[0].groups[0].elements[0] {
            Element::Sensor {
                device_ref,
                show_temp,
                show_humidity,
            } => {
                assert!(matches!(device_ref, DeviceRef::ByName(n) if n == "Living Room Sensor"));
                assert_eq!(*show_temp, true);
                assert_eq!(*show_humidity, false);
            }
            _ => panic!("Expected Sensor element"),
        }
    }

    #[test]
    fn test_parse_light_by_name() {
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        light "Accent" {
            device "My Light"
            switch true
            slider false
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();

        match &screens[0].groups[0].elements[0] {
            Element::Light {
                name,
                device_ref,
                show_switch,
                show_slider,
            } => {
                assert_eq!(name, "Accent");
                assert!(matches!(device_ref, DeviceRef::ByName(n) if n == "My Light"));
                assert_eq!(*show_switch, true);
                assert_eq!(*show_slider, false);
            }
            _ => panic!("Expected Light element"),
        }
    }

    #[test]
    fn test_parse_light_by_id() {
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        light "Main" {
            device-id "abc123_1"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();

        match &screens[0].groups[0].elements[0] {
            Element::Light { device_ref, .. } => {
                assert!(matches!(device_ref, DeviceRef::ById(id) if id == "abc123_1"));
            }
            _ => panic!("Expected Light element"),
        }
    }

    #[test]
    fn test_parse_outlet() {
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        outlet "TV Plug" {
            device-id "outlet-1"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();

        match &screens[0].groups[0].elements[0] {
            Element::Outlet { name, device_ref } => {
                assert_eq!(name, "TV Plug");
                assert!(matches!(device_ref, DeviceRef::ById(id) if id == "outlet-1"));
            }
            _ => panic!("Expected Outlet element"),
        }
    }

    #[test]
    fn test_parse_multiple_groups() {
        let kdl = r#"
screen "test" {
    title "Test"

    group {
        sensor {
            device "Sensor 1"
        }
    }

    group {
        light "Light 1" {
            device-id "l1"
        }
        light "Light 2" {
            device-id "l2"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens[0].groups.len(), 2);
        assert_eq!(screens[0].groups[0].elements.len(), 1);
        assert_eq!(screens[0].groups[1].elements.len(), 2);
    }

    #[test]
    fn test_parse_with_comments() {
        let kdl = r#"
// This is a comment
screen "test" {
    title "Test"
    // Another comment
    group {
        // Comment before light
        light "Main" {
            device-id "abc"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens.len(), 1);
        assert_eq!(screens[0].groups[0].elements.len(), 1);
    }

    #[test]
    fn test_parse_default_config() {
        let screens = parse_screens(default_config()).unwrap();
        assert!(!screens.is_empty());
        assert!(!screens[0].groups.is_empty());
    }

    #[test]
    fn test_default_values() {
        // switch and slider should default to true
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        light "Main" {
            device-id "abc"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();

        match &screens[0].groups[0].elements[0] {
            Element::Light {
                show_switch,
                show_slider,
                ..
            } => {
                assert_eq!(*show_switch, true);
                assert_eq!(*show_slider, true);
            }
            _ => panic!("Expected Light element"),
        }
    }

    #[test]
    fn test_sensor_defaults() {
        let kdl = r#"
screen "test" {
    title "Test"
    group {
        sensor {
            device "Sensor"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();

        match &screens[0].groups[0].elements[0] {
            Element::Sensor {
                show_temp,
                show_humidity,
                ..
            } => {
                assert_eq!(*show_temp, true);
                assert_eq!(*show_humidity, true);
            }
            _ => panic!("Expected Sensor element"),
        }
    }

    #[test]
    fn test_title_defaults_to_name() {
        let kdl = r#"
screen "my-screen" {
    group {
        light "Main" {
            device-id "abc"
        }
    }
}
"#;

        let screens = parse_screens(kdl).unwrap();
        assert_eq!(screens[0].title, "my-screen");
    }

    #[test]
    fn test_empty_config() {
        let kdl = "";
        let screens = parse_screens(kdl).unwrap();
        assert!(screens.is_empty());
    }

    #[test]
    fn test_only_comments() {
        let kdl = r#"
// Just a comment
// Another comment
"#;
        let screens = parse_screens(kdl).unwrap();
        assert!(screens.is_empty());
    }
}
