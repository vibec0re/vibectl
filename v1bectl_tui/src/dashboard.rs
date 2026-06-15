// 🔥 VIBEC0RE DASHBOARD - TOTAL CYBER AESTHETIC! 💖

use crate::App;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use v1bectl_sync::DeviceStateValue;

// 🔥 MAIN DASHBOARD RENDER - STACKED CYBER CARDS! 💖
pub fn render_dashboard(f: &mut Frame, app: &mut App, area: Rect) {
    // 🔥 Get favorite devices IN ORDER! 💖
    let mut favorites: Vec<_> = Vec::new();
    for fav_id in &app.favorites {
        if let Some(device) = app.devices.iter().find(|d| &d.info.device_id == fav_id) {
            favorites.push(device);
        }
    }

    if favorites.is_empty() {
        render_no_favorites(f, area);
        return;
    }

    // 🔥 FIND SELECTED FAVORITE INDEX! 💖
    let selected_fav_index = if !favorites.is_empty() && app.selected_device < app.devices.len() {
        let selected_device_id = &app.devices[app.selected_device].info.device_id;
        favorites
            .iter()
            .position(|d| d.info.device_id == *selected_device_id)
            .unwrap_or(0)
    } else {
        0
    };

    // 🔥 STACKED LAYOUT - COMPACT 2-LINE CARDS! SCROLLABLE! 💖
    let card_height = 4; // Height of each card (2 lines + borders)
    let visible_cards = (area.height as usize).saturating_sub(2) / card_height;

    // No container border - just use the full area!
    let inner = area;

    // 🔥 SMART SCROLL TRACKING - KEEPS SELECTED IN VIEW! 💖
    // Calculate scroll offset to keep selected item visible
    let scroll_offset = if favorites.len() <= visible_cards {
        // All items fit - no scrolling needed
        0
    } else if selected_fav_index < app.scroll_offset {
        // Selected is above viewport - scroll up
        selected_fav_index
    } else if selected_fav_index >= app.scroll_offset + visible_cards {
        // Selected is below viewport - scroll down
        selected_fav_index.saturating_sub(visible_cards - 1)
    } else {
        // Selected is already visible - keep current scroll
        app.scroll_offset
    };

    // Update scroll offset in app state
    app.scroll_offset = scroll_offset;

    // Render visible cards
    let cards_to_render = favorites
        .iter()
        .skip(scroll_offset)
        .take(visible_cards)
        .enumerate();

    for (index, device) in cards_to_render {
        let card_area = Rect {
            x: inner.x,
            y: inner.y + (index as u16 * card_height as u16),
            width: inner.width,
            height: (card_height as u16).min(
                inner
                    .height
                    .saturating_sub(index as u16 * card_height as u16),
            ),
        };

        let is_selected = (scroll_offset + index) == selected_fav_index;
        render_device_card(f, device, card_area, is_selected);
    }

    // 🔥 RENDER SCROLLBAR IF NEEDED! 💖
    if favorites.len() > visible_cards {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("▲"))
            .end_symbol(Some("▼"))
            .track_symbol(Some("│"))
            .thumb_symbol("█")
            .style(Style::default().fg(Color::Cyan));

        let mut scrollbar_state = ScrollbarState::new(favorites.len()).position(selected_fav_index);

        f.render_stateful_widget(scrollbar, inner, &mut scrollbar_state);
    }
}

// 🔥 RENDER INDIVIDUAL DEVICE CARD - SPLIT LAYOUT! 💖
fn render_device_card(f: &mut Frame, device: &&crate::AppDevice, area: Rect, is_selected: bool) {
    // 🔥 DYNAMIC BORDER COLOR BASED ON STATE! 💖
    let border_color = if is_selected {
        Color::Yellow
    } else {
        match &device.state {
            DeviceStateValue::Light(light) if light.is_on => Color::Yellow,
            DeviceStateValue::Sensor(_) => Color::Cyan,
            DeviceStateValue::Outlet(outlet) if outlet.is_on => Color::Green,
            _ => Color::DarkGray,
        }
    };

    // 🔥 GLOW EFFECT WITH BOLD BORDER! 💖
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded) // ALWAYS ROUNDED!
        .border_style(
            Style::default()
                .fg(border_color)
                .add_modifier(if is_selected {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
        )
        .style(if is_selected {
            Style::default().bg(Color::Rgb(40, 40, 40)) // DARKER GRAY!
        } else {
            Style::default()
        });

    let inner = block.inner(area);
    f.render_widget(block, area);

    // 🔥 SPLIT LAYOUT - LEFT INFO, RIGHT STATUS! 💖
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Left: Device info
            Constraint::Percentage(50), // Right: Status/controls
        ])
        .split(inner);

    // LEFT SIDE - DEVICE INFO (with padding)
    let device_icon = match device.info.device_type {
        v1bectl_sync::DeviceType::Light => "💡",
        v1bectl_sync::DeviceType::Sensor => "🌡️",
        v1bectl_sync::DeviceType::Switch => "🔘",
        v1bectl_sync::DeviceType::Outlet => "🔌",
        _ => "❓",
    };

    let status_icon = if device.info.reachable {
        "🟢"
    } else {
        "🔴"
    };
    let favorite_icon = "💖";

    let info_lines = vec![
        Line::from(vec![
            Span::raw(" "), // Left padding
            Span::styled(
                format!("{} ", device_icon),
                Style::default().fg(Color::White),
            ),
            Span::styled(
                truncate_string(&device.info.name, 20),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(favorite_icon, Style::default().fg(Color::Magenta)),
            Span::raw(" "),
            Span::styled(status_icon, Style::default()),
        ]),
        Line::from(vec![
            Span::raw(" "), // Left padding
            Span::styled(
                truncate_string(&device.info.device_groups.join(" • "), 30),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
    ];

    let info_widget = Paragraph::new(info_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });
    f.render_widget(info_widget, chunks[0]);

    // RIGHT SIDE - STATUS/CONTROLS
    match &device.state {
        DeviceStateValue::Light(light) => {
            render_light_status(f, light, chunks[1], is_selected);
        }
        DeviceStateValue::Sensor(sensor) => {
            render_sensor_status(f, sensor, chunks[1], is_selected);
        }
        DeviceStateValue::Outlet(outlet) => {
            render_outlet_status(f, outlet, chunks[1], is_selected);
        }
        DeviceStateValue::Switch(switch) => {
            render_switch_status(f, switch, chunks[1], is_selected);
        }
        _ => {}
    }
}

// 💡 RENDER LIGHT STATUS - COMPACT 2-LINE! 💖
fn render_light_status(
    f: &mut Frame,
    light: &v1bectl_sync::LightState,
    area: Rect,
    is_selected: bool,
) {
    let power_text = if light.is_on {
        vec![
            Span::styled(
                "POWER: ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "⚡ ON",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("POWER: ", Style::default().fg(Color::DarkGray)),
            Span::styled("OFF", Style::default().fg(Color::DarkGray)),
        ]
    };

    let brightness_text = if let Some(brightness) = light.brightness {
        let bar = create_mini_brightness_bar(brightness);
        vec![
            Span::styled(
                "DIM: ",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(bar),
            Span::styled(
                format!(" {}%", brightness),
                Style::default()
                    .fg(brightness_color(brightness))
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("DIM: ", Style::default().fg(Color::DarkGray)),
            Span::styled("N/A", Style::default().fg(Color::DarkGray)),
        ]
    };

    let status_lines = vec![Line::from(power_text), Line::from(brightness_text)];

    let status_widget = Paragraph::new(status_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });
    f.render_widget(status_widget, area);
}

// 🌡️ RENDER SENSOR STATUS - COMPACT 2-LINE! 💖
fn render_sensor_status(
    f: &mut Frame,
    sensor: &v1bectl_sync::SensorState,
    area: Rect,
    is_selected: bool,
) {
    let temp_text = if let Some(temp) = sensor.temperature {
        vec![
            Span::styled(
                "TEMP: ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:.1}°C", temp),
                Style::default()
                    .fg(temp_color(temp))
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("TEMP: ", Style::default().fg(Color::DarkGray)),
            Span::styled("--", Style::default().fg(Color::DarkGray)),
        ]
    };

    let humidity_text = if let Some(humidity) = sensor.humidity {
        vec![
            Span::styled(
                "HUM: ",
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}%", humidity),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("HUM: ", Style::default().fg(Color::DarkGray)),
            Span::styled("--", Style::default().fg(Color::DarkGray)),
        ]
    };

    let status_lines = vec![Line::from(temp_text), Line::from(humidity_text)];

    let status_widget = Paragraph::new(status_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });
    f.render_widget(status_widget, area);
}

// 🔌 RENDER OUTLET STATUS - COMPACT 2-LINE! 💖
fn render_outlet_status(
    f: &mut Frame,
    outlet: &v1bectl_sync::OutletState,
    area: Rect,
    is_selected: bool,
) {
    let power_text = if outlet.is_on {
        vec![
            Span::styled(
                "OUTLET: ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "🔌 ON",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("OUTLET: ", Style::default().fg(Color::DarkGray)),
            Span::styled("OFF", Style::default().fg(Color::DarkGray)),
        ]
    };

    let status_text = if outlet.is_on {
        vec![
            Span::styled("STATUS: ", Style::default().fg(Color::Green)),
            Span::styled(
                "POWERED",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled("STATUS: ", Style::default().fg(Color::DarkGray)),
            Span::styled("IDLE", Style::default().fg(Color::DarkGray)),
        ]
    };

    let status_lines = vec![Line::from(power_text), Line::from(status_text)];

    let status_widget = Paragraph::new(status_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });
    f.render_widget(status_widget, area);
}

// 🔘 RENDER SWITCH STATUS - COMPACT 2-LINE! 💖
fn render_switch_status(
    f: &mut Frame,
    switch: &v1bectl_sync::SwitchState,
    area: Rect,
    is_selected: bool,
) {
    let switch_text = if switch.is_pressed {
        vec![
            Span::styled(
                "SWITCH: ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "🔘 PRESSED",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD | Modifier::RAPID_BLINK),
            ),
        ]
    } else {
        vec![
            Span::styled("SWITCH: ", Style::default().fg(Color::DarkGray)),
            Span::styled("○ IDLE", Style::default().fg(Color::DarkGray)),
        ]
    };

    let status_lines = vec![
        Line::from(switch_text),
        Line::from(""), // Empty second line for switches
    ];

    let status_widget = Paragraph::new(status_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });
    f.render_widget(status_widget, area);
}

// 🔥 CREATE MINI BRIGHTNESS BAR - 10 CHARS! 💖
fn create_mini_brightness_bar(brightness: u8) -> String {
    let percentage = brightness as f32 / 100.0;
    let bar_width = 10;
    let filled_chars = (percentage * bar_width as f32).round() as usize;

    let mut bar = String::new();
    for i in 0..bar_width {
        if i < filled_chars {
            bar.push('█');
        } else {
            bar.push('░');
        }
    }
    bar
}

// 🚫 RENDER NO FAVORITES MESSAGE
fn render_no_favorites(f: &mut Frame, area: Rect) {
    // No border - just center the message

    let message = vec![
        Line::from(""),
        Line::from(""),
        Line::from(""),
        Line::from(vec![Span::styled(
            "🔥 NO FAVORITES YET! 🔥",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Press ", Style::default().fg(Color::White)),
            Span::styled(
                "F",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " on any device to add to favorites!",
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Tab → Devices",
            Style::default().fg(Color::DarkGray),
        )]),
    ];

    let paragraph = Paragraph::new(message)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });

    f.render_widget(paragraph, area);
}

// HELPER FUNCTIONS

fn temp_color(temp: f32) -> Color {
    match temp {
        t if t < 18.0 => Color::Blue,
        t if t < 22.0 => Color::Cyan,
        t if t < 26.0 => Color::Green,
        t if t < 30.0 => Color::Yellow,
        _ => Color::Red,
    }
}

fn brightness_color(brightness: u8) -> Color {
    match brightness {
        0..=20 => Color::Red,
        21..=40 => Color::Yellow,
        41..=70 => Color::Green,
        71..=90 => Color::Cyan,
        _ => Color::White,
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len - 1])
    }
}
