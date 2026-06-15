// 🔥 VIBEC0RE DASHBOARD WITH TUI-SCROLLVIEW - SMOOTH SCROLLING! 💖

use crate::App;
use ratatui::layout::{Position, Size};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};
use tui_scrollview::{ScrollView, ScrollViewState};
use v1bectl_sync::DeviceStateValue;

// 🔥 MAIN DASHBOARD RENDER WITH SCROLLVIEW! 💖
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

    // Initialize scroll state if needed
    if app.dashboard_scroll_state.is_none() {
        app.dashboard_scroll_state = Some(ScrollViewState::default());
    }

    // 🔥 CALCULATE CONTENT SIZE AND SCROLL! 💖
    let card_height = 4u16;
    let total_height = (favorites.len() as u16 * card_height).max(area.height);

    // Create scroll view
    let mut scroll_view = ScrollView::new(Size::new(area.width, total_height));

    // 🔥 RENDER ALL CARDS INTO SCROLL VIEW! 💖
    for (index, device) in favorites.iter().enumerate() {
        let card_area = Rect {
            x: 0,
            y: (index as u16 * card_height),
            width: area.width,
            height: card_height,
        };

        let is_selected = index == selected_fav_index;
        render_device_card(&mut scroll_view, device, card_area, is_selected);
    }

    // 🔥 AUTO-SCROLL TO KEEP SELECTED IN VIEW! 💖
    if let Some(ref mut scroll_state) = app.dashboard_scroll_state {
        let selected_y = selected_fav_index as u16 * card_height;
        let viewport_height = area.height;

        // Ensure selected item is visible
        if selected_y < scroll_state.offset().y {
            // Scroll up to show selected
            scroll_state.set_offset(Position::new(0, selected_y));
        } else if selected_y + card_height > scroll_state.offset().y + viewport_height {
            // Scroll down to show selected
            let new_y = (selected_y + card_height).saturating_sub(viewport_height);
            scroll_state.set_offset(Position::new(0, new_y));
        }

        // Render the scroll view
        f.render_stateful_widget(scroll_view, area, scroll_state);
    }
}

// 🔥 RENDER DEVICE CARD! 💖
fn render_device_card(
    scroll_view: &mut ScrollView,
    device: &&crate::AppDevice,
    area: Rect,
    is_selected: bool,
) {
    // 🔥 DYNAMIC BORDER COLOR! 💖
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

    // 🔥 CARD BLOCK WITH ROUNDED BORDERS! 💖
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
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
            Style::default().bg(Color::Rgb(40, 40, 40))
        } else {
            Style::default()
        });

    // Render block
    scroll_view.render_widget(block, area);

    // Inner area for content
    let inner = Rect {
        x: area.x + 1,
        y: area.y + 1,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    };

    // 🔥 SPLIT LAYOUT! 💖
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Left: Device info
            Constraint::Percentage(50), // Right: Status
        ])
        .split(inner);

    // LEFT SIDE - DEVICE INFO
    render_device_info(scroll_view, device, chunks[0], is_selected);

    // RIGHT SIDE - STATUS
    render_device_status(scroll_view, device, chunks[1], is_selected);
}

// 🔥 RENDER DEVICE INFO! 💖
fn render_device_info(
    scroll_view: &mut ScrollView,
    device: &&crate::AppDevice,
    area: Rect,
    is_selected: bool,
) {
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

    let info_lines = vec![
        Line::from(vec![
            Span::raw(" "),
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
            Span::styled("💖", Style::default().fg(Color::Magenta)),
            Span::raw(" "),
            Span::styled(status_icon, Style::default()),
        ]),
        Line::from(vec![
            Span::raw(" "),
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

    scroll_view.render_widget(info_widget, area);
}

// 🔥 RENDER DEVICE STATUS! 💖
fn render_device_status(
    scroll_view: &mut ScrollView,
    device: &&crate::AppDevice,
    area: Rect,
    is_selected: bool,
) {
    let status_lines = match &device.state {
        DeviceStateValue::Light(light) => {
            let power = if light.is_on {
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

            let brightness = if let Some(b) = light.brightness {
                vec![
                    Span::styled(
                        "DIM: ",
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(create_mini_brightness_bar(b)),
                    Span::styled(
                        format!(" {}%", b),
                        Style::default()
                            .fg(brightness_color(b))
                            .add_modifier(Modifier::BOLD),
                    ),
                ]
            } else {
                vec![
                    Span::styled("DIM: ", Style::default().fg(Color::DarkGray)),
                    Span::styled("N/A", Style::default().fg(Color::DarkGray)),
                ]
            };

            vec![Line::from(power), Line::from(brightness)]
        }
        DeviceStateValue::Sensor(sensor) => {
            let temp = if let Some(t) = sensor.temperature {
                vec![
                    Span::styled(
                        "TEMP: ",
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("{:.1}°C", t),
                        Style::default()
                            .fg(temp_color(t))
                            .add_modifier(Modifier::BOLD),
                    ),
                ]
            } else {
                vec![
                    Span::styled("TEMP: ", Style::default().fg(Color::DarkGray)),
                    Span::styled("--", Style::default().fg(Color::DarkGray)),
                ]
            };

            let humidity = if let Some(h) = sensor.humidity {
                vec![
                    Span::styled(
                        "HUM: ",
                        Style::default()
                            .fg(Color::Blue)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("{}%", h),
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

            vec![Line::from(temp), Line::from(humidity)]
        }
        DeviceStateValue::Outlet(outlet) => {
            let power = if outlet.is_on {
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

            let status = if outlet.is_on {
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

            vec![Line::from(power), Line::from(status)]
        }
        DeviceStateValue::Switch(switch) => {
            let state = if switch.is_pressed {
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

            vec![Line::from(state), Line::from("")]
        }
        _ => vec![Line::from(""), Line::from("")],
    };

    let status_widget = Paragraph::new(status_lines).style(if is_selected {
        Style::default().bg(Color::Rgb(40, 40, 40))
    } else {
        Style::default()
    });

    scroll_view.render_widget(status_widget, area);
}

// 🔥 NO FAVORITES MESSAGE! 💖
fn render_no_favorites(f: &mut Frame, area: Rect) {
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

// 🔥 HELPER FUNCTIONS! 💖
fn create_mini_brightness_bar(brightness: u8) -> String {
    let percentage = brightness as f32 / 100.0;
    let bar_width = 10;
    let filled_chars = (percentage * bar_width as f32).round() as usize;

    (0..bar_width)
        .map(|i| if i < filled_chars { '█' } else { '░' })
        .collect()
}

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
