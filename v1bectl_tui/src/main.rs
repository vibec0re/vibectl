// kept: original flat dashboard renderer, superseded by the scrollview
// dashboard below but retained as a reference/fallback implementation.
#[allow(dead_code)]
mod dashboard;
mod dashboard_scrollview; // 🔥 NEW SCROLLVIEW DASHBOARD! 💖  // 🔥 VIBEC0RE DASHBOARD MODULE! 💖

use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Gauge, List, ListItem, ListState, Paragraph, Tabs, Wrap},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tui_scrollview::ScrollViewState; // 🔥 FOR SCROLLVIEW DASHBOARD! 💖
use uuid::Uuid;
use v1bectl_sync::*;
use v1bectl_virtual::*;

#[derive(Parser)]
#[command(name = "v1bectl_tui")]
#[command(about = "🔥 VIBEC0RE TUI - Ultimate Home Automation Interface! 🚀")]
struct Cli {
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:31337")]
    server: String,
}

// WebSocket API Messages - CBOR encoded! 🔥
#[derive(Serialize, Deserialize, Debug)]
struct ApiMessage {
    correlation_id: String,
    message_type: ApiMessageType,
    payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiRequest {
    DiscoverDevices,
    GetDeviceState {
        device_id: String,
    },
    SetLightState {
        device_id: String,
        is_on: Option<bool>,
        brightness: Option<u8>,
        color_temp: Option<u16>,
        rgb_color: Option<RgbColor>,
    },
    SetOutletState {
        device_id: String,
        is_on: bool,
    },
    CreateVirtualDevice {
        config: VirtualDeviceConfig,
    },
    ActivateScene {
        device_id: String,
        scene_name: String,
    },
    Subscribe {
        device_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    }, // 🔥 Fixed to match server!
    DeviceState {
        state: DeviceStateValue,
    },
    LightUpdated {
        new_state: LightState,
    },
    VirtualDeviceCreated {
        device_id: String,
    },
    SceneActivated {
        device_id: String,
        scene_name: String,
    },
    SubscriptionStarted {
        subscriber_id: String,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug)]
struct AppDevice {
    info: DeviceInfo,
    state: DeviceStateValue,
    last_updated: Instant,
}

struct App {
    devices: Vec<AppDevice>,
    selected_device: usize,
    current_tab: usize,
    tab_names: Vec<&'static str>,
    server_url: String,
    status_message: String,
    // kept: UI state for in-progress slider editing and the not-yet-finished
    // virtual-device creation dialog (see CLAUDE.md known limitations).
    #[allow(dead_code)]
    brightness_slider: u8,
    #[allow(dead_code)]
    color_temp_slider: u16,
    show_help: bool,
    #[allow(dead_code)]
    show_create_virtual: bool,
    #[allow(dead_code)]
    virtual_device_name: String,
    events: Vec<String>,
    should_quit: bool,
    favorites: Vec<String>,  // 🔥 ORDERED FAVORITE DEVICE IDS! 💖
    favorites_path: PathBuf, // 🔥 XDG STATE PATH! 💖
    scroll_offset: usize,    // 🔥 TRACK SCROLL POSITION FOR SMOOTH NAV! 💖
    dashboard_scroll_state: Option<ScrollViewState>, // 🔥 TUI-SCROLLVIEW STATE! 💖
}

impl App {
    fn new(server_url: String) -> Self {
        // 🔥 GET XDG STATE DIR FOR FAVORITES! 💖
        let favorites_path = Self::get_favorites_path();
        let favorites = Self::load_favorites(&favorites_path);

        Self {
            devices: Vec::new(),
            selected_device: 0,
            current_tab: 0,
            tab_names: vec![
                "🔥 Dashboard",
                "🏠 Devices",
                "🎬 Scenes",
                "📊 Stats",
                "⚙️ Virtual",
            ],
            server_url,
            status_message: "🔥 VIBEC0RE TUI Starting...".to_string(),
            brightness_slider: 50,
            color_temp_slider: 2700,
            show_help: false,
            show_create_virtual: false,
            virtual_device_name: String::new(),
            events: Vec::new(),
            should_quit: false,
            favorites,
            favorites_path,
            scroll_offset: 0,
            dashboard_scroll_state: None,
        }
    }

    // 🔥 XDG STATE DIR HELPERS! 💖
    fn get_favorites_path() -> PathBuf {
        let state_dir = dirs::state_dir()
            .or_else(dirs::data_local_dir)
            .unwrap_or_else(|| PathBuf::from("."));

        let v1bectl_dir = state_dir.join("v1bectl");
        fs::create_dir_all(&v1bectl_dir).ok();

        v1bectl_dir.join("favorites.json")
    }

    fn load_favorites(path: &PathBuf) -> Vec<String> {
        if let Ok(contents) = fs::read_to_string(path) {
            // Try to load as Vec first (new format), fallback to HashSet (old format)
            if let Ok(vec) = serde_json::from_str::<Vec<String>>(&contents) {
                vec
            } else if let Ok(set) = serde_json::from_str::<HashSet<String>>(&contents) {
                // Convert old HashSet format to Vec
                set.into_iter().collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    fn save_favorites(&self) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(&self.favorites)?;
        fs::write(&self.favorites_path, json)?;
        Ok(())
    }

    fn toggle_favorite(&mut self, device_id: String) -> anyhow::Result<()> {
        if let Some(pos) = self.favorites.iter().position(|id| id == &device_id) {
            self.favorites.remove(pos);
            self.add_event(format!("💔 Removed {} from favorites", device_id));
        } else {
            self.favorites.push(device_id.clone());
            self.add_event(format!("💖 Added {} to favorites", device_id));
        }
        self.save_favorites()?;
        Ok(())
    }

    // 🔥 MOVE FAVORITE UP IN ORDER! 💖
    fn move_favorite_up(&mut self) -> anyhow::Result<()> {
        if self.current_tab != 0 {
            return Ok(()); // Only works in dashboard
        }

        // Get device info before mutable borrow
        let (device_id, device_name) = if let Some(device) = self.get_selected_device() {
            (device.info.device_id.clone(), device.info.name.clone())
        } else {
            return Ok(());
        };

        if let Some(pos) = self.favorites.iter().position(|id| id == &device_id) {
            if pos > 0 {
                self.favorites.swap(pos, pos - 1);
                self.add_event(format!("⬆️ Moved {} up", device_name));
                self.save_favorites()?;

                // 🔥 UPDATE SCROLL TO FOLLOW MOVED ITEM! 💖
                // Reset scroll states to force recalculation
                self.scroll_offset = 0;

                // Clear ScrollViewState to trigger fresh calculation
                if self.dashboard_scroll_state.is_some() {
                    self.dashboard_scroll_state = Some(ScrollViewState::default());
                }
            }
        }
        Ok(())
    }

    // 🔥 MOVE FAVORITE DOWN IN ORDER! 💖
    fn move_favorite_down(&mut self) -> anyhow::Result<()> {
        if self.current_tab != 0 {
            return Ok(()); // Only works in dashboard
        }

        // Get device info before mutable borrow
        let (device_id, device_name) = if let Some(device) = self.get_selected_device() {
            (device.info.device_id.clone(), device.info.name.clone())
        } else {
            return Ok(());
        };

        if let Some(pos) = self.favorites.iter().position(|id| id == &device_id) {
            if pos < self.favorites.len() - 1 {
                self.favorites.swap(pos, pos + 1);
                self.add_event(format!("⬇️ Moved {} down", device_name));
                self.save_favorites()?;

                // 🔥 UPDATE SCROLL TO FOLLOW MOVED ITEM! 💖
                // Reset scroll states to force recalculation
                self.scroll_offset = 0;

                // Clear ScrollViewState to trigger fresh calculation
                if self.dashboard_scroll_state.is_some() {
                    self.dashboard_scroll_state = Some(ScrollViewState::default());
                }
            }
        }
        Ok(())
    }

    fn next_device(&mut self) {
        // 🔥 NAVIGATE BASED ON CURRENT TAB! 💖
        if self.current_tab == 0 {
            // 🔥 Dashboard - navigate favorites IN ORDER! 💖
            if self.favorites.is_empty() {
                return;
            }

            // Get current device ID
            let current_device_id = if self.selected_device < self.devices.len() {
                self.devices[self.selected_device].info.device_id.clone()
            } else {
                return;
            };

            // Find position in favorites order
            let current_fav_index = self
                .favorites
                .iter()
                .position(|id| id == &current_device_id)
                .unwrap_or(0);

            // Move to next favorite
            let next_fav_index = (current_fav_index + 1) % self.favorites.len();
            let next_device_id = &self.favorites[next_fav_index];

            // Find this device in the main list and select it
            for (i, device) in self.devices.iter().enumerate() {
                if &device.info.device_id == next_device_id {
                    self.selected_device = i;
                    break;
                }
            }
        } else {
            // All devices tab
            if !self.devices.is_empty() {
                self.selected_device = (self.selected_device + 1) % self.devices.len();
            }
        }
    }

    fn previous_device(&mut self) {
        // 🔥 NAVIGATE BASED ON CURRENT TAB! 💖
        if self.current_tab == 0 {
            // 🔥 Dashboard - navigate favorites IN ORDER! 💖
            if self.favorites.is_empty() {
                return;
            }

            // Get current device ID
            let current_device_id = if self.selected_device < self.devices.len() {
                self.devices[self.selected_device].info.device_id.clone()
            } else {
                return;
            };

            // Find position in favorites order
            let current_fav_index = self
                .favorites
                .iter()
                .position(|id| id == &current_device_id)
                .unwrap_or(0);

            // Move to previous favorite
            let prev_fav_index = if current_fav_index == 0 {
                self.favorites.len() - 1
            } else {
                current_fav_index - 1
            };
            let prev_device_id = &self.favorites[prev_fav_index];

            // Find this device in the main list and select it
            for (i, device) in self.devices.iter().enumerate() {
                if &device.info.device_id == prev_device_id {
                    self.selected_device = i;
                    break;
                }
            }
        } else {
            // All devices tab
            if !self.devices.is_empty() {
                self.selected_device = if self.selected_device == 0 {
                    self.devices.len() - 1
                } else {
                    self.selected_device - 1
                };
            }
        }
    }

    fn next_tab(&mut self) {
        self.current_tab = (self.current_tab + 1) % self.tab_names.len();
        self.scroll_offset = 0; // 🔥 RESET SCROLL ON TAB CHANGE! 💖
    }

    fn previous_tab(&mut self) {
        self.current_tab = if self.current_tab == 0 {
            self.tab_names.len() - 1
        } else {
            self.current_tab - 1
        };
        self.scroll_offset = 0; // 🔥 RESET SCROLL ON TAB CHANGE! 💖
    }

    fn get_selected_device(&self) -> Option<&AppDevice> {
        self.devices.get(self.selected_device)
    }

    // kept: mutable accessor mirroring `get_selected_device`, for in-place
    // device edits; not used by the current key handlers yet.
    #[allow(dead_code)]
    fn get_selected_device_mut(&mut self) -> Option<&mut AppDevice> {
        self.devices.get_mut(self.selected_device)
    }

    fn add_event(&mut self, event: String) {
        self.events.push(format!(
            "{}: {}",
            chrono::Utc::now().format("%H:%M:%S"),
            event
        ));
        if self.events.len() > 50 {
            self.events.remove(0);
        }
    }

    fn update_device_state(&mut self, device_id: &str, new_state: DeviceStateValue) {
        for device in &mut self.devices {
            if device.info.device_id == device_id {
                device.state = new_state;
                device.last_updated = Instant::now();
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and run
    let app = App::new(format!("ws://{}/", cli.server));
    let res = run_app(&mut terminal, app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

async fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> anyhow::Result<()> {
    // Connect to WebSocket
    let (ws_stream, _) = connect_async(&app.server_url).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Discover devices initially
    let correlation_id = Uuid::new_v4().to_string();
    let request = ApiRequest::DiscoverDevices;
    let request_payload = {
        let mut buf = Vec::new();
        ciborium::into_writer(&request, &mut buf)?;
        buf
    };

    let api_message = ApiMessage {
        correlation_id: correlation_id.clone(),
        message_type: ApiMessageType::Request,
        payload: request_payload,
    };

    let message_bytes = {
        let mut buf = Vec::new();
        ciborium::into_writer(&api_message, &mut buf)?;
        buf
    };

    ws_sender.send(Message::Binary(message_bytes)).await?;
    app.add_event("🔍 Discovering devices...".to_string());

    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(100);

    loop {
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        // Handle WebSocket messages
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                last_tick = Instant::now();
            }

            msg = ws_receiver.next() => {
                if let Some(Ok(Message::Binary(data))) = msg {
                    if let Ok(api_message) = ciborium::from_reader::<ApiMessage, _>(data.as_slice()) {
                        match api_message.message_type {
                            ApiMessageType::Response => {
                                if let Ok(response) = ciborium::from_reader::<ApiResponse, _>(api_message.payload.as_slice()) {
                                    handle_api_response(&mut app, response).await;
                                }
                            }
                            ApiMessageType::Event => {
                                if let Ok(event) = ciborium::from_reader::<DeviceEvent, _>(api_message.payload.as_slice()) {
                                    handle_device_event(&mut app, event).await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            _ = async {
                if event::poll(Duration::from_millis(50))? {
                    if let Event::Key(key) = event::read()? {
                        if key.kind == KeyEventKind::Press {
                            match handle_key_event(&mut app, &mut ws_sender, key).await {
                                Ok(should_quit) => {
                                    if should_quit {
                                        app.should_quit = true;
                                    }
                                }
                                Err(e) => {
                                    app.status_message = format!("❌ Error: {}", e);
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            } => {}
        }

        terminal.draw(|f| ui(f, &mut app))?;

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

async fn handle_api_response(app: &mut App, response: ApiResponse) {
    match response {
        ApiResponse::DeviceList {
            devices,
            total_count,
        } => {
            // 🔥 Use DeviceState directly from server - it already has the state!
            app.devices = devices
                .into_iter()
                .map(|device_state| AppDevice {
                    info: device_state.device_info,
                    state: device_state.state,
                    last_updated: Instant::now(),
                })
                .collect();

            app.status_message = format!("🎯 Found {} devices!", total_count);
            app.add_event(format!("📡 Loaded {} devices", total_count));
        }
        ApiResponse::DeviceState { state } => {
            // For now, just update the first matching device by type
            // TODO: Need device_id in the state response to match properly
            for device in app.devices.iter_mut() {
                let type_matches = matches!(
                    (&device.state, &state),
                    (DeviceStateValue::Light(_), DeviceStateValue::Light(_))
                        | (DeviceStateValue::Outlet(_), DeviceStateValue::Outlet(_))
                        | (DeviceStateValue::Switch(_), DeviceStateValue::Switch(_))
                        | (DeviceStateValue::Sensor(_), DeviceStateValue::Sensor(_))
                        | (
                            DeviceStateValue::MotionSensor(_),
                            DeviceStateValue::MotionSensor(_)
                        )
                );

                if type_matches {
                    device.state = state.clone();
                    device.last_updated = Instant::now();
                    break;
                }
            }
        }
        ApiResponse::LightUpdated { new_state: _ } => {
            app.status_message = "💡 Light updated!".to_string();
            app.add_event("💡 Light state changed".to_string());
        }
        ApiResponse::Error { code, message } => {
            app.status_message = format!("❌ {}: {}", code, message);
            app.add_event(format!("❌ Error: {}", message));
        }
        _ => {}
    }
}

async fn handle_device_event(app: &mut App, event: DeviceEvent) {
    // 🔥 Handle new event format
    if let EventType::AttributeChanged {
        attribute,
        new_value,
        ..
    } = &event.event_type
    {
        if attribute == "state" {
            if let Ok(new_state) = serde_json::from_value::<DeviceStateValue>(new_value.clone()) {
                app.update_device_state(&event.device_id, new_state);
                app.add_event(format!("⚡ {} updated", event.device_id));
            }
        }
    }
}

async fn handle_key_event(
    app: &mut App,
    ws_sender: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    key: event::KeyEvent,
) -> anyhow::Result<bool> {
    use event::KeyModifiers;

    if app.show_help {
        match key.code {
            KeyCode::Esc | KeyCode::Char('h') => app.show_help = false,
            _ => {}
        }
        return Ok(false);
    }

    // 🔥 CHECK FOR SHIFT MODIFIER! 💖
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        match key.code {
            KeyCode::Up => {
                app.move_favorite_up()?;
                return Ok(false);
            }
            KeyCode::Down => {
                app.move_favorite_down()?;
                return Ok(false);
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Char('h') => app.show_help = true,
        KeyCode::Tab => app.next_tab(),
        KeyCode::BackTab => app.previous_tab(),
        KeyCode::Up | KeyCode::Char('k') => app.previous_device(),
        KeyCode::Down | KeyCode::Char('j') => app.next_device(),
        KeyCode::Enter | KeyCode::Char(' ') => {
            if let Some(device) = app.get_selected_device() {
                match device.info.device_type {
                    DeviceType::Light => {
                        let is_on = match &device.state {
                            DeviceStateValue::Light(light) => !light.is_on,
                            _ => true,
                        };
                        send_light_command(
                            ws_sender,
                            &device.info.device_id,
                            Some(is_on),
                            None,
                            None,
                        )
                        .await?;
                        app.add_event(format!("💡 Toggled {}", device.info.name));
                    }
                    DeviceType::Outlet => {
                        let is_on = match &device.state {
                            DeviceStateValue::Outlet(outlet) => !outlet.is_on,
                            _ => true,
                        };
                        send_outlet_command(ws_sender, &device.info.device_id, is_on).await?;
                        app.add_event(format!("🔌 Toggled {}", device.info.name));
                    }
                    _ => {}
                }
            }
        }
        KeyCode::Right | KeyCode::Char('+') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    let current_brightness = match &device.state {
                        DeviceStateValue::Light(light) => light.brightness.unwrap_or(0),
                        _ => 0,
                    };
                    let new_brightness = (current_brightness + 10).min(100);

                    // 🔥 FIX: Don't change ON/OFF state - ONLY brightness!
                    send_light_command(
                        ws_sender,
                        &device.info.device_id,
                        None,
                        Some(new_brightness),
                        None,
                    )
                    .await?;
                    app.add_event(format!("🔆 Brightness up: {}%", new_brightness));
                }
            }
        }
        KeyCode::Left | KeyCode::Char('-') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    let current_brightness = match &device.state {
                        DeviceStateValue::Light(light) => light.brightness.unwrap_or(0),
                        _ => 0,
                    };
                    let new_brightness = current_brightness.saturating_sub(10);

                    // 🔥 FIX: Don't change ON/OFF state - ONLY brightness!
                    send_light_command(
                        ws_sender,
                        &device.info.device_id,
                        None,
                        Some(new_brightness),
                        None,
                    )
                    .await?;
                    app.add_event(format!("🔅 Brightness down: {}%", new_brightness));
                }
            }
        }
        KeyCode::Char('w') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, None, Some(6500))
                        .await?;
                    app.add_event("🔵 Cool white (6500K)".to_string());
                }
            }
        }
        KeyCode::Char('r') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, None, Some(2700))
                        .await?;
                    app.add_event("🟡 Warm white (2700K)".to_string());
                }
            }
        }
        // 🔥 BRIGHTNESS SHORTCUTS - KITCHEN LIGHT FIXED!
        KeyCode::Char('1') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, Some(10), None)
                        .await?;
                    app.add_event("💡 10% brightness".to_string());
                }
            }
        }
        KeyCode::Char('2') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, Some(25), None)
                        .await?;
                    app.add_event("💡 25% brightness".to_string());
                }
            }
        }
        KeyCode::Char('5') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, Some(50), None)
                        .await?;
                    app.add_event("💡 50% brightness".to_string());
                }
            }
        }
        KeyCode::Char('7') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, Some(75), None)
                        .await?;
                    app.add_event("💡 75% brightness".to_string());
                }
            }
        }
        KeyCode::Char('0') => {
            if let Some(device) = app.get_selected_device() {
                if matches!(device.info.device_type, DeviceType::Light) {
                    send_light_command(ws_sender, &device.info.device_id, None, Some(100), None)
                        .await?;
                    app.add_event("💡 100% FULL BRIGHTNESS!".to_string());
                }
            }
        }
        // 🔥 TOGGLE FAVORITE WITH 'f' KEY! 💖
        KeyCode::Char('f') => {
            if let Some(device) = app.get_selected_device() {
                app.toggle_favorite(device.info.device_id.clone())?;
            }
        }
        _ => {}
    }

    Ok(false)
}

async fn send_outlet_command(
    ws_sender: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    device_id: &str,
    is_on: bool,
) -> anyhow::Result<()> {
    let correlation_id = Uuid::new_v4().to_string();
    let request = ApiRequest::SetOutletState {
        device_id: device_id.to_string(),
        is_on,
    };

    let request_payload = {
        let mut buf = Vec::new();
        ciborium::into_writer(&request, &mut buf)?;
        buf
    };

    let api_message = ApiMessage {
        correlation_id,
        message_type: ApiMessageType::Request,
        payload: request_payload,
    };

    let message_data = {
        let mut buf = Vec::new();
        ciborium::into_writer(&api_message, &mut buf)?;
        buf
    };

    ws_sender.send(Message::Binary(message_data)).await?;
    Ok(())
}

async fn send_light_command(
    ws_sender: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    device_id: &str,
    is_on: Option<bool>,
    brightness: Option<u8>,
    color_temp: Option<u16>,
) -> anyhow::Result<()> {
    let correlation_id = Uuid::new_v4().to_string();
    let request = ApiRequest::SetLightState {
        device_id: device_id.to_string(),
        is_on,
        brightness,
        color_temp,
        rgb_color: None,
    };

    let request_payload = {
        let mut buf = Vec::new();
        ciborium::into_writer(&request, &mut buf)?;
        buf
    };

    let api_message = ApiMessage {
        correlation_id,
        message_type: ApiMessageType::Request,
        payload: request_payload,
    };

    let message_bytes = {
        let mut buf = Vec::new();
        ciborium::into_writer(&api_message, &mut buf)?;
        buf
    };

    ws_sender.send(Message::Binary(message_bytes)).await?;
    Ok(())
}

fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Status bar
        ])
        .split(f.area());

    // Header with tabs
    let tab_titles: Vec<Line> = app.tab_names.iter().cloned().map(Line::from).collect();
    let tabs = Tabs::new(tab_titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("🔥 VIBEC0RE TUI 🔥"),
        )
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .select(app.current_tab);
    f.render_widget(tabs, chunks[0]);

    // Main content based on selected tab
    match app.current_tab {
        0 => dashboard_scrollview::render_dashboard(f, app, chunks[1]), // 🔥 TUI-SCROLLVIEW DASHBOARD! 💖
        1 => render_devices_tab(f, app, chunks[1]),
        2 => render_scenes_tab(f, app, chunks[1]),
        3 => render_stats_tab(f, app, chunks[1]),
        4 => render_virtual_tab(f, app, chunks[1]),
        _ => {}
    }

    // Status bar
    let status = Paragraph::new(app.status_message.clone())
        .block(Block::default().borders(Borders::ALL).title("Status"))
        .style(Style::default().fg(Color::Green));
    f.render_widget(status, chunks[2]);

    // Help popup
    if app.show_help {
        render_help_popup(f);
    }
}

fn render_devices_tab(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Device list
    let devices: Vec<ListItem> = app
        .devices
        .iter()
        .enumerate()
        .map(|(i, device)| {
            let style = if i == app.selected_device {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };

            // 🔥 SHOW FAVORITE HEART! 💖
            let fav_icon = if app.favorites.iter().any(|id| id == &device.info.device_id) {
                "💖 "
            } else {
                ""
            };

            let icon = match device.info.device_type {
                DeviceType::Light => "💡",
                DeviceType::Sensor => "🌡️",
                DeviceType::Switch => "🔘",
                _ => "❓",
            };

            let status = match &device.state {
                DeviceStateValue::Light(light) => {
                    if light.is_on {
                        format!("ON {}%", light.brightness.unwrap_or(100))
                    } else {
                        "OFF".to_string()
                    }
                }
                DeviceStateValue::Sensor(sensor) => {
                    format!(
                        "{}°C {}%",
                        sensor.temperature.unwrap_or(0.0),
                        sensor.humidity.unwrap_or(0.0)
                    )
                }
                DeviceStateValue::Switch(switch) => {
                    if switch.is_pressed { "PRESSED" } else { "IDLE" }.to_string()
                }
                _ => "UNKNOWN".to_string(),
            };

            ListItem::new(Line::from(vec![
                Span::raw(fav_icon),
                Span::raw(format!("{} ", icon)),
                Span::styled(device.info.name.clone(), style),
                Span::raw(format!(" [{}]", status)),
            ]))
        })
        .collect();

    let devices_list = List::new(devices)
        .block(Block::default().borders(Borders::ALL).title("🏠 Devices"))
        .highlight_style(Style::default().bg(Color::DarkGray));

    let mut list_state = ListState::default();
    list_state.select(Some(app.selected_device));
    f.render_stateful_widget(devices_list, chunks[0], &mut list_state);

    // Device details and controls
    if let Some(device) = app.get_selected_device() {
        render_device_details(f, device, chunks[1]);
    }
}

fn render_device_details(f: &mut Frame, device: &AppDevice, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // Device info
            Constraint::Min(0),    // Controls
        ])
        .split(area);

    // Device info
    let info_text = vec![
        Line::from(vec![
            Span::styled("Name: ", Style::default().fg(Color::Yellow)),
            Span::raw(&device.info.name),
        ]),
        Line::from(vec![
            Span::styled("Type: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:?}", device.info.device_type)),
        ]),
        Line::from(vec![
            Span::styled("ID: ", Style::default().fg(Color::Yellow)),
            Span::raw(&device.info.device_id),
        ]),
        Line::from(vec![
            Span::styled("Groups: ", Style::default().fg(Color::Yellow)),
            Span::raw(device.info.device_groups.join(", ")),
        ]),
        Line::from(vec![
            Span::styled("Reachable: ", Style::default().fg(Color::Yellow)),
            Span::raw(if device.info.reachable {
                "✅ Yes"
            } else {
                "❌ No"
            }),
        ]),
        Line::from(vec![
            Span::styled("Updated: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!(
                "{:.1}s ago",
                device.last_updated.elapsed().as_secs_f64()
            )),
        ]),
    ];

    let info_paragraph = Paragraph::new(info_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("📋 Device Info"),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(info_paragraph, chunks[0]);

    // Controls based on device type
    match device.info.device_type {
        DeviceType::Light => render_light_controls(f, device, chunks[1]),
        DeviceType::Sensor => render_sensor_display(f, device, chunks[1]),
        DeviceType::Switch => render_switch_display(f, device, chunks[1]),
        _ => {}
    }
}

fn render_light_controls(f: &mut Frame, device: &AppDevice, area: Rect) {
    if let DeviceStateValue::Light(light) = &device.state {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // On/Off
                Constraint::Length(3), // Brightness
                Constraint::Length(3), // Color temp
                Constraint::Min(0),    // Controls help
            ])
            .split(area);

        // On/Off status
        let status_text = if light.is_on { "🟢 ON" } else { "🔴 OFF" };
        let status_color = if light.is_on {
            Color::Green
        } else {
            Color::Red
        };
        let status = Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Power"))
            .style(
                Style::default()
                    .fg(status_color)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(status, chunks[0]);

        // Brightness gauge
        let brightness = light.brightness.unwrap_or(0);
        let brightness_gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("💡 Brightness"),
            )
            .gauge_style(Style::default().fg(Color::Yellow))
            .ratio(brightness as f64 / 100.0)
            .label(format!("{}%", brightness));
        f.render_widget(brightness_gauge, chunks[1]);

        // Color temperature
        if let Some(temp) = light.color_temp {
            let temp_text = format!("{}K", temp);
            let temp_color = if temp < 3000 {
                Color::Red
            } else if temp > 5000 {
                Color::Blue
            } else {
                Color::Yellow
            };
            let temp_widget = Paragraph::new(temp_text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("🌡️ Color Temp"),
                )
                .style(Style::default().fg(temp_color));
            f.render_widget(temp_widget, chunks[2]);
        }

        // Controls help
        let help_text = vec![
            Line::from("Controls:"),
            Line::from("  SPACE/ENTER - Toggle On/Off"),
            Line::from("  +/RIGHT - Brightness Up"),
            Line::from("  -/LEFT - Brightness Down"),
            Line::from("  W - Cool White (6500K)"),
            Line::from("  R - Warm White (2700K)"),
        ];
        let help = Paragraph::new(help_text)
            .block(Block::default().borders(Borders::ALL).title("🎮 Controls"))
            .style(Style::default().fg(Color::Cyan));
        f.render_widget(help, chunks[3]);
    }
}

fn render_sensor_display(f: &mut Frame, device: &AppDevice, area: Rect) {
    if let DeviceStateValue::Sensor(sensor) = &device.state {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Length(3)])
            .split(area);

        if let Some(temp) = sensor.temperature {
            let temp_gauge = Gauge::default()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("🌡️ Temperature"),
                )
                .gauge_style(Style::default().fg(Color::Red))
                .ratio((temp.clamp(0.0, 50.0) / 50.0) as f64)
                .label(format!("{:.1}°C", temp));
            f.render_widget(temp_gauge, chunks[0]);
        }

        if let Some(humidity) = sensor.humidity {
            let humidity_gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("💧 Humidity"))
                .gauge_style(Style::default().fg(Color::Blue))
                .ratio((humidity / 100.0) as f64)
                .label(format!("{:.1}%", humidity));
            f.render_widget(humidity_gauge, chunks[1]);
        }
    }
}

fn render_switch_display(f: &mut Frame, device: &AppDevice, area: Rect) {
    if let DeviceStateValue::Switch(switch) = &device.state {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Length(3)])
            .split(area);

        let status_text = if switch.is_pressed {
            "🔴 PRESSED"
        } else {
            "🟢 IDLE"
        };
        let status_color = if switch.is_pressed {
            Color::Red
        } else {
            Color::Green
        };
        let status = Paragraph::new(status_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Switch Status"),
            )
            .style(
                Style::default()
                    .fg(status_color)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(status, chunks[0]);

        if let Some(battery) = switch.battery_level {
            let battery_gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("🔋 Battery"))
                .gauge_style(Style::default().fg(Color::Green))
                .ratio(battery as f64 / 100.0)
                .label(format!("{}%", battery));
            f.render_widget(battery_gauge, chunks[1]);
        }
    }
}

fn render_scenes_tab(f: &mut Frame, _app: &mut App, area: Rect) {
    let placeholder =
        Paragraph::new("🎬 Scene Management\n\nComing soon! Create and manage automation scenes.")
            .block(Block::default().borders(Borders::ALL).title("Scenes"))
            .style(Style::default().fg(Color::Yellow));
    f.render_widget(placeholder, area);
}

fn render_stats_tab(f: &mut Frame, app: &mut App, area: Rect) {
    // 🔥 THREE-COLUMN LAYOUT FOR MAXIMUM STATS! 💖
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40), // Event logs (moved from dashboard!)
            Constraint::Percentage(30), // System status (moved from dashboard!)
            Constraint::Percentage(30), // Device breakdown
        ])
        .split(area);

    // 📡 LIVE EVENT LOGS - MOVED FROM DASHBOARD! 💖
    let events_block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(Color::Magenta))
        .title("╣ 📡 LIVE EVENTS ╠")
        .title_alignment(Alignment::Center);

    let events: Vec<ListItem> = app
        .events
        .iter()
        .rev()
        .take(main_chunks[0].height as usize - 2)
        .map(|e| {
            ListItem::new(Line::from(vec![
                Span::styled("→ ", Style::default().fg(Color::Magenta)),
                Span::raw(e),
            ]))
        })
        .collect();

    let events_list = List::new(events)
        .block(events_block)
        .style(Style::default().fg(Color::White));

    f.render_widget(events_list, main_chunks[0]);

    // 💚 SYSTEM STATUS - MOVED FROM DASHBOARD! 💖
    let stats_block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(Color::Green))
        .title("╣ 💚 SYSTEM STATUS ╠")
        .title_alignment(Alignment::Center);

    // 🔥 SHOW FAVORITE STATS! 💖
    let total_devices = app.devices.len();
    let favorited = app.favorites.len();
    let online_devices = app.devices.iter().filter(|d| d.info.reachable).count();
    let lights_on = app
        .devices
        .iter()
        .filter(|d| app.favorites.iter().any(|id| id == &d.info.device_id))
        .filter(|d| {
            if let DeviceStateValue::Light(state) = &d.state {
                state.is_on
            } else {
                false
            }
        })
        .count();

    let stats_content = vec![
        Line::from(vec![
            Span::styled("DEVICES: ", Style::default().fg(Color::Green)),
            Span::styled(
                format!("{}/{}", online_devices, total_devices),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("FAVS:    ", Style::default().fg(Color::Magenta)),
            Span::styled(
                format!("💖 {}", favorited),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("LIGHTS:  ", Style::default().fg(Color::Green)),
            Span::styled(
                format!("{} ON", lights_on),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("STATUS:  ", Style::default().fg(Color::Green)),
            Span::styled(
                "VIBEC0RE",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    let stats = Paragraph::new(stats_content).block(stats_block);

    f.render_widget(stats, main_chunks[1]);

    // 📊 DEVICE BREAKDOWN - ENHANCED! 💖
    let breakdown_block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan))
        .title("╣ 📊 DEVICE TYPES ╠")
        .title_alignment(Alignment::Center);

    let lights_count = app
        .devices
        .iter()
        .filter(|d| matches!(d.info.device_type, DeviceType::Light))
        .count();
    let sensors_count = app
        .devices
        .iter()
        .filter(|d| matches!(d.info.device_type, DeviceType::Sensor))
        .count();
    let switches_count = app
        .devices
        .iter()
        .filter(|d| matches!(d.info.device_type, DeviceType::Switch))
        .count();
    let outlets_count = app
        .devices
        .iter()
        .filter(|d| matches!(d.info.device_type, DeviceType::Outlet))
        .count();

    let breakdown_content = vec![
        Line::from(vec![
            Span::styled("💡 ", Style::default().fg(Color::Yellow)),
            Span::styled("LIGHTS: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{}", lights_count),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("🌡️ ", Style::default().fg(Color::Blue)),
            Span::styled("SENSORS:", Style::default().fg(Color::White)),
            Span::styled(
                format!("{}", sensors_count),
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("🔘 ", Style::default().fg(Color::Gray)),
            Span::styled("SWITCHES:", Style::default().fg(Color::White)),
            Span::styled(
                format!("{}", switches_count),
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("🔌 ", Style::default().fg(Color::Red)),
            Span::styled("OUTLETS: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{}", outlets_count),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("TOTAL: ", Style::default().fg(Color::Green)),
            Span::styled(
                format!("{}", total_devices),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    let breakdown = Paragraph::new(breakdown_content).block(breakdown_block);

    f.render_widget(breakdown, main_chunks[2]);
}

fn render_virtual_tab(f: &mut Frame, _app: &mut App, area: Rect) {
    let placeholder = Paragraph::new(
        "⚙️ Virtual Device Management\n\nComing soon! Create virtual light groups and scenes.",
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Virtual Devices"),
    )
    .style(Style::default().fg(Color::Magenta));
    f.render_widget(placeholder, area);
}

fn render_help_popup(f: &mut Frame) {
    let area = centered_rect(60, 60, f.area());

    let help_text = vec![
        Line::from("🔥 VIBEC0RE TUI Help 🔥"),
        Line::from(""),
        Line::from("Global Keys:"),
        Line::from("  q - Quit"),
        Line::from("  h - Toggle this help"),
        Line::from("  TAB - Next tab"),
        Line::from("  SHIFT+TAB - Previous tab"),
        Line::from(""),
        Line::from("Device Navigation:"),
        Line::from("  ↑/k - Previous device"),
        Line::from("  ↓/j - Next device"),
        Line::from(""),
        Line::from("Light Controls:"),
        Line::from("  SPACE/ENTER - Toggle On/Off"),
        Line::from("  +/→ - Brightness Up (+10%)"),
        Line::from("  -/← - Brightness Down (-10%)"),
        Line::from("  1/2/5/7/0 - Set 10/25/50/75/100%"),
        Line::from("  w - Cool White (6500K)"),
        Line::from("  r - Warm White (2700K)"),
        Line::from("  f - Toggle Favorite 💖"),
        Line::from(""),
        Line::from("Press ESC or h to close"),
    ];

    f.render_widget(Clear, area);
    let help_popup = Paragraph::new(help_text)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });
    f.render_widget(help_popup, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
