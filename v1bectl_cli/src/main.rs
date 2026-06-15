// kept: alternative CLI transport backends (raw TCP/CBOR and HTTP). The CLI
// currently talks to the server over the WebSocket client; these remain as
// supported, not-yet-wired transports.
#[allow(dead_code)]
mod client;
// kept: see note on `client` above.
#[allow(dead_code)]
mod http_client;
mod websocket_client;

use clap::{Parser, Subcommand};
use tabled::{Table, Tabled};
use tracing::Level;
use v1bectl_sync::*;
use websocket_client::WebSocketClient;

#[derive(Parser)]
#[command(name = "v1bectl")]
#[command(about = "CLI client for v1bectl home automation system")]
struct Cli {
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:31337")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List all devices
    List,
    /// Get device state
    Get {
        /// Device ID to query
        device_id: String,
    },
    /// Control lights
    Light {
        /// Device ID
        device_id: String,
        /// Turn on/off
        #[arg(long)]
        on: Option<bool>,
        /// Set brightness (0-100)
        #[arg(long)]
        brightness: Option<u8>,
        /// Set color temperature in Kelvin
        #[arg(long)]
        color_temp: Option<u16>,
    },
    /// Subscribe to device events in real-time
    Subscribe {
        /// Device IDs to subscribe to (empty = all devices)
        device_ids: Vec<String>,
        /// Show JSON output instead of pretty format
        #[arg(long)]
        json: bool,
    },
    /// Debug dump raw Dirigera data to file (CHOOOM REQUESTED! 💖)
    DebugDump {
        /// Output file path
        #[arg(default_value = "dirigera_dump.json")]
        output: String,
        /// Dirigera host (e.g., gw2-xxxx.local)
        #[arg(long)]
        host: Option<String>,
    },
}

#[derive(Tabled)]
struct DeviceRow {
    id: String,
    name: String,
    #[tabled(rename = "type")]
    device_type: String,
    groups: String,
    reachable: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();
    let client = WebSocketClient::new(cli.server);

    match cli.command {
        Commands::List => {
            let (devices, total_count) = client.discover_devices().await?;

            println!("Found {} devices:\n", total_count);

            let rows: Vec<DeviceRow> = devices
                .into_iter()
                .map(|d| DeviceRow {
                    id: d.device_info.device_id,
                    name: d.device_info.name,
                    device_type: format!("{:?}", d.device_info.device_type),
                    groups: d.device_info.device_groups.join(", "),
                    reachable: if d.device_info.reachable {
                        "✓"
                    } else {
                        "✗"
                    }
                    .to_string(),
                })
                .collect();

            let table = Table::new(rows);
            println!("{}", table);
        }
        Commands::Get { device_id } => {
            let state = client.get_device_state(&device_id).await?;

            println!("Device: {}\n", device_id);

            match state {
                DeviceStateValue::Light(light) => {
                    println!("Type: Light");
                    println!("State: {}", if light.is_on { "ON" } else { "OFF" });
                    if let Some(brightness) = light.brightness {
                        println!("Brightness: {}%", brightness);
                    }
                    if let Some(temp) = light.color_temp {
                        println!("Color Temperature: {}K", temp);
                    }
                    if let Some(rgb) = light.rgb_color {
                        println!("RGB Color: ({}, {}, {})", rgb.r, rgb.g, rgb.b);
                    }
                }
                DeviceStateValue::Switch(switch) => {
                    println!("Type: Switch");
                    println!("Pressed: {}", switch.is_pressed);
                    if let Some(battery) = switch.battery_level {
                        println!("Battery: {}%", battery);
                    }
                }
                DeviceStateValue::Sensor(sensor) => {
                    println!("Type: Sensor");
                    if let Some(temp) = sensor.temperature {
                        println!("Temperature: {:.1}°C", temp);
                    }
                    if let Some(humidity) = sensor.humidity {
                        println!("Humidity: {:.1}%", humidity);
                    }
                }
                DeviceStateValue::Empty => {
                    println!("Type: Virtual Controller (no state)");
                }
                _ => {
                    println!("State: {:?}", state);
                }
            }
        }
        Commands::Light {
            device_id,
            on,
            brightness,
            color_temp,
        } => {
            // First get current state
            let current_state = client.get_device_state(&device_id).await?;

            if let DeviceStateValue::Light(mut light_state) = current_state {
                // Update only specified fields
                if let Some(on) = on {
                    light_state.is_on = on;
                    // 🔥 When turning OFF, clear brightness to avoid confusing the gateway! CHOOOM FIX! 💖
                    if !on && brightness.is_none() {
                        light_state.brightness = None;
                    }
                }
                if let Some(brightness) = brightness {
                    light_state.brightness = Some(brightness);
                }
                if let Some(temp) = color_temp {
                    light_state.color_temp = Some(temp);
                }

                // Send update
                client.set_light_state(&device_id, light_state).await?;

                println!("✓ Light {} updated successfully", device_id);

                // Show new state
                let new_state = client.get_device_state(&device_id).await?;
                if let DeviceStateValue::Light(light) = new_state {
                    println!("\nNew state:");
                    println!("  On: {}", light.is_on);
                    if let Some(b) = light.brightness {
                        println!("  Brightness: {}%", b);
                    }
                    if let Some(t) = light.color_temp {
                        println!("  Color temp: {}K", t);
                    }
                }
            } else {
                anyhow::bail!("{} is not a light device", device_id);
            }
        }
        Commands::Subscribe { device_ids, json } => {
            use futures_util::stream::StreamExt;

            println!("🔥 Starting real-time WebSocket subscription...");
            if device_ids.is_empty() {
                println!("📡 Subscribing to ALL device events");
            } else {
                println!("📡 Subscribing to devices: {}", device_ids.join(", "));
            }
            println!("🚀 Press Ctrl+C to stop\n");

            let event_stream = client.subscribe_to_events(device_ids).await?;
            let mut event_stream = Box::pin(event_stream);

            while let Some(event_result) = event_stream.next().await {
                match event_result {
                    Ok(event) => {
                        if json {
                            // JSON output for scripting
                            println!("{}", serde_json::to_string(&event)?);
                        } else {
                            // Pretty formatted output
                            let timestamp_millis = event
                                .timestamp
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis()
                                as i64;
                            let timestamp =
                                chrono::DateTime::from_timestamp_millis(timestamp_millis)
                                    .unwrap_or_else(chrono::Utc::now)
                                    .format("%H:%M:%S%.3f");

                            print!("⚡ [{}] ", timestamp);

                            match &event.event_type {
                                EventType::AttributeChanged {
                                    attribute,
                                    new_value,
                                    ..
                                } => {
                                    print!("🔄 ATTRIBUTE CHANGED");
                                    print!(" - {} {}: {}", event.device_id, attribute, new_value);
                                }
                                EventType::DeviceAdded { device_type } => {
                                    print!(
                                        "➕ DEVICE ADDED - {} ({})",
                                        event.device_id, device_type
                                    );
                                }
                                EventType::DeviceRemoved => {
                                    print!("➖ DEVICE REMOVED - {}", event.device_id);
                                }
                                EventType::DeviceReachabilityChanged { reachable } => {
                                    print!(
                                        "📶 CONNECTIVITY - {} is now {}",
                                        event.device_id,
                                        if *reachable { "ONLINE" } else { "OFFLINE" }
                                    );
                                }
                                EventType::ButtonPressed {
                                    button_id,
                                    press_type,
                                } => {
                                    print!(
                                        "🔘 BUTTON PRESS - {} {} ({:?})",
                                        event.device_id, button_id, press_type
                                    );
                                }
                                EventType::SceneActivated { scene_id } => {
                                    print!(
                                        "🎬 SCENE ACTIVATED - {} ({})",
                                        event.device_id, scene_id
                                    );
                                }
                                EventType::BatteryLevelChanged { new_level, .. } => {
                                    print!("🔋 BATTERY - {} at {}%", event.device_id, new_level);
                                }
                                _ => {
                                    print!("📨 {:?} - {}", event.event_type, event.device_id);
                                }
                            }
                            println!();
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Error receiving event: {}", e);
                        break;
                    }
                }
            }

            println!("🔌 Subscription ended");
        }
        Commands::DebugDump { output, host } => {
            println!("🔥 DIRIGERA DEBUG DUMP - CHOOOM REQUESTED! 💖");

            // Get host from argument or environment
            let dirigera_host = host
                .or_else(|| std::env::var("DIRIGERA_HOST").ok())
                .unwrap_or_else(|| {
                    eprintln!("❌ Please provide --host or set DIRIGERA_HOST env var");
                    std::process::exit(1);
                });

            // Read access token
            let token_path = format!(
                "{}/.local/state/v1bectl/access.token",
                std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
            );

            let access_token = tokio::fs::read_to_string(&token_path)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to read access token from {}: {}", token_path, e)
                })?
                .trim()
                .to_string();

            println!("📡 Connecting to Dirigera at {}...", dirigera_host);

            // Create HTTP client that accepts self-signed certs
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .timeout(std::time::Duration::from_secs(10))
                .build()?;

            // Fetch raw device data
            let url = format!("https://{}:8443/v1/devices", dirigera_host);
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", access_token))
                .send()
                .await?;

            if !response.status().is_success() {
                eprintln!("❌ Dirigera API error: {}", response.status());
                eprintln!("Response: {}", response.text().await?);
                std::process::exit(1);
            }

            // Get raw JSON
            let raw_json = response.text().await?;

            // Pretty print it
            let parsed: serde_json::Value = serde_json::from_str(&raw_json)?;
            let pretty = serde_json::to_string_pretty(&parsed)?;

            // Write to file
            tokio::fs::write(&output, &pretty).await?;

            println!("✅ Dumped raw Dirigera data to: {}", output);
            println!("📊 Total size: {} bytes", pretty.len());

            // Quick analysis
            if let Some(devices) = parsed.as_array() {
                println!("\n🔍 Quick Analysis:");
                println!("  Total devices: {}", devices.len());

                // Count device types
                let mut type_counts = std::collections::HashMap::new();
                for device in devices {
                    if let Some(dtype) = device.get("type").and_then(|t| t.as_str()) {
                        *type_counts.entry(dtype).or_insert(0) += 1;
                    }
                }

                println!("\n  Device types:");
                for (dtype, count) in type_counts {
                    println!("    {}: {}", dtype, count);
                }

                println!("\n💡 TIP: Check the dump file to see actual capabilities fields!");
                println!("  Look for patterns in attributes like:");
                println!("  - lightLevel (brightness support)");
                println!("  - colorTemperature (color temp support)");
                println!("  - colorHue/colorSaturation (RGB support)");
            }
        }
    }

    Ok(())
}
