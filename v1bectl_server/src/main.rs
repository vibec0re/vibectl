use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn, Level};
use v1bectl_api::AxumServer;
use v1bectl_gateway::{DirigeraGateway, Gateway};
use v1bectl_sync::*;
use v1bectl_virtual::DummyGateway;

#[derive(Parser)]
#[command(name = "v1bectl_server")]
#[command(about = "🔥 VIBEC0RE Home Automation Server - PURE ASYNC WEBSOCKET VIBES! 🚀")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run with dummy gateway for testing 🧪
    Dummy {
        /// Dummy scenario to use
        #[arg(short, long, default_value = "basic_home")]
        scenario: String,
        /// Server port
        #[arg(short, long, default_value = "31337")]
        port: u16,
    },
    /// Run with REAL Dirigera hub integration! 🔥
    Dirigera {
        /// Dirigera hub IP address
        #[arg(long, default_value = "192.168.1.1")]
        host: String,
        /// Server port  
        #[arg(short, long, default_value = "31337")]
        port: u16,
        /// HTTP timeout in seconds
        #[arg(short, long, default_value = "10")]
        timeout: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with VIBEC0RE vibes! 🔥
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Dummy { scenario, port } => {
            info!("🧪 Starting v1bectl server with DUMMY gateway for testing...");
            info!("📋 Scenario: {}", scenario);

            let gateway: Arc<dyn Gateway> = Arc::new(DummyGateway::new(&scenario));
            run_server(gateway, port).await
        }
        Commands::Dirigera {
            host,
            port,
            timeout,
        } => {
            info!("🔥 Starting v1bectl server with REAL Dirigera integration! 🚀");
            info!("🏠 Dirigera Hub: {}", host);
            info!("⚡ Timeout: {}s", timeout);

            let gateway: Arc<dyn Gateway> = Arc::new(
                DirigeraGateway::from_token_file(&host, Duration::from_secs(timeout))
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create Dirigera gateway: {}", e))?,
            );

            run_server(gateway, port).await
        }
    }
}

async fn run_server(gateway: Arc<dyn Gateway>, port: u16) -> anyhow::Result<()> {
    info!("🚀 VIBEC0RE SERVER STARTING - PURE ASYNC WEBSOCKET POWER! 🔥");

    // Initialize components
    let state_store = StateStore::new();
    let event_bus = Arc::new(EventBus::new(1000)); // Keep last 1000 events

    // 🔥 START GATEWAY EVENT STREAM FOR REAL-TIME EVENTS! 💖
    // This connects the gateway's WebSocket events to our EventBus
    match gateway.event_stream().await {
        Ok(mut gateway_events) => {
            let event_bus_clone = event_bus.clone();
            tokio::spawn(async move {
                info!("🎯 Gateway event stream connected - forwarding to EventBus!");
                while let Ok(event) = gateway_events.recv().await {
                    // Forward gateway events to our main EventBus
                    event_bus_clone.publish(event).await;
                }
                warn!("⚠️ Gateway event stream ended");
            });
        }
        Err(e) => {
            warn!("⚠️ Gateway does not support event stream: {}", e);
        }
    }

    // Initialize sync engine
    let sync_engine = Arc::new(SyncEngine::new(
        state_store.clone(),
        event_bus.clone(),
        gateway.clone(),
        None, // Use default config - VIBEOPTIMIZED! 🔥
    ));

    // Discover and populate initial devices - VIBEC0RE AUTODISCOVERY! 🔍
    info!("🔍 Discovering devices from gateway...");
    let devices = gateway.discover_devices().await?;
    info!("🎯 Found {} devices from gateway!", devices.len());

    for device_info in devices {
        // Get initial state from gateway
        match gateway.get_device_state(&device_info.device_id).await {
            Ok(initial_state) => {
                state_store
                    .add_device(device_info.clone(), initial_state)
                    .await;
                info!(
                    "✅ Added device: {} ({}) - {:?}",
                    device_info.name, device_info.device_id, device_info.device_type
                );

                // Publish device added event
                event_bus
                    .publish(DeviceEvent {
                        timestamp: std::time::SystemTime::now(),
                        device_id: device_info.device_id.clone(),
                        event_type: EventType::DeviceAdded {
                            device_type: format!("{:?}", device_info.device_type),
                        },
                    })
                    .await;
            }
            Err(e) => {
                warn!(
                    "❌ Failed to get initial state for device {}: {}",
                    device_info.device_id, e
                );
            }
        }
    }

    info!("🔥 VIBEC0RE SERVER READY! 🔥");
    info!("📊 Stats:");
    info!("  - Total devices: {}", state_store.device_count().await);
    info!(
        "  - Reachable devices: {}",
        state_store.reachable_device_count().await
    );
    info!("  - Server port: {}", port);
    info!("  - WebSocket endpoint: ws://0.0.0.0:{}/", port);

    // Start Axum WebSocket-ONLY server on VIBEC0RE port 🔥
    let axum_server = AxumServer::new(
        port,
        state_store.clone(),
        event_bus.clone(),
        gateway.clone(),
    )
    .with_sync_engine(sync_engine.clone()); // 🔥 ENABLE OPTIMISTIC UPDATES FOR INSTANT UI!

    // 🔥 GET VIRTUAL DEVICE MANAGER BEFORE STARTING SERVER! 💖
    let virtual_device_manager = axum_server.virtual_device_manager();

    // 🔥 LOAD VIRTUAL DEVICES FROM TOML CONFIGS! 💖
    info!("📁 Loading virtual devices from virtual_devices/*.toml...");
    let virtual_dir = std::path::Path::new("virtual_devices");

    match v1bectl_virtual::load_virtual_devices_from_dir(virtual_dir).await {
        Ok(configs) => {
            info!("🔥 Found {} virtual device configs!", configs.len());

            for config in configs {
                match config {
                    v1bectl_virtual::VirtualDeviceTomlConfig::LightGroup(cfg) => {
                        info!("💡 Creating light group: {} ({})", cfg.name, cfg.device_id);

                        // Expand wildcards in members
                        let mut resolved_members = Vec::new();
                        for pattern in &cfg.members {
                            if pattern.contains('*') {
                                // Wildcard pattern - match all devices
                                let prefix = pattern.trim_end_matches('*');
                                let all_devices = state_store.list_devices().await;
                                for device in all_devices {
                                    if device.device_info.device_id.starts_with(prefix) {
                                        // Check if not excluded
                                        let is_excluded = cfg.settings.exclude.iter().any(|ex| {
                                            if ex.contains('*') {
                                                let ex_prefix = ex.trim_end_matches('*');
                                                device.device_info.device_id.starts_with(ex_prefix)
                                            } else {
                                                device.device_info.device_id == *ex
                                            }
                                        });

                                        if !is_excluded {
                                            resolved_members
                                                .push(device.device_info.device_id.clone());
                                        }
                                    }
                                }
                            } else {
                                // Direct device ID
                                resolved_members.push(pattern.clone());
                            }
                        }

                        info!("  Members: {:?}", resolved_members);

                        // Create VirtualDeviceConfig
                        // 🔥 CREATE DEFAULT BRIGHTNESS CURVES FOR EACH LIGHT! 💖
                        let mut brightness_curves = serde_json::Map::new();
                        for member in &resolved_members {
                            // Linear 1:1 mapping - group brightness = device brightness
                            brightness_curves.insert(
                                member.clone(),
                                serde_json::json!({
                                    "breakpoints": [[0, 0], [100, 100]]
                                }),
                            );
                        }

                        let vd_config = v1bectl_virtual::VirtualDeviceConfig {
                            device_id: cfg.device_id.clone(),
                            name: cfg.name.clone(),
                            description: Some(format!(
                                "Light group with {} members",
                                resolved_members.len()
                            )),
                            enabled: true,
                            device_type: v1bectl_virtual::VirtualDeviceType::LightGroup,
                            config: serde_json::json!({
                                "lights": resolved_members,
                                "aggregation": cfg.settings.aggregation,
                                "brightness_curves": brightness_curves,
                            }),
                        };

                        // Create and register the light group
                        match v1bectl_virtual::LightGroup::new(vd_config, state_store.clone()) {
                            Ok(light_group) => {
                                // 🔥 REGISTER WITH VIRTUAL DEVICE MANAGER! 💖
                                match virtual_device_manager
                                    .add_virtual_device(Box::new(light_group))
                                    .await
                                {
                                    Ok(_) => {
                                        info!(
                                            "✅ Light group {} registered successfully!",
                                            cfg.device_id
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "❌ Failed to register light group {}: {}",
                                            cfg.device_id, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!("❌ Failed to create light group {}: {}", cfg.device_id, e);
                            }
                        }
                    }
                    v1bectl_virtual::VirtualDeviceTomlConfig::LightGroupLinear(cfg) => {
                        info!(
                            "💡 Creating LINEAR light group: {} ({})",
                            cfg.name, cfg.device_id
                        );
                        info!("  Members: {:?}", cfg.members);
                        info!("  Brightness ranges: {:?}", cfg.brightness);

                        // Convert brightness array to tuples
                        let mut brightness_ranges = std::collections::HashMap::new();
                        for (name, range) in &cfg.brightness {
                            brightness_ranges.insert(name.clone(), (range[0], range[1]));
                        }

                        // Create VirtualDeviceConfig
                        let vd_config = v1bectl_virtual::VirtualDeviceConfig {
                            device_id: cfg.device_id.clone(),
                            name: cfg.name.clone(),
                            description: Some(format!(
                                "Linear light group with {} members",
                                cfg.members.len()
                            )),
                            enabled: true,
                            device_type: v1bectl_virtual::VirtualDeviceType::LightGroupLinear,
                            config: serde_json::json!({
                                "members": cfg.members,
                                "brightness_ranges": brightness_ranges,
                            }),
                        };

                        // Create and register the linear light group
                        match v1bectl_virtual::LightGroupLinear::new(
                            vd_config,
                            cfg.members.clone(),
                            brightness_ranges,
                            state_store.clone(),
                        ) {
                            Ok(light_group) => {
                                // 🔥 REGISTER WITH VIRTUAL DEVICE MANAGER! 💖
                                match virtual_device_manager
                                    .add_virtual_device(Box::new(light_group))
                                    .await
                                {
                                    Ok(_) => {
                                        info!(
                                            "✅ Linear light group {} registered successfully!",
                                            cfg.device_id
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "❌ Failed to register linear light group {}: {}",
                                            cfg.device_id, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "❌ Failed to create linear light group {}: {}",
                                    cfg.device_id, e
                                );
                            }
                        }
                    }
                    v1bectl_virtual::VirtualDeviceTomlConfig::ButtonController(cfg) => {
                        info!(
                            "🎮 Creating button controller: {} ({})",
                            cfg.name, cfg.device_id
                        );
                        info!("  Button: {}", cfg.button);
                        info!("  Press ON: {:?}", cfg.press_on);
                        info!("  Press OFF: {:?}", cfg.press_off);

                        // Create VirtualDeviceConfig
                        let vd_config = v1bectl_virtual::VirtualDeviceConfig {
                            device_id: cfg.device_id.clone(),
                            name: cfg.name.clone(),
                            description: Some(format!("Button controller for {}", cfg.button)),
                            enabled: true,
                            device_type: v1bectl_virtual::VirtualDeviceType::ButtonController,
                            config: serde_json::json!({
                                "button": cfg.button,
                                "press_on": cfg.press_on,
                                "press_off": cfg.press_off,
                                "press_on_long": cfg.press_on_long,
                                "press_off_long": cfg.press_off_long,
                            }),
                        };

                        // Create and register the button controller
                        match v1bectl_virtual::ButtonController::new(
                            vd_config,
                            cfg.button.clone(),
                            cfg.press_on.clone(),
                            cfg.press_off.clone(),
                            cfg.press_on_long.clone(),
                            cfg.press_off_long.clone(),
                            state_store.clone(),
                            event_bus.clone(),
                        ) {
                            Ok(button_controller) => {
                                // 🔥 REGISTER WITH VIRTUAL DEVICE MANAGER! 💖
                                match virtual_device_manager
                                    .add_virtual_device(Box::new(button_controller))
                                    .await
                                {
                                    Ok(_) => {
                                        info!(
                                            "✅ Button controller {} registered successfully!",
                                            cfg.device_id
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "❌ Failed to register button controller {}: {}",
                                            cfg.device_id, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "❌ Failed to create button controller {}: {}",
                                    cfg.device_id, e
                                );
                            }
                        }
                    }
                    v1bectl_virtual::VirtualDeviceTomlConfig::SceneController(cfg) => {
                        info!(
                            "🎬 Creating scene controller: {} ({})",
                            cfg.name, cfg.device_id
                        );

                        // TODO: Implement scene controller creation
                        info!("⚠️ Scene controllers not yet implemented!");
                    }
                }
            }
        }
        Err(e) => {
            warn!("⚠️ Failed to load virtual devices: {}", e);
        }
    }

    info!("📊 Updated Stats:");
    info!("  - Total devices: {}", state_store.device_count().await);
    info!("  - Virtual devices registered: Check logs above");
    let _server_handle = tokio::spawn(async move {
        if let Err(e) = axum_server.start().await {
            error!("❌ Axum server error: {}", e);
        }
    });

    // Start sync engine - BIDIRECTIONAL SYNC MAGIC! ⚡
    let sync_handle = {
        let sync_engine = sync_engine.clone();
        tokio::spawn(async move {
            if let Err(e) = sync_engine.start().await {
                error!("❌ Sync engine error: {}", e);
            }
        })
    };

    // 🔥 START EVENT LOGGER - LOG ALL EVENTS ESPECIALLY BUTTON/SWITCH! 💖
    {
        let mut event_rx = event_bus.subscribe();
        tokio::spawn(async move {
            info!("🎯 Event logger started - monitoring all device events!");

            while let Ok(event) = event_rx.recv().await {
                match &event.event_type {
                    EventType::ButtonPressed {
                        button_id,
                        press_type,
                    } => {
                        info!(
                            "🔘 BUTTON EVENT: Device {} - Button {} - Type: {:?}",
                            event.device_id, button_id, press_type
                        );
                    }
                    EventType::AttributeChanged {
                        attribute,
                        old_value,
                        new_value,
                    } => {
                        // Check if this is a switch/button state change
                        if attribute == "state" {
                            if let Some(state_obj) = new_value.as_object() {
                                if let Some(is_pressed) =
                                    state_obj.get("is_pressed").and_then(|v| v.as_bool())
                                {
                                    info!(
                                        "🎛️ SWITCH STATE: Device {} - Pressed: {} -> {}",
                                        event.device_id,
                                        old_value
                                            .as_object()
                                            .and_then(|o| o.get("is_pressed"))
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false),
                                        is_pressed
                                    );
                                }
                            }
                        } else if attribute == "isPressed" || attribute == "buttonState" {
                            info!(
                                "🔲 BUTTON ATTRIBUTE: Device {} - {} changed from {} to {}",
                                event.device_id, attribute, old_value, new_value
                            );
                        }

                        // Log all other attribute changes at debug level
                        debug!(
                            "📝 Attribute change: {} - {} -> {}",
                            event.device_id, attribute, new_value
                        );
                    }
                    EventType::StateChanged {
                        old_state,
                        new_state,
                    } => {
                        // Check for switch state changes
                        if let (
                            Some(DeviceStateValue::Switch(old)),
                            Some(DeviceStateValue::Switch(new)),
                        ) = (old_state, new_state)
                        {
                            info!(
                                "🔄 SWITCH STATE CHANGED: Device {} - Pressed: {} -> {}",
                                event.device_id, old.is_pressed, new.is_pressed
                            );
                        }
                    }
                    EventType::DeviceAdded { device_type } => {
                        info!("➕ Device added: {} ({})", event.device_id, device_type);
                    }
                    EventType::DeviceRemoved => {
                        info!("➖ Device removed: {}", event.device_id);
                    }
                    EventType::DeviceReachabilityChanged { reachable } => {
                        info!(
                            "📶 Device {} is now {}",
                            event.device_id,
                            if *reachable { "ONLINE" } else { "OFFLINE" }
                        );
                    }
                    _ => {
                        debug!(
                            "📨 Event: {:?} for device {}",
                            event.event_type, event.device_id
                        );
                    }
                }
            }

            warn!("⚠️ Event logger stopped - no more events");
        });
    }

    // Start periodic sync stats reporter - VIBEC0RE TELEMETRY! 📈
    let sync_engine_stats = sync_engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            let stats = sync_engine_stats.get_sync_stats().await;
            info!(
                "📊 Sync Stats - InSync: {} | Pending: {} | Failed: {} | Conflicts: {} | Queue: {}",
                stats.in_sync_devices,
                stats.pending_sync_devices,
                stats.failed_devices,
                stats.conflict_devices,
                stats.pending_sync_tasks
            );
        }
    });

    // Start gateway health monitor - VIBEC0RE MONITORING! 💓
    let gateway_monitor = gateway.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            match gateway_monitor.health_check().await {
                Ok(health) => {
                    if health.reachable {
                        info!(
                            "💚 Gateway healthy - Response: {}ms",
                            health.response_time_ms
                        );
                    } else {
                        warn!(
                            "💔 Gateway unhealthy - {}",
                            health
                                .last_error
                                .unwrap_or_else(|| "Unknown error".to_string())
                        );
                    }
                }
                Err(e) => {
                    error!("❌ Gateway health check failed: {}", e);
                }
            }
        }
    });

    // Wait for shutdown signal - VIBEC0RE GRACEFUL SHUTDOWN! 🛑
    info!("🚀 VIBEC0RE SERVER RUNNING - Press Ctrl+C to shutdown");
    tokio::signal::ctrl_c().await?;
    info!("🛑 Shutdown signal received...");

    // Stop sync engine
    sync_engine.stop().await;

    // Wait for sync worker to finish
    if let Err(e) = sync_handle.await {
        warn!("⚠️ Sync engine shutdown error: {}", e);
    }

    info!("✅ VIBEC0RE SERVER SHUTDOWN COMPLETE! 🔥");
    Ok(())
}
