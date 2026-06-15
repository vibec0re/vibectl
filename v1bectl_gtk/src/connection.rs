// 🔥 WEBSOCKET CONNECTION MANAGER - AUTO-RECONNECT ENGAGED!!! 🚀

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::bridge::{ConnectionState, GtkMsg, ServerCmd};
use crate::config::ServerConfig;
use crate::protocol::*;
use v1bectl_sync::*;

// ============================================================
// 🚀 MAIN RECONNECT LOOP
// ============================================================

/// Run the WebSocket connection loop on the Tokio runtime.
/// This function never returns — it reconnects forever.
pub async fn run(
    server: ServerConfig,
    gtk_tx: mpsc::UnboundedSender<GtkMsg>,
    mut cmd_rx: mpsc::Receiver<ServerCmd>,
) {
    let mut attempt: u32 = 0;

    loop {
        let url = format!("ws://{}:{}", server.host, server.port);
        let _ = gtk_tx.send(GtkMsg::ConnectionStatus(ConnectionState::Connecting));

        match connect_and_run(&url, &gtk_tx, &mut cmd_rx).await {
            Ok(()) => {
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!("🔥 Connection failed: {} — reconnecting...", e);
                attempt += 1;
            }
        }

        let _ = gtk_tx.send(GtkMsg::ConnectionStatus(ConnectionState::Reconnecting(
            attempt,
        )));

        // Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 30s
        let delay = Duration::from_secs((1u64 << attempt.min(4)).min(30));
        tokio::time::sleep(delay).await;
    }
}

// ============================================================
// 🔧 CONNECTION LIFECYCLE
// ============================================================

async fn connect_and_run(
    url: &str,
    gtk_tx: &mpsc::UnboundedSender<GtkMsg>,
    cmd_rx: &mut mpsc::Receiver<ServerCmd>,
) -> anyhow::Result<()> {
    tracing::info!("🚀 Connecting to VIBEC0RE server: {}", url);

    let (ws_stream, _) = connect_async(url).await?;
    let (mut sender, mut receiver) = ws_stream.split();

    tracing::info!("✅ WebSocket connected!");
    let _ = gtk_tx.send(GtkMsg::ConnectionStatus(ConnectionState::Connected));

    // 🔥 Step 1: Discover all devices
    let devices = discover(&mut sender, &mut receiver).await?;
    tracing::info!("✅ Discovered {} devices!", devices.len());
    let _ = gtk_tx.send(GtkMsg::DevicesDiscovered(devices));

    // 🔥 Step 2: Subscribe to all events
    subscribe_all(&mut sender, &mut receiver).await?;
    tracing::info!("✅ Subscribed to all events!");

    // 🔥 Step 3: Main event loop
    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
    ping_interval.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            // Incoming WebSocket messages
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        handle_incoming(&data, gtk_tx)?;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        sender.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::info!("🔧 Server closed the connection");
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        return Err(anyhow::anyhow!("WebSocket error: {}", e));
                    }
                    None => {
                        return Err(anyhow::anyhow!("WebSocket stream ended"));
                    }
                    _ => {}
                }
            }

            // Commands from the GTK thread
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        send_command(&mut sender, cmd).await?;
                    }
                    None => {
                        // Channel closed — GTK is shutting down
                        tracing::info!("🔧 Command channel closed, shutting down connection");
                        return Ok(());
                    }
                }
            }

            // Periodic ping to keep the connection alive
            _ = ping_interval.tick() => {
                tracing::debug!("🔧 Sending ping");
                sender.send(Message::Ping(vec![])).await?;
            }
        }
    }
}

// ============================================================
// 🔧 PROTOCOL HELPERS
// ============================================================

/// Discover all devices: send DiscoverDevices, await matching DeviceList response.
async fn discover<S, R>(sender: &mut S, receiver: &mut R) -> anyhow::Result<Vec<DeviceState>>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
    R: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let correlation_id = send_request(sender, ApiRequest::DiscoverDevices).await?;

    // Wait for the matching DeviceList response
    while let Some(msg) = receiver.next().await {
        let data = match msg? {
            Message::Binary(d) => d,
            _ => continue,
        };

        let api_msg: ApiMessage = match ciborium::from_reader(data.as_slice()) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if api_msg.correlation_id != correlation_id {
            continue;
        }

        if matches!(api_msg.message_type, ApiMessageType::Response) {
            let response: ApiResponse = ciborium::from_reader(api_msg.payload.as_slice())?;
            if let ApiResponse::DeviceList { devices, .. } = response {
                return Ok(devices);
            }
        }
    }

    Err(anyhow::anyhow!("Connection lost during device discovery"))
}

/// Subscribe to all device events: send Subscribe{device_ids: ["*"]}, await SubscriptionStarted.
async fn subscribe_all<S, R>(sender: &mut S, receiver: &mut R) -> anyhow::Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
    R: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let correlation_id = send_request(
        sender,
        ApiRequest::Subscribe {
            device_ids: vec!["*".to_string()],
        },
    )
    .await?;

    // Wait for the SubscriptionStarted acknowledgement
    while let Some(msg) = receiver.next().await {
        let data = match msg? {
            Message::Binary(d) => d,
            _ => continue,
        };

        let api_msg: ApiMessage = match ciborium::from_reader(data.as_slice()) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if api_msg.correlation_id != correlation_id {
            continue;
        }

        if matches!(api_msg.message_type, ApiMessageType::Response) {
            let response: ApiResponse = ciborium::from_reader(api_msg.payload.as_slice())?;
            if let ApiResponse::SubscriptionStarted { subscriber_id } = response {
                tracing::debug!("🔧 Subscription started: {}", subscriber_id);
                return Ok(());
            }
        }
    }

    Err(anyhow::anyhow!("Connection lost during subscribe"))
}

/// Decode an incoming binary WS message; if it's an Event with StateChanged, forward to GTK.
fn handle_incoming(data: &[u8], gtk_tx: &mpsc::UnboundedSender<GtkMsg>) -> anyhow::Result<()> {
    let api_msg: ApiMessage = match ciborium::from_reader(data) {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!("🔧 Failed to decode ApiMessage: {}", e);
            return Ok(());
        }
    };

    if !matches!(api_msg.message_type, ApiMessageType::Event) {
        return Ok(());
    }

    let event: DeviceEvent = match ciborium::from_reader(api_msg.payload.as_slice()) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!("🔧 Failed to decode DeviceEvent: {}", e);
            return Ok(());
        }
    };

    // 🔥 Two event variants carry state updates:
    //   - StateChanged: emitted by the legacy (non-sync-engine) write path
    //   - AttributeChanged{attribute:"state", new_value:<serialized DeviceStateValue>}:
    //     emitted by the sync engine's optimistic update path AND by pull-worker
    //     conflict resolution. This is what the webapp listens for and is the
    //     dominant event type when sync_engine is enabled (the default).
    match event.event_type {
        EventType::StateChanged {
            new_state: Some(new_state),
            ..
        } => {
            tracing::debug!("🔧 StateChanged for device {}", event.device_id);
            let _ = gtk_tx.send(GtkMsg::DeviceStateChanged {
                device_id: event.device_id,
                new_state,
            });
        }
        EventType::AttributeChanged {
            attribute,
            new_value,
            ..
        } if attribute == "state" => match serde_json::from_value::<DeviceStateValue>(new_value) {
            Ok(new_state) => {
                tracing::debug!("🔧 AttributeChanged(state) for device {}", event.device_id);
                let _ = gtk_tx.send(GtkMsg::DeviceStateChanged {
                    device_id: event.device_id,
                    new_state,
                });
            }
            Err(e) => {
                tracing::debug!(
                    "🔧 Failed to decode AttributeChanged.new_value for {}: {}",
                    event.device_id,
                    e
                );
            }
        },
        _ => {}
    }

    Ok(())
}

/// Map a ServerCmd to an ApiRequest and send it over the WebSocket.
async fn send_command<S>(sender: &mut S, cmd: ServerCmd) -> anyhow::Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let request = match cmd {
        ServerCmd::SetLightState {
            device_id,
            is_on,
            brightness,
            color_temp,
        } => ApiRequest::SetLightState {
            device_id,
            is_on,
            brightness,
            color_temp,
            rgb_color: None,
        },
        ServerCmd::SetOutletState { device_id, is_on } => {
            ApiRequest::SetOutletState { device_id, is_on }
        }
        ServerCmd::Rediscover => ApiRequest::DiscoverDevices,
    };

    send_request(sender, request).await?;
    Ok(())
}

/// Encode an ApiRequest into an ApiMessage, send as binary WS frame, return correlation_id.
async fn send_request<S>(sender: &mut S, request: ApiRequest) -> anyhow::Result<String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    // 🔥 Encode request payload
    let mut payload = Vec::new();
    ciborium::into_writer(&request, &mut payload)?;

    let correlation_id = Uuid::new_v4().to_string();

    let api_msg = ApiMessage {
        correlation_id: correlation_id.clone(),
        message_type: ApiMessageType::Request,
        payload,
    };

    let mut bytes = Vec::new();
    ciborium::into_writer(&api_msg, &mut bytes)?;

    sender.send(Message::Binary(bytes)).await?;

    Ok(correlation_id)
}
