//! The widget's own I/O: a WebSocket client to `v1bectl_server`.
//!
//! Mirrors the server's crate-private API envelope (the established pattern —
//! see `v1bectl_cli/src/websocket_client.rs`); the *inner* types
//! (`DeviceState`, `DeviceEvent`, …) come straight from `v1bectl_state`, the
//! same source of truth the server serializes from.
//!
//! [`client`] is the long-running task: connect → `DiscoverDevices` →
//! select-loop over {server frames, widget commands, keepalive} — with
//! exponential-backoff reconnect (1→16 s, the same curve as the GTK client).
//! It reports into the reducer via [`WsMsg`] and takes [`Cmd`]s from it.
//! Frame decode/encode are pure functions ([`decode_inbound`] /
//! [`encode_request`]) so the protocol layer is unit-testable without a
//! socket.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;
use v1bectl_state::{DeviceEvent, DeviceState, RgbColor};

/// Where the server lives: `$V1BECTL_SERVER` (host:port) or the default every
/// v1bectl client uses.
pub fn server_url() -> String {
    let addr = std::env::var("V1BECTL_SERVER").unwrap_or_else(|_| "127.0.0.1:31337".to_string());
    if addr.starts_with("ws://") || addr.starts_with("wss://") {
        addr
    } else {
        format!("ws://{}/", addr)
    }
}

// ── Mirrored API envelope (subset) ───────────────────────────────────────────

/// Outer envelope — mirror of the server's `ApiMessage` (CBOR body inside
/// CBOR envelope, binary WS frames).
#[derive(Debug, Serialize, Deserialize)]
struct ApiMessage {
    correlation_id: String,
    message_type: ApiMessageType,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

/// The requests this widget sends (subset of the server's `ApiRequest`;
/// external tagging means unmirrored variants simply don't exist for us).
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ApiRequest {
    DiscoverDevices,
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
    Ping,
}

/// The responses this widget understands (subset). Anything else decodes to
/// an error and is dropped — events carry the authoritative state anyway.
#[derive(Debug, Serialize, Deserialize)]
enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    },
    Pong,
    Error {
        code: String,
        message: String,
    },
}

// ── Reducer-facing messages & commands ───────────────────────────────────────

/// Connection state as the reducer sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conn {
    Connecting,
    Online,
    Offline,
}

/// What the WS task reports to the reducer (arrives as `Input::App`).
#[derive(Debug)]
pub enum WsMsg {
    Status(Conn),
    Devices(Vec<DeviceState>),
    Event(DeviceEvent),
}

/// What the reducer asks the WS task to do (via the model's `cmd_tx`).
#[derive(Debug, PartialEq)]
pub enum Cmd {
    SetLight {
        device_id: String,
        is_on: Option<bool>,
        brightness: Option<u8>,
    },
    SetOutlet {
        device_id: String,
        is_on: bool,
    },
    Refresh,
}

// ── Pure frame codec ─────────────────────────────────────────────────────────

/// A decoded inbound frame, reduced to what the widget cares about.
#[derive(Debug)]
pub enum Inbound {
    Devices(Vec<DeviceState>),
    Event(DeviceEvent),
    /// Valid envelope, but nothing we act on (acks, unknown responses, ...).
    Ignored,
}

/// Decode one binary WS frame. `None` = not even an envelope (log and drop).
pub fn decode_inbound(buf: &[u8]) -> Option<Inbound> {
    let msg: ApiMessage = ciborium::from_reader(buf).ok()?;
    let inbound = match msg.message_type {
        ApiMessageType::Event => match ciborium::from_reader::<DeviceEvent, _>(&msg.payload[..]) {
            Ok(ev) => Inbound::Event(ev),
            Err(_) => Inbound::Ignored,
        },
        ApiMessageType::Response => {
            match ciborium::from_reader::<ApiResponse, _>(&msg.payload[..]) {
                Ok(ApiResponse::DeviceList { devices, .. }) => Inbound::Devices(devices),
                // Pong acks, LightUpdated acks (unmirrored → Err), errors:
                // events are the source of truth, so all of these are noise.
                _ => Inbound::Ignored,
            }
        }
        // The server never sends Request frames to clients; Error carries
        // nothing an echo-driven widget acts on.
        ApiMessageType::Request | ApiMessageType::Error => Inbound::Ignored,
    };
    Some(inbound)
}

/// Encode a request into a whole binary frame with a fresh correlation id.
pub fn encode_request(req: &ApiRequest) -> Vec<u8> {
    let mut payload = Vec::new();
    ciborium::into_writer(req, &mut payload).expect("CBOR of a plain request cannot fail");
    let msg = ApiMessage {
        correlation_id: Uuid::new_v4().to_string(),
        message_type: ApiMessageType::Request,
        payload,
    };
    let mut frame = Vec::new();
    ciborium::into_writer(&msg, &mut frame).expect("CBOR of the envelope cannot fail");
    frame
}

/// Translate a reducer command into the wire request.
fn request_for(cmd: Cmd) -> ApiRequest {
    match cmd {
        Cmd::SetLight {
            device_id,
            is_on,
            brightness,
        } => ApiRequest::SetLightState {
            device_id,
            is_on,
            brightness,
            color_temp: None,
            rgb_color: None,
        },
        Cmd::SetOutlet { device_id, is_on } => ApiRequest::SetOutletState { device_id, is_on },
        Cmd::Refresh => ApiRequest::DiscoverDevices,
    }
}

// ── The client task ──────────────────────────────────────────────────────────

/// Reconnect backoff, the GTK-client curve: 1, 2, 4, 8, then 16 s forever.
fn backoff_secs(attempt: u32) -> u64 {
    (1u64 << attempt.min(4)).min(30)
}

/// A session must live this long before the backoff resets — an
/// accept-then-drop server must not defeat the curve (same guard as the
/// hytte-plugin runtime's shell-socket backoff).
const STABLE_SESSION: Duration = Duration::from_secs(30);

/// The long-running WS client. Exits when the reducer side is gone (`msg_tx`
/// closed) — i.e. when the plugin session ends and its sources are dropped.
pub async fn client(
    mut cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    msg_tx: mpsc::UnboundedSender<WsMsg>,
) {
    let url = server_url();
    let mut attempt: u32 = 0;
    loop {
        if msg_tx.send(WsMsg::Status(Conn::Connecting)).is_err() {
            return;
        }
        match connect_async(&url).await {
            Ok((stream, _)) => {
                // Anything queued while the server was unreachable (or while
                // the connect was in flight) is stale user intent — drop it
                // rather than replaying a toggle burst into the fresh session.
                while cmd_rx.try_recv().is_ok() {}
                let started = Instant::now();
                if run_conn(stream, &mut cmd_rx, &msg_tx).await.is_none() {
                    return; // reducer gone
                }
                if started.elapsed() >= STABLE_SESSION {
                    attempt = 0;
                }
                if msg_tx.send(WsMsg::Status(Conn::Offline)).is_err() {
                    return;
                }
            }
            Err(e) => {
                eprintln!("[v1bectl-widget] connect {url}: {e}");
                if msg_tx.send(WsMsg::Status(Conn::Offline)).is_err() {
                    return;
                }
            }
        }
        sleep(Duration::from_secs(backoff_secs(attempt))).await;
        attempt = attempt.saturating_add(1);
    }
}

/// Drive one live connection. Returns `Some(())` on server disconnect
/// (caller backs off and redials), `None` when the reducer side is gone.
async fn run_conn(
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    cmd_rx: &mut mpsc::UnboundedReceiver<Cmd>,
    msg_tx: &mpsc::UnboundedSender<WsMsg>,
) -> Option<()> {
    let (mut sink, mut source) = stream.split();

    if msg_tx.send(WsMsg::Status(Conn::Online)).is_err() {
        return None;
    }
    // Initial population; every socket is auto-subscribed to all events.
    if sink
        .send(Message::Binary(encode_request(
            &ApiRequest::DiscoverDevices,
        )))
        .await
        .is_err()
    {
        return Some(());
    }

    let mut keepalive = interval(Duration::from_secs(30));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await; // consume the immediate first tick

    // Read-side watchdog: a healthy connection has inbound traffic at least
    // every keepalive round (the server Pongs our Ping), so a long silence
    // means a half-open TCP connection — reconnect instead of sitting
    // "Online" with dead controls until the kernel notices.
    const SILENCE_LIMIT: Duration = Duration::from_secs(90);
    let mut last_inbound = Instant::now();

    loop {
        tokio::select! {
            frame = source.next() => match frame {
                Some(Ok(Message::Binary(buf))) => {
                    last_inbound = Instant::now();
                    match decode_inbound(&buf) {
                        Some(Inbound::Devices(devices)) => {
                            if msg_tx.send(WsMsg::Devices(devices)).is_err() {
                                return None;
                            }
                        }
                        Some(Inbound::Event(ev)) => {
                            if msg_tx.send(WsMsg::Event(ev)).is_err() {
                                return None;
                            }
                        }
                        Some(Inbound::Ignored) => {}
                        None => eprintln!("[v1bectl-widget] undecodable frame dropped"),
                    }
                }
                Some(Ok(Message::Close(_))) | None => return Some(()),
                Some(Ok(_)) => {
                    // text/ping/pong frames — tungstenite answers pongs itself
                    last_inbound = Instant::now();
                }
                Some(Err(e)) => {
                    eprintln!("[v1bectl-widget] ws error: {e}");
                    return Some(());
                }
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(cmd) => {
                    let frame = encode_request(&request_for(cmd));
                    if sink.send(Message::Binary(frame)).await.is_err() {
                        return Some(());
                    }
                }
                // The model (and its cmd_tx) lives as long as the session;
                // channel closed = session gone.
                None => return None,
            },
            _ = keepalive.tick() => {
                if last_inbound.elapsed() > SILENCE_LIMIT {
                    eprintln!("[v1bectl-widget] no inbound traffic for {SILENCE_LIMIT:?}; reconnecting");
                    return Some(());
                }
                let frame = encode_request(&ApiRequest::Ping);
                if sink.send(Message::Binary(frame)).await.is_err() {
                    return Some(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use v1bectl_state::{DeviceInfo, DeviceType, EventType, LightState};

    fn frame(message_type: ApiMessageType, correlation_id: &str, payload: Vec<u8>) -> Vec<u8> {
        let msg = ApiMessage {
            correlation_id: correlation_id.to_string(),
            message_type,
            payload,
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&msg, &mut buf).unwrap();
        buf
    }

    fn cbor<T: Serialize>(value: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(value, &mut buf).unwrap();
        buf
    }

    #[test]
    fn encode_request_round_trips_through_the_envelope() {
        let buf = encode_request(&ApiRequest::SetLightState {
            device_id: "l1".into(),
            is_on: Some(true),
            brightness: Some(50),
            color_temp: None,
            rgb_color: None,
        });
        let msg: ApiMessage = ciborium::from_reader(&buf[..]).unwrap();
        assert!(matches!(msg.message_type, ApiMessageType::Request));
        let req: ApiRequest = ciborium::from_reader(&msg.payload[..]).unwrap();
        assert!(
            matches!(req, ApiRequest::SetLightState { device_id, is_on: Some(true), brightness: Some(50), .. } if device_id == "l1")
        );
    }

    #[test]
    fn decode_inbound_event_frame() {
        let ev = DeviceEvent {
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            device_id: "l1".into(),
            event_type: EventType::StateChanged {
                old_state: None,
                new_state: Some(v1bectl_state::DeviceStateValue::Light(LightState {
                    is_on: true,
                    brightness: Some(80),
                    color_temp: None,
                    rgb_color: None,
                })),
            },
        };
        let buf = frame(ApiMessageType::Event, "x", cbor(&ev));
        assert!(matches!(decode_inbound(&buf), Some(Inbound::Event(got)) if got.device_id == "l1"));
    }

    #[test]
    fn decode_inbound_device_list_response() {
        let dev = DeviceState {
            device_id: "l1".into(),
            device_info: DeviceInfo {
                device_id: "l1".into(),
                name: "Lamp".into(),
                device_type: DeviceType::Light,
                capabilities: vec![],
                device_groups: vec![],
                manufacturer: None,
                model: None,
                firmware_version: None,
                battery_powered: false,
                reachable: true,
                last_seen: 0,
                custom_attributes: Default::default(),
            },
            state: v1bectl_state::DeviceStateValue::Empty,
            last_updated: 0,
            last_synced_to_gateway: None,
            last_synced_from_gateway: None,
        };
        let payload = cbor(&ApiResponse::DeviceList {
            devices: vec![dev],
            total_count: 1,
        });
        let buf = frame(ApiMessageType::Response, "x", payload);
        assert!(
            matches!(decode_inbound(&buf), Some(Inbound::Devices(d)) if d.len() == 1 && d[0].device_id == "l1")
        );
    }

    #[test]
    fn unmirrored_response_variants_are_ignored_not_fatal() {
        // A response variant this widget doesn't mirror (e.g. LightUpdated):
        // build it as raw CBOR external tagging {"LightUpdated": {...}}.
        let payload = cbor(&ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("LightUpdated".into()),
            ciborium::value::Value::Map(vec![]),
        )]));
        let buf = frame(ApiMessageType::Response, "x", payload);
        assert!(matches!(decode_inbound(&buf), Some(Inbound::Ignored)));
    }

    #[test]
    fn garbage_frames_decode_to_none() {
        assert!(decode_inbound(&[0xde, 0xad, 0xbe, 0xef]).is_none());
    }

    #[test]
    fn backoff_curve_matches_the_gtk_client() {
        let curve: Vec<u64> = (0..7).map(backoff_secs).collect();
        assert_eq!(curve, vec![1, 2, 4, 8, 16, 16, 16]);
    }
}

/// Live end-to-end tests — need a running server:
/// `cargo run -p v1bectl_server -- dummy` then
/// `cargo test -p v1bectl_widget -- --ignored`.
#[cfg(test)]
mod live_tests {
    use super::*;
    use tokio::time::timeout;
    use v1bectl_state::DeviceStateValue;

    const DEADLINE: Duration = Duration::from_secs(5);

    #[tokio::test]
    #[ignore = "needs a running v1bectl_server on 127.0.0.1:31337"]
    async fn live_round_trip_discover_toggle_event_echo() {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        tokio::spawn(client(cmd_rx, msg_tx));

        // Connect → discover.
        let devices = loop {
            match timeout(DEADLINE, msg_rx.recv())
                .await
                .expect("a message before the deadline")
                .expect("client task alive")
            {
                WsMsg::Devices(d) => break d,
                WsMsg::Status(Conn::Offline) => {
                    panic!("server unreachable — start v1bectl_server first")
                }
                _ => {}
            }
        };
        assert!(!devices.is_empty(), "the dummy scenario provides devices");

        // The event-decode pipeline first: the dummy scenario flaps
        // reachability every few seconds, so ANY decoded event inside the
        // window proves the DeviceEvent path.
        loop {
            if let WsMsg::Event(ev) = timeout(Duration::from_secs(10), msg_rx.recv())
                .await
                .expect("the dummy's periodic events decode and arrive")
                .expect("client task alive")
            {
                eprintln!(
                    "[live] event pipeline ok: {:?} for {}",
                    ev.event_type, ev.device_id
                );
                break;
            }
        }

        // A PHYSICAL light — virtual light groups also carry Light-shaped
        // state, but the dummy scenario's group has broken member refs.
        let light = devices
            .iter()
            .find(|d| {
                d.device_info.device_type == v1bectl_state::DeviceType::Light
                    && matches!(d.state, DeviceStateValue::Light(_))
            })
            .expect("the dummy scenario provides a physical light")
            .clone();
        let was_on = matches!(&light.state, DeviceStateValue::Light(l) if l.is_on);
        eprintln!(
            "[live] {} devices; toggling {:?} (was_on={})",
            devices.len(),
            light.device_info.name,
            was_on
        );

        // Toggle → the sync engine's event echo is the proof of the loop.
        cmd_tx
            .send(Cmd::SetLight {
                device_id: light.device_id.clone(),
                is_on: Some(!was_on),
                brightness: None,
            })
            .unwrap();
        loop {
            match timeout(DEADLINE, msg_rx.recv())
                .await
                .expect("the toggle's event echo before the deadline")
                .expect("client task alive")
            {
                WsMsg::Event(ev) if ev.device_id == light.device_id => break,
                _ => {}
            }
        }

        // Leave the dummy as we found it — and AWAIT the restore's echo:
        // ending the test here would cancel the client task at runtime
        // teardown before the frame is ever written.
        cmd_tx
            .send(Cmd::SetLight {
                device_id: light.device_id.clone(),
                is_on: Some(was_on),
                brightness: None,
            })
            .unwrap();
        loop {
            match timeout(DEADLINE, msg_rx.recv())
                .await
                .expect("the restore's event echo before the deadline")
                .expect("client task alive")
            {
                WsMsg::Event(ev) if ev.device_id == light.device_id => break,
                _ => {}
            }
        }
    }
}
