//! #8, item 3: the API round trip, over a real socket.
//!
//! The real axum server runs on an ephemeral port, wired the way
//! `v1bectl_server dummy` wires it: the dummy `basic_home` hub seeds the
//! store, a sync engine is attached and running, and the shipped Bedroom
//! Lights group (`virtual_devices/bedroom_lights.toml`) is registered with
//! the server's virtual device manager before the server starts. Left out:
//! the dummy hub's event stream, which the server forwards onto the bus.
//! It's random (an attribute or reachability event now and then), and
//! nothing here reads it.
//!
//! A tokio-tungstenite client talks to it the way the CLI and the TUI do:
//! CBOR `ApiMessage` envelopes around CBOR requests. It keeps its own copy
//! of the wire types, as every client does (the server's are private), so
//! a rename on either side breaks these tests the way it breaks a client.
//!
//! Nothing waits on a clock. Every wait ends on the frame it waits for, and
//! a timeout only bounds a failure. Paused time doesn't fit here: it
//! auto-advances whenever the runtime is idle, waiting on the socket
//! included, so a bounded wait would time out while a frame is on its way
//! (and the sync engine's timers would run ahead of the socket).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;
use v1bectl_api::AxumServer;
use v1bectl_sync::{
    DeviceEvent, DeviceStateValue, EventBus, EventType, Gateway, LightState, RgbColor, StateStore,
    SyncEngine,
};
use v1bectl_virtual::{
    load_virtual_devices_from_dir, DummyGateway, LightGroupLinear, VirtualDeviceConfig,
    VirtualDeviceTomlConfig, VirtualDeviceType,
};

// ── The wire, as a client sees it ────────────────────────────────────────────

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

/// The requests these tests send: variants go by name, so a subset is fine.
#[derive(Serialize, Debug)]
enum ApiRequest {
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
    Ping,
}

/// The answers to them.
#[derive(Deserialize, Debug)]
enum ApiResponse {
    DeviceState {
        device_id: String,
        state: DeviceStateValue,
    },
    LightUpdated {
        new_state: LightState,
    },
    Pong,
    Error {
        code: String,
        message: String,
    },
}

/// `DeviceState` as a client from before #10 knows it (the web UI's copy
/// still has this shape): no `device_id`.
#[derive(Deserialize, Debug)]
enum OlderClientResponse {
    DeviceState { state: DeviceStateValue },
}

fn cbor(value: &impl Serialize) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("encode CBOR");
    bytes
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> T {
    ciborium::from_reader(bytes).unwrap_or_else(|e| {
        panic!(
            "decode a {} from the server: {e}",
            std::any::type_name::<T>()
        )
    })
}

/// The state `event` echoes, in the `AttributeChanged{attribute: "state"}`
/// shape every client decodes (the sync engine's and the virtual device
/// manager's), or `None` for any other event.
fn state_echo(event: &DeviceEvent) -> Option<DeviceStateValue> {
    match &event.event_type {
        EventType::AttributeChanged {
            attribute,
            new_value,
            ..
        } if attribute == "state" => serde_json::from_value(new_value.clone()).ok(),
        _ => None,
    }
}

// ── A client ─────────────────────────────────────────────────────────────────

/// How long any one wait may take. It only bounds a failure: every wait
/// ends on the frame it waits for.
const PATIENCE: Duration = Duration::from_secs(10);

struct Client {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Events that came in while it waited for something else, in order.
    events: VecDeque<DeviceEvent>,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let (socket, _) = timeout(PATIENCE, connect_async(format!("ws://{addr}/")))
            .await
            .expect("the server never finished the WebSocket handshake")
            .expect("connect");
        Self {
            socket,
            events: VecDeque::new(),
        }
    }

    /// Send `request` under `correlation_id`.
    async fn send(&mut self, correlation_id: &str, request: &ApiRequest) {
        let message = ApiMessage {
            correlation_id: correlation_id.to_string(),
            message_type: ApiMessageType::Request,
            payload: cbor(request),
        };
        self.socket
            .send(Message::Binary(cbor(&message)))
            .await
            .expect("send");
    }

    /// The next envelope from the server.
    async fn next_message(&mut self) -> ApiMessage {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Binary(frame))) => return decode(&frame),
                Some(Ok(Message::Close(frame))) => {
                    panic!("the server closed the socket: {frame:?}")
                }
                // Pings, pongs: nothing a client reads.
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("socket: {e}"),
                None => panic!("the socket ended"),
            }
        }
    }

    /// The next response on the socket: its correlation id, and its
    /// payload. Events that come first are kept for [`Self::echo`].
    async fn next_response(&mut self) -> (String, Vec<u8>) {
        timeout(PATIENCE, async {
            loop {
                let message = self.next_message().await;
                match message.message_type {
                    ApiMessageType::Response => return (message.correlation_id, message.payload),
                    ApiMessageType::Event => self.events.push_back(decode(&message.payload)),
                    _ => panic!("unexpected message: {message:?}"),
                }
            }
        })
        .await
        .expect("no response from the server")
    }

    /// Send `request` under a fresh correlation id, and return the payload
    /// of the answer. That's the next response on the socket, and it must
    /// carry the same id: the CLI and the web UI match answers by it.
    async fn request_payload(&mut self, request: &ApiRequest) -> Vec<u8> {
        let correlation_id = Uuid::new_v4().to_string();
        self.send(&correlation_id, request).await;
        let (answered, payload) = self.next_response().await;
        assert_eq!(
            answered, correlation_id,
            "the answer to {request:?} must carry the request's correlation id"
        );
        payload
    }

    /// [`Self::request_payload`], decoded. An `Error` fails the test.
    async fn request(&mut self, request: &ApiRequest) -> ApiResponse {
        let response = decode(&self.request_payload(request).await);
        if let ApiResponse::Error { code, message } = &response {
            panic!("the server refused {request:?}: {code}: {message}");
        }
        response
    }

    /// The state in the first event for `device_id` that this client
    /// hasn't taken yet, whether it came in already or is still to come.
    /// It must be a state echo.
    async fn echo(&mut self, device_id: &str) -> DeviceStateValue {
        let event = timeout(PATIENCE, async {
            loop {
                if let Some(at) = self.events.iter().position(|e| e.device_id == device_id) {
                    return self.events.remove(at).expect("found above");
                }
                let message = self.next_message().await;
                match message.message_type {
                    ApiMessageType::Event => self.events.push_back(decode(&message.payload)),
                    _ => panic!("unexpected message: {message:?}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{device_id} was never echoed over the socket"));
        state_echo(&event).unwrap_or_else(|| {
            panic!("the first event for {device_id} isn't a state echo: {event:?}")
        })
    }
}

// ── The server ───────────────────────────────────────────────────────────────

/// The shipped Bedroom Lights (`virtual_devices/bedroom_lights.toml`).
const GROUP: &str = "virtual_bedroom_lights";

/// A light of the dummy `basic_home` hub, on at 75 at start. It's also a
/// member of Bedroom Lights.
const LIGHT: &str = "light_kitchen";

/// (member, its brightness when Bedroom Lights is on at 50%): the linear
/// ranges of the shipped `virtual_devices/bedroom_lights.toml`.
const MEMBERS_AT_50: [(&str, u8); 3] = [
    ("light_bedroom", 90),
    ("light_living_room", 65),
    ("light_kitchen", 25),
];

/// A running server, and the store behind it.
struct Home {
    addr: SocketAddr,
    store: Arc<StateStore>,
}

/// The shipped Bedroom Lights, read from its file and built the way
/// `v1bectl_server` builds a linear group.
async fn bedroom_lights(store: &Arc<StateStore>) -> LightGroupLinear {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../virtual_devices");
    let configs = load_virtual_devices_from_dir(&dir)
        .await
        .expect("load virtual_devices/");
    let Some(c) = configs.into_iter().find_map(|config| match config {
        VirtualDeviceTomlConfig::LightGroupLinear(c) if c.device_id == GROUP => Some(c),
        _ => None,
    }) else {
        panic!("virtual_devices/ no longer ships {GROUP}");
    };
    let ranges: HashMap<String, (u8, u8)> = c
        .brightness
        .iter()
        .map(|(name, [min, max])| (name.clone(), (*min, *max)))
        .collect();
    let config = VirtualDeviceConfig {
        device_id: c.device_id.clone(),
        device_type: VirtualDeviceType::LightGroupLinear,
        name: c.name.clone(),
        description: None,
        enabled: true,
        config: serde_json::json!({}),
    };
    LightGroupLinear::new(config, c.members, ranges, Arc::clone(store)).expect("Bedroom Lights")
}

/// The server `v1bectl_server dummy` runs, on an ephemeral port of
/// `127.0.0.1`.
async fn serve_dummy_home() -> Home {
    let store = StateStore::new();
    let bus = Arc::new(EventBus::new(1000));
    let gateway: Arc<dyn Gateway> = Arc::new(DummyGateway::new("basic_home"));
    let engine = Arc::new(SyncEngine::new(
        Arc::clone(&store),
        Arc::clone(&bus),
        Arc::clone(&gateway),
        None,
    ));
    for info in gateway.discover_devices().await.expect("discover") {
        let state = gateway
            .get_device_state(&info.device_id)
            .await
            .expect("initial state");
        store.add_device(info, state).await;
    }

    let server =
        AxumServer::new(0, Arc::clone(&store), bus, gateway).with_sync_engine(Arc::clone(&engine));
    server
        .virtual_device_manager()
        .add_virtual_device(Box::new(bedroom_lights(&store).await))
        .await
        .expect("register Bedroom Lights");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("the bound address");
    tokio::spawn(async move { server.serve(listener).await.expect("serve") });
    tokio::spawn(async move { engine.start().await.expect("sync engine") });
    Home { addr, store }
}

async fn stored(store: &StateStore, device_id: &str) -> DeviceStateValue {
    store
        .get_device(&device_id.to_string())
        .await
        .unwrap_or_else(|| panic!("{device_id} isn't in the store"))
        .state
}

fn set_light(device_id: &str, is_on: bool, brightness: u8) -> ApiRequest {
    ApiRequest::SetLightState {
        device_id: device_id.to_string(),
        is_on: Some(is_on),
        brightness: Some(brightness),
        color_temp: None,
        rgb_color: None,
    }
}

fn light_level(state: &DeviceStateValue) -> (bool, Option<u8>) {
    match state {
        DeviceStateValue::Light(light) => (light.is_on, light.brightness),
        other => panic!("not a light: {other:?}"),
    }
}

// ── The round trips ──────────────────────────────────────────────────────────

/// A write to a physical light comes back over the socket as its echo,
/// with the state the answer announced and the store now holds. Bedroom
/// Lights, which the light feeds, follows it: the input tracking the server
/// starts re-derives the group, and that comes back too.
#[tokio::test]
async fn a_physical_light_write_comes_back_over_the_socket() {
    let home = serve_dummy_home().await;
    let mut client = Client::connect(home.addr).await;

    let response = client.request(&set_light(LIGHT, true, 40)).await;
    let ApiResponse::LightUpdated { new_state } = response else {
        panic!("unexpected response: {response:?}");
    };
    assert_eq!((new_state.is_on, new_state.brightness), (true, Some(40)));

    let echo = client.echo(LIGHT).await;
    assert_eq!(
        echo,
        DeviceStateValue::Light(new_state),
        "the echo must carry the state the answer announced"
    );
    assert_eq!(echo, stored(&home.store, LIGHT).await, "and the store's");

    let group = client.echo(GROUP).await;
    assert!(light_level(&group).0, "the group follows its lit member");
    assert_eq!(group, stored(&home.store, GROUP).await, "the group's echo");
}

/// #1: a write to a virtual device comes back over the socket too. It
/// used to publish nothing, so no client ever saw a group change. The group
/// is echoed with the state the answer announced, and every member the
/// write fanned out to with its share of it.
#[tokio::test]
async fn a_virtual_group_write_comes_back_over_the_socket() {
    let home = serve_dummy_home().await;
    let mut client = Client::connect(home.addr).await;

    let response = client.request(&set_light(GROUP, true, 50)).await;
    let ApiResponse::LightUpdated { new_state } = response else {
        panic!("unexpected response: {response:?}");
    };
    assert_eq!((new_state.is_on, new_state.brightness), (true, Some(50)));

    let echo = client.echo(GROUP).await;
    assert_eq!(
        echo,
        DeviceStateValue::Light(new_state),
        "the group's echo must carry the state the answer announced"
    );
    assert_eq!(echo, stored(&home.store, GROUP).await, "and the store's");

    for (member, brightness) in MEMBERS_AT_50 {
        let echo = client.echo(member).await;
        assert_eq!(light_level(&echo), (true, Some(brightness)), "{member}");
        assert_eq!(echo, stored(&home.store, member).await, "{member}'s echo");
    }
}

/// #8 (from the #49 review): the answer to a request carries the request's
/// correlation id, which the server copies from the envelope. The CLI
/// matches answers by it, and so does the web UI's tab-return probe. Two
/// pings in a row, each answered with a `Pong` under its own id, in order.
#[tokio::test]
async fn a_ping_is_answered_under_its_own_correlation_id() {
    let home = serve_dummy_home().await;
    let mut client = Client::connect(home.addr).await;

    let ids = [Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
    for id in &ids {
        client.send(id, &ApiRequest::Ping).await;
    }
    for id in &ids {
        let (answered, payload) = client.next_response().await;
        assert_eq!(&answered, id, "a Pong must echo its Ping's correlation id");
        let response: ApiResponse = decode(&payload);
        assert!(matches!(response, ApiResponse::Pong), "{response:?}");
    }
}

/// #10: the answer to `GetDeviceState` names the device, so a client can
/// tell apart the answers for two devices of the same type (the TUI took
/// one light's for another's). A client that doesn't know the field still
/// decodes the answer.
#[tokio::test]
async fn a_device_state_answer_names_its_device() {
    let home = serve_dummy_home().await;
    let mut client = Client::connect(home.addr).await;

    for device_id in ["light_kitchen", "light_bedroom"] {
        let payload = client
            .request_payload(&ApiRequest::GetDeviceState {
                device_id: device_id.to_string(),
            })
            .await;
        let response: ApiResponse = decode(&payload);
        let ApiResponse::DeviceState {
            device_id: answered,
            state,
        } = response
        else {
            panic!("unexpected response: {response:?}");
        };
        assert_eq!(answered, device_id, "whose state it is");
        assert_eq!(state, stored(&home.store, device_id).await);

        let OlderClientResponse::DeviceState { state: older } = decode(&payload);
        assert_eq!(older, state, "an older client decodes the same state");
    }
}
