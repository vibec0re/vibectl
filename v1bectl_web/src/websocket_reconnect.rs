// 🔥 WEBSOCKET WITH AUTO-RECONNECT - NEVER GIVE UP! CHOOOM FIX! 💖
//
// 🔧 How the connection lifecycle hangs together (#18):
//
// - All connection bookkeeping lives in ONE `Rc<RefCell<WsState>>` from
//   `use_mut_ref`, which is never replaced. `connect` is a plain function over
//   a cloneable `Ctx`, so every reconnect path (socket closed, open failed,
//   watchdog, visibilitychange / pageshow) runs the same code against the same
//   live state. There is no stored "reconnect callback" left to be captured
//   while it's still `None`.
// - The long-lived closures hold `UseStateSetter`s, never `UseStateHandle`s. A
//   handle captured in a closure keeps dereferencing to the value it had when
//   it was captured; a setter can only `set()`, which always reaches the live
//   state. Reading a stale snapshot is now a compile error, not a silent bug.
// - Every attempt gets a new *generation*. A socket's task only touches shared
//   state while its generation is current, so an old read loop that ends after
//   a newer connection opened can't clobber that connection's status or
//   schedule a duplicate reconnect.
// - One task owns each socket and drives its ping and watchdog timers, so the
//   timers stop when the socket goes away. Superseding a connection closes its
//   socket instead of leaking it.

use futures::channel::{mpsc, oneshot};
use futures::{Future, FutureExt, SinkExt, StreamExt};
use gloo_events::EventListener;
use gloo_net::http::Request;
use gloo_net::websocket::{futures::WebSocket, Message, State, WebSocketError};
use gloo_timers::future::{IntervalStream, TimeoutFuture};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

// Import types from v1bectl_models
use v1bectl_state::{DeviceEvent, RgbColor};

// Config structure
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Config {
    socket: String,
}

// Load config from static file
async fn load_config() -> Option<String> {
    match Request::get("/static/config.json").send().await {
        Ok(resp) => {
            if resp.ok() {
                match resp.json::<Config>().await {
                    Ok(config) => {
                        log::info!("✅ Loaded config: {}", config.socket);
                        Some(config.socket)
                    }
                    Err(e) => {
                        log::error!("❌ Failed to parse config: {}", e);
                        None
                    }
                }
            } else {
                log::warn!("⚠️ Config not found (status: {})", resp.status());
                None
            }
        }
        Err(e) => {
            log::error!("❌ Failed to fetch config: {}", e);
            None
        }
    }
}

// API Message wrapper for CBOR encoding
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApiMessage {
    pub correlation_id: String,
    pub message_type: ApiMessageType,
    pub payload: Vec<u8>, // CBOR-encoded payload
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ApiMessageType {
    Request,
    Response,
    Event,
    Error,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ApiRequest {
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
    Subscribe {
        device_ids: Vec<String>,
    },
    Ping, // 🔥 PING FOR KEEPALIVE! CHOOOM FIX! 💖
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ApiResponse {
    DeviceList {
        devices: Vec<DeviceState>,
        total_count: u32,
    },
    DeviceInfo {
        device: DeviceInfo,
    },
    DeviceState {
        state: serde_json::Value,
    },
    LightUpdated {
        new_state: serde_json::Value,
    },
    VirtualDeviceCreated {
        device_id: String,
    },
    VirtualDeviceRemoved {
        device_id: String,
    },
    SceneActivated {
        device_id: String,
        scene_name: String,
    },
    SubscriptionStarted {
        subscriber_id: String,
    },
    Pong, // 🔥 PONG RESPONSE! 💖
    Error {
        code: String,
        message: String,
    },
}

// Re-export the v1bectl_state types the UI uses
pub use v1bectl_state::{DeviceInfo, DeviceState, DeviceStateValue};

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
    Reconnecting(u32), // 🔥 Show reconnect attempt count! 💖
    Error(String),
}

// 🔧 CONNECTION TUNING
/// Keepalive ping period.
const PING_INTERVAL_MS: u32 = 30_000;
/// The watchdog gives up on a socket that has been silent this long: about two
/// ping intervals, plus slack for a slow pong.
const WATCHDOG_TIMEOUT_MS: f64 = 65_000.0;
/// ...and only once a ping sent after the last inbound message has gone
/// unanswered for this long. Background tabs throttle timers (Chrome: to once a
/// minute), so "silent for 65 s" alone can just mean we weren't allowed to ping
/// yet. An unanswered ping means the server really isn't hearing us.
const PONG_GRACE_MS: f64 = 5_000.0;
/// How often the watchdog checks.
const WATCHDOG_CHECK_MS: u32 = 5_000;
/// Give up on a config fetch or a socket that hasn't opened after this long.
/// This keeps an attempt from sitting in "connecting" forever.
const CONNECT_TIMEOUT_MS: u32 = 10_000;
/// Reconnect backoff: 1 s, doubling per consecutive failure, capped at 30 s.
const BACKOFF_BASE_MS: u32 = 1_000;
const BACKOFF_MAX_MS: u32 = 30_000;

/// Delay before the next attempt after `failures` consecutive failed
/// connections: 1 s, 2 s, 4 s, ... capped at 30 s. `0` counts as `1`, so there
/// is no `failures - 1` underflow.
fn backoff_delay_ms(failures: u32) -> u32 {
    let doublings = failures.saturating_sub(1);
    BACKOFF_BASE_MS
        .saturating_mul(2_u32.saturating_pow(doublings))
        .min(BACKOFF_MAX_MS)
}

/// Is the socket dead? True once nothing has arrived for `WATCHDOG_TIMEOUT_MS`
/// *and* a ping sent after the last inbound message has gone unanswered for
/// `PONG_GRACE_MS`. All times are `Date.now()` milliseconds.
fn watchdog_expired(now_ms: f64, last_inbound_ms: f64, last_ping_ms: Option<f64>) -> bool {
    let unanswered_for = match last_ping_ms {
        Some(sent) if sent > last_inbound_ms => now_ms - sent,
        _ => return false,
    };
    now_ms - last_inbound_ms >= WATCHDOG_TIMEOUT_MS && unanswered_for >= PONG_GRACE_MS
}

/// Does a task started for connection `mine` still own the connection? Every
/// new attempt bumps the generation, so a stale task must neither touch shared
/// state nor schedule a reconnect.
fn is_current_generation(mine: u64, current: u64) -> bool {
    mine == current
}

/// Where the (single) current connection is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// No attempt started yet.
    Idle,
    /// Fetching the config / waiting for the socket to open.
    Connecting,
    /// Socket open.
    Open,
    /// Socket gone, the next attempt is scheduled.
    Backoff,
    /// The hook was torn down; never reconnect.
    Stopped,
}

/// Pure connection bookkeeping: generations, backoff, and phase. It doesn't
/// touch the browser, so the rules are unit-tested natively.
#[derive(Debug)]
struct Lifecycle {
    generation: u64,
    failures: u32,
    phase: Phase,
}

impl Lifecycle {
    fn new() -> Self {
        Self {
            generation: 0,
            failures: 0,
            phase: Phase::Idle,
        }
    }

    fn is_current(&self, generation: u64) -> bool {
        is_current_generation(generation, self.generation)
    }

    /// Start a new attempt. This makes every older generation stale. Returns the
    /// new attempt's generation.
    fn begin_attempt(&mut self) -> u64 {
        self.generation += 1;
        self.phase = Phase::Connecting;
        self.generation
    }

    /// Status to show while the current attempt connects.
    fn attempt_status(&self) -> ConnectionStatus {
        match self.failures {
            0 => ConnectionStatus::Connecting,
            n => ConnectionStatus::Reconnecting(n),
        }
    }

    /// The socket of `generation` opened: reset the backoff. Returns `false`
    /// (changing nothing) if that generation is stale.
    fn on_open(&mut self, generation: u64) -> bool {
        if !self.is_current(generation) {
            return false;
        }
        self.failures = 0;
        self.phase = Phase::Open;
        true
    }

    /// The socket of `generation` is gone (closed, errored, never opened, or
    /// declared dead by the watchdog). Returns the delay before the next
    /// attempt, or `None` if that generation is stale: a newer connection
    /// owns the status and the reconnect schedule now.
    fn on_closed(&mut self, generation: u64) -> Option<u32> {
        if !self.is_current(generation) {
            return None;
        }
        self.failures = self.failures.saturating_add(1);
        self.phase = Phase::Backoff;
        Some(backoff_delay_ms(self.failures))
    }

    /// A backoff timer armed when `generation` closed has fired: start the next
    /// attempt? Not if anything (a forced reconnect, a teardown) happened since.
    fn should_retry(&self, generation: u64) -> bool {
        self.is_current(generation) && self.phase == Phase::Backoff
    }

    /// The app came back to the foreground: tear down and reconnect now? Not
    /// while an attempt is still connecting (iOS fires visibilitychange and
    /// pageshow back to back, and pageshow also fires on first load), and never
    /// after teardown. If yes, the backoff starts over.
    fn request_forced_reconnect(&mut self) -> bool {
        match self.phase {
            Phase::Connecting | Phase::Stopped => false,
            Phase::Idle | Phase::Open | Phase::Backoff => {
                self.failures = 0;
                true
            }
        }
    }

    /// Tear down: make every in-flight task and timer stale for good.
    fn stop(&mut self) {
        self.generation += 1;
        self.phase = Phase::Stopped;
    }
}

/// All connection bookkeeping, in one `use_mut_ref` cell that is never
/// replaced, so every reader sees the live value.
struct WsState {
    lifecycle: Lifecycle,
    /// Outbound queue of the open socket; `None` unless connected.
    sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
    /// Dropping this tells the current socket's task to close its socket and
    /// exit without reconnecting.
    closer: Option<oneshot::Sender<()>>,
}

impl WsState {
    fn new() -> Self {
        Self {
            lifecycle: Lifecycle::new(),
            sender: None,
            closer: None,
        }
    }

    /// Let go of the current socket: its task sees the closer drop and closes it.
    fn release_socket(&mut self) {
        self.sender = None;
        self.closer = None;
    }
}

/// What the connection tasks need. Cheap to clone. Setters only: see the
/// module comment.
#[derive(Clone)]
struct Ctx {
    status: UseStateSetter<ConnectionStatus>,
    last_response: UseStateSetter<Option<ApiResponse>>,
    last_event: UseStateSetter<Option<serde_json::Value>>,
    state: Rc<RefCell<WsState>>,
}

impl Ctx {
    fn is_current(&self, generation: u64) -> bool {
        self.state.borrow().lifecycle.is_current(generation)
    }
}

fn now_ms() -> f64 {
    web_sys::js_sys::Date::now()
}

/// Wrap a request in the CBOR `ApiMessage` envelope the server expects.
fn encode_request(request: &ApiRequest) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    ciborium::into_writer(request, &mut payload)
        .map_err(|e| format!("failed to encode request: {}", e))?;
    let api_message = ApiMessage {
        correlation_id: Uuid::new_v4().to_string(),
        message_type: ApiMessageType::Request,
        payload,
    };
    let mut data = Vec::new();
    ciborium::into_writer(&api_message, &mut data)
        .map_err(|e| format!("failed to encode API message: {}", e))?;
    Ok(data)
}

/// 🚀 Start a new connection attempt, superseding (and closing) the current
/// socket, if any. Every reconnect path ends up here.
fn connect(ctx: &Ctx) {
    let (generation, status, closer) = {
        let mut state = ctx.state.borrow_mut();
        state.release_socket();
        let generation = state.lifecycle.begin_attempt();
        let (closer_tx, closer_rx) = oneshot::channel();
        state.closer = Some(closer_tx);
        (generation, state.lifecycle.attempt_status(), closer_rx)
    };

    match &status {
        ConnectionStatus::Reconnecting(n) => {
            log::info!(
                "🔄 Reconnection attempt #{} (connection #{})",
                n,
                generation
            )
        }
        _ => log::info!(
            "🔥 Connecting to VIBEC0RE server! (connection #{})",
            generation
        ),
    }
    ctx.status.set(status);

    spawn_local(run_connection(ctx.clone(), generation, closer));
}

/// 💔 The socket of `generation` is gone. If it is still the current
/// connection, show `status` and schedule the next attempt with backoff. A
/// stale generation does nothing.
fn connection_lost(ctx: &Ctx, generation: u64, status: ConnectionStatus) {
    let delay = {
        let mut state = ctx.state.borrow_mut();
        let Some(delay) = state.lifecycle.on_closed(generation) else {
            log::debug!(
                "🔧 Connection #{} ended after it was superseded - ignoring",
                generation
            );
            return;
        };
        state.release_socket();
        delay
    };

    ctx.status.set(status);
    log::info!("⏰ Will reconnect in {}ms", delay);

    let ctx = ctx.clone();
    spawn_local(async move {
        TimeoutFuture::new(delay).await;
        let retry = ctx.state.borrow().lifecycle.should_retry(generation);
        if retry {
            connect(&ctx);
        }
    });
}

/// 👁️ The app came back to the foreground; iOS webapps in particular can come
/// back holding a dead socket that still looks open. Close it and reconnect
/// now, unless an attempt is already connecting.
fn force_reconnect(ctx: &Ctx, why: &str) {
    let go = ctx.state.borrow_mut().lifecycle.request_forced_reconnect();
    if go {
        log::info!("{} - forcing reconnect!", why);
        connect(ctx);
    } else {
        log::info!("{} - already connecting, leaving it be", why);
    }
}

/// Hook teardown: close the socket and make every pending task and timer stale.
fn shutdown(ctx: &Ctx) {
    let mut state = ctx.state.borrow_mut();
    state.lifecycle.stop();
    state.release_socket();
}

/// How a bounded step of a connection attempt ended.
enum Race<T> {
    Done(T),
    TimedOut,
    /// A newer attempt (or teardown) took over.
    Superseded,
}

/// Run `fut` until it finishes, `CONNECT_TIMEOUT_MS` passes, or the attempt is
/// superseded (its closer dropped).
async fn race_attempt<T>(
    fut: impl Future<Output = T>,
    closer: &mut oneshot::Receiver<()>,
) -> Race<T> {
    let fut = fut.fuse();
    let timeout = TimeoutFuture::new(CONNECT_TIMEOUT_MS).fuse();
    futures::pin_mut!(fut, timeout);
    futures::select_biased! {
        _ = &mut *closer => Race::Superseded,
        out = fut => Race::Done(out),
        _ = timeout => Race::TimedOut,
    }
}

/// One connection attempt: load the config, open the socket, then pump it
/// until it dies or is superseded. The task owns the socket and its timers,
/// so they all go away together.
async fn run_connection(ctx: Ctx, generation: u64, mut closer: oneshot::Receiver<()>) {
    let ws_url = match race_attempt(load_config(), &mut closer).await {
        Race::Superseded => return,
        Race::Done(Some(url)) => url,
        Race::Done(None) | Race::TimedOut => {
            log::warn!("⚠️ Failed to load config, using default ws://localhost:31337");
            "ws://localhost:31337".to_string()
        }
    };
    if !ctx.is_current(generation) {
        return;
    }

    log::info!("🚀 Connecting to: {}", ws_url);
    let mut ws = match WebSocket::open(&ws_url) {
        Ok(ws) => ws,
        Err(e) => {
            log::error!("❌ Failed to connect: {:?}", e);
            connection_lost(
                &ctx,
                generation,
                ConnectionStatus::Error(format!("{:?}", e)),
            );
            return;
        }
    };

    // `WebSocket::open` returns while the socket is still CONNECTING.
    // `poll_ready` wakes on `open`, and on `error` (after which the socket is
    // CLOSED), so check which one it was.
    let opened = race_attempt(
        futures::future::poll_fn(|cx| ws.poll_ready_unpin(cx)),
        &mut closer,
    )
    .await;
    match opened {
        Race::Superseded => return, // dropping `ws` closes it
        Race::TimedOut => {
            log::warn!(
                "⏰ WebSocket didn't open within {}ms - giving up on it",
                CONNECT_TIMEOUT_MS
            );
            connection_lost(&ctx, generation, ConnectionStatus::Disconnected);
            return;
        }
        Race::Done(Ok(())) if matches!(ws.state(), State::Open) => {}
        Race::Done(_) => {
            log::warn!("💔 WebSocket failed to open");
            connection_lost(&ctx, generation, ConnectionStatus::Disconnected);
            return;
        }
    }

    let (tx, rx) = mpsc::unbounded::<Vec<u8>>();
    {
        let mut state = ctx.state.borrow_mut();
        if !state.lifecycle.on_open(generation) {
            return;
        }
        state.sender = Some(tx);
    }
    log::info!("✅ WebSocket connection #{} opened!", generation);
    ctx.status.set(ConnectionStatus::Connected);

    match pump(&ctx, generation, ws, rx, &mut closer).await {
        PumpEnd::Superseded => {
            log::info!("🔌 Connection #{} superseded - closed it", generation)
        }
        PumpEnd::Lost => connection_lost(&ctx, generation, ConnectionStatus::Disconnected),
    }
}

/// Why `pump` returned.
enum PumpEnd {
    /// A newer attempt (or teardown) took over: exit quietly.
    Superseded,
    /// The socket died: reconnect.
    Lost,
}

/// Pump one open socket: inbound messages to the UI, the outbound queue to the
/// server, keepalive pings, and the watchdog. Returns when the socket dies or
/// a newer connection takes over. Either way the socket is closed on the way
/// out, and the ping and watchdog timers are dropped with this frame.
async fn pump(
    ctx: &Ctx,
    generation: u64,
    ws: WebSocket,
    mut outbound: mpsc::UnboundedReceiver<Vec<u8>>,
    closer: &mut oneshot::Receiver<()>,
) -> PumpEnd {
    let (mut write, read) = ws.split();
    let mut read = read.fuse();
    let mut ping_timer = IntervalStream::new(PING_INTERVAL_MS).fuse();
    let mut watchdog_timer = IntervalStream::new(WATCHDOG_CHECK_MS).fuse();
    let mut last_inbound_ms = now_ms();
    let mut last_ping_ms: Option<f64> = None;

    let end = loop {
        futures::select_biased! {
            _ = &mut *closer => break PumpEnd::Superseded,
            msg = read.next() => match msg {
                Some(Ok(msg)) => {
                    last_inbound_ms = now_ms();
                    if !ctx.is_current(generation) {
                        break PumpEnd::Superseded;
                    }
                    if let Message::Bytes(data) = msg {
                        handle_message(ctx, &data);
                    }
                }
                Some(Err(WebSocketError::ConnectionClose(event))) => {
                    log::warn!(
                        "💔 WebSocket connection closed! (code {}, reason {:?})",
                        event.code,
                        event.reason
                    );
                    break PumpEnd::Lost;
                }
                Some(Err(e)) => {
                    log::error!("❌ WebSocket error: {:?}", e);
                    break PumpEnd::Lost;
                }
                None => {
                    log::warn!("💔 WebSocket connection closed!");
                    break PumpEnd::Lost;
                }
            },
            data = outbound.next() => match data {
                Some(data) => {
                    if let Err(e) = write.send(Message::Bytes(data)).await {
                        log::error!("❌ Failed to send: {:?}", e);
                        break PumpEnd::Lost;
                    }
                }
                // Our sender was dropped: a newer connection replaced us.
                None => break PumpEnd::Superseded,
            },
            _ = ping_timer.next() => match encode_request(&ApiRequest::Ping) {
                Ok(data) => {
                    log::debug!("🏓 Sending PING to keep connection alive!");
                    if let Err(e) = write.send(Message::Bytes(data)).await {
                        log::error!("❌ Failed to send PING: {:?}", e);
                        break PumpEnd::Lost;
                    }
                    last_ping_ms = Some(now_ms());
                }
                Err(e) => log::error!("❌ Failed to encode PING: {}", e),
            },
            _ = watchdog_timer.next() => {
                let now = now_ms();
                if watchdog_expired(now, last_inbound_ms, last_ping_ms) {
                    log::warn!(
                        "💔 Nothing from the server for {:.0}s and the last PING went unanswered - dropping the socket",
                        (now - last_inbound_ms) / 1000.0
                    );
                    break PumpEnd::Lost;
                }
            },
        }
    };

    // Close with a clean 1000 instead of leaving it to the drop.
    if let Ok(ws) = read.into_inner().reunite(write) {
        let _ = ws.close(Some(1000), Some("v1bectl web: reconnecting"));
    }
    end
}

/// Decode one inbound CBOR frame and hand it to the UI.
fn handle_message(ctx: &Ctx, data: &[u8]) {
    let api_msg = match ciborium::from_reader::<ApiMessage, _>(data) {
        Ok(api_msg) => api_msg,
        Err(e) => {
            log::error!("❌ Failed to decode CBOR message: {}", e);
            return;
        }
    };

    match api_msg.message_type {
        ApiMessageType::Response => {
            match ciborium::from_reader::<ApiResponse, _>(api_msg.payload.as_slice()) {
                Ok(ApiResponse::Pong) => {
                    log::debug!("🏓 PONG received - connection alive!");
                }
                Ok(response) => {
                    log::info!("📥 Received response: {:?}", response);
                    ctx.last_response.set(Some(response));
                }
                Err(e) => {
                    log::error!("❌ Failed to decode response: {}", e);
                }
            }
        }
        ApiMessageType::Event => {
            // Handle events - CBOR to JSON for now (TODO: pure CBOR) 🔥
            log::info!("📢 Got Event message!");
            match ciborium::from_reader::<DeviceEvent, _>(api_msg.payload.as_slice()) {
                Ok(device_event) => {
                    log::info!(
                        "🔥 DeviceEvent: device_id={}, event_type={:?}",
                        device_event.device_id,
                        device_event.event_type
                    );

                    // Convert to JSON for UI compatibility (temporary)
                    match serde_json::to_value(&device_event) {
                        Ok(event_json) => ctx.last_event.set(Some(event_json)),
                        Err(e) => {
                            log::error!("❌ Failed to convert DeviceEvent to JSON: {}", e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("❌ Failed to decode DeviceEvent: {}", e);
                }
            }
        }
        _ => {}
    }
}

// 🔥 WEBSOCKET HOOK WITH AUTO-RECONNECT, PING AND WATCHDOG! CHOOOM FIX! 💖
#[hook]
pub fn use_websocket() -> UseWebSocketHandle {
    let status = use_state(|| ConnectionStatus::Connecting);
    let last_response = use_state(|| None::<ApiResponse>);
    let last_event = use_state(|| None::<serde_json::Value>);
    let state = use_mut_ref(WsState::new);

    let ctx = Ctx {
        status: status.setter(),
        last_response: last_response.setter(),
        last_event: last_event.setter(),
        state: state.clone(),
    };

    // 🚀 Initial connection; everything after that reconnects by itself.
    {
        let ctx = ctx.clone();
        use_effect_with((), move |_| {
            connect(&ctx);
            move || shutdown(&ctx)
        });
    }

    // 🔥 iOS WAKE DETECTION - FORCE RECONNECT! 💖
    // iOS webapps need both visibilitychange AND pageshow
    use_effect_with((), move |_| {
        let window = web_sys::window().expect("window");
        let document = window.document().expect("document");

        let visibility_listener = EventListener::new(&document, "visibilitychange", {
            let ctx = ctx.clone();
            let document = document.clone();
            move |_| {
                if document.visibility_state() == web_sys::VisibilityState::Visible {
                    force_reconnect(&ctx, "👁️ visibilitychange: visible");
                }
            }
        });

        // 🔥 iOS PAGESHOW - MORE RELIABLE FOR WEBAPPS! 💖
        let pageshow_listener = EventListener::new(&window, "pageshow", move |_| {
            force_reconnect(&ctx, "📱 pageshow");
        });

        move || {
            drop(visibility_listener);
            drop(pageshow_listener);
        }
    });

    let send_request = {
        let state = state.clone();

        Callback::from(move |request: ApiRequest| {
            let state = state.clone();

            spawn_local(async move {
                let data = match encode_request(&request) {
                    Ok(data) => data,
                    Err(e) => {
                        log::error!("❌ {}", e);
                        return;
                    }
                };

                log::info!("📤 Sending request: {:?}", request);

                // Send via channel
                if let Some(sender) = &state.borrow().sender {
                    if let Err(e) = sender.unbounded_send(data) {
                        log::error!("❌ Failed to queue message: {:?}", e);
                    }
                } else {
                    log::warn!("⚠️ WebSocket not connected, can't send request");
                }
            });
        })
    };

    UseWebSocketHandle {
        status: (*status).clone(),
        last_response: (*last_response).clone(),
        last_event: (*last_event).clone(),
        send_request,
    }
}

#[derive(Clone)]
pub struct UseWebSocketHandle {
    pub status: ConnectionStatus,
    pub last_response: Option<ApiResponse>,
    pub last_event: Option<serde_json::Value>,
    pub send_request: Callback<ApiRequest>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: f64 = 1_000.0;

    #[test]
    fn backoff_doubles_from_one_second_and_caps_at_thirty() {
        let delays: Vec<u32> = (1..=8).map(backoff_delay_ms).collect();
        assert_eq!(
            delays,
            [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]
        );
    }

    #[test]
    fn backoff_never_underflows_or_overflows() {
        // The old code did `2.pow(reconnect_count - 1)`: 0 underflowed.
        assert_eq!(backoff_delay_ms(0), 1_000);
        assert_eq!(backoff_delay_ms(33), 30_000);
        assert_eq!(backoff_delay_ms(u32::MAX), 30_000);
    }

    #[test]
    fn watchdog_is_quiet_on_a_healthy_socket() {
        // Just opened, nothing sent yet.
        assert!(!watchdog_expired(0.0, 0.0, None));
        // Ping at 30 s answered right away; 60 s later all is well.
        assert!(!watchdog_expired(90.0 * S, 30.1 * S, Some(30.0 * S)));
    }

    #[test]
    fn watchdog_fires_after_two_unanswered_ping_intervals() {
        // Opened at 0, pings at 30 s and 60 s both unanswered.
        assert!(!watchdog_expired(60.0 * S, 0.0, Some(60.0 * S)));
        assert!(!watchdog_expired(64.9 * S, 0.0, Some(60.0 * S)));
        assert!(watchdog_expired(65.0 * S, 0.0, Some(60.0 * S)));
        assert!(watchdog_expired(600.0 * S, 0.0, Some(60.0 * S)));
    }

    #[test]
    fn watchdog_gives_a_fresh_ping_a_grace_period() {
        // Silent for ages (tab was frozen), but the ping only just went out:
        // wait for its pong before calling the socket dead.
        assert!(!watchdog_expired(3_600.0 * S, 0.0, Some(3_599.0 * S)));
        assert!(watchdog_expired(3_605.0 * S, 0.0, Some(3_600.0 * S)));
    }

    #[test]
    fn watchdog_needs_an_unanswered_ping_not_just_silence() {
        // Throttled background tab: timers fire late, so we're 70 s past the
        // last pong without having been allowed to ping again. Not dead.
        assert!(!watchdog_expired(100.0 * S, 30.1 * S, Some(30.0 * S)));
        // Never pinged at all.
        assert!(!watchdog_expired(100.0 * S, 0.0, None));
    }

    #[test]
    fn watchdog_survives_the_clock_going_backwards() {
        assert!(!watchdog_expired(10.0 * S, 100.0 * S, Some(101.0 * S)));
    }

    #[test]
    fn generation_check_is_exact() {
        assert!(is_current_generation(3, 3));
        assert!(!is_current_generation(2, 3));
        assert!(!is_current_generation(4, 3));
    }

    #[test]
    fn failures_back_off_and_an_open_resets_them() {
        let mut lc = Lifecycle::new();
        let g1 = lc.begin_attempt();
        assert_eq!(lc.attempt_status(), ConnectionStatus::Connecting);
        assert_eq!(lc.on_closed(g1), Some(1_000));
        assert!(lc.should_retry(g1));

        let g2 = lc.begin_attempt();
        assert_eq!(lc.attempt_status(), ConnectionStatus::Reconnecting(1));
        assert_eq!(lc.on_closed(g2), Some(2_000));

        let g3 = lc.begin_attempt();
        assert_eq!(lc.attempt_status(), ConnectionStatus::Reconnecting(2));
        assert_eq!(lc.on_closed(g3), Some(4_000));

        // Server is back.
        let g4 = lc.begin_attempt();
        assert!(lc.on_open(g4));
        assert_eq!(lc.phase, Phase::Open);
        // The next drop starts the ladder over at 1 s.
        assert_eq!(lc.on_closed(g4), Some(1_000));
    }

    #[test]
    fn server_restart_reconnects_without_any_user_action() {
        // #18: the tab stays visible, so no visibilitychange/pageshow fires.
        // Every close of the current connection must lead to a retry that
        // actually fires, however many attempts fail, until one opens.
        let mut lc = Lifecycle::new();
        let mut g = lc.begin_attempt();
        assert!(lc.on_open(g));
        for attempt in 1..=10 {
            assert!(
                lc.on_closed(g).is_some(),
                "attempt {attempt}: no retry scheduled"
            );
            assert!(
                lc.should_retry(g),
                "attempt {attempt}: retry timer would not fire"
            );
            g = lc.begin_attempt();
            assert_eq!(lc.attempt_status(), ConnectionStatus::Reconnecting(attempt));
        }
        assert!(lc.on_open(g));
        assert_eq!(lc.phase, Phase::Open);
        assert_eq!(lc.failures, 0);
    }

    #[test]
    fn stale_socket_closing_after_newer_open_is_ignored() {
        // #18: a forced reconnect opens connection 2 while connection 1's read
        // loop is still winding down. When 1 finally ends it must not schedule
        // a reconnect or touch the backoff, and 2 stays open.
        let mut lc = Lifecycle::new();
        let old = lc.begin_attempt();
        assert!(lc.on_open(old));
        assert!(lc.request_forced_reconnect());
        let new = lc.begin_attempt();
        assert!(lc.on_open(new));

        assert_eq!(lc.on_closed(old), None);
        assert_eq!(lc.phase, Phase::Open);
        assert_eq!(lc.failures, 0);
        assert!(lc.is_current(new));
        assert!(!lc.is_current(old));
    }

    #[test]
    fn stale_socket_opening_late_does_not_claim_the_connection() {
        let mut lc = Lifecycle::new();
        let old = lc.begin_attempt();
        let new = lc.begin_attempt();
        assert!(!lc.on_open(old));
        assert_eq!(lc.phase, Phase::Connecting);
        assert!(lc.on_open(new));
    }

    #[test]
    fn backoff_timer_is_dropped_once_something_newer_started() {
        let mut lc = Lifecycle::new();
        let g1 = lc.begin_attempt();
        assert!(lc.on_closed(g1).is_some());
        // The user comes back before the timer fires.
        assert!(lc.request_forced_reconnect());
        let g2 = lc.begin_attempt();
        assert!(!lc.should_retry(g1));
        // Even once g2 is in backoff itself, g1's old timer stays dead.
        assert!(lc.on_closed(g2).is_some());
        assert!(!lc.should_retry(g1));
        assert!(lc.should_retry(g2));
    }

    #[test]
    fn retry_only_fires_from_backoff() {
        let mut lc = Lifecycle::new();
        let g = lc.begin_attempt();
        assert!(!lc.should_retry(g)); // still connecting
        assert!(lc.on_open(g));
        assert!(!lc.should_retry(g)); // open
    }

    #[test]
    fn forced_reconnect_waits_for_an_attempt_in_flight() {
        let mut lc = Lifecycle::new();
        let g = lc.begin_attempt();
        // visibilitychange + pageshow back to back while connecting.
        assert!(!lc.request_forced_reconnect());
        assert!(lc.on_open(g));
        assert!(lc.request_forced_reconnect());
    }

    #[test]
    fn forced_reconnect_restarts_the_backoff() {
        let mut lc = Lifecycle::new();
        for _ in 0..4 {
            let g = lc.begin_attempt();
            lc.on_closed(g);
        }
        assert_eq!(lc.failures, 4);
        assert!(lc.request_forced_reconnect());
        let g = lc.begin_attempt();
        assert_eq!(lc.attempt_status(), ConnectionStatus::Connecting);
        assert_eq!(lc.on_closed(g), Some(1_000));
    }

    #[test]
    fn stop_makes_everything_stale_for_good() {
        let mut lc = Lifecycle::new();
        let g = lc.begin_attempt();
        assert!(lc.on_closed(g).is_some());
        lc.stop();
        assert!(!lc.should_retry(g));
        assert!(!lc.on_open(g));
        assert_eq!(lc.on_closed(g), None);
        assert!(!lc.request_forced_reconnect());
    }

    #[test]
    fn ping_request_round_trips_through_the_envelope() {
        let data = encode_request(&ApiRequest::Ping).expect("encode");
        let msg: ApiMessage = ciborium::from_reader(data.as_slice()).expect("envelope");
        assert_eq!(msg.message_type, ApiMessageType::Request);
        assert!(!msg.correlation_id.is_empty());
        let req: ApiRequest = ciborium::from_reader(msg.payload.as_slice()).expect("payload");
        assert!(matches!(req, ApiRequest::Ping));
    }
}
