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
//   socket instead of leaking it: its task sees its fused "closer" fire and its
//   outbound queue end (#29).
// - Coming back to the page (#29): a plain tab switch pings an open socket
//   first and reconnects unless that ping's PONG (matched by correlation id)
//   is back within `PROBE_TIMEOUT_MS`. A page restored from the back/forward
//   cache, or a socket that has been silent for longer than the watchdog
//   allows (an iOS resume, a sleeping laptop), reconnects straight away. A
//   wake-up may pre-empt an attempt that has been connecting for
//   `CONNECT_PREEMPT_MS`.

use futures::channel::{mpsc, oneshot};
use futures::future::Fuse;
use futures::stream::SplitSink;
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

/// What an attempt dials when no config has loaded yet this session.
const DEFAULT_SOCKET_URL: &str = "ws://localhost:31337";

/// The socket URL for each attempt. An attempt whose config fetch fails (a
/// flaky phone network, a web server restarting next to the API server)
/// reuses the last URL a config fetch returned this session. It only falls
/// back to `DEFAULT_SOCKET_URL` if no config has ever loaded (#9). Pure, so
/// it's unit-tested natively.
#[derive(Debug, Default)]
struct SocketUrl {
    last_good: Option<String>,
}

impl SocketUrl {
    /// The URL for an attempt whose config fetch returned `loaded` (`None`:
    /// it failed or timed out).
    fn resolve(&mut self, loaded: Option<String>) -> String {
        if let Some(url) = loaded {
            self.last_good = Some(url.clone());
            return url;
        }
        if let Some(url) = &self.last_good {
            log::warn!("⚠️ Failed to load config, reusing the last good URL {url}");
            url.clone()
        } else {
            log::warn!(
                "⚠️ Failed to load config (none loaded yet), using default {DEFAULT_SOCKET_URL}"
            );
            DEFAULT_SOCKET_URL.to_string()
        }
    }
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
                        log::error!("❌ Failed to parse config: {e}");
                        None
                    }
                }
            } else {
                log::warn!("⚠️ Config not found (status: {})", resp.status());
                None
            }
        }
        Err(e) => {
            log::error!("❌ Failed to fetch config: {e}");
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
    /// Boxed: a `DeviceInfo` is over 250 bytes on 64-bit targets (clippy's
    /// `large_enum_variant`, which only fires on native builds). The wire
    /// format is unchanged, since serde encodes a `Box<T>` as a `T`.
    DeviceInfo {
        device: Box<DeviceInfo>,
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
/// A wake-up may pre-empt an attempt that has been connecting at least this
/// long; it's probably stuck on a config fetch or an open, so don't make the
/// user wait out `CONNECT_TIMEOUT_MS`. A younger attempt is left alone: it's
/// the one the first load or a wake-up just started (iOS fires
/// `visibilitychange` and `pageshow` back to back).
const CONNECT_PREEMPT_MS: f64 = 2_000.0;
/// Coming back to a tab with an open socket sends a PING first, and only
/// reconnects if that PING's PONG doesn't arrive within this long.
const PROBE_TIMEOUT_MS: u32 = 2_000;

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
/// *and* a ping has gone unanswered for `PONG_GRACE_MS`. `unanswered_ping_ms`
/// is when the latest ping that nothing has arrived after was sent (see
/// `Liveness`). All times are `Date.now()` milliseconds.
fn watchdog_expired(now_ms: f64, last_inbound_ms: f64, unanswered_ping_ms: Option<f64>) -> bool {
    let Some(sent) = unanswered_ping_ms else {
        return false;
    };
    now_ms - last_inbound_ms >= WATCHDOG_TIMEOUT_MS && now_ms - sent >= PONG_GRACE_MS
}

/// Liveness of one open socket, fed with events in the order its task sees
/// them. Whether anything has arrived since the last ping is tracked by that
/// order, not by comparing `Date.now()` stamps, so a ping and a message in the
/// same millisecond are never ambiguous. Pure, so it's unit-tested natively.
#[derive(Debug, Clone)]
struct Liveness {
    last_inbound_ms: f64,
    /// When the latest ping was sent, if nothing has arrived since.
    unanswered_ping_ms: Option<f64>,
    /// A tab-return probe is waiting for the PONG to its PING, which carries
    /// this correlation id.
    probe: Option<String>,
}

/// How a socket's task checks its socket when the tab comes back.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeStep {
    /// Send a ping with this correlation id, and reconnect unless its pong
    /// arrives within `PROBE_TIMEOUT_MS`.
    SendPing(String),
    /// A probe is already waiting for its answer: leave it be.
    AlreadyProbing,
    /// Nothing has arrived for `WATCHDOG_TIMEOUT_MS`, so the page was frozen or
    /// suspended (an iOS resume, a sleeping laptop). Don't wait on a pong that
    /// probably won't come: reconnect now, as every tab return used to.
    Reconnect,
}

impl Liveness {
    fn new(now_ms: f64) -> Self {
        Self {
            last_inbound_ms: now_ms,
            unanswered_ping_ms: None,
            probe: None,
        }
    }

    /// Something arrived. For the watchdog that answers any ping. It does not
    /// answer a probe: see `on_frame`.
    fn on_inbound(&mut self, now_ms: f64) {
        self.last_inbound_ms = now_ms;
        self.unanswered_ping_ms = None;
    }

    /// A decoded frame arrived (after `on_inbound` for it). A PONG stops
    /// here: only the pong to the probe's own ping answers the probe.
    /// Anything else may have been sitting in the socket's buffers since
    /// before it went half-open: an event, or the pong to an older keepalive
    /// ping. Counting that as an answer would keep a dead socket until the
    /// watchdog gives up on it, 65 s later. Every other frame is handed back
    /// for the UI. The pump gets its UI frames only through here, so it can't
    /// skip the probe check without also cutting off the UI.
    fn on_frame(&mut self, inbound: Inbound) -> Option<Inbound> {
        let Inbound::Pong { correlation_id } = inbound else {
            return Some(inbound);
        };
        if self.probe.as_deref() == Some(correlation_id.as_str()) {
            self.probe = None;
            log::debug!("🏓 PONG to the tab-return PING");
        } else {
            log::debug!("🏓 PONG received - connection alive!");
        }
        None
    }

    fn on_ping_sent(&mut self, now_ms: f64) {
        self.unanswered_ping_ms = Some(now_ms);
    }

    fn watchdog_expired(&self, now_ms: f64) -> bool {
        watchdog_expired(now_ms, self.last_inbound_ms, self.unanswered_ping_ms)
    }

    /// The tab came back: how do we check the socket? `SendPing(id)` means:
    /// send a ping with correlation id `id`. The probe then waits for that
    /// ping's pong (`on_frame`). The id is minted here and handed out, so the
    /// ping that goes out can't carry a different id from the one the probe
    /// waits for (#49 review).
    fn begin_probe(&mut self, now_ms: f64) -> ProbeStep {
        if self.probe.is_some() {
            return ProbeStep::AlreadyProbing;
        }
        if now_ms - self.last_inbound_ms >= WATCHDOG_TIMEOUT_MS {
            return ProbeStep::Reconnect;
        }
        let ping_id = new_correlation_id();
        self.probe = Some(ping_id.clone());
        ProbeStep::SendPing(ping_id)
    }

    /// The probe's `PROBE_TIMEOUT_MS` is up: is it still unanswered?
    fn probe_unanswered(&self) -> bool {
        self.probe.is_some()
    }
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

/// Why the page is looking at its connection again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// `visibilitychange` to visible: a desktop tab switch, or an iOS webapp
    /// coming back to the front.
    Visible,
    /// `pageshow` with `persisted`: the page was restored from the
    /// back/forward cache, so it was frozen along with its socket.
    Restored,
}

/// What a wake-up should do about the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeAction {
    /// Leave it be.
    Nothing,
    /// Ping the open socket first; its task reconnects if nothing answers.
    Probe,
    /// Tear down and reconnect now.
    Reconnect,
}

/// Pure connection bookkeeping: generations, backoff, and phase. It doesn't
/// touch the browser, so the rules are unit-tested natively.
#[derive(Debug)]
struct Lifecycle {
    generation: u64,
    failures: u32,
    phase: Phase,
    /// When the current attempt started (`Date.now()` ms).
    attempt_started_ms: f64,
}

impl Lifecycle {
    fn new() -> Self {
        Self {
            generation: 0,
            failures: 0,
            phase: Phase::Idle,
            attempt_started_ms: 0.0,
        }
    }

    fn is_current(&self, generation: u64) -> bool {
        is_current_generation(generation, self.generation)
    }

    /// Start a new attempt at `now_ms`. This makes every older generation
    /// stale. Returns the new attempt's generation.
    fn begin_attempt(&mut self, now_ms: f64) -> u64 {
        self.generation += 1;
        self.phase = Phase::Connecting;
        self.attempt_started_ms = now_ms;
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

    /// The page came back to the foreground at `now_ms`. What now?
    /// - Torn down: nothing, ever.
    /// - Connecting for less than `CONNECT_PREEMPT_MS`: nothing (see there).
    ///   Connecting for longer: pre-empt it with a fresh attempt.
    /// - Open: a tab switch probes the socket (ping first). A page restored
    ///   from the back/forward cache was frozen, so it reconnects.
    /// - Backing off: retry now.
    ///
    /// A reconnect starts the backoff over: someone is looking, so try fast.
    fn on_wake(&mut self, wake: Wake, now_ms: f64) -> WakeAction {
        match self.phase {
            Phase::Stopped => WakeAction::Nothing,
            Phase::Connecting if now_ms - self.attempt_started_ms < CONNECT_PREEMPT_MS => {
                WakeAction::Nothing
            }
            Phase::Open if wake == Wake::Visible => WakeAction::Probe,
            Phase::Idle | Phase::Connecting | Phase::Open | Phase::Backoff => {
                self.failures = 0;
                WakeAction::Reconnect
            }
        }
    }

    /// Tear down: make every in-flight task and timer stale for good.
    fn stop(&mut self) {
        self.generation += 1;
        self.phase = Phase::Stopped;
    }
}

/// The receiving end of an attempt's closer. It must be fused. A bare
/// `oneshot::Receiver` reports `is_terminated()` as soon as its sender is
/// dropped without sending, and `select!` skips terminated branches, so the
/// drop that supersedes an attempt would never wake that branch (#29). A
/// `Fuse` only terminates after it has returned `Ready`, so the drop is seen.
type Closer = Fuse<oneshot::Receiver<()>>;

fn closer_channel() -> (oneshot::Sender<()>, Closer) {
    let (tx, rx) = oneshot::channel();
    (tx, rx.fuse())
}

/// What the hook hands to the task that owns the open socket.
#[derive(Debug, PartialEq)]
enum Outbound {
    /// A CBOR frame to send.
    Frame(Vec<u8>),
    /// The tab came back: check that the server still answers.
    Probe,
}

/// All connection bookkeeping, in one `use_mut_ref` cell that is never
/// replaced, so every reader sees the live value.
struct WsState {
    lifecycle: Lifecycle,
    /// Outbound queue of the open socket; `None` unless connected.
    sender: Option<mpsc::UnboundedSender<Outbound>>,
    /// Dropping this tells the current attempt's task to close its socket and
    /// exit without reconnecting.
    closer: Option<oneshot::Sender<()>>,
    /// Where the attempts connect to; outlives every attempt.
    socket_url: SocketUrl,
}

impl WsState {
    fn new() -> Self {
        Self {
            lifecycle: Lifecycle::new(),
            sender: None,
            closer: None,
            socket_url: SocketUrl::default(),
        }
    }

    /// Start a new attempt at `now_ms`, superseding the current one: let go of
    /// its socket, then return the new generation, the status to show, and
    /// the new attempt's closer.
    fn begin_attempt(&mut self, now_ms: f64) -> (u64, ConnectionStatus, Closer) {
        self.release_socket();
        let generation = self.lifecycle.begin_attempt(now_ms);
        let (closer_tx, closer) = closer_channel();
        self.closer = Some(closer_tx);
        (generation, self.lifecycle.attempt_status(), closer)
    }

    /// The socket of `generation` opened: from now on everything queued goes
    /// to `sender`. Returns `false` (changing nothing) if it's stale.
    fn on_open(&mut self, generation: u64, sender: mpsc::UnboundedSender<Outbound>) -> bool {
        if !self.lifecycle.on_open(generation) {
            return false;
        }
        self.sender = Some(sender);
        true
    }

    /// Hand `out` to the open socket's task. `false` if there's no open socket
    /// (or its task is gone).
    fn queue(&self, out: Outbound) -> bool {
        self.sender
            .as_ref()
            .is_some_and(|sender| sender.unbounded_send(out).is_ok())
    }

    /// The page came back: decide what to do (see `Lifecycle::on_wake`) and
    /// hand a probe to the open socket's task. With no task to take it,
    /// reconnect instead.
    fn on_wake(&mut self, wake: Wake, now_ms: f64) -> WakeAction {
        match self.lifecycle.on_wake(wake, now_ms) {
            WakeAction::Probe if !self.queue(Outbound::Probe) => WakeAction::Reconnect,
            action => action,
        }
    }

    /// Let go of the current socket. Its task sees its closer fire and its
    /// outbound queue end, closes the socket, and exits.
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

/// A fresh correlation id for an outgoing request. The server echoes it on
/// its response.
fn new_correlation_id() -> String {
    Uuid::new_v4().to_string()
}

/// Wrap a request in the CBOR `ApiMessage` envelope the server expects, under
/// `correlation_id`.
fn encode_request(request: &ApiRequest, correlation_id: String) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    ciborium::into_writer(request, &mut payload)
        .map_err(|e| format!("failed to encode request: {e}"))?;
    let api_message = ApiMessage {
        correlation_id,
        message_type: ApiMessageType::Request,
        payload,
    };
    let mut data = Vec::new();
    ciborium::into_writer(&api_message, &mut data)
        .map_err(|e| format!("failed to encode API message: {e}"))?;
    Ok(data)
}

/// 🚀 Start a new connection attempt, superseding (and closing) the current
/// socket, if any. Every reconnect path ends up here.
fn connect(ctx: &Ctx) {
    let (generation, status, closer) = ctx.state.borrow_mut().begin_attempt(now_ms());

    match &status {
        ConnectionStatus::Reconnecting(n) => {
            log::info!("🔄 Reconnection attempt #{n} (connection #{generation})");
        }
        _ => log::info!("🔥 Connecting to VIBEC0RE server! (connection #{generation})"),
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
            log::debug!("🔧 Connection #{generation} ended after it was superseded - ignoring");
            return;
        };
        state.release_socket();
        delay
    };

    ctx.status.set(status);
    log::info!("⏰ Will reconnect in {delay}ms");

    let ctx = ctx.clone();
    spawn_local(async move {
        TimeoutFuture::new(delay).await;
        let retry = ctx.state.borrow().lifecycle.should_retry(generation);
        if retry {
            connect(&ctx);
        }
    });
}

/// 👁️ The app came back to the foreground. It may be holding a dead socket
/// that still looks open (iOS webapps especially), or a connect that hung
/// while it was away. See `Lifecycle::on_wake` for what happens when.
fn wake_up(ctx: &Ctx, wake: Wake, why: &str) {
    let action = ctx.state.borrow_mut().on_wake(wake, now_ms());
    match action {
        WakeAction::Nothing => log::info!("{why} - already connecting, leaving it be"),
        WakeAction::Probe => log::info!("{why} - pinging before deciding to reconnect 🏓"),
        WakeAction::Reconnect => {
            log::info!("{why} - forcing reconnect!");
            connect(ctx);
        }
    }
}

/// Is this `pageshow` a restore from the back/forward cache
/// (`PageTransitionEvent.persisted`)? Read by reflection because web-sys's
/// `PageTransitionEvent` binding isn't enabled for this crate.
fn page_restored(event: &web_sys::Event) -> bool {
    web_sys::js_sys::Reflect::get(event, &"persisted".into())
        .ok()
        .and_then(|persisted| persisted.as_bool())
        .unwrap_or(false)
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

/// Run `fut` until it finishes, `timeout` fires, or the attempt is superseded
/// (its closer's sender dropped). A supersede wins over everything else.
async fn race<T>(
    fut: impl Future<Output = T>,
    timeout: impl Future<Output = ()>,
    closer: &mut Closer,
) -> Race<T> {
    let fut = fut.fuse();
    let timeout = timeout.fuse();
    futures::pin_mut!(fut, timeout);
    futures::select_biased! {
        _ = &mut *closer => Race::Superseded,
        out = fut => Race::Done(out),
        () = timeout => Race::TimedOut,
    }
}

/// `race` against `CONNECT_TIMEOUT_MS`.
async fn race_attempt<T>(fut: impl Future<Output = T>, closer: &mut Closer) -> Race<T> {
    race(fut, TimeoutFuture::new(CONNECT_TIMEOUT_MS), closer).await
}

/// One connection attempt: load the config, open the socket, then pump it
/// until it dies or is superseded. The task owns the socket and its timers,
/// so they all go away together.
async fn run_connection(ctx: Ctx, generation: u64, mut closer: Closer) {
    let loaded = match race_attempt(load_config(), &mut closer).await {
        Race::Superseded => return,
        Race::Done(loaded) => loaded,
        Race::TimedOut => None,
    };
    let ws_url = {
        let mut state = ctx.state.borrow_mut();
        if !state.lifecycle.is_current(generation) {
            return;
        }
        state.socket_url.resolve(loaded)
    };

    log::info!("🚀 Connecting to: {ws_url}");
    let mut ws = match WebSocket::open(&ws_url) {
        Ok(ws) => ws,
        Err(e) => {
            log::error!("❌ Failed to connect: {e:?}");
            connection_lost(&ctx, generation, ConnectionStatus::Error(format!("{e:?}")));
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
            log::warn!("⏰ WebSocket didn't open within {CONNECT_TIMEOUT_MS}ms - giving up on it");
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

    let (tx, rx) = mpsc::unbounded::<Outbound>();
    if !ctx.state.borrow_mut().on_open(generation, tx) {
        return;
    }
    log::info!("✅ WebSocket connection #{generation} opened!");
    ctx.status.set(ConnectionStatus::Connected);

    match pump(&ctx, generation, ws, rx, &mut closer).await {
        PumpEnd::Superseded => {
            log::info!("🔌 Connection #{generation} superseded - closed it");
        }
        PumpEnd::Lost => connection_lost(&ctx, generation, ConnectionStatus::Disconnected),
        // Someone is looking at the page, and the backoff was reset when this
        // socket opened: reconnect right away.
        PumpEnd::Unresponsive => {
            if ctx.is_current(generation) {
                connect(&ctx);
            }
        }
    }
}

/// Why `pump` returned.
enum PumpEnd {
    /// A newer attempt (or teardown) took over: exit quietly.
    Superseded,
    /// The socket died: reconnect with backoff.
    Lost,
    /// A tab-return probe found the socket unresponsive: reconnect now.
    Unresponsive,
}

/// Send one ping with `correlation_id` and note it in `liveness`. A ping that
/// can't be encoded (it's a unit variant, so it can't really happen) is logged
/// and skipped.
async fn send_ping(
    write: &mut SplitSink<WebSocket, Message>,
    liveness: &mut Liveness,
    correlation_id: String,
) -> Result<(), WebSocketError> {
    match encode_request(&ApiRequest::Ping, correlation_id) {
        Ok(data) => {
            write.send(Message::Bytes(data)).await?;
            liveness.on_ping_sent(now_ms());
        }
        Err(e) => log::error!("❌ Failed to encode PING: {e}"),
    }
    Ok(())
}

/// Pump one open socket: inbound messages to the UI, the outbound queue to the
/// server, keepalive pings, tab-return probes, and the watchdog. Returns when
/// the socket dies, stops answering, or a newer connection takes over. Every
/// way out closes the socket, and the timers are dropped with this frame.
async fn pump(
    ctx: &Ctx,
    generation: u64,
    ws: WebSocket,
    mut outbound: mpsc::UnboundedReceiver<Outbound>,
    closer: &mut Closer,
) -> PumpEnd {
    let (mut write, read) = ws.split();
    let mut read = read.fuse();
    let mut ping_timer = IntervalStream::new(PING_INTERVAL_MS).fuse();
    let mut watchdog_timer = IntervalStream::new(WATCHDOG_CHECK_MS).fuse();
    // Armed while a tab-return probe waits for its answer.
    let mut probe_timer: Fuse<TimeoutFuture> = Fuse::terminated();
    let mut liveness = Liveness::new(now_ms());

    let end = loop {
        futures::select_biased! {
            _ = &mut *closer => break PumpEnd::Superseded,
            msg = read.next() => match msg {
                Some(Ok(msg)) => {
                    liveness.on_inbound(now_ms());
                    if !ctx.is_current(generation) {
                        break PumpEnd::Superseded;
                    }
                    if let Message::Bytes(data) = msg {
                        match decode_frame(&data) {
                            Ok(inbound) => {
                                if let Some(inbound) = liveness.on_frame(inbound) {
                                    handle_message(ctx, inbound);
                                }
                            }
                            Err(e) => log::error!("❌ {e}"),
                        }
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
                    log::error!("❌ WebSocket error: {e:?}");
                    break PumpEnd::Lost;
                }
                None => {
                    log::warn!("💔 WebSocket connection closed!");
                    break PumpEnd::Lost;
                }
            },
            out = outbound.next() => match out {
                Some(Outbound::Frame(data)) => {
                    if let Err(e) = write.send(Message::Bytes(data)).await {
                        log::error!("❌ Failed to send: {e:?}");
                        break PumpEnd::Lost;
                    }
                }
                Some(Outbound::Probe) => {
                    match liveness.begin_probe(now_ms()) {
                        ProbeStep::SendPing(ping_id) => {
                            log::debug!("🏓 Tab is back - PING first, reconnect only if no answer");
                            if let Err(e) = send_ping(&mut write, &mut liveness, ping_id).await {
                                log::error!("❌ Failed to send PING: {e:?}");
                                break PumpEnd::Lost;
                            }
                            probe_timer = TimeoutFuture::new(PROBE_TIMEOUT_MS).fuse();
                        }
                        ProbeStep::AlreadyProbing => {
                            log::debug!("🏓 Already waiting for an answer to the last probe");
                        }
                        ProbeStep::Reconnect => {
                            log::info!(
                                "💔 Nothing from the server for {:.0}s - not waiting for a PONG, reconnecting",
                                (now_ms() - liveness.last_inbound_ms) / 1000.0
                            );
                            break PumpEnd::Unresponsive;
                        }
                    }
                }
                // Our sender was dropped: a newer connection replaced us.
                None => break PumpEnd::Superseded,
            },
            () = probe_timer => {
                if liveness.probe_unanswered() {
                    log::warn!(
                        "💔 No answer to the tab-return PING within {PROBE_TIMEOUT_MS}ms - reconnecting"
                    );
                    break PumpEnd::Unresponsive;
                }
                log::debug!("💚 Server answered the tab-return PING - keeping the socket");
            },
            _ = ping_timer.next() => {
                log::debug!("🏓 Sending PING to keep connection alive!");
                if let Err(e) = send_ping(&mut write, &mut liveness, new_correlation_id()).await {
                    log::error!("❌ Failed to send PING: {e:?}");
                    break PumpEnd::Lost;
                }
            },
            _ = watchdog_timer.next() => {
                let now = now_ms();
                if liveness.watchdog_expired(now) {
                    log::warn!(
                        "💔 Nothing from the server for {:.0}s and the last PING went unanswered - dropping the socket",
                        (now - liveness.last_inbound_ms) / 1000.0
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

/// One inbound frame, decoded.
#[derive(Debug, PartialEq)]
enum Inbound {
    /// The PONG to the ping that carried this correlation id. It goes to the
    /// socket's task (see `Liveness::on_frame`), not to the UI.
    Pong { correlation_id: String },
    /// Any other response, for the UI.
    Response(ApiResponse),
    /// A device event, for the UI.
    Event(DeviceEvent),
    /// A message type the UI doesn't handle.
    Other,
}

/// Decode one inbound CBOR frame. Pure, so it's unit-tested natively.
fn decode_frame(data: &[u8]) -> Result<Inbound, String> {
    let api_msg = ciborium::from_reader::<ApiMessage, _>(data)
        .map_err(|e| format!("Failed to decode CBOR message: {e}"))?;
    let payload = api_msg.payload.as_slice();
    match api_msg.message_type {
        ApiMessageType::Response => match ciborium::from_reader::<ApiResponse, _>(payload) {
            Ok(ApiResponse::Pong) => Ok(Inbound::Pong {
                correlation_id: api_msg.correlation_id,
            }),
            Ok(response) => Ok(Inbound::Response(response)),
            Err(e) => Err(format!("Failed to decode response: {e}")),
        },
        ApiMessageType::Event => ciborium::from_reader::<DeviceEvent, _>(payload)
            .map(Inbound::Event)
            .map_err(|e| format!("Failed to decode DeviceEvent: {e}")),
        ApiMessageType::Request | ApiMessageType::Error => Ok(Inbound::Other),
    }
}

/// Hand one decoded frame to the UI.
fn handle_message(ctx: &Ctx, inbound: Inbound) {
    match inbound {
        Inbound::Response(response) => {
            log::info!("📥 Received response: {response:?}");
            ctx.last_response.set(Some(response));
        }
        Inbound::Event(device_event) => {
            // Handle events - CBOR to JSON for now (TODO: pure CBOR) 🔥
            log::info!(
                "🔥 DeviceEvent: device_id={}, event_type={:?}",
                device_event.device_id,
                device_event.event_type
            );

            // Convert to JSON for UI compatibility (temporary)
            match serde_json::to_value(&device_event) {
                Ok(event_json) => ctx.last_event.set(Some(event_json)),
                Err(e) => {
                    log::error!("❌ Failed to convert DeviceEvent to JSON: {e}");
                }
            }
        }
        Inbound::Pong { .. } | Inbound::Other => {}
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
        use_effect_with((), move |()| {
            connect(&ctx);
            move || shutdown(&ctx)
        });
    }

    // 🔥 iOS WAKE DETECTION - CHECK THE CONNECTION ON RETURN! 💖
    // iOS webapps need both visibilitychange AND pageshow
    use_effect_with((), move |()| {
        let window = web_sys::window().expect("window");
        let document = window.document().expect("document");

        let visibility_listener = EventListener::new(&document, "visibilitychange", {
            let ctx = ctx.clone();
            let document = document.clone();
            move |_| {
                if document.visibility_state() == web_sys::VisibilityState::Visible {
                    wake_up(&ctx, Wake::Visible, "👁️ visibilitychange: visible");
                }
            }
        });

        // 🔥 iOS PAGESHOW - MORE RELIABLE FOR WEBAPPS! 💖
        // A restore from the back/forward cache means the page (and its
        // socket) was frozen: reconnect. Any other pageshow is the first
        // load's, whose connect is already running.
        let pageshow_listener = EventListener::new(&window, "pageshow", move |event| {
            if page_restored(event) {
                wake_up(&ctx, Wake::Restored, "📱 pageshow: restored");
            } else {
                log::debug!("📱 pageshow: first load - the initial connect is already running");
            }
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
                let data = match encode_request(&request, new_correlation_id()) {
                    Ok(data) => data,
                    Err(e) => {
                        log::error!("❌ {e}");
                        return;
                    }
                };

                log::info!("📤 Sending request: {request:?}");

                // Send via channel
                if !state.borrow().queue(Outbound::Frame(data)) {
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
    use futures::future::{pending, ready, FusedFuture};

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
        assert!(!Liveness::new(0.0).watchdog_expired(600.0 * S));
        // Ping at 30 s answered right away; 60 s later all is well.
        let mut live = Liveness::new(0.0);
        live.on_ping_sent(30.0 * S);
        live.on_inbound(30.1 * S);
        assert!(!live.watchdog_expired(90.0 * S));
    }

    #[test]
    fn watchdog_fires_after_two_unanswered_ping_intervals() {
        // Opened at 0, pings at 30 s and 60 s both unanswered.
        assert!(!watchdog_expired(60.0 * S, 0.0, Some(60.0 * S)));
        assert!(!watchdog_expired(64.9 * S, 0.0, Some(60.0 * S)));
        assert!(watchdog_expired(65.0 * S, 0.0, Some(60.0 * S)));
        assert!(watchdog_expired(600.0 * S, 0.0, Some(60.0 * S)));
        // Same ladder from a message at 100 s: an unanswered ping past its
        // grace isn't enough while the server was heard from < 65 s ago.
        assert!(!watchdog_expired(164.9 * S, 100.0 * S, Some(130.0 * S)));
        assert!(watchdog_expired(165.0 * S, 100.0 * S, Some(130.0 * S)));
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
        let mut live = Liveness::new(0.0);
        live.on_ping_sent(30.0 * S);
        live.on_inbound(30.1 * S);
        assert!(!live.watchdog_expired(100.0 * S));
        // Never pinged at all.
        assert!(!watchdog_expired(100.0 * S, 0.0, None));
        // Once a ping goes out and stays unanswered, it is.
        live.on_ping_sent(100.0 * S);
        assert!(!live.watchdog_expired(104.9 * S));
        assert!(live.watchdog_expired(105.0 * S));
    }

    #[test]
    fn watchdog_survives_the_clock_going_backwards() {
        assert!(!watchdog_expired(10.0 * S, 100.0 * S, Some(101.0 * S)));
    }

    #[test]
    fn watchdog_orders_a_ping_and_a_message_in_the_same_millisecond() {
        // #29, the `>` vs `>=` mutant: the old check compared stamps
        // (`sent > last_inbound`), so a ping and a message stamped with the
        // same millisecond were ambiguous. Liveness goes by event order.
        let t = 1_000.0 * S;
        let later = t + WATCHDOG_TIMEOUT_MS;
        // Message, then ping: the message can't be that ping's answer.
        let mut live = Liveness::new(0.0);
        live.on_inbound(t);
        live.on_ping_sent(t);
        assert!(live.watchdog_expired(later));
        // Ping, then message: answered.
        let mut live = Liveness::new(0.0);
        live.on_ping_sent(t);
        live.on_inbound(t);
        assert!(!live.watchdog_expired(later));
    }

    /// Feed `live` one inbound PONG the way the pump does: a real frame,
    /// through `decode_frame` and then `on_frame`. Returns whether it
    /// answered the probe.
    fn pong(live: &mut Liveness, now_ms: f64, correlation_id: &str) -> bool {
        let was_waiting = live.probe_unanswered();
        live.on_inbound(now_ms);
        let inbound = decode_frame(&response_frame(correlation_id, &ApiResponse::Pong))
            .expect("a PONG frame decodes");
        assert_eq!(live.on_frame(inbound), None, "a PONG never reaches the UI");
        was_waiting && !live.probe_unanswered()
    }

    /// The correlation id a `SendPing` step asks the pump to send.
    fn probe_ping(step: ProbeStep) -> String {
        match step {
            ProbeStep::SendPing(ping_id) => ping_id,
            other => panic!("expected SendPing, got {other:?}"),
        }
    }

    #[test]
    fn tab_return_probe_pings_first_and_its_pong_keeps_the_socket() {
        let mut live = Liveness::new(0.0);
        live.on_inbound(10.0 * S);
        let probe = probe_ping(live.begin_probe(20.0 * S));
        live.on_ping_sent(20.0 * S);
        assert!(live.probe_unanswered());
        // The probe's pong beats the probe timer.
        assert!(pong(&mut live, 20.05 * S, &probe));
        assert!(!live.probe_unanswered());
        // The next tab switch probes afresh, under a new id.
        assert_ne!(probe_ping(live.begin_probe(40.0 * S)), probe);
    }

    #[test]
    fn a_frame_buffered_before_the_probe_does_not_answer_it() {
        // #36 review: the socket went half-open with frames still in flight,
        // an event and the pong to an earlier keepalive ping. They show up
        // after the tab-return probe's ping went out, but they aren't its
        // answer. Counting them kept the dead socket until the watchdog
        // dropped it 65 s later.
        let mut live = Liveness::new(0.0);
        live.on_ping_sent(30.0 * S); // keepalive, id "keepalive"
        let probe = probe_ping(live.begin_probe(30.5 * S));
        live.on_ping_sent(30.5 * S);
        live.on_inbound(30.51 * S); // the buffered event
        assert!(!pong(&mut live, 30.52 * S, "keepalive"));
        // The probe timer fires: still unanswered, so reconnect.
        assert!(live.probe_unanswered());
        // The watchdog is unchanged: any inbound frame counts for it.
        assert!(!live.watchdog_expired(30.52 * S + WATCHDOG_TIMEOUT_MS - 1.0));
        // Had the probe's own pong come back, that would have answered it.
        assert!(pong(&mut live, 30.6 * S, &probe));
        assert!(!live.probe_unanswered());
        // A late duplicate of it changes nothing.
        assert!(!pong(&mut live, 30.7 * S, &probe));
    }

    #[test]
    fn frames_other_than_a_pong_go_on_to_the_ui() {
        let mut live = Liveness::new(0.0);
        let probe = probe_ping(live.begin_probe(1.0 * S));
        // A response under the probe's id is still not its PONG.
        let ack = ApiResponse::SubscriptionStarted {
            subscriber_id: "sub".to_string(),
        };
        let inbound = decode_frame(&response_frame(&probe, &ack)).expect("decode");
        assert_eq!(live.on_frame(inbound), Some(Inbound::Response(ack)));
        assert!(live.probe_unanswered());
        assert_eq!(live.on_frame(Inbound::Other), Some(Inbound::Other));
    }

    #[test]
    fn tab_return_probe_without_an_answer_reconnects() {
        let mut live = Liveness::new(0.0);
        probe_ping(live.begin_probe(5.0 * S));
        live.on_ping_sent(5.0 * S);
        // The probe timer fires and nothing has arrived.
        assert!(live.probe_unanswered());
    }

    #[test]
    fn tab_return_probe_is_not_restarted_while_it_waits() {
        // visibilitychange twice in a row: one ping, one deadline.
        let mut live = Liveness::new(0.0);
        assert!(!live.probe_unanswered());
        let first = probe_ping(live.begin_probe(1.0 * S));
        assert_eq!(live.begin_probe(1.5 * S), ProbeStep::AlreadyProbing);
        assert!(live.probe_unanswered());
        // The probe still waits for the first ping's pong: no second ping
        // went out, so no other pong answers it.
        assert!(!pong(&mut live, 1.6 * S, "some-other-ping"));
        assert!(pong(&mut live, 1.7 * S, &first));
    }

    #[test]
    fn tab_return_after_a_long_silence_reconnects_without_waiting() {
        // The page was frozen (iOS resume, laptop lid): don't wait on a pong
        // that probably won't come.
        let mut live = Liveness::new(0.0);
        live.on_inbound(100.0 * S);
        let mut just_in_time = live.clone();
        probe_ping(just_in_time.begin_probe(100.0 * S + WATCHDOG_TIMEOUT_MS - 1.0));
        assert_eq!(
            live.begin_probe(100.0 * S + WATCHDOG_TIMEOUT_MS),
            ProbeStep::Reconnect
        );
        assert!(!live.probe_unanswered());
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
        let g1 = lc.begin_attempt(0.0);
        assert_eq!(lc.attempt_status(), ConnectionStatus::Connecting);
        assert_eq!(lc.on_closed(g1), Some(1_000));
        assert!(lc.should_retry(g1));

        let g2 = lc.begin_attempt(1.0 * S);
        assert_eq!(lc.attempt_status(), ConnectionStatus::Reconnecting(1));
        assert_eq!(lc.on_closed(g2), Some(2_000));

        let g3 = lc.begin_attempt(3.0 * S);
        assert_eq!(lc.attempt_status(), ConnectionStatus::Reconnecting(2));
        assert_eq!(lc.on_closed(g3), Some(4_000));

        // Server is back.
        let g4 = lc.begin_attempt(7.0 * S);
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
        let mut g = lc.begin_attempt(0.0);
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
            g = lc.begin_attempt(f64::from(attempt) * 30.0 * S);
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
        let old = lc.begin_attempt(0.0);
        assert!(lc.on_open(old));
        assert_eq!(lc.on_wake(Wake::Restored, 60.0 * S), WakeAction::Reconnect);
        let new = lc.begin_attempt(60.0 * S);
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
        let old = lc.begin_attempt(0.0);
        let new = lc.begin_attempt(0.0);
        assert!(!lc.on_open(old));
        assert_eq!(lc.phase, Phase::Connecting);
        assert!(lc.on_open(new));
    }

    #[test]
    fn backoff_timer_is_dropped_once_something_newer_started() {
        let mut lc = Lifecycle::new();
        let g1 = lc.begin_attempt(0.0);
        assert!(lc.on_closed(g1).is_some());
        // The user comes back before the timer fires.
        assert_eq!(lc.on_wake(Wake::Visible, 0.5 * S), WakeAction::Reconnect);
        let g2 = lc.begin_attempt(0.5 * S);
        assert!(!lc.should_retry(g1));
        // Even once g2 is in backoff itself, g1's old timer stays dead.
        assert!(lc.on_closed(g2).is_some());
        assert!(!lc.should_retry(g1));
        assert!(lc.should_retry(g2));
    }

    #[test]
    fn retry_only_fires_from_backoff() {
        let mut lc = Lifecycle::new();
        let g = lc.begin_attempt(0.0);
        assert!(!lc.should_retry(g)); // still connecting
        assert!(lc.on_open(g));
        assert!(!lc.should_retry(g)); // open
    }

    #[test]
    fn wake_leaves_a_young_attempt_alone() {
        let mut lc = Lifecycle::new();
        let started = 10.0 * S;
        let g = lc.begin_attempt(started);
        // The first load's pageshow, or visibilitychange + pageshow back to
        // back: the attempt one of them just started stays.
        assert_eq!(lc.on_wake(Wake::Visible, started), WakeAction::Nothing);
        assert_eq!(
            lc.on_wake(Wake::Restored, started + CONNECT_PREEMPT_MS - 1.0),
            WakeAction::Nothing
        );
        assert_eq!(lc.phase, Phase::Connecting);
        assert!(lc.on_open(g));
    }

    #[test]
    fn wake_preempts_an_attempt_stuck_connecting() {
        // #29: after an iOS resume a hung config fetch or open used to hold
        // things for up to 2 x CONNECT_TIMEOUT_MS.
        let mut lc = Lifecycle::new();
        for _ in 0..3 {
            let g = lc.begin_attempt(0.0);
            lc.on_closed(g);
        }
        let hung = lc.begin_attempt(10.0 * S);
        let now = 10.0 * S + CONNECT_PREEMPT_MS;
        assert_eq!(lc.on_wake(Wake::Visible, now), WakeAction::Reconnect);
        // A fresh attempt with a fresh backoff.
        let next = lc.begin_attempt(now);
        assert_eq!(lc.attempt_status(), ConnectionStatus::Connecting);
        // The hung attempt is stale now: a late open or failure does nothing.
        assert!(!lc.on_open(hung));
        assert_eq!(lc.on_closed(hung), None);
        // The new attempt is young, so an immediate second wake leaves it be.
        assert_eq!(lc.on_wake(Wake::Restored, now + 1.0), WakeAction::Nothing);
        assert!(lc.on_open(next));
    }

    #[test]
    fn wake_on_an_open_socket_probes_a_tab_switch_but_reconnects_a_restore() {
        let mut lc = Lifecycle::new();
        let g = lc.begin_attempt(0.0);
        assert!(lc.on_open(g));
        // Desktop tab switch: ping first, the socket stays current meanwhile.
        assert_eq!(lc.on_wake(Wake::Visible, 60.0 * S), WakeAction::Probe);
        assert_eq!(lc.phase, Phase::Open);
        assert!(lc.is_current(g));
        // Restored from the back/forward cache: frozen, reconnect.
        assert_eq!(lc.on_wake(Wake::Restored, 60.0 * S), WakeAction::Reconnect);
    }

    #[test]
    fn ios_wake_pair_reconnects_once_in_either_order() {
        for pair in [
            [Wake::Visible, Wake::Restored],
            [Wake::Restored, Wake::Visible],
        ] {
            let mut lc = Lifecycle::new();
            let g = lc.begin_attempt(0.0);
            assert!(lc.on_open(g));
            let t = 600.0 * S;
            let mut reconnects = 0;
            for wake in pair {
                if lc.on_wake(wake, t) == WakeAction::Reconnect {
                    reconnects += 1;
                    lc.begin_attempt(t);
                }
            }
            assert_eq!(reconnects, 1, "{pair:?}");
        }
    }

    #[test]
    fn wake_in_backoff_retries_now_and_restarts_the_backoff() {
        let mut lc = Lifecycle::new();
        for _ in 0..4 {
            let g = lc.begin_attempt(0.0);
            lc.on_closed(g);
        }
        assert_eq!(lc.failures, 4);
        assert_eq!(lc.on_wake(Wake::Visible, 0.0), WakeAction::Reconnect);
        let g = lc.begin_attempt(0.0);
        assert_eq!(lc.attempt_status(), ConnectionStatus::Connecting);
        assert_eq!(lc.on_closed(g), Some(1_000));
    }

    #[test]
    fn stop_makes_everything_stale_for_good() {
        let mut lc = Lifecycle::new();
        let earlier = lc.begin_attempt(0.0);
        let g = lc.begin_attempt(0.0);
        assert!(lc.on_closed(g).is_some());
        lc.stop();
        // No generation ever handed out is current again.
        assert!(!lc.is_current(earlier));
        assert!(!lc.is_current(g));
        assert!(!lc.should_retry(g));
        assert!(!lc.on_open(g));
        assert_eq!(lc.on_closed(g), None);
        assert_eq!(lc.on_wake(Wake::Visible, 0.0), WakeAction::Nothing);
        assert_eq!(lc.on_wake(Wake::Restored, 3_600.0 * S), WakeAction::Nothing);
    }

    #[test]
    fn a_new_attempt_supersedes_the_old_socket_and_its_open_replaces_the_sender() {
        // #29, the "connect() keeps the old sender" mutant: a new attempt that
        // kept connection 1's sender and closer would leave connection 1
        // alive next to connection 2, still taking requests.
        let mut state = WsState::new();
        let (g1, _, closer1) = state.begin_attempt(0.0);
        let (tx1, mut rx1) = mpsc::unbounded();
        assert!(state.on_open(g1, tx1));
        assert!(state.queue(Outbound::Frame(vec![1])));
        assert_eq!(
            rx1.next().now_or_never(),
            Some(Some(Outbound::Frame(vec![1])))
        );

        let (g2, _, _closer2) = state.begin_attempt(60.0 * S);
        // Connection 1's task sees both signals: its closer fires and its
        // queue ends.
        assert!(closer1.now_or_never().is_some());
        assert_eq!(rx1.next().now_or_never(), Some(None));
        // Nothing can be queued while connection 2 connects,
        assert!(!state.queue(Outbound::Frame(vec![2])));
        // a late open of connection 1 can't claim the queue back,
        let (stale_tx, _stale_rx) = mpsc::unbounded();
        assert!(!state.on_open(g1, stale_tx));
        assert!(!state.queue(Outbound::Frame(vec![2])));
        // and once connection 2 opens, requests go to it.
        let (tx2, mut rx2) = mpsc::unbounded();
        assert!(state.on_open(g2, tx2));
        assert!(state.queue(Outbound::Frame(vec![3])));
        assert_eq!(
            rx2.next().now_or_never(),
            Some(Some(Outbound::Frame(vec![3])))
        );
    }

    #[test]
    fn a_tab_switch_probes_the_open_socket_or_reconnects_without_one() {
        let mut state = WsState::new();
        let (g, _, _closer) = state.begin_attempt(0.0);
        let (tx, mut rx) = mpsc::unbounded();
        assert!(state.on_open(g, tx));
        assert_eq!(state.on_wake(Wake::Visible, 60.0 * S), WakeAction::Probe);
        assert_eq!(rx.next().now_or_never(), Some(Some(Outbound::Probe)));
        // The socket's task is gone but hasn't reported yet: nobody would
        // answer a probe, so reconnect instead.
        drop(rx);
        assert_eq!(
            state.on_wake(Wake::Visible, 61.0 * S),
            WakeAction::Reconnect
        );
    }

    #[test]
    fn a_raw_oneshot_receiver_is_terminated_as_soon_as_its_sender_drops() {
        // Why `Closer` is fused (#29): `select!` skips terminated branches,
        // and a bare receiver counts as terminated the moment its sender is
        // dropped, before anyone has seen the drop.
        let (tx, rx) = oneshot::channel::<()>();
        assert!(!rx.is_terminated());
        drop(tx);
        assert!(rx.is_terminated());
        // A `Closer` stays live until it has actually reported the drop.
        let (tx, mut closer) = closer_channel();
        drop(tx);
        assert!(!closer.is_terminated());
        assert!((&mut closer).now_or_never().is_some());
        assert!(closer.is_terminated());
    }

    #[test]
    fn dropping_the_closer_supersedes_an_attempt_in_flight() {
        // #29: this is how `release_socket()` supersedes an attempt that's
        // still fetching its config or opening its socket. With a bare
        // receiver, the closer's branch is skipped and the attempt carries on
        // until its own timeout.
        let (closer_tx, mut closer) = closer_channel();
        let mut attempt = Box::pin(race(pending::<()>(), pending::<()>(), &mut closer));
        assert!(attempt.as_mut().now_or_never().is_none());
        drop(closer_tx);
        assert!(matches!(
            attempt.as_mut().now_or_never(),
            Some(Race::Superseded)
        ));
    }

    #[test]
    fn a_closer_dropped_before_the_race_still_supersedes_it() {
        // Superseded between two steps (config fetched, socket not yet
        // opened): the next step must still see it.
        let (closer_tx, mut closer) = closer_channel();
        drop(closer_tx);
        let step = race(pending::<()>(), pending::<()>(), &mut closer).now_or_never();
        assert!(matches!(step, Some(Race::Superseded)));
    }

    #[test]
    fn race_reports_the_result_or_the_timeout_and_a_supersede_wins() {
        let (_closer_tx, mut closer) = closer_channel();
        let done = race(ready(7), pending::<()>(), &mut closer).now_or_never();
        assert!(matches!(done, Some(Race::Done(7))));
        let timed_out = race(pending::<u8>(), ready(()), &mut closer).now_or_never();
        assert!(matches!(timed_out, Some(Race::TimedOut)));
        // Everything ready at once: the supersede wins, so a superseded
        // attempt never goes on to touch shared state.
        let (closer_tx, mut closer) = closer_channel();
        drop(closer_tx);
        let all = race(ready(7), ready(()), &mut closer).now_or_never();
        assert!(matches!(all, Some(Race::Superseded)));
    }

    #[test]
    fn a_failed_config_fetch_reuses_the_last_good_url() {
        // #9: a reconnect whose config fetch failed used to dial
        // ws://localhost:31337, which is wrong for any page not served from
        // the server's own host (a phone on the LAN).
        let mut url = SocketUrl::default();
        // Nothing has loaded yet: the default is all there is.
        assert_eq!(url.resolve(None), DEFAULT_SOCKET_URL);
        let hub = "ws://192.168.1.20:31337";
        assert_eq!(url.resolve(Some(hub.to_string())), hub);
        // Failed or timed-out fetches keep dialing the last good URL.
        assert_eq!(url.resolve(None), hub);
        assert_eq!(url.resolve(None), hub);
        // A newer config replaces it.
        let moved = "wss://nest.example:8443/ws";
        assert_eq!(url.resolve(Some(moved.to_string())), moved);
        assert_eq!(url.resolve(None), moved);
    }

    #[test]
    fn ping_request_round_trips_through_the_envelope() {
        let id = new_correlation_id();
        assert_ne!(id, new_correlation_id());
        let data = encode_request(&ApiRequest::Ping, id.clone()).expect("encode");
        let msg: ApiMessage = ciborium::from_reader(data.as_slice()).expect("envelope");
        assert_eq!(msg.message_type, ApiMessageType::Request);
        assert_eq!(msg.correlation_id, id);
        let req: ApiRequest = ciborium::from_reader(msg.payload.as_slice()).expect("payload");
        assert!(matches!(req, ApiRequest::Ping));
    }

    /// A frame the way the server sends a response: `response` in an
    /// `ApiMessage` that echoes the request's `correlation_id`.
    fn response_frame(correlation_id: &str, response: &ApiResponse) -> Vec<u8> {
        let mut payload = Vec::new();
        ciborium::into_writer(response, &mut payload).expect("payload");
        let msg = ApiMessage {
            correlation_id: correlation_id.to_string(),
            message_type: ApiMessageType::Response,
            payload,
        };
        let mut data = Vec::new();
        ciborium::into_writer(&msg, &mut data).expect("envelope");
        data
    }

    #[test]
    fn a_pong_frame_decodes_with_the_correlation_id_it_answers() {
        // The pump matches this id against the probe's ping.
        assert_eq!(
            decode_frame(&response_frame("probe-7", &ApiResponse::Pong)),
            Ok(Inbound::Pong {
                correlation_id: "probe-7".to_string()
            })
        );
        // Other responses still go to the UI.
        let ack = ApiResponse::SubscriptionStarted {
            subscriber_id: "sub".to_string(),
        };
        assert_eq!(
            decode_frame(&response_frame("probe-7", &ack)),
            Ok(Inbound::Response(ack))
        );
        assert!(decode_frame(b"not cbor").is_err());
    }
}
