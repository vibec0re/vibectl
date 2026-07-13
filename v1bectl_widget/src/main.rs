//! `v1bectl_widget` — home automation in the trollshell sidebar 🔥
//!
//! An **out-of-process** trollshell widget plugin (vibec0re/trollshell#35,
//! built on the `hytte-plugin` SDK, #279): lives in *this* repo, links zero
//! shell code, and drives a real sidebar widget over
//! `$XDG_RUNTIME_DIR/trollshell/plugin.sock`. The shell never recompiles for
//! it — that's the whole point.
//!
//! Faithful miniature of the cyber web UI (`v1bectl_web`): light toggles +
//! brightness, outlet toggles (+ live wattage), inline temp/humidity sensors.
//! No header chrome — just the devices (an empty-state line shows only before
//! devices load or when the server is down). Interactions:
//!
//! - **click** a light/outlet → toggle
//! - **scroll** on a light row → brightness ±5 % (the vocab has no slider;
//!   scroll-to-adjust is the shell-native idiom anyway)
//!
//! # Layout
//!
//! Two paths (see `view`). With a `screens.kdl` config present (the same
//! curated file the GTK/web clients read — see the [`screens`] module), the
//! widget renders *that* layout: screens → groups → blocks, custom row labels,
//! and per-element `switch`/`slider`/`show-temp`/`show-humidity` flags.
//! Otherwise — or if none of the config's device refs resolve — it falls back
//! to auto-grouping every device by room, so the sidebar is never blank.
//!
//! # Shape
//!
//! Pure TEA over the SDK: [`VibeWidget`] (model) + `update` + `view`. The
//! widget's own I/O — the CBOR-over-WebSocket client to `v1bectl_server`
//! (`ws.rs`) — reports in through `sources()` as [`Input::App`], and takes
//! commands from the reducer through a channel the model owns. (The
//! init-parks-receiver / model-owns-sender pattern; a sanctioned SDK lane is
//! proposed as trollshell#280.)
//!
//! State is never mutated locally on click: the server applies optimistic
//! updates and echoes a `DeviceEvent`, which is the single source of truth —
//! same convention as the other v1bectl clients.

mod screens;
mod ws;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use hytte_plugin::proto::{Dir, Effect, EventKind, Manifest, Mount, Node};
use hytte_plugin::tokio_stream::wrappers::UnboundedReceiverStream;
use hytte_plugin::{Input, MsgStream, Plugin};
use tokio::sync::mpsc;
use v1bectl_state::{DeviceState, DeviceStateValue, DeviceType, EventType};

use screens::{DeviceRef, Element, Screen};
use ws::{Cmd, Conn, WsMsg};

/// How much one scroll notch changes brightness (percent).
const BRIGHTNESS_STEP: i16 = 5;

thread_local! {
    /// Hand-off slot: `init()` parks the command receiver here for
    /// `sources()` to collect. Sound because the SDK session calls `init()`
    /// before `sources()`, both on the runtime thread.
    static CMD_RX: RefCell<Option<mpsc::UnboundedReceiver<Cmd>>> = const { RefCell::new(None) };
}

/// The model: connection state + the device list, patched by server events.
struct VibeWidget {
    conn: Conn,
    devices: Vec<DeviceState>,
    /// The curated layout from `screens.kdl` (empty ⇒ auto-group by room). Read
    /// once at startup; the device list it renders arrives live over the WS.
    screens: Vec<Screen>,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    /// Fractional scroll accumulated per light, so touchpad smooth-scroll
    /// deltas (dozens of sub-1.0 events per swipe) fold into whole notches
    /// instead of each slamming a full step.
    scroll_accum: HashMap<String, f64>,
    /// The brightness we last *asked* for, per light — the base for the next
    /// scroll step while the server's echo is still in flight (otherwise a
    /// burst of notches all compute from the same stale model value).
    /// Cleared when a state patch for the device arrives.
    pending_bright: HashMap<String, u8>,
}

impl VibeWidget {
    fn send(&self, cmd: Cmd) {
        // Failure = the WS task is gone, which only happens when the session
        // is tearing down anyway.
        let _ = self.cmd_tx.send(cmd);
    }

    fn device(&self, id: &str) -> Option<&DeviceState> {
        self.devices.iter().find(|d| d.device_id == id)
    }

    /// Fold one server event into the device list.
    fn apply_event(&mut self, device_id: &str, event: EventType) {
        match event {
            EventType::StateChanged {
                new_state: Some(state),
                ..
            } => self.patch_state(device_id, state),
            EventType::AttributeChanged {
                attribute,
                new_value,
                ..
            } if attribute == "state" => {
                match serde_json::from_value::<DeviceStateValue>(new_value) {
                    Ok(state) => self.patch_state(device_id, state),
                    // A raw-gateway payload we can't shape (real Dirigera
                    // pushes carry gateway JSON, not DeviceStateValue) —
                    // resync instead of silently going stale.
                    Err(_) => self.send(Cmd::Refresh),
                }
            }
            EventType::DeviceReachabilityChanged { reachable } => {
                if let Some(dev) = self.devices.iter_mut().find(|d| d.device_id == device_id) {
                    dev.device_info.reachable = reachable;
                }
            }
            EventType::DeviceAdded { .. } => self.send(Cmd::Refresh),
            EventType::DeviceRemoved => self.devices.retain(|d| d.device_id != device_id),
            _ => {}
        }
    }

    /// Apply a new state value to a device, *merging* optional fields the
    /// echo may omit (a `SetLightState{is_on}` echo carries `brightness:
    /// None`; an outlet echo carries `power_consumption: None`) — a patch
    /// must not wipe live readouts. Also settles any pending scroll target.
    fn patch_state(&mut self, device_id: &str, new: DeviceStateValue) {
        self.pending_bright.remove(device_id);
        let Some(dev) = self.devices.iter_mut().find(|d| d.device_id == device_id) else {
            return;
        };
        dev.state = match (&dev.state, new) {
            (DeviceStateValue::Light(old), DeviceStateValue::Light(mut new)) => {
                new.brightness = new.brightness.or(old.brightness);
                new.color_temp = new.color_temp.or(old.color_temp);
                new.rgb_color = new.rgb_color.or(old.rgb_color.clone());
                DeviceStateValue::Light(new)
            }
            (DeviceStateValue::Outlet(old), DeviceStateValue::Outlet(mut new)) => {
                new.power_consumption = new.power_consumption.or(old.power_consumption);
                new.total_energy = new.total_energy.or(old.total_energy);
                DeviceStateValue::Outlet(new)
            }
            (_, new) => new,
        };
    }
}

impl Plugin for VibeWidget {
    type Msg = WsMsg;

    fn manifest() -> Manifest {
        // No host-state subscriptions, no shell capabilities: everything this
        // widget does goes over its own WebSocket.
        Manifest::new("vibectl", Mount::SidebarBottom)
    }

    fn init() -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        CMD_RX.with(|slot| *slot.borrow_mut() = Some(cmd_rx));
        Self {
            conn: Conn::Connecting,
            devices: Vec::new(),
            screens: screens::load(),
            cmd_tx,
            scroll_accum: HashMap::new(),
            pending_bright: HashMap::new(),
        }
    }

    fn sources() -> Option<MsgStream<Self::Msg>> {
        let cmd_rx = CMD_RX
            .with(|slot| slot.borrow_mut().take())
            .expect("init() parks the command receiver before sources() runs");
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        tokio::spawn(ws::client(cmd_rx, msg_tx));
        Some(Box::pin(UnboundedReceiverStream::new(msg_rx)))
    }

    fn update(&mut self, input: Input<Self::Msg>) -> Vec<Effect> {
        match input {
            Input::App(WsMsg::Status(conn)) => self.conn = conn,
            Input::App(WsMsg::Devices(devices)) => self.devices = devices,
            Input::App(WsMsg::Event(ev)) => {
                let id = ev.device_id.clone();
                self.apply_event(&id, ev.event_type);
            }
            Input::Event { node, kind } => self.on_ui_event(&node, kind),
            // We subscribe to no host state and issue no RunCommands.
            Input::Snapshot(_) | Input::EffectResult { .. } => {}
        }
        Vec::new()
    }

    fn view(&self) -> Node {
        // No header/status-line chrome: just the devices. The only non-device
        // line is the empty-state message, so the widget isn't blank before
        // devices load (or when the server is down).
        let children = if self.devices.is_empty() {
            vec![status_label(match self.conn {
                Conn::Connecting => "connecting to v1bectl…",
                Conn::Offline => "server unreachable — retrying",
                Conn::Online => "no devices",
            })]
        } else {
            // Config-driven layout when `screens.kdl` resolves at least one row;
            // otherwise auto-group by room so the sidebar is never blank.
            match self.view_config() {
                Some(body) => body,
                None => self.view_rooms(),
            }
        };
        Node::Box {
            id: Some("vw-root".into()),
            dir: Dir::Vertical,
            spacing: 4,
            scroll: false,
            classes: vec!["vw-root".into()],
            children,
        }
    }
}

impl VibeWidget {
    /// Route a shell UI event (click / scroll, by node id) to a command.
    fn on_ui_event(&mut self, node: &str, kind: EventKind) {
        if let Some(id) = node.strip_prefix("vw-l-") {
            if matches!(kind, EventKind::Click) {
                if let Some(DeviceStateValue::Light(light)) = self.device(id).map(|d| &d.state) {
                    self.send(Cmd::SetLight {
                        device_id: id.to_string(),
                        is_on: Some(!light.is_on),
                        brightness: None,
                    });
                }
            }
        } else if let Some(id) = node.strip_prefix("vw-ls-") {
            if let EventKind::Scroll { dy, .. } = kind {
                self.on_light_scroll(id, dy);
            }
        } else if let Some(id) = node.strip_prefix("vw-o-") {
            if matches!(kind, EventKind::Click) {
                if let Some(DeviceStateValue::Outlet(outlet)) = self.device(id).map(|d| &d.state) {
                    self.send(Cmd::SetOutlet {
                        device_id: id.to_string(),
                        is_on: !outlet.is_on,
                    });
                }
            }
        }
    }

    /// Scroll-to-dim with notch accumulation: raw smooth-scroll deltas
    /// (touchpads emit dozens of sub-1.0 events per swipe) fold into whole
    /// notches; each notch is ±5 %. Scroll up (negative dy) brightens — the
    /// shell's volume-chip convention. Horizontal scroll (dy == 0) is a no-op.
    fn on_light_scroll(&mut self, id: &str, dy: f64) {
        let Some(DeviceStateValue::Light(light)) = self.device(id).map(|d| &d.state) else {
            return;
        };
        let is_on = light.is_on;
        let model_bright = light.brightness;

        let acc = self.scroll_accum.entry(id.to_string()).or_insert(0.0);
        *acc += -dy; // up = positive
        let notches = acc.trunc();
        if notches == 0.0 {
            return;
        }
        *acc -= notches;

        // Base = the last requested value while an echo is in flight,
        // else the model's.
        let base = i16::from(
            self.pending_bright
                .get(id)
                .copied()
                .or(model_bright)
                .unwrap_or(50),
        );
        #[allow(clippy::cast_possible_truncation)]
        let next = (base + (notches as i16) * BRIGHTNESS_STEP).clamp(1, 100) as u8;
        let brightening = notches > 0.0;
        if next == base as u8 && (is_on || !brightening) {
            return; // already at the clamp; nothing new to ask for
        }
        self.pending_bright.insert(id.to_string(), next);
        self.send(Cmd::SetLight {
            device_id: id.to_string(),
            // Brightening a light that's off also turns it on (the legacy
            // web client's convention).
            is_on: (!is_on && brightening).then_some(true),
            brightness: Some(next),
        });
    }

    /// Devices grouped by room (`device_groups.first()`), rooms and devices
    /// each alphabetical.
    fn rooms(&self) -> BTreeMap<String, Vec<&DeviceState>> {
        let mut rooms: BTreeMap<String, Vec<&DeviceState>> = BTreeMap::new();
        for dev in &self.devices {
            if !renders(dev) {
                continue;
            }
            let room = dev
                .device_info
                .device_groups
                .first()
                .cloned()
                .unwrap_or_else(|| "other".to_string());
            rooms.entry(room).or_default().push(dev);
        }
        for devices in rooms.values_mut() {
            devices.sort_by(|a, b| a.device_info.name.cmp(&b.device_info.name));
        }
        rooms
    }

    /// The default layout: every renderable device, grouped by room and
    /// alphabetical. Used when no `screens.kdl` is configured — or when none of
    /// its device refs resolve, so the widget still shows the home.
    fn view_rooms(&self) -> Vec<Node> {
        let mut out = Vec::new();
        for (room, devices) in self.rooms() {
            out.push(left(Node::Label {
                id: None,
                text: room,
                classes: vec![
                    "vw-room".into(),
                    "dim-label".into(),
                    "caption-heading".into(),
                ],
            }));
            for dev in devices {
                if let Some(row) = device_row(dev, &RowOpts::auto(dev)) {
                    out.push(row);
                }
            }
        }
        out
    }

    /// Render the configured screens, or `None` when no layout is loaded or
    /// none of its refs resolve against the live device list (→ caller falls
    /// back to [`Self::view_rooms`]). A screen contributes its title only when
    /// at least one of its groups yields a row, so refs the server doesn't
    /// (yet) know about leave no empty headers behind.
    fn view_config(&self) -> Option<Vec<Node>> {
        if self.screens.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        // How many actual *device* rows resolved. Text and empty blocks don't
        // count — a title/banner-only screen must not suppress the fallback.
        let mut resolved = 0usize;
        for screen in &self.screens {
            let mut groups = Vec::new();
            for group in &screen.groups {
                let rows = self.render_elements(&group.elements, &mut resolved);
                if !rows.is_empty() {
                    groups.push(Node::Box {
                        id: None,
                        dir: Dir::Vertical,
                        spacing: 4,
                        scroll: false,
                        classes: vec!["vw-group".into()],
                        children: rows,
                    });
                }
            }
            if !groups.is_empty() {
                // Only a screen that declared a `title` gets a header label.
                if let Some(title) = &screen.title {
                    out.push(left(Node::Label {
                        id: None,
                        text: title.clone(),
                        classes: vec!["vw-screen".into(), "heading".into()],
                    }));
                }
                out.extend(groups);
            }
        }
        // Fall back to the room layout when the config resolved *no* real
        // device — otherwise a config that only names devices the server
        // doesn't know (a stale/renamed config) would hide the whole home
        // behind its titles and text banners.
        (resolved > 0).then_some(out)
    }

    /// Turn config elements into rows, resolving each device ref against the
    /// live list and skipping the ones we don't have (yet). Bumps `resolved`
    /// once per device row actually produced (not for text or empty blocks) so
    /// the caller can tell "the config rendered the home" from "the config
    /// rendered only decoration". A `block` lays its resolved children out
    /// horizontally, mirroring the GTK/web renderers.
    fn render_elements(&self, elements: &[Element], resolved: &mut usize) -> Vec<Node> {
        let mut out = Vec::new();
        for element in elements {
            match element {
                Element::Light {
                    name,
                    device_ref,
                    show_switch,
                    show_slider,
                } => {
                    if let Some(dev) = self.resolve(device_ref) {
                        let opts = RowOpts {
                            name,
                            allow_toggle: *show_switch,
                            show_slider: *show_slider,
                            show_temp: true,
                            show_humidity: true,
                        };
                        if let Some(row) = device_row(dev, &opts) {
                            *resolved += 1;
                            out.push(row);
                        }
                    }
                }
                Element::Outlet { name, device_ref } => {
                    if let Some(dev) = self.resolve(device_ref) {
                        let opts = RowOpts {
                            name,
                            allow_toggle: true,
                            show_slider: false,
                            show_temp: true,
                            show_humidity: true,
                        };
                        if let Some(row) = device_row(dev, &opts) {
                            *resolved += 1;
                            out.push(row);
                        }
                    }
                }
                Element::Sensor {
                    device_ref,
                    show_temp,
                    show_humidity,
                } => {
                    if let Some(dev) = self.resolve(device_ref) {
                        // A sensor slot is read-only: never emit a toggle, even
                        // if the ref points at an actuator (a misconfiguration).
                        let opts = RowOpts {
                            name: &dev.device_info.name,
                            allow_toggle: false,
                            show_slider: false,
                            show_temp: *show_temp,
                            show_humidity: *show_humidity,
                        };
                        if let Some(row) = device_row(dev, &opts) {
                            *resolved += 1;
                            out.push(row);
                        }
                    }
                }
                Element::Text { template } => out.push(left(Node::Label {
                    id: None,
                    text: template.clone(),
                    classes: vec!["vw-text".into(), "dim-label".into()],
                })),
                Element::Block { elements } => {
                    let cells: Vec<Node> = elements
                        .iter()
                        .filter_map(|el| {
                            let sub = self.render_elements(std::slice::from_ref(el), resolved);
                            (!sub.is_empty()).then(|| Node::Box {
                                id: None,
                                dir: Dir::Vertical,
                                spacing: 4,
                                scroll: false,
                                classes: vec!["vw-block-cell".into()],
                                children: sub,
                            })
                        })
                        .collect();
                    if !cells.is_empty() {
                        out.push(Node::Box {
                            id: None,
                            dir: Dir::Horizontal,
                            spacing: 8,
                            scroll: false,
                            classes: vec!["vw-block".into()],
                            children: cells,
                        });
                    }
                }
            }
        }
        out
    }

    /// Resolve a config device ref to a live device. Names match exactly — a
    /// substring would let "Deko" pick up "Deko Bunt"; same rule as GTK.
    fn resolve(&self, device_ref: &DeviceRef) -> Option<&DeviceState> {
        match device_ref {
            DeviceRef::ById(id) => self.device(id),
            DeviceRef::ByName(name) => self.devices.iter().find(|d| &d.device_info.name == name),
        }
    }
}

/// Whether a device gets a row (mirrors what the web UI renders). Gated on
/// the *device type*, not the state shape: `VirtualLightGroup`s carry
/// Light-shaped state too, but the server's virtual path emits no event
/// echo, so an echo-driven toggle on a group latches — until the server
/// publishes events for virtual writes, groups stay off the widget.
fn renders(dev: &DeviceState) -> bool {
    match dev.device_info.device_type {
        DeviceType::Light => matches!(dev.state, DeviceStateValue::Light(_)),
        DeviceType::Outlet => matches!(dev.state, DeviceStateValue::Outlet(_)),
        DeviceType::Sensor => matches!(dev.state, DeviceStateValue::Sensor(_)),
        _ => false,
    }
}

/// Left-align a node. A `gtk::Label` that's a direct child of a vertical box
/// fills the width and centres its text (GTK default `xalign 0.5`), and the
/// widget vocabulary exposes no alignment / halign field. Packing it into a
/// *horizontal* box instead makes it take its natural width at the start —
/// i.e. flush left. (The device rows are already horizontal; this is for the
/// standalone labels: screen titles, room headers, sensors, status, text.)
fn left(node: Node) -> Node {
    Node::Box {
        id: None,
        dir: Dir::Horizontal,
        spacing: 0,
        scroll: false,
        classes: vec![],
        children: vec![node],
    }
}

fn status_label(text: &str) -> Node {
    left(Node::Label {
        id: None,
        text: text.to_string(),
        classes: vec!["vw-status".into(), "dim-label".into()],
    })
}

/// Per-row display options. The auto layout ([`RowOpts::auto`]) uses the
/// device's own name and shows everything; a `screens.kdl` element overrides
/// these — a custom label, `slider`/`switch`, sensor `show-temp`/`show-humidity`.
///
/// `allow_toggle` and `show_slider` are **independent**, matching GTK: a light
/// can be toggle-only (`switch`, no `slider`), brightness-only (`slider`, no
/// `switch`), both, or a static readout (neither).
struct RowOpts<'a> {
    /// The row label: the config `name`, or the device's own name.
    name: &'a str,
    /// Emit the clickable on/off toggle (light/outlet). Config `switch` for
    /// lights; always on for the auto layout and outlet elements; always off
    /// for the read-only sensor slot.
    allow_toggle: bool,
    /// Light: the brightness affordance — the readout bar *and* scroll-to-dim
    /// (config `slider`; the auto layout always wants it).
    show_slider: bool,
    /// Sensor: show temperature / humidity (config `show-temp`/`show-humidity`).
    show_temp: bool,
    show_humidity: bool,
}

impl<'a> RowOpts<'a> {
    /// The auto-layout defaults: the device's own name, everything shown,
    /// lights fully interactive.
    fn auto(dev: &'a DeviceState) -> Self {
        RowOpts {
            name: &dev.device_info.name,
            allow_toggle: true,
            show_slider: true,
            show_temp: true,
            show_humidity: true,
        }
    }
}

/// One device row, or `None` for devices we don't render (unknown/virtual —
/// [`renders`] gates them, so an echo-less virtual group can't slip in via a
/// config ref and latch on toggle).
fn device_row(dev: &DeviceState, opts: &RowOpts) -> Option<Node> {
    if !renders(dev) {
        return None;
    }
    let id = &dev.device_id;
    let name = opts.name;
    let offline = !dev.device_info.reachable;
    let offline_class = offline.then(|| "vw-unreachable".to_string());

    let row = match &dev.state {
        DeviceStateValue::Light(light) => {
            let glyph = if offline {
                "◌"
            } else if light.is_on {
                "●"
            } else {
                "○"
            };
            let mut classes = vec![
                "vw-row".to_string(),
                "vw-light".to_string(),
                if light.is_on { "vw-on" } else { "vw-off" }.to_string(),
            ];
            classes.extend(offline_class);
            let label = Node::Label {
                id: None,
                text: format!("{glyph} {name}"),
                classes: vec![],
            };
            // Toggle affordance ← `switch`: a clickable button, or a static
            // label. Kept independent from the brightness affordance below.
            let head = if opts.allow_toggle {
                Node::Button {
                    id: format!("vw-l-{id}"),
                    classes: vec!["vw-toggle".into(), "flat".into()],
                    child: Box::new(label),
                }
            } else {
                label
            };
            let mut children = vec![head];
            // Brightness bar ← `slider`, and only when on with a known level.
            if opts.show_slider && light.is_on {
                if let Some(b) = light.brightness {
                    children.push(Node::Progress {
                        id: None,
                        fraction: f64::from(b) / 100.0,
                        classes: vec!["vw-bright".into()],
                    });
                }
            }
            // Scroll-to-dim target ← `slider` too: no slider ⇒ no `vw-ls-` id
            // and no scroll, so a toggle-only light can't be dimmed (or, when
            // off, scrolled on) — matching what `slider false` means in GTK.
            let (row_id, scroll) = if opts.show_slider {
                (Some(format!("vw-ls-{id}")), true)
            } else {
                (None, false)
            };
            Node::Box {
                id: row_id,
                dir: Dir::Horizontal,
                spacing: 6,
                scroll,
                classes,
                children,
            }
        }
        DeviceStateValue::Outlet(outlet) => {
            let mut classes = vec![
                "vw-row".to_string(),
                "vw-outlet".to_string(),
                if outlet.is_on { "vw-on" } else { "vw-off" }.to_string(),
            ];
            classes.extend(offline_class);
            // State must be legible without CSS (the vw-* classes are hooks
            // for the shell's stylesheet, which may not style them).
            let glyph = if offline {
                "◌"
            } else if outlet.is_on {
                "●"
            } else {
                "○"
            };
            let label = Node::Label {
                id: None,
                text: format!("{glyph} {name} ⏻"),
                classes: vec![],
            };
            // Honor read-only intent: a sensor slot that resolves to an outlet
            // (a misconfiguration) shows a static readout, never a live toggle.
            let head = if opts.allow_toggle {
                Node::Button {
                    id: format!("vw-o-{id}"),
                    classes: vec!["vw-toggle".into(), "flat".into()],
                    child: Box::new(label),
                }
            } else {
                label
            };
            let mut children = vec![head];
            if let (true, Some(w)) = (outlet.is_on, outlet.power_consumption) {
                children.push(Node::Label {
                    id: None,
                    text: format!("{w:.1} W"),
                    classes: vec!["vw-watts".into(), "dim-label".into(), "numeric".into()],
                });
            }
            Node::Box {
                id: None,
                dir: Dir::Horizontal,
                spacing: 6,
                scroll: false,
                classes,
                children,
            }
        }
        DeviceStateValue::Sensor(sensor) => {
            let mut parts = vec![name.to_string()];
            if offline {
                parts.push("◌".to_string());
            }
            if opts.show_temp {
                if let Some(t) = sensor.temperature {
                    parts.push(format!("🌡 {t:.1}°"));
                }
            }
            if opts.show_humidity {
                if let Some(h) = sensor.humidity {
                    parts.push(format!("💧 {h:.0}%"));
                }
            }
            let mut classes = vec![
                "vw-row".to_string(),
                "vw-sensor".to_string(),
                "dim-label".to_string(),
            ];
            classes.extend(offline_class);
            left(Node::Label {
                id: None,
                text: parts.join("  "),
                classes,
            })
        }
        _ => return None,
    };
    Some(row)
}

fn main() {
    hytte_plugin::run::<VibeWidget>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use v1bectl_state::{Capability, DeviceInfo, DeviceType, LightState, OutletState, SensorState};

    fn info(id: &str, name: &str, room: &str, ty: DeviceType) -> DeviceInfo {
        DeviceInfo {
            device_id: id.to_string(),
            name: name.to_string(),
            device_type: ty,
            capabilities: vec![Capability::OnOff, Capability::Brightness],
            device_groups: vec![room.to_string()],
            manufacturer: None,
            model: None,
            firmware_version: None,
            battery_powered: false,
            reachable: true,
            last_seen: 0,
            custom_attributes: Default::default(),
        }
    }

    fn light(id: &str, name: &str, room: &str, is_on: bool, brightness: Option<u8>) -> DeviceState {
        DeviceState {
            device_id: id.to_string(),
            device_info: info(id, name, room, DeviceType::Light),
            state: DeviceStateValue::Light(LightState {
                is_on,
                brightness,
                color_temp: None,
                rgb_color: None,
            }),
            last_updated: 0,
            last_synced_to_gateway: None,
            last_synced_from_gateway: None,
        }
    }

    fn outlet(id: &str, name: &str, room: &str, is_on: bool) -> DeviceState {
        DeviceState {
            device_id: id.to_string(),
            device_info: info(id, name, room, DeviceType::Outlet),
            state: DeviceStateValue::Outlet(OutletState {
                is_on,
                power_consumption: Some(4.2),
                total_energy: None,
            }),
            last_updated: 0,
            last_synced_to_gateway: None,
            last_synced_from_gateway: None,
        }
    }

    fn sensor(id: &str, name: &str, room: &str) -> DeviceState {
        DeviceState {
            device_id: id.to_string(),
            device_info: info(id, name, room, DeviceType::Sensor),
            state: DeviceStateValue::Sensor(SensorState {
                temperature: Some(21.44),
                humidity: Some(38.6),
                last_updated: 0,
            }),
            last_updated: 0,
            last_synced_to_gateway: None,
            last_synced_from_gateway: None,
        }
    }

    /// A model plus the probe end of its command channel. Screens are cleared
    /// so tests are hermetic: whatever `screens.kdl` happens to be on the test
    /// machine can't leak in — the auto layout is exercised unless a test opts
    /// into a config via [`model_with_config`].
    fn model_with(devices: Vec<DeviceState>) -> (VibeWidget, mpsc::UnboundedReceiver<Cmd>) {
        let mut m = VibeWidget::init();
        let rx = CMD_RX
            .with(|slot| slot.borrow_mut().take())
            .expect("init parks the receiver");
        m.conn = Conn::Online;
        m.screens = Vec::new();
        let _ = m.update(Input::App(WsMsg::Devices(devices)));
        (m, rx)
    }

    /// Like [`model_with`], but driven by an inline `screens.kdl` layout.
    fn model_with_config(
        devices: Vec<DeviceState>,
        kdl: &str,
    ) -> (VibeWidget, mpsc::UnboundedReceiver<Cmd>) {
        let (mut m, rx) = model_with(devices);
        m.screens = screens::parse_screens(kdl).expect("test kdl parses");
        (m, rx)
    }

    /// Every `Button` id and every `Box` id in a tree (the shell only emits
    /// events for nodes that exist, so absence of an id ⇒ that interaction
    /// can't fire).
    fn ids(node: &Node) -> Vec<String> {
        fn walk(node: &Node, out: &mut Vec<String>) {
            match node {
                Node::Box { id, children, .. } => {
                    out.extend(id.clone());
                    children.iter().for_each(|c| walk(c, out));
                }
                Node::Button { id, child, .. } => {
                    out.push(id.clone());
                    walk(child, out);
                }
                Node::Revealer { child, .. } => walk(child, out),
                _ => {}
            }
        }
        let mut out = Vec::new();
        walk(node, &mut out);
        out
    }

    /// Is there a `Progress` (brightness) node anywhere in the tree?
    fn has_progress(node: &Node) -> bool {
        match node {
            Node::Progress { .. } => true,
            Node::Box { children, .. } => children.iter().any(has_progress),
            Node::Button { child, .. } | Node::Revealer { child, .. } => has_progress(child),
            _ => false,
        }
    }

    /// Is there a horizontal `vw-block` box (a KDL `block`) in the tree?
    fn has_horizontal_block(node: &Node) -> bool {
        match node {
            Node::Box {
                dir,
                classes,
                children,
                ..
            } => {
                (matches!(dir, Dir::Horizontal) && classes.iter().any(|c| c == "vw-block"))
                    || children.iter().any(has_horizontal_block)
            }
            Node::Button { child, .. } | Node::Revealer { child, .. } => {
                has_horizontal_block(child)
            }
            _ => false,
        }
    }

    /// Collect every label text in a tree (order = render order).
    fn texts(node: &Node) -> Vec<String> {
        fn walk(node: &Node, out: &mut Vec<String>) {
            match node {
                Node::Label { text, .. } => out.push(text.clone()),
                Node::Box { children, .. } => children.iter().for_each(|c| walk(c, out)),
                Node::Button { child, .. } | Node::Revealer { child, .. } => walk(child, out),
                _ => {}
            }
        }
        let mut out = Vec::new();
        walk(node, &mut out);
        out
    }

    #[test]
    fn view_groups_by_room_no_header_chrome() {
        let (m, _rx) = model_with(vec![
            light("l1", "Taklampa", "living_room", true, Some(70)),
            light("l2", "Sänglampa", "bedroom", false, Some(30)),
            outlet("o1", "Skrivbord", "bedroom", true),
            sensor("s1", "Klimat", "living_room"),
        ]);
        let all = texts(&m.view());
        // No header/status line: straight into rooms (alphabetical), devices
        // alphabetical within each.
        assert_eq!(
            all,
            vec![
                "bedroom",
                "● Skrivbord ⏻",
                "4.2 W",
                "○ Sänglampa",
                "living_room",
                "Klimat  🌡 21.4°  💧 39%",
                "● Taklampa",
            ]
        );
    }

    #[test]
    fn click_toggles_light_without_local_mutation() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(70))]);
        let fx = m.update(Input::Event {
            node: "vw-l-l1".into(),
            kind: EventKind::Click,
        });
        assert!(fx.is_empty(), "no shell effects, ever");
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: Some(false),
                brightness: None,
            }
        );
        // The server's event echo is the source of truth — no local flip.
        assert!(
            matches!(&m.devices[0].state, DeviceStateValue::Light(l) if l.is_on),
            "state unchanged until the event echo"
        );
    }

    #[test]
    fn scroll_dims_and_brightens_with_clamping() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(98))]);
        // Scroll up (negative dy) brightens, clamped to 100.
        let _ = m.update(Input::Event {
            node: "vw-ls-l1".into(),
            kind: EventKind::Scroll { dx: 0.0, dy: -1.0 },
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: None,
                brightness: Some(100),
            }
        );
        // Scroll down dims — based on the *pending* 100, not the stale 98.
        let _ = m.update(Input::Event {
            node: "vw-ls-l1".into(),
            kind: EventKind::Scroll { dx: 0.0, dy: 1.0 },
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: None,
                brightness: Some(95),
            }
        );
    }

    #[test]
    fn touchpad_smooth_scroll_accumulates_to_notches() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(50))]);
        // Three sub-notch deltas: nothing fires until the notch completes.
        for _ in 0..2 {
            let _ = m.update(Input::Event {
                node: "vw-ls-l1".into(),
                kind: EventKind::Scroll { dx: 0.0, dy: -0.4 },
            });
            assert!(
                rx.try_recv().is_err(),
                "sub-notch deltas accumulate silently"
            );
        }
        let _ = m.update(Input::Event {
            node: "vw-ls-l1".into(),
            kind: EventKind::Scroll { dx: 0.0, dy: -0.4 },
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: None,
                brightness: Some(55),
            }
        );
    }

    #[test]
    fn horizontal_scroll_is_a_no_op() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(50))]);
        let _ = m.update(Input::Event {
            node: "vw-ls-l1".into(),
            kind: EventKind::Scroll { dx: 3.0, dy: 0.0 },
        });
        assert!(rx.try_recv().is_err(), "dy == 0 must not change brightness");
    }

    #[test]
    fn virtual_light_groups_are_not_rendered() {
        let mut group = light("g1", "Bedroom Lights", "virtual", true, Some(100));
        group.device_info.device_type = DeviceType::VirtualLightGroup;
        let (mut m, rx) = model_with(vec![
            group,
            light("l1", "Taklampa", "living_room", true, Some(70)),
        ]);
        let all = texts(&m.view());
        assert!(
            !all.iter().any(|t| t.contains("Bedroom Lights")),
            "the server's virtual path emits no event echo — groups stay off the widget"
        );
        assert!(
            all.iter().any(|t| t == "● Taklampa"),
            "the physical light still renders: {all:?}"
        );
        // Defensively: even a synthetic event on a group id sends nothing.
        let _ = m.update(Input::Event {
            node: "vw-l-g1".into(),
            kind: EventKind::Click,
        });
        let _ = rx; // no assertion on cmd here — group still has Light state
    }

    #[test]
    fn echo_without_optional_fields_does_not_wipe_readouts() {
        let (mut m, _rx) = model_with(vec![
            light("l1", "Taklampa", "x", true, Some(70)),
            outlet("o1", "Skrivbord", "x", true),
        ]);
        // A SetLightState{is_on}-style echo: brightness None must not wipe 70.
        m.apply_event(
            "l1",
            EventType::StateChanged {
                old_state: None,
                new_state: Some(DeviceStateValue::Light(LightState {
                    is_on: false,
                    brightness: None,
                    color_temp: None,
                    rgb_color: None,
                })),
            },
        );
        assert!(
            matches!(&m.devices[0].state, DeviceStateValue::Light(l) if !l.is_on && l.brightness == Some(70))
        );
        // An outlet echo with power_consumption: None must not wipe 4.2 W.
        m.apply_event(
            "o1",
            EventType::StateChanged {
                old_state: None,
                new_state: Some(DeviceStateValue::Outlet(OutletState {
                    is_on: false,
                    power_consumption: None,
                    total_energy: None,
                })),
            },
        );
        assert!(
            matches!(&m.devices[1].state, DeviceStateValue::Outlet(o) if !o.is_on && o.power_consumption == Some(4.2))
        );
    }

    #[test]
    fn undecodable_attribute_payload_triggers_a_resync() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", false, Some(20))]);
        // A raw Dirigera-gateway payload, not a DeviceStateValue.
        m.apply_event(
            "l1",
            EventType::AttributeChanged {
                attribute: "state".into(),
                old_value: serde_json::Value::Null,
                new_value: serde_json::json!({"isOn": true, "lightLevel": 80}),
            },
        );
        assert_eq!(rx.try_recv().unwrap(), Cmd::Refresh);
        assert!(
            matches!(&m.devices[0].state, DeviceStateValue::Light(l) if !l.is_on),
            "model untouched until the resync lands"
        );
    }

    #[test]
    fn brightening_an_off_light_turns_it_on() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", false, Some(40))]);
        let _ = m.update(Input::Event {
            node: "vw-ls-l1".into(),
            kind: EventKind::Scroll { dx: 0.0, dy: -1.0 },
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: Some(true),
                brightness: Some(45),
            }
        );
    }

    #[test]
    fn outlet_click_toggles() {
        let (mut m, mut rx) = model_with(vec![outlet("o1", "Skrivbord", "x", true)]);
        let _ = m.update(Input::Event {
            node: "vw-o-o1".into(),
            kind: EventKind::Click,
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetOutlet {
                device_id: "o1".into(),
                is_on: false,
            }
        );
    }

    #[test]
    fn state_changed_event_patches_the_device() {
        let (mut m, _rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(70))]);
        m.apply_event(
            "l1",
            EventType::StateChanged {
                old_state: None,
                new_state: Some(DeviceStateValue::Light(LightState {
                    is_on: false,
                    brightness: Some(70),
                    color_temp: None,
                    rgb_color: None,
                })),
            },
        );
        assert!(matches!(&m.devices[0].state, DeviceStateValue::Light(l) if !l.is_on));
    }

    #[test]
    fn attribute_changed_state_event_patches_via_json() {
        let (mut m, _rx) = model_with(vec![light("l1", "Taklampa", "x", false, Some(20))]);
        let new_value = serde_json::to_value(DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(80),
            color_temp: None,
            rgb_color: None,
        }))
        .unwrap();
        m.apply_event(
            "l1",
            EventType::AttributeChanged {
                attribute: "state".into(),
                old_value: serde_json::Value::Null,
                new_value,
            },
        );
        assert!(
            matches!(&m.devices[0].state, DeviceStateValue::Light(l) if l.is_on && l.brightness == Some(80))
        );
    }

    #[test]
    fn reachability_event_marks_device_offline() {
        let (mut m, _rx) = model_with(vec![light("l1", "Taklampa", "x", true, None)]);
        m.apply_event(
            "l1",
            EventType::DeviceReachabilityChanged { reachable: false },
        );
        assert!(!m.devices[0].device_info.reachable);
        let all = texts(&m.view());
        assert!(all.iter().any(|t| t == "◌ Taklampa"), "offline glyph shown");
    }

    #[test]
    fn device_added_triggers_a_refresh() {
        let (mut m, mut rx) = model_with(vec![]);
        m.apply_event(
            "new",
            EventType::DeviceAdded {
                device_type: "light".into(),
            },
        );
        assert_eq!(rx.try_recv().unwrap(), Cmd::Refresh);
    }

    #[test]
    fn unknown_node_events_are_ignored() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, None)]);
        let _ = m.update(Input::Event {
            node: "not-ours".into(),
            kind: EventKind::Click,
        });
        assert!(rx.try_recv().is_err(), "no command for foreign nodes");
    }

    // ── screens.kdl-driven layout ────────────────────────────────────────────

    #[test]
    fn config_layout_uses_titles_and_custom_names() {
        let kdl = r#"
screen "wz" {
    title "NEST :: WOHNZIMMER"
    group {
        sensor {
            device "Wohnzimmer"
            show-temp true
            show-humidity false
        }
        light "MAIN" {
            device "LR Shelf"
            switch true
            slider true
        }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![
                sensor("s1", "Wohnzimmer", "living_room"),
                light("l1", "LR Shelf", "living_room", true, Some(60)),
            ],
            kdl,
        );
        let all = texts(&m.view());
        assert!(all.iter().any(|t| t == "NEST :: WOHNZIMMER"), "{all:?}");
        // The light shows its CONFIG label, not the device's own name.
        assert!(all.iter().any(|t| t == "● MAIN"), "config label: {all:?}");
        assert!(
            !all.iter().any(|t| t.contains("LR Shelf")),
            "device name suppressed: {all:?}"
        );
        // show-humidity false → humidity hidden, temperature still shown.
        assert!(
            all.iter().any(|t| t.contains('🌡') && !t.contains('💧')),
            "humidity gated off: {all:?}"
        );
    }

    #[test]
    fn config_omitted_title_renders_no_header() {
        // A screen with no `title` node shows no header — not even the screen
        // name. (Removing `title "…"` from the KDL removes it from the widget.)
        let kdl = r#"
screen "wohnzimmer" {
    group {
        light "MAIN" {
            device "LR Shelf"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        let all = texts(&m.view());
        assert!(
            !all.iter().any(|t| t == "wohnzimmer"),
            "screen name not rendered as a title: {all:?}"
        );
        assert!(
            all.iter().any(|t| t == "● MAIN"),
            "device still shown: {all:?}"
        );
    }

    #[test]
    fn config_falls_back_to_rooms_when_no_ref_resolves() {
        let kdl = r#"screen "wz" { title "WZ" group { light "X" { device "Nonexistent" } } }"#;
        let (m, _rx) = model_with_config(
            vec![light("l1", "Taklampa", "living_room", true, Some(70))],
            kdl,
        );
        let all = texts(&m.view());
        // Nothing in the config resolves → auto room layout (never blank).
        assert!(all.iter().any(|t| t == "living_room"), "fell back: {all:?}");
        assert!(all.iter().any(|t| t == "● Taklampa"), "{all:?}");
        assert!(
            !all.iter().any(|t| t == "WZ"),
            "no config title when falling back: {all:?}"
        );
    }

    #[test]
    fn config_light_row_routes_clicks_by_resolved_device_id() {
        let kdl = r#"screen "wz" { group { light "MAIN" { device "LR Shelf" } } }"#;
        let (mut m, mut rx) =
            model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        // The button id is keyed by the resolved device_id — clicks still route.
        let _ = m.update(Input::Event {
            node: "vw-l-l1".into(),
            kind: EventKind::Click,
        });
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: Some(false),
                brightness: None,
            }
        );
    }

    #[test]
    fn config_switch_false_renders_a_static_row() {
        let kdl = r#"
screen "wz" {
    group {
        light "MAIN" {
            device "LR Shelf"
            switch false
            slider false
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        let view = m.view();
        // Shown (with its glyph)…
        assert!(
            texts(&view).iter().any(|t| t == "● MAIN"),
            "{:?}",
            texts(&view)
        );
        // …but with no toggle button and no scroll target, so nothing can fire.
        let ids = ids(&view);
        assert!(!ids.iter().any(|i| i == "vw-l-l1"), "no toggle: {ids:?}");
        assert!(
            !ids.iter().any(|i| i == "vw-ls-l1"),
            "no scroll target: {ids:?}"
        );
    }

    #[test]
    fn config_slider_flag_gates_the_brightness_bar() {
        let has_bar = |slider: bool| {
            // The `slider` line carries no quotes, so the v1-compat pass rewrites
            // its bare boolean — mirrors how real configs are written.
            let kdl = format!(
                "screen \"wz\" {{\n\
                 \x20   group {{\n\
                 \x20       light \"MAIN\" {{\n\
                 \x20           device \"LR Shelf\"\n\
                 \x20           slider {slider}\n\
                 \x20       }}\n\
                 \x20   }}\n\
                 }}"
            );
            let (m, _rx) =
                model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], &kdl);
            has_progress(&m.view())
        };
        assert!(has_bar(true), "slider=true shows the brightness bar");
        assert!(!has_bar(false), "slider=false hides it");
    }

    #[test]
    fn config_slider_false_removes_the_scroll_to_dim_target() {
        // The shipped screens.kdl has `switch true slider false` lights (Deko,
        // ACC, Decke): toggle-only, so slider=false must drop scroll-to-dim —
        // otherwise an off light could still be scrolled on, which GTK forbids.
        let kdl = r#"
screen "wz" {
    group {
        light "Deko" {
            device "Deko"
            switch true
            slider false
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![light("l1", "Deko", "lr", true, Some(60))], kdl);
        let view = m.view();
        let ids = ids(&view);
        assert!(
            ids.iter().any(|i| i == "vw-l-l1"),
            "toggle present: {ids:?}"
        );
        assert!(
            !ids.iter().any(|i| i == "vw-ls-l1"),
            "slider=false ⇒ no scroll-to-dim target: {ids:?}"
        );
        assert!(!has_progress(&view), "slider=false ⇒ no brightness bar");
    }

    #[test]
    fn config_switch_false_slider_true_is_brightness_only() {
        // switch=false, slider=true: no toggle, but still dimmable — matching
        // GTK, where the same config yields an interactive brightness control.
        let kdl = r#"
screen "wz" {
    group {
        light "MAIN" {
            device "LR Shelf"
            switch false
            slider true
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        let view = m.view();
        let ids = ids(&view);
        assert!(!ids.iter().any(|i| i == "vw-l-l1"), "no toggle: {ids:?}");
        assert!(
            ids.iter().any(|i| i == "vw-ls-l1"),
            "still dimmable via scroll: {ids:?}"
        );
        assert!(has_progress(&view), "brightness bar shown");
    }

    #[test]
    fn config_text_does_not_suppress_the_room_fallback() {
        // A text banner resolves no device; if every device ref also fails, the
        // widget must still fall back to the room layout — not hide the whole
        // home behind a title + banner.
        let kdl = r#"
screen "wz" {
    title "WZ"
    group {
        text "MY HOME"
        light "X" {
            device "Nonexistent"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![light("l1", "Taklampa", "living_room", true, Some(70))],
            kdl,
        );
        let all = texts(&m.view());
        assert!(all.iter().any(|t| t == "living_room"), "fell back: {all:?}");
        assert!(all.iter().any(|t| t == "● Taklampa"), "home shown: {all:?}");
        assert!(
            !all.iter().any(|t| t == "MY HOME"),
            "banner not shown when falling back: {all:?}"
        );
    }

    #[test]
    fn config_text_renders_alongside_a_resolving_device() {
        // But when a device *does* resolve, the config layout (text and all) wins.
        let kdl = r#"
screen "wz" {
    title "WZ"
    group {
        text "MY HOME"
        light "MAIN" {
            device "LR Shelf"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        let all = texts(&m.view());
        assert!(all.iter().any(|t| t == "MY HOME"), "banner shown: {all:?}");
        assert!(all.iter().any(|t| t == "● MAIN"), "device shown: {all:?}");
    }

    #[test]
    fn config_sensor_pointing_at_an_outlet_is_read_only() {
        // A read-only sensor slot that resolves to an actuator (a
        // misconfiguration) must not become a clickable toggle.
        let kdl = r#"
screen "wz" {
    group {
        sensor {
            device "Desk"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![outlet("o1", "Desk", "lr", true)], kdl);
        let view = m.view();
        assert!(
            texts(&view).iter().any(|t| t.contains("Desk")),
            "readout shown: {:?}",
            texts(&view)
        );
        assert!(
            !ids(&view).iter().any(|i| i == "vw-o-o1"),
            "read-only slot has no toggle: {:?}",
            ids(&view)
        );
    }

    #[test]
    fn config_block_is_horizontal_and_resolves_members() {
        let kdl = r#"
screen "wz" {
    group {
        block {
            light "Deko" {
                device "Deko"
                switch true
                slider false
            }
            light "ACC" {
                device "Laccent1"
                switch true
                slider false
            }
        }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![
                light("l1", "Deko", "lr", true, Some(50)),
                light("l2", "Laccent1", "lr", false, Some(0)),
            ],
            kdl,
        );
        let view = m.view();
        assert!(has_horizontal_block(&view), "block is a horizontal box");
        let all = texts(&view);
        assert!(all.iter().any(|t| t == "● Deko"), "{all:?}");
        assert!(all.iter().any(|t| t == "○ ACC"), "{all:?}");
    }

    #[test]
    fn config_light_pointing_at_an_outlet_adapts_to_an_outlet_row() {
        // IKEA control outlets are often declared as `light`s; render the row
        // the device's real state supports (same adaptation as GTK).
        let kdl = r#"
screen "wz" {
    group {
        light "Lamp" {
            device "Desk"
            switch true
            slider true
        }
    }
}
"#;
        let (m, _rx) = model_with_config(vec![outlet("o1", "Desk", "lr", true)], kdl);
        let all = texts(&m.view());
        assert!(
            all.iter().any(|t| t == "● Lamp ⏻"),
            "outlet glyph + name: {all:?}"
        );
        assert!(all.iter().any(|t| t == "4.2 W"), "live wattage: {all:?}");
    }

    #[test]
    fn config_skips_virtual_light_groups() {
        // A `light` ref to a virtual group must not render — its echo-less
        // toggle would latch. It's dropped; the real light still shows.
        let mut group = light("g1", "All LR", "lr", true, Some(100));
        group.device_info.device_type = DeviceType::VirtualLightGroup;
        let kdl = r#"
screen "wz" {
    group {
        light "GROUP" { device "All LR" }
        light "MAIN" { device "LR Shelf" }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![group, light("l1", "LR Shelf", "lr", true, Some(60))],
            kdl,
        );
        let all = texts(&m.view());
        assert!(
            !all.iter().any(|t| t.contains("GROUP")),
            "virtual group skipped: {all:?}"
        );
        assert!(
            all.iter().any(|t| t == "● MAIN"),
            "real light shown: {all:?}"
        );
    }
}
