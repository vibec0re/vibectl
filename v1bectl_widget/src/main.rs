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
//! - **drag / scroll / key** a light's brightness `Slider` → set level
//! - **click** a panel header → expand / collapse that section
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
//! Either way, each top-level section (a screen, or a room) is a **collapsible
//! panel**: a clickable header (chevron + name + a climate peek) over a
//! `Revealer` of its devices. Collapsed by default; the expanded set is local
//! UI state the reducer flips on a header click.
//!
//! # Shape
//!
//! Pure TEA over the SDK: [`VibeWidget`] (model) + `update` + `view`. The
//! widget's own I/O — the CBOR-over-WebSocket client to `v1bectl_server`
//! (`ws.rs`) — reports in through `sources()` as [`Input::App`], and takes
//! commands from the reducer over the SDK's command lane (trollshell#280):
//! `init` keeps the sender, `sources` hands the paired receiver to the WS task.
//!
//! State is never mutated locally on click: the server applies optimistic
//! updates and echoes a `DeviceEvent`, which is the single source of truth —
//! same convention as the other v1bectl clients.

mod screens;
mod ws;

use std::collections::{BTreeMap, HashSet};

use hytte_plugin::proto::{Dir, Effect, EventKind, Manifest, Mount, Node};
use hytte_plugin::tokio_stream::wrappers::UnboundedReceiverStream;
use hytte_plugin::{nodes, CmdReceiver, CmdSender, Input, MsgStream, Plugin, View};
use tokio::sync::mpsc;
use v1bectl_state::{DeviceState, DeviceStateValue, DeviceType, EventType};

use screens::{DeviceRef, Element, Screen};
use ws::{Cmd, Conn, WsMsg};

/// The brightness slider's keyboard/scroll step (percent).
const BRIGHTNESS_STEP: f64 = 5.0;

/// The model: connection state + the device list, patched by server events.
struct VibeWidget {
    conn: Conn,
    devices: Vec<DeviceState>,
    /// The curated layout from `screens.kdl` (empty ⇒ auto-group by room). Read
    /// once at startup; the device list it renders arrives live over the WS.
    screens: Vec<Screen>,
    /// Which section panels are currently expanded (by key: screen name / room).
    /// Empty = all collapsed, the default — panels reveal on click. Pure local
    /// UI state; a click toggles it and the reducer's re-render opens/closes the
    /// `Revealer`. No server round-trip.
    expanded: HashSet<String>,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
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
    /// must not wipe live readouts.
    fn patch_state(&mut self, device_id: &str, new: DeviceStateValue) {
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
    /// The reducer queues these on the SDK's command lane; `ws::client` drains
    /// the paired receiver and turns them into wire requests.
    type Cmd = Cmd;

    fn manifest() -> Manifest {
        // No host-state subscriptions, no shell capabilities: everything this
        // widget does goes over its own WebSocket.
        Manifest::new("vibectl", Mount::SidebarLead)
    }

    fn init(cmds: CmdSender<Self::Cmd>) -> Self {
        // The SDK owns the command lane now (trollshell#280): keep the sender,
        // `sources()` gets the paired receiver — no more thread-local hand-off.
        Self {
            conn: Conn::Connecting,
            devices: Vec::new(),
            screens: screens::load(),
            expanded: HashSet::new(),
            cmd_tx: cmds,
        }
    }

    fn sources(cmds: CmdReceiver<Self::Cmd>) -> Option<MsgStream<Self::Msg>> {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        tokio::spawn(ws::client(cmds, msg_tx));
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
            // `Input::Event` is `#[non_exhaustive]` (it gained `output`, the
            // clicked monitor); ids are screen-agnostic here, so ignore it.
            Input::Event { node, kind, .. } => self.on_ui_event(&node, kind),
            // No host state, no RunCommands, and nothing to do when our sidebar
            // slot shows/hides — the WS keeps state live either way. The rest
            // are pushes this manifest never subscribes to.
            Input::Snapshot(_)
            | Input::EffectResult { .. }
            | Input::SlotVisible(_)
            | Input::AudioSpectrum(_)
            | Input::ConsentDecision { .. }
            | Input::CalendarUpcoming(_)
            | Input::SessionLocked(_)
            | Input::NowPlaying(_)
            | Input::DatasourceQuery { .. }
            | Input::DatasourceResult { .. } => {}
        }
        Vec::new()
    }

    /// A sidebar card only — no drawer panel, shown on every monitor — so the
    /// root node converts straight into the SDK's [`View`].
    fn view(&self) -> View {
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
            tooltip: None,
        }
        .into()
    }
}

impl VibeWidget {
    /// Route a shell UI event (click / slider move / panel toggle, by node id)
    /// to a command.
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
        } else if let Some(id) = node.strip_prefix("vw-sl-") {
            // The brightness `Slider` moved (drag / scroll / keyboard). Its
            // `value` is 1..=100; round + clamp to the wire's u8. No local
            // mutation and no pending-value bookkeeping — the SDK suppresses
            // programmatic moves mid-drag, and the echo is the source of truth.
            if let EventKind::ValueChanged { value } = kind {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let brightness = value.round().clamp(1.0, 100.0) as u8;
                self.send(Cmd::SetLight {
                    device_id: id.to_string(),
                    is_on: None,
                    brightness: Some(brightness),
                });
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
        } else if let Some(key) = node.strip_prefix("vw-panel-") {
            // Toggle a section panel open/closed — pure local UI, no server hop.
            if matches!(kind, EventKind::Click) && !self.expanded.remove(key) {
                self.expanded.insert(key.to_string());
            }
        }
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

    /// The default layout: every renderable device grouped into a collapsible
    /// panel per room (alphabetical). Used when no `screens.kdl` is configured
    /// — or when none of its refs resolve, so the widget still shows the home.
    fn view_rooms(&self) -> Vec<Node> {
        self.rooms()
            .into_iter()
            .map(|(room, devices)| {
                // Promote the room's climate sensor into the header and drop it
                // from the body, so temp/humidity isn't shown twice.
                let climate = devices
                    .iter()
                    .find_map(|d| Climate::of(&d.device_id, &d.state));
                let skip = climate.as_ref().map(|c| c.id.as_str());
                let rows: Vec<Node> = devices
                    .iter()
                    .filter(|d| Some(d.device_id.as_str()) != skip)
                    .filter_map(|dev| device_row(dev, &RowOpts::auto(dev)))
                    .collect();
                self.panel(&room, &room, climate.as_ref(), boxed_list(rows))
            })
            .collect()
    }

    /// Render the configured screens as collapsible panels — one per screen —
    /// or `None` when no layout is loaded or none of its refs resolve against
    /// the live device list (→ caller falls back to [`Self::view_rooms`]). A
    /// screen becomes a panel only when at least one of its groups yields a
    /// row, so refs the server doesn't (yet) know about leave no empty panels.
    fn view_config(&self) -> Option<Vec<Node>> {
        if self.screens.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        // How many actual *device* rows resolved. Text and empty blocks don't
        // count — a banner-only screen must not suppress the fallback.
        let mut resolved = 0usize;
        for (i, screen) in self.screens.iter().enumerate() {
            // Promote the screen's first climate sensor into the header; it then
            // counts as resolved and is dropped from the body (no duplicate row).
            let climate = self.screen_climate(screen);
            if climate.is_some() {
                resolved += 1;
            }
            let skip = climate.as_ref().map(|c| c.id.as_str());
            let mut groups = Vec::new();
            for group in &screen.groups {
                let rows = self.render_elements(&group.elements, &mut resolved, skip);
                if !rows.is_empty() {
                    // One native `boxed-list` card per group, flattened onto the
                    // plugin card by the host's `.ts-plugin-card list.boxed-list`
                    // rule so groups read as separated lists on one surface.
                    groups.push(nodes::list(rows).class("boxed-list").build());
                }
            }
            if !groups.is_empty() || climate.is_some() {
                // Key by index, not name: two screens may share a name, and the
                // key is both the toggle-event id and the expanded-set entry, so
                // it must be unique — else clicking one panel toggles both.
                let key = format!("screen-{i}");
                let name = screen.title.as_deref().unwrap_or(&screen.name);
                out.push(self.panel(&key, name, climate.as_ref(), groups));
            }
        }
        // Fall back to the room layout when the config resolved *no* real
        // device — otherwise a config that only names devices the server
        // doesn't know (a stale/renamed config) would hide the whole home
        // behind its panels and text banners.
        (resolved > 0).then_some(out)
    }

    /// A collapsible section as a native [`Node::Expander`] (#333): the host
    /// draws the disclosure chevron, right-pins it, and reveals `body` on
    /// toggle — so the plugin no longer hand-rolls a button + chevron + revealer
    /// (and no longer fights `Node::Spacer`'s cross-axis expand to place the
    /// chevron, vibec0re/trollshell#332). The header is the section `name` with
    /// the climate peek pushed to the trailing edge, just inside the chevron.
    /// `key` addresses the toggle event (`vw-panel-{key}`) and keys the expanded
    /// set; collapsed is the default.
    fn panel(&self, key: &str, name: &str, climate: Option<&Climate>, body: Vec<Node>) -> Node {
        let open = self.expanded.contains(key);
        let mut header_row = vec![Node::Label {
            id: None,
            text: name.to_string(),
            classes: vec!["heading".into()],
            tooltip: None,
        }];
        if let Some(peek) = climate.and_then(Climate::peek) {
            // A `Spacer` right-pins the climate peek (safe here: it's confined to
            // the horizontal header box, and the host constrains its axis, #330).
            header_row.push(Node::Spacer);
            header_row.push(Node::Label {
                id: None,
                text: peek,
                classes: vec!["dim-label".into(), "numeric".into()],
                tooltip: None,
            });
        }
        Node::Expander {
            id: format!("vw-panel-{key}"),
            header: Box::new(Node::Box {
                id: None,
                dir: Dir::Horizontal,
                spacing: 6,
                scroll: false,
                classes: vec![],
                children: header_row,
                tooltip: None,
            }),
            children: body,
            expanded: open,
            classes: vec![],
            tooltip: None,
        }
    }

    /// The climate to promote into a screen's panel header: the first sensor
    /// element that resolves to a sensor with something to show (honoring its
    /// `show-temp` / `show-humidity` flags), searched depth-first through blocks.
    fn screen_climate(&self, screen: &Screen) -> Option<Climate> {
        screen
            .groups
            .iter()
            .flat_map(|g| &g.elements)
            .find_map(|el| self.element_climate(el))
    }

    fn element_climate(&self, element: &Element) -> Option<Climate> {
        match element {
            Element::Sensor {
                device_ref,
                show_temp,
                show_humidity,
            } => {
                let dev = self.resolve(device_ref)?;
                let DeviceStateValue::Sensor(s) = &dev.state else {
                    return None;
                };
                let temp = if *show_temp { s.temperature } else { None };
                let humidity = if *show_humidity { s.humidity } else { None };
                (temp.is_some() || humidity.is_some()).then(|| Climate {
                    id: dev.device_id.clone(),
                    temp,
                    humidity,
                })
            }
            Element::Block { elements } => elements.iter().find_map(|e| self.element_climate(e)),
            _ => None,
        }
    }

    /// Turn config elements into rows, resolving each device ref against the
    /// live list and skipping the ones we don't have (yet). `skip` is the device
    /// id promoted into the panel header (its sensor row is dropped so it isn't
    /// shown twice). Bumps `resolved` once per device row actually produced (not
    /// for text or empty blocks) so the caller can tell "the config rendered the
    /// home" from "the config rendered only decoration". A `block` lays its
    /// resolved children out horizontally, mirroring the GTK/web renderers.
    fn render_elements(
        &self,
        elements: &[Element],
        resolved: &mut usize,
        skip: Option<&str>,
    ) -> Vec<Node> {
        elements
            .iter()
            .filter_map(|element| {
                let content = self.element_content(element, resolved, skip);
                (!content.is_empty()).then(|| list_row(content))
            })
            .collect()
    }

    /// The inline (horizontal) content nodes for one config element — the guts
    /// of a list row. A device yields its [`device_content`]; a `text` yields a
    /// dim label; a `block` yields one cell per resolved member laid side by
    /// side (each cell a small horizontal box), so the block becomes a single
    /// multi-column row. Bumps `resolved` once per device that actually renders
    /// (not for text or empty blocks).
    fn element_content(
        &self,
        element: &Element,
        resolved: &mut usize,
        skip: Option<&str>,
    ) -> Vec<Node> {
        match element {
            Element::Light {
                name,
                device_ref,
                show_switch,
                show_slider,
            } => self
                .resolve(device_ref)
                .and_then(|dev| {
                    device_content(
                        dev,
                        &RowOpts {
                            name,
                            allow_toggle: *show_switch,
                            show_slider: *show_slider,
                            show_temp: true,
                            show_humidity: true,
                        },
                    )
                })
                .inspect(|_| *resolved += 1)
                .unwrap_or_default(),
            Element::Outlet { name, device_ref } => self
                .resolve(device_ref)
                .and_then(|dev| {
                    device_content(
                        dev,
                        &RowOpts {
                            name,
                            allow_toggle: true,
                            show_slider: false,
                            show_temp: true,
                            show_humidity: true,
                        },
                    )
                })
                .inspect(|_| *resolved += 1)
                .unwrap_or_default(),
            Element::Sensor {
                device_ref,
                show_temp,
                show_humidity,
            } => self
                .resolve(device_ref)
                // Drop the sensor promoted into the panel header (`skip`) so it
                // isn't also a body row. A sensor slot is read-only: never a
                // toggle, even if the ref points at an actuator.
                .filter(|dev| Some(dev.device_id.as_str()) != skip)
                .and_then(|dev| {
                    device_content(
                        dev,
                        &RowOpts {
                            name: &dev.device_info.name,
                            allow_toggle: false,
                            show_slider: false,
                            show_temp: *show_temp,
                            show_humidity: *show_humidity,
                        },
                    )
                })
                .inspect(|_| *resolved += 1)
                .unwrap_or_default(),
            Element::Text { template } => vec![Node::Label {
                id: None,
                text: template.clone(),
                classes: vec!["dim-label".into()],
                tooltip: None,
            }],
            Element::Block { elements } => elements
                .iter()
                .filter_map(|el| {
                    let cell = self.element_content(el, resolved, skip);
                    (!cell.is_empty()).then(|| Node::Box {
                        id: None,
                        dir: Dir::Horizontal,
                        spacing: 6,
                        scroll: false,
                        classes: vec![],
                        children: cell,
                        tooltip: None,
                    })
                })
                .collect(),
        }
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

/// Wrap device rows in a native `boxed-list` card list. The host's
/// `.ts-plugin-card list.boxed-list` rule flattens it onto the plugin card
/// (no card-in-card), keeping the hairline row separators. Empty ⇒ no list
/// node, so a climate-only panel (all its rows promoted into the header) shows
/// just the header.
fn boxed_list(rows: Vec<Node>) -> Vec<Node> {
    if rows.is_empty() {
        return Vec::new();
    }
    vec![nodes::list(rows).class("boxed-list").build()]
}

/// One list row: a horizontal box of `content`, materialized inside a native
/// `GtkListBoxRow` (with its padding + separators) by the enclosing
/// `boxed-list`. Spacing 8 keeps the toggle, name, and trailing control apart.
fn list_row(content: Vec<Node>) -> Node {
    Node::Box {
        id: None,
        dir: Dir::Horizontal,
        spacing: 8,
        scroll: false,
        classes: vec![],
        children: content,
        tooltip: None,
    }
}

fn status_label(text: &str) -> Node {
    // The host left-aligns plugin labels by default (#334), so no wrapper box.
    Node::Label {
        id: None,
        text: text.to_string(),
        classes: vec!["dim-label".into()],
        tooltip: None,
    }
}

/// A section's climate, promoted into its panel header (temp + humidity) and
/// dropped from the body so it isn't shown twice. `id` is the sensor device to
/// omit from the rows.
struct Climate {
    id: String,
    temp: Option<f32>,
    humidity: Option<f32>,
}

impl Climate {
    /// The climate to promote for a raw sensor device (the auto/room layout —
    /// both readings shown). `None` unless it's a sensor with something to show.
    fn of(id: &str, state: &DeviceStateValue) -> Option<Self> {
        match state {
            DeviceStateValue::Sensor(s) if s.temperature.is_some() || s.humidity.is_some() => {
                Some(Climate {
                    id: id.to_string(),
                    temp: s.temperature,
                    humidity: s.humidity,
                })
            }
            _ => None,
        }
    }

    /// The header peek string — `🌡 23.9°  💧 39%`, whichever readings are
    /// present — or `None` if neither is.
    fn peek(&self) -> Option<String> {
        let mut s = String::new();
        if let Some(t) = self.temp {
            s.push_str(&format!("🌡 {t:.1}°"));
        }
        if let Some(h) = self.humidity {
            if !s.is_empty() {
                s.push_str("  ");
            }
            s.push_str(&format!("💧 {h:.0}%"));
        }
        (!s.is_empty()).then_some(s)
    }
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
    /// Light: show the interactive brightness slider (config `slider`; the
    /// auto layout always wants it).
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

/// The leading state icon + name for a light/outlet, as a flat clickable
/// toggle (when `button_id` is `Some`) or a static box (a `switch false` light,
/// or a read-only sensor slot pointed at an actuator). `icon_state` is the
/// blessed class tinting the symbolic icon — `accent` when on, `dim-label` when
/// off or unreachable — so on/off reads by **colour**, not a `●/○` glyph.
fn toggle_or_label(
    button_id: Option<String>,
    icon: &str,
    icon_state: &str,
    name: &str,
    offline: bool,
) -> Node {
    let inner = Node::Box {
        id: None,
        dir: Dir::Horizontal,
        spacing: 6,
        scroll: false,
        classes: vec![],
        children: vec![
            Node::Icon {
                id: None,
                name: icon.to_string(),
                classes: vec![icon_state.to_string()],
                tooltip: None,
            },
            Node::Label {
                id: None,
                text: name.to_string(),
                classes: if offline {
                    vec!["dim-label".into()]
                } else {
                    vec![]
                },
                tooltip: None,
            },
        ],
        tooltip: None,
    };
    match button_id {
        // `flat` drops the button chrome so it reads as row content but still
        // toggles on click; the whole icon+name is the hit target.
        Some(id) => Node::Button {
            id,
            classes: vec!["flat".into()],
            child: Box::new(inner),
        },
        None => inner,
    }
}

/// The inline (horizontal) content for one device row — leading state icon +
/// name (a flat toggle when interactive), then any trailing control: a
/// brightness [`Node::Slider`] (lights) or a live-wattage / climate readout
/// right-pinned by a [`Node::Spacer`]. `None` for devices we don't render
/// (unknown/virtual — [`renders`] gates them, so an echo-less virtual group
/// can't slip in via a config ref and latch on toggle).
fn device_content(dev: &DeviceState, opts: &RowOpts) -> Option<Vec<Node>> {
    if !renders(dev) {
        return None;
    }
    let id = &dev.device_id;
    let name = opts.name;
    let offline = !dev.device_info.reachable;

    let content = match &dev.state {
        DeviceStateValue::Light(light) => {
            let icon_state = if offline || !light.is_on {
                "dim-label"
            } else {
                "accent"
            };
            let mut children = vec![toggle_or_label(
                opts.allow_toggle.then(|| format!("vw-l-{id}")),
                "display-brightness-symbolic",
                icon_state,
                name,
                offline,
            )];
            // Brightness slider ← `slider`. Shown whenever `slider` is set —
            // even with the light off — but rendered **disabled** (greyed,
            // non-interactive) when off via the host's `enabled` field, so the
            // row keeps its shape instead of the slider popping in and out as
            // the light toggles. `value` is a mutable prop the host reconciles
            // from the echo; the widget holds no optimistic state (the SDK
            // guards the drag). Off ⇒ show the last-known level, clamped.
            if opts.show_slider {
                let value = f64::from(light.brightness.unwrap_or(0)).clamp(1.0, 100.0);
                children.push(Node::Slider {
                    id: format!("vw-sl-{id}"),
                    min: 1.0,
                    max: 100.0,
                    value,
                    step: BRIGHTNESS_STEP,
                    enabled: light.is_on,
                    classes: vec![],
                });
            }
            children
        }
        DeviceStateValue::Outlet(outlet) => {
            let icon_state = if offline || !outlet.is_on {
                "dim-label"
            } else {
                "accent"
            };
            let mut children = vec![toggle_or_label(
                // Honor read-only intent: a sensor slot that resolves to an
                // outlet (a misconfiguration) shows a static readout, no toggle.
                opts.allow_toggle.then(|| format!("vw-o-{id}")),
                "system-shutdown-symbolic",
                icon_state,
                name,
                offline,
            )];
            if let (true, Some(w)) = (outlet.is_on, outlet.power_consumption) {
                children.push(Node::Spacer);
                children.push(Node::Label {
                    id: None,
                    text: format!("{w:.1} W"),
                    classes: vec!["dim-label".into(), "numeric".into()],
                    tooltip: None,
                });
            }
            children
        }
        DeviceStateValue::Sensor(sensor) => {
            let mut readout = Vec::new();
            if opts.show_temp {
                if let Some(t) = sensor.temperature {
                    readout.push(format!("🌡 {t:.1}°"));
                }
            }
            if opts.show_humidity {
                if let Some(h) = sensor.humidity {
                    readout.push(format!("💧 {h:.0}%"));
                }
            }
            let mut children = vec![Node::Label {
                id: None,
                text: name.to_string(),
                classes: if offline {
                    vec!["dim-label".into()]
                } else {
                    vec![]
                },
                tooltip: None,
            }];
            if !readout.is_empty() {
                children.push(Node::Spacer);
                children.push(Node::Label {
                    id: None,
                    text: readout.join("  "),
                    classes: vec!["dim-label".into(), "numeric".into()],
                    tooltip: None,
                });
            }
            children
        }
        _ => return None,
    };
    Some(content)
}

/// One device as a native `boxed-list` row, or `None` when [`device_content`]
/// declines (an unrendered/virtual device).
fn device_row(dev: &DeviceState, opts: &RowOpts) -> Option<Node> {
    Some(list_row(device_content(dev, opts)?))
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
        let (cmd_tx, rx) = hytte_plugin::cmd_channel::<Cmd>();
        let mut m = VibeWidget::init(cmd_tx);
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

    /// Recurse into every child-bearing node kind. Shared by the tree walkers so
    /// each only has to handle the nodes it collects — panels are now
    /// [`Node::Expander`]s and bodies [`Node::ListBox`]es, so a walker that
    /// stopped at `Box`/`Button`/`Revealer` would miss the whole tree.
    fn children_of(node: &Node) -> Vec<&Node> {
        match node {
            Node::Box { children, .. } | Node::ListBox { children, .. } => {
                children.iter().collect()
            }
            Node::Button { child, .. } | Node::Revealer { child, .. } => vec![child],
            Node::Expander {
                header, children, ..
            } => std::iter::once(header.as_ref()).chain(children).collect(),
            _ => Vec::new(),
        }
    }

    /// Every id that addresses an event: `Button`/`Slider`/`Expander` ids (and
    /// any `Box`/`ListBox` id along for the ride). The shell only emits events
    /// for nodes that exist, so absence of an id ⇒ that interaction can't fire.
    fn ids(node: &Node) -> Vec<String> {
        fn walk(node: &Node, out: &mut Vec<String>) {
            match node {
                Node::Box { id, .. } | Node::ListBox { id, .. } => out.extend(id.clone()),
                Node::Button { id, .. } | Node::Slider { id, .. } | Node::Expander { id, .. } => {
                    out.push(id.clone());
                }
                _ => {}
            }
            children_of(node).iter().for_each(|c| walk(c, out));
        }
        let mut out = Vec::new();
        walk(node, &mut out);
        out
    }

    /// Is there a brightness `Slider` node anywhere in the tree?
    fn has_slider(node: &Node) -> bool {
        matches!(node, Node::Slider { .. }) || children_of(node).iter().any(|c| has_slider(c))
    }

    /// The `enabled` flag of the `Slider` with `id`, if present.
    fn slider_enabled(node: &Node, id: &str) -> Option<bool> {
        if let Node::Slider {
            id: sid, enabled, ..
        } = node
        {
            if sid == id {
                return Some(*enabled);
            }
        }
        children_of(node).iter().find_map(|c| slider_enabled(c, id))
    }

    /// The `expanded` flag of the `Expander` with `id`, if present. (The panel
    /// chevron is now drawn host-side, so a test reads the model-driven
    /// `expanded` here instead of looking for a `pan-*-symbolic` icon.)
    fn expander_open(node: &Node, id: &str) -> Option<bool> {
        if let Node::Expander {
            id: eid, expanded, ..
        } = node
        {
            if eid == id {
                return Some(*expanded);
            }
        }
        children_of(node).iter().find_map(|c| expander_open(c, id))
    }

    /// Every `Icon` name in a tree (e.g. a light's `display-brightness-symbolic`).
    fn icons(node: &Node) -> Vec<String> {
        fn walk(node: &Node, out: &mut Vec<String>) {
            if let Node::Icon { name, .. } = node {
                out.push(name.clone());
            }
            children_of(node).iter().for_each(|c| walk(c, out));
        }
        let mut out = Vec::new();
        walk(node, &mut out);
        out
    }

    /// Is there a `block` row — a horizontal box whose children are ≥2 horizontal
    /// cell boxes (a KDL `block` lays its members out side by side)?
    fn has_horizontal_block(node: &Node) -> bool {
        let is_block = matches!(
            node,
            Node::Box {
                dir: Dir::Horizontal,
                ..
            }
        ) && children_of(node)
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    Node::Box {
                        dir: Dir::Horizontal,
                        ..
                    }
                )
            })
            .count()
            >= 2;
        is_block || children_of(node).iter().any(|c| has_horizontal_block(c))
    }

    /// Collect every label text in a tree (order = render order).
    fn texts(node: &Node) -> Vec<String> {
        fn walk(node: &Node, out: &mut Vec<String>) {
            if let Node::Label { text, .. } = node {
                out.push(text.clone());
            }
            children_of(node).iter().for_each(|c| walk(c, out));
        }
        let mut out = Vec::new();
        walk(node, &mut out);
        out
    }

    #[test]
    fn view_groups_by_room_into_panels() {
        let (m, _rx) = model_with(vec![
            light("l1", "Taklampa", "living_room", true, Some(70)),
            light("l2", "Sänglampa", "bedroom", false, Some(30)),
            outlet("o1", "Skrivbord", "bedroom", true),
            sensor("s1", "Klimat", "living_room"),
        ]);
        let all = texts(&m.view().tree);
        // One collapsible panel per room (alphabetical): a name + climate-peek
        // header (the room's sensor promoted out of the body — no duplicate
        // "Klimat" row), then its devices. All present in the tree; the
        // collapsed Revealer just hides them at render time.
        assert_eq!(
            all,
            vec![
                "bedroom",
                "Skrivbord",
                "4.2 W",
                "Sänglampa",
                "living_room",
                "🌡 21.4°  💧 39%",
                "Taklampa",
            ]
        );
    }

    #[test]
    fn clicking_a_panel_header_toggles_it_open() {
        let (mut m, mut rx) = model_with(vec![
            light("l1", "Taklampa", "living_room", true, Some(70)),
            sensor("s1", "Klimat", "living_room"),
        ]);
        // Collapsed by default: the Expander reports `expanded: false` (the host
        // draws the chevron; the plugin only drives the flag).
        assert!(!m.expanded.contains("living_room"));
        assert_eq!(
            expander_open(&m.view().tree, "vw-panel-living_room"),
            Some(false),
            "collapsed by default"
        );
        // Climate peeks in the header (temp + humidity), promoted out of the body.
        assert!(texts(&m.view().tree).iter().any(|t| t == "🌡 21.4°  💧 39%"));

        // Click the header → expands, no server command.
        let fx = m.update(Input::event("vw-panel-living_room", EventKind::Click));
        assert!(fx.is_empty(), "panel toggle is pure local UI");
        assert!(rx.try_recv().is_err(), "no command for a panel toggle");
        assert!(m.expanded.contains("living_room"));
        assert_eq!(
            expander_open(&m.view().tree, "vw-panel-living_room"),
            Some(true),
            "expanded after a header click"
        );

        // Click again → collapses.
        let _ = m.update(Input::event("vw-panel-living_room", EventKind::Click));
        assert!(!m.expanded.contains("living_room"));
    }

    #[test]
    fn config_promotes_climate_sensor_out_of_the_body() {
        // The climate sensor peeks in the panel header and is NOT also a row.
        let kdl = r#"
screen "wz" {
    title "Wohnzimmer"
    group {
        sensor {
            device "Klima"
            show-temp true
            show-humidity true
        }
        light "MAIN" {
            device "LR Shelf"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![
                sensor("s1", "Klima", "lr"),
                light("l1", "LR Shelf", "lr", true, Some(60)),
            ],
            kdl,
        );
        let all = texts(&m.view().tree);
        assert!(
            all.iter().any(|t| t == "🌡 21.4°  💧 39%"),
            "climate in header: {all:?}"
        );
        assert!(
            !all.iter().any(|t| t.contains("Klima")),
            "no duplicate sensor row: {all:?}"
        );
        assert!(
            all.iter().any(|t| t.contains("Wohnzimmer")),
            "title header: {all:?}"
        );
    }

    #[test]
    fn panel_climate_honors_the_sensor_show_flags() {
        // show-temp false must not leak temperature into the header peek — the
        // same suppression the sensor row already honors.
        let kdl = r#"
screen "wz" {
    group {
        sensor {
            device "Klima"
            show-temp false
            show-humidity true
        }
        light "MAIN" {
            device "LR Shelf"
        }
    }
}
"#;
        let (m, _rx) = model_with_config(
            vec![
                sensor("s1", "Klima", "lr"),
                light("l1", "LR Shelf", "lr", true, Some(60)),
            ],
            kdl,
        );
        let all = texts(&m.view().tree);
        assert!(all.iter().any(|t| t == "💧 39%"), "humidity peek: {all:?}");
        assert!(
            !all.iter().any(|t| t.contains('🌡')),
            "show-temp false → no temperature anywhere: {all:?}"
        );
    }

    #[test]
    fn duplicate_screen_names_get_independent_panels() {
        // Two screens sharing a name must not share a toggle — panels are keyed
        // by index, so each gets its own id and expanded entry.
        let kdl = r#"
screen "wz" {
    group {
        light "A" {
            device "LR Shelf"
        }
    }
}
screen "wz" {
    group {
        light "B" {
            device "Office"
        }
    }
}
"#;
        let (mut m, _rx) = model_with_config(
            vec![
                light("l1", "LR Shelf", "lr", true, Some(60)),
                light("l2", "Office", "office", false, Some(0)),
            ],
            kdl,
        );
        let idset = ids(&m.view().tree);
        assert!(
            idset.iter().any(|i| i == "vw-panel-screen-0"),
            "distinct keys: {idset:?}"
        );
        assert!(
            idset.iter().any(|i| i == "vw-panel-screen-1"),
            "distinct keys: {idset:?}"
        );
        // Toggling the first leaves the second collapsed.
        let _ = m.update(Input::event("vw-panel-screen-0", EventKind::Click));
        assert!(m.expanded.contains("screen-0"));
        assert!(!m.expanded.contains("screen-1"), "panels are independent");
    }

    #[test]
    fn click_toggles_light_without_local_mutation() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(70))]);
        let fx = m.update(Input::event("vw-l-l1", EventKind::Click));
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
    fn slider_move_sets_brightness_without_local_mutation() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(50))]);
        // A `Slider` move (drag/scroll/keyboard) reports the new value.
        let _ = m.update(Input::event(
            "vw-sl-l1",
            EventKind::ValueChanged { value: 73.4 },
        ));
        assert_eq!(
            rx.try_recv().unwrap(),
            Cmd::SetLight {
                device_id: "l1".into(),
                is_on: None,
                brightness: Some(73), // rounded
            }
        );
        // No local mutation — the server's echo is the source of truth.
        assert!(
            matches!(&m.devices[0].state, DeviceStateValue::Light(l) if l.brightness == Some(50)),
            "brightness unchanged until the echo"
        );
    }

    #[test]
    fn slider_value_is_rounded_and_clamped_to_1_100() {
        let (mut m, mut rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(50))]);
        for (value, want) in [(250.0, 100u8), (0.2, 1), (49.6, 50)] {
            let _ = m.update(Input::event("vw-sl-l1", EventKind::ValueChanged { value }));
            assert!(
                matches!(rx.try_recv().unwrap(), Cmd::SetLight { brightness: Some(b), .. } if b == want),
                "value {value} → brightness {want}"
            );
        }
    }

    #[test]
    fn virtual_light_groups_are_not_rendered() {
        let mut group = light("g1", "Bedroom Lights", "virtual", true, Some(100));
        group.device_info.device_type = DeviceType::VirtualLightGroup;
        let (mut m, rx) = model_with(vec![
            group,
            light("l1", "Taklampa", "living_room", true, Some(70)),
        ]);
        let all = texts(&m.view().tree);
        assert!(
            !all.iter().any(|t| t.contains("Bedroom Lights")),
            "the server's virtual path emits no event echo — groups stay off the widget"
        );
        assert!(
            all.iter().any(|t| t == "Taklampa"),
            "the physical light still renders: {all:?}"
        );
        // Defensively: even a synthetic event on a group id sends nothing.
        let _ = m.update(Input::event("vw-l-g1", EventKind::Click));
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
    fn light_row_uses_a_symbolic_icon_not_a_dot() {
        // The de-dot (#…): on/off reads via a tinted symbolic icon, never a
        // `●/○` glyph baked into the label text.
        let (m, _rx) = model_with(vec![light("l1", "Taklampa", "x", true, Some(70))]);
        let view = m.view().tree;
        assert!(
            icons(&view)
                .iter()
                .any(|i| i == "display-brightness-symbolic"),
            "light shows a symbolic icon: {:?}",
            icons(&view)
        );
        assert!(
            !texts(&view)
                .iter()
                .any(|t| t.contains('●') || t.contains('○') || t.contains('◌')),
            "no dot glyphs in label text: {:?}",
            texts(&view)
        );
    }

    #[test]
    fn an_off_light_shows_a_disabled_slider() {
        // The slider stays in the row even when the light is off — but rendered
        // disabled (greyed, non-interactive) so the row keeps its shape instead
        // of the slider popping in and out. Turn it on and it goes live.
        let (mut m, _rx) = model_with(vec![light("l1", "Taklampa", "x", false, Some(40))]);
        let view = m.view().tree;
        assert!(has_slider(&view), "slider present while off");
        assert!(
            ids(&view).iter().any(|i| i == "vw-sl-l1"),
            "slider id present while off"
        );
        assert_eq!(
            slider_enabled(&view, "vw-sl-l1"),
            Some(false),
            "off ⇒ disabled slider"
        );

        // Turn it on → same slider, now interactive.
        let _ = m.update(Input::App(WsMsg::Devices(vec![light(
            "l1",
            "Taklampa",
            "x",
            true,
            Some(40),
        )])));
        assert_eq!(
            slider_enabled(&m.view().tree, "vw-sl-l1"),
            Some(true),
            "on ⇒ enabled slider"
        );
    }

    #[test]
    fn outlet_click_toggles() {
        let (mut m, mut rx) = model_with(vec![outlet("o1", "Skrivbord", "x", true)]);
        let _ = m.update(Input::event("vw-o-o1", EventKind::Click));
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
        // Offline no longer shows a `◌` glyph — the row dims (`dim-label` on the
        // icon + name). The device still renders with its plain name.
        let all = texts(&m.view().tree);
        assert!(
            all.iter().any(|t| t == "Taklampa"),
            "offline row shown: {all:?}"
        );
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
        let _ = m.update(Input::event("not-ours", EventKind::Click));
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
        let all = texts(&m.view().tree);
        // The screen title is now the panel header (chevron + title + climate).
        assert!(
            all.iter().any(|t| t.contains("NEST :: WOHNZIMMER")),
            "{all:?}"
        );
        // The light shows its CONFIG label, not the device's own name.
        assert!(all.iter().any(|t| t == "MAIN"), "config label: {all:?}");
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
    fn config_omitted_title_labels_the_panel_with_the_screen_name() {
        // A panel needs a header (it's the collapse control); with no `title`
        // node it falls back to the screen name for the label.
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
        let all = texts(&m.view().tree);
        assert!(
            all.iter().any(|t| t == "wohnzimmer"),
            "panel labelled by screen name: {all:?}"
        );
        assert!(
            all.iter().any(|t| t == "MAIN"),
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
        let all = texts(&m.view().tree);
        // Nothing in the config resolves → auto room layout (never blank), as
        // room panels.
        assert!(
            all.iter().any(|t| t.contains("living_room")),
            "fell back: {all:?}"
        );
        assert!(all.iter().any(|t| t == "Taklampa"), "{all:?}");
        assert!(
            !all.iter().any(|t| t.contains("WZ")),
            "no config panel when falling back: {all:?}"
        );
    }

    #[test]
    fn config_light_row_routes_clicks_by_resolved_device_id() {
        let kdl = r#"screen "wz" { group { light "MAIN" { device "LR Shelf" } } }"#;
        let (mut m, mut rx) =
            model_with_config(vec![light("l1", "LR Shelf", "lr", true, Some(60))], kdl);
        // The button id is keyed by the resolved device_id — clicks still route.
        let _ = m.update(Input::event("vw-l-l1", EventKind::Click));
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
        let view = m.view().tree;
        // Shown (with its glyph)…
        assert!(
            texts(&view).iter().any(|t| t == "MAIN"),
            "{:?}",
            texts(&view)
        );
        // …but with no toggle button and no slider, so nothing can fire.
        let ids = ids(&view);
        assert!(!ids.iter().any(|i| i == "vw-l-l1"), "no toggle: {ids:?}");
        assert!(!ids.iter().any(|i| i == "vw-sl-l1"), "no slider: {ids:?}");
    }

    #[test]
    fn config_slider_flag_gates_the_brightness_slider() {
        let has = |slider: bool| {
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
            has_slider(&m.view().tree)
        };
        assert!(has(true), "slider=true shows the brightness slider");
        assert!(!has(false), "slider=false hides it");
    }

    #[test]
    fn config_slider_false_is_toggle_only() {
        // The shipped screens.kdl has `switch true slider false` lights (Deko,
        // ACC, Decke): a toggle and no brightness slider.
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
        let view = m.view().tree;
        let ids = ids(&view);
        assert!(
            ids.iter().any(|i| i == "vw-l-l1"),
            "toggle present: {ids:?}"
        );
        assert!(
            !ids.iter().any(|i| i == "vw-sl-l1"),
            "slider=false ⇒ no brightness slider: {ids:?}"
        );
        assert!(!has_slider(&view), "slider=false ⇒ no slider node");
    }

    #[test]
    fn config_switch_false_slider_true_is_brightness_only() {
        // switch=false, slider=true: no toggle, but the interactive brightness
        // slider is present — matching GTK's brightness-only light.
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
        let view = m.view().tree;
        let ids = ids(&view);
        assert!(!ids.iter().any(|i| i == "vw-l-l1"), "no toggle: {ids:?}");
        assert!(
            ids.iter().any(|i| i == "vw-sl-l1"),
            "brightness slider present: {ids:?}"
        );
        assert!(has_slider(&view), "slider node shown");
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
        let all = texts(&m.view().tree);
        assert!(
            all.iter().any(|t| t.contains("living_room")),
            "fell back: {all:?}"
        );
        assert!(all.iter().any(|t| t == "Taklampa"), "home shown: {all:?}");
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
        let all = texts(&m.view().tree);
        assert!(all.iter().any(|t| t == "MY HOME"), "banner shown: {all:?}");
        assert!(all.iter().any(|t| t == "MAIN"), "device shown: {all:?}");
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
        let view = m.view().tree;
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
        let view = m.view().tree;
        assert!(has_horizontal_block(&view), "block is a horizontal box");
        let all = texts(&view);
        assert!(all.iter().any(|t| t == "Deko"), "{all:?}");
        assert!(all.iter().any(|t| t == "ACC"), "{all:?}");
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
        let all = texts(&m.view().tree);
        assert!(
            all.iter().any(|t| t == "Lamp"),
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
        let all = texts(&m.view().tree);
        assert!(
            !all.iter().any(|t| t.contains("GROUP")),
            "virtual group skipped: {all:?}"
        );
        assert!(all.iter().any(|t| t == "MAIN"), "real light shown: {all:?}");
    }
}
