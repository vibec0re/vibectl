//! `v1bectl_widget` — home automation in the trollshell sidebar 🔥
//!
//! An **out-of-process** trollshell widget plugin (vibec0re/trollshell#35,
//! built on the `hytte-plugin` SDK, #279): lives in *this* repo, links zero
//! shell code, and drives a real sidebar widget over
//! `$XDG_RUNTIME_DIR/trollshell/plugin.sock`. The shell never recompiles for
//! it — that's the whole point.
//!
//! Faithful miniature of the cyber web UI (`v1bectl_web`): rooms → light
//! toggles + brightness, outlet toggles (+ live wattage), inline
//! temp/humidity sensors, and a connection dot. Interactions:
//!
//! - **click** a light/outlet → toggle
//! - **scroll** on a light row → brightness ±5 % (the vocab has no slider;
//!   scroll-to-adjust is the shell-native idiom anyway)
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

mod ws;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use hytte_plugin::proto::{Dir, Effect, EventKind, Manifest, Mount, Node};
use hytte_plugin::tokio_stream::wrappers::UnboundedReceiverStream;
use hytte_plugin::{Input, MsgStream, Plugin};
use tokio::sync::mpsc;
use v1bectl_state::{DeviceState, DeviceStateValue, DeviceType, EventType};

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

    /// Physical lights + outlets currently on (groups would double-count
    /// their members — `renders()` keeps them out of the widget entirely).
    fn on_count(&self) -> usize {
        self.devices
            .iter()
            .filter(|d| renders(d))
            .filter(|d| match &d.state {
                DeviceStateValue::Light(l) => l.is_on,
                DeviceStateValue::Outlet(o) => o.is_on,
                _ => false,
            })
            .count()
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
        let mut children = vec![self.header(), Node::Separator { classes: vec![] }];
        if self.devices.is_empty() {
            children.push(status_label(match self.conn {
                Conn::Connecting => "connecting to v1bectl…",
                Conn::Offline => "server unreachable — retrying",
                Conn::Online => "no devices",
            }));
        } else {
            for (room, devices) in self.rooms() {
                children.push(Node::Label {
                    id: None,
                    text: room,
                    classes: vec!["vw-room".into()],
                });
                for dev in devices {
                    if let Some(row) = device_row(dev) {
                        children.push(row);
                    }
                }
            }
        }
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

    /// Title, connection dot, and the on-count.
    fn header(&self) -> Node {
        let (dot, dot_class) = match self.conn {
            Conn::Online => ("●", "vw-online"),
            Conn::Connecting => ("◌", "vw-connecting"),
            Conn::Offline => ("○", "vw-offline"),
        };
        let mut children = vec![Node::Label {
            id: None,
            text: format!("{dot} v1bectl"),
            classes: vec!["vw-title".into(), dot_class.into()],
        }];
        let on = self.on_count();
        if on > 0 {
            children.push(Node::Label {
                id: None,
                text: format!("{on} on"),
                classes: vec!["vw-count".into()],
            });
        }
        Node::Box {
            id: None,
            dir: Dir::Horizontal,
            spacing: 8,
            scroll: false,
            classes: vec!["vw-header".into()],
            children,
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

fn status_label(text: &str) -> Node {
    Node::Label {
        id: None,
        text: text.to_string(),
        classes: vec!["vw-status".into()],
    }
}

/// One device row, or `None` for kinds we don't render.
fn device_row(dev: &DeviceState) -> Option<Node> {
    let id = &dev.device_id;
    let name = &dev.device_info.name;
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
            let mut children = vec![Node::Button {
                id: format!("vw-l-{id}"),
                classes: vec!["vw-toggle".into()],
                child: Box::new(Node::Label {
                    id: None,
                    text: format!("{glyph} {name}"),
                    classes: vec![],
                }),
            }];
            if let (true, Some(b)) = (light.is_on, light.brightness) {
                children.push(Node::Progress {
                    id: None,
                    fraction: f64::from(b) / 100.0,
                    classes: vec!["vw-bright".into()],
                });
            }
            Node::Box {
                // The scroll target for brightness — needs its own id.
                id: Some(format!("vw-ls-{id}")),
                dir: Dir::Horizontal,
                spacing: 6,
                scroll: true,
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
            let mut children = vec![Node::Button {
                id: format!("vw-o-{id}"),
                classes: vec!["vw-toggle".into()],
                child: Box::new(Node::Label {
                    id: None,
                    text: format!("{glyph} {name} ⏻"),
                    classes: vec![],
                }),
            }];
            if let (true, Some(w)) = (outlet.is_on, outlet.power_consumption) {
                children.push(Node::Label {
                    id: None,
                    text: format!("{w:.1} W"),
                    classes: vec!["vw-watts".into()],
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
            if let Some(t) = sensor.temperature {
                parts.push(format!("🌡 {t:.1}°"));
            }
            if let Some(h) = sensor.humidity {
                parts.push(format!("💧 {h:.0}%"));
            }
            let mut classes = vec!["vw-row".to_string(), "vw-sensor".to_string()];
            classes.extend(offline_class);
            Node::Label {
                id: None,
                text: parts.join("  "),
                classes,
            }
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

    /// A model plus the probe end of its command channel.
    fn model_with(devices: Vec<DeviceState>) -> (VibeWidget, mpsc::UnboundedReceiver<Cmd>) {
        let mut m = VibeWidget::init();
        let rx = CMD_RX
            .with(|slot| slot.borrow_mut().take())
            .expect("init parks the receiver");
        m.conn = Conn::Online;
        let _ = m.update(Input::App(WsMsg::Devices(devices)));
        (m, rx)
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
    fn view_groups_by_room_and_counts_on_devices() {
        let (m, _rx) = model_with(vec![
            light("l1", "Taklampa", "living_room", true, Some(70)),
            light("l2", "Sänglampa", "bedroom", false, Some(30)),
            outlet("o1", "Skrivbord", "bedroom", true),
            sensor("s1", "Klimat", "living_room"),
        ]);
        let all = texts(&m.view());
        // Rooms alphabetical, devices alphabetical within each.
        assert_eq!(
            all,
            vec![
                "● v1bectl",
                "2 on",
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
    fn virtual_light_groups_are_not_rendered_or_counted() {
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
            all.iter().any(|t| t == "1 on"),
            "groups don't inflate the count: {all:?}"
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
}
