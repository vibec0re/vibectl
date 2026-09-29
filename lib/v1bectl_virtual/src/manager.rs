use crate::button_controller::ButtonAction;
use crate::virtual_device::{
    VirtualDevice, VirtualDeviceConfig, VirtualDeviceError, VirtualDeviceType, VirtualWrite,
};
use crate::write_queue::{WriteQueue, WriteTurn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{RwLock, RwLockWriteGuard};
use v1bectl_sync::{
    DeviceEvent, DeviceId, DeviceInfo, DeviceState, DeviceStateValue, DeviceType, EventBus,
    EventType, LagAwareReceiver, Recv, StateError, StateStore, SyncEngine,
};

/// Virtual Device Manager - coordinates all virtual devices 🔥
///
/// A write to a virtual device ([`Self::set_virtual_device_state`]) fans out
/// to its members. Input tracking ([`Self::start`]) re-derives a virtual
/// device when one of its inputs changes from outside, and runs a button
/// controller's action when its button is pressed ([`Self::handle_event`]).
/// Each of these runs start to finish under the `virtual_devices` lock: the
/// device's new state, its members' commits, its store state and every
/// echo. So they never interleave. A second write can't land between a
/// first write's fan-out and its commits, and echoes go out in the order the
/// store changed.
///
/// The one exception is a scene's transition, which waits between its steps
/// (#58). It runs without that lock, and only its commit takes it, so a fade
/// doesn't hold up what it has nothing to do with. A write that shares a
/// device with it still waits for it, through the write queue every write
/// joins first (see [`Self::set_virtual_device_state`]).
pub struct VirtualDeviceManager {
    /// All virtual devices indexed by device ID
    virtual_devices: Arc<RwLock<HashMap<DeviceId, Box<dyn VirtualDevice>>>>,
    /// Maps physical device ID -> virtual device IDs that depend on it
    input_mappings: Arc<RwLock<HashMap<DeviceId, Vec<DeviceId>>>>,
    /// Maps virtual device ID -> physical device IDs it controls
    output_mappings: Arc<RwLock<HashMap<DeviceId, Vec<DeviceId>>>>,
    /// State store for device operations
    state_store: Arc<StateStore>,
    /// Event bus for notifications
    event_bus: Arc<EventBus>,
    /// The physical write path. When one is attached, a virtual write
    /// commits each member it changes through
    /// `SyncEngine::apply_optimistic_update`, the call a direct write to
    /// that member makes, so the member reaches the gateway. Without one,
    /// the manager writes the members into the store itself, and echoes
    /// them.
    sync_engine: Arc<OnceLock<Arc<SyncEngine>>>,
    /// Set once [`Self::start`] has run (see [`Self::add_virtual_device`]).
    tracking: Arc<AtomicBool>,
    /// Every write's place in line, on the devices it writes, from before it
    /// plans to after it commits (see [`Self::set_virtual_device_state`]).
    /// Waited in before the `virtual_devices` lock, never while holding it.
    write_queue: Arc<WriteQueue>,
}

/// A state echo for `device_id`, in the same `AttributeChanged{attribute:
/// "state"}` shape the sync engine publishes for a physical write. That's the
/// shape every client (TUI, widget, GTK, web) and this manager's own input
/// tracking decode.
///
/// `old_value` is the state the store held before, as in the engine's
/// `GatewayWins` and confirmation echoes. (The engine's optimistic echo sends
/// `Null` there.) Clients read only `new_value`; the server's event log
/// prints both.
fn state_event(
    device_id: &DeviceId,
    old_state: Option<&DeviceStateValue>,
    new_state: &DeviceStateValue,
) -> DeviceEvent {
    DeviceEvent {
        timestamp: std::time::SystemTime::now(),
        device_id: device_id.clone(),
        event_type: EventType::AttributeChanged {
            attribute: "state".to_string(),
            old_value: old_state
                .and_then(|s| serde_json::to_value(s).ok())
                .unwrap_or(serde_json::Value::Null),
            new_value: serde_json::to_value(new_state).unwrap_or(serde_json::Value::Null),
        },
    }
}

fn warn_dangling(virtual_id: &DeviceId, device_id: &DeviceId) {
    tracing::warn!(
        "⚠️ Virtual device {} references unknown device {}",
        virtual_id,
        device_id
    );
}

impl VirtualDeviceManager {
    pub fn new(state_store: Arc<StateStore>, event_bus: Arc<EventBus>) -> Self {
        Self {
            virtual_devices: Arc::new(RwLock::new(HashMap::new())),
            input_mappings: Arc::new(RwLock::new(HashMap::new())),
            output_mappings: Arc::new(RwLock::new(HashMap::new())),
            state_store,
            event_bus,
            sync_engine: Arc::new(OnceLock::new()),
            tracking: Arc::new(AtomicBool::new(false)),
            write_queue: Arc::default(),
        }
    }

    /// Send member writes through `sync_engine`, like direct writes, so they
    /// reach the gateway. Without this the 2s pull worker (`GatewayWins`)
    /// reverts them. Only the first attach counts.
    pub fn attach_sync_engine(&self, sync_engine: Arc<SyncEngine>) {
        if self.sync_engine.set(sync_engine).is_err() {
            tracing::warn!("⚠️ VirtualDeviceManager already has a sync engine; ignoring");
        }
    }

    /// Publish a state echo for `device_id`.
    async fn publish_state(
        &self,
        device_id: &DeviceId,
        old_state: Option<&DeviceStateValue>,
        new_state: &DeviceStateValue,
    ) {
        self.event_bus
            .publish(state_event(device_id, old_state, new_state))
            .await;
    }

    /// Store `state` as virtual device `device_id`'s and echo it. If the
    /// store already holds exactly that, do neither, unless `even_unchanged`.
    async fn store_state(
        &self,
        device_id: &DeviceId,
        state: DeviceStateValue,
        even_unchanged: bool,
    ) -> Result<(), StateError> {
        let old_state = self
            .state_store
            .get_device(device_id)
            .await
            .map(|d| d.state);
        if !even_unchanged && old_state.as_ref() == Some(&state) {
            return Ok(());
        }
        self.state_store
            .update_device_state(device_id, state.clone())
            .await?;
        self.publish_state(device_id, old_state.as_ref(), &state)
            .await;
        Ok(())
    }

    /// Queue a write on the devices `keys` names (see
    /// [`Self::set_virtual_device_state`]), wait for its turn, then take the
    /// `virtual_devices` lock. `keys` reads them off the devices the manager
    /// has: a write's device and every member it may write. Keep the
    /// returned turn until the write is committed.
    ///
    /// The write joins the queue under a brief read of the devices, so the
    /// devices it names are the ones the manager had when it asked. It waits
    /// for its turn without the `virtual_devices` lock, which nothing holds
    /// while it waits in the queue, so the two can't deadlock.
    ///
    /// A write ahead of it in the queue can change the devices it names: its
    /// target *replaced* (removed and re-added under the same id, which
    /// queues on the target too, as a config reload does) can add ones it
    /// wasn't queued on. A removal alone never does: it only drops devices
    /// from what a write names, and those are already covered. So it reads
    /// `keys` again under the lock, and if that names a device it wasn't
    /// queued on, it leaves the queue and joins again with the new devices,
    /// **at the back**: it's asked again, against the devices as they are
    /// now. It can't keep its old place, even on a device it already held,
    /// so an older write can end up landing last. Keeping its place would
    /// risk a deadlock instead: it would wait for writes that asked after it
    /// (on the devices it now names), while others that asked after it wait
    /// for it.
    async fn lock_for_write(
        &self,
        keys: impl Fn(&HashMap<DeviceId, Box<dyn VirtualDevice>>) -> Vec<DeviceId>,
    ) -> (
        WriteTurn,
        RwLockWriteGuard<'_, HashMap<DeviceId, Box<dyn VirtualDevice>>>,
    ) {
        loop {
            let mut turn = {
                let devices = self.virtual_devices.read().await;
                self.write_queue.join(keys(&devices))
            };
            turn.ready().await;
            let devices = self.virtual_devices.write().await;
            if turn.covers(&keys(&devices)) {
                return (turn, devices);
            }
        }
    }

    /// The devices a write of `new_state` to `device_id` queues on: itself,
    /// and every member it may write ([`VirtualDevice::writes_to`]).
    fn write_keys(
        devices: &HashMap<DeviceId, Box<dyn VirtualDevice>>,
        device_id: &DeviceId,
        new_state: &DeviceStateValue,
    ) -> Vec<DeviceId> {
        let mut keys = devices
            .get(device_id)
            .map(|device| device.writes_to(new_state))
            .unwrap_or_default();
        keys.push(device_id.clone());
        keys
    }

    /// Commit one member write of a virtual write, or a button action's
    /// write to a light: `new_state` for `member_id`, which the store holds
    /// as `old_state`. Nothing has written it anywhere yet (#55).
    ///
    /// A physical member goes through the sync engine when one is attached,
    /// the way a direct write to it does. That arms the member's protection
    /// window and queues the gateway push before anything changes the store.
    /// So no pull can find the store ahead of the hub with nothing pending,
    /// take that for an outside change and revert it (`GatewayWins`). With
    /// optimistic updates on (the default) the engine also writes the store
    /// and publishes the echo, so it is the member's only writer and
    /// publisher.
    ///
    /// Otherwise the store write and the echo are ours: with no engine, for
    /// a virtual member (the gateway doesn't know it), and with optimistic
    /// updates off. In that mode a direct write is echoed once a pull
    /// confirms it. But a virtual write puts its members in the store right
    /// away, as it always has (after the engine armed the window), so that
    /// pull finds nothing new and would never echo it.
    async fn commit_member_write(
        &self,
        member_id: &DeviceId,
        old_state: &DeviceStateValue,
        new_state: &DeviceStateValue,
        member_is_virtual: bool,
    ) -> Result<(), VirtualDeviceError> {
        if let (Some(engine), false) = (self.sync_engine.get(), member_is_virtual) {
            if let Err(e) = engine
                .apply_optimistic_update(member_id, new_state.clone())
                .await
            {
                // Its only failure is the store write: the member has left
                // the store since the caller found it there.
                tracing::warn!(
                    "❌ Failed to sync member {} of a virtual write: {}",
                    member_id,
                    e
                );
                return Err(e.downcast::<StateError>().map_or_else(
                    |_| VirtualDeviceError::DeviceNotFound(member_id.clone()),
                    VirtualDeviceError::StateStore,
                ));
            }
            if engine.config().optimistic_updates {
                return Ok(());
            }
        }
        self.state_store
            .update_device_state(member_id, new_state.clone())
            .await?;
        self.publish_state(member_id, Some(old_state), new_state)
            .await;
        Ok(())
    }

    /// Commit `members`, the member writes of a virtual write, in order,
    /// each through [`Self::commit_member_write`]. `devices` is what the
    /// `virtual_devices` lock guards, which the caller holds: a member in
    /// it is virtual.
    ///
    /// A member listed more than once is committed once, where it first
    /// comes, at the last state it's listed with. One the write leaves as
    /// it is has nothing to push or echo, and is skipped. The first that
    /// fails, such as a member the store doesn't have (#2), ends the write:
    /// the ones before it stay committed, and the ones after it are never
    /// made.
    async fn commit_members(
        &self,
        devices: &HashMap<DeviceId, Box<dyn VirtualDevice>>,
        members: Vec<(DeviceId, DeviceStateValue)>,
    ) -> Result<(), VirtualDeviceError> {
        let mut writes: Vec<(DeviceId, DeviceStateValue)> = Vec::with_capacity(members.len());
        for (id, state) in members {
            match writes.iter_mut().find(|(listed, _)| *listed == id) {
                Some((_, last)) => *last = state,
                None => writes.push((id, state)),
            }
        }

        for (id, state) in writes {
            let Some(old_state) = self.state_store.get_device(&id).await.map(|d| d.state) else {
                return Err(StateError::DeviceNotFound(id).into());
            };
            if old_state == state {
                continue;
            }
            self.commit_member_write(&id, &old_state, &state, devices.contains_key(&id))
                .await?;
        }
        Ok(())
    }

    /// Add a virtual device to the manager. It first takes its state from
    /// its inputs as the store holds them (see
    /// [`VirtualDevice::seed_from_inputs`]), so register a group after its
    /// members are in the store. Once it's in the store, a `DeviceAdded`
    /// event announces it, so clients pick it up without a refetch (#16).
    pub async fn add_virtual_device(
        &self,
        mut device: Box<dyn VirtualDevice>,
    ) -> Result<(), VirtualDeviceError> {
        let device_id = device.device_id().clone();
        if let Err(e) = device.seed_from_inputs().await {
            tracing::warn!(
                "⚠️ Virtual device {} couldn't take its state from its inputs: {}",
                device_id,
                e
            );
        }
        let input_devices = device.input_devices();
        let output_devices = device.output_devices();

        // A dangling reference doesn't fail registration, only a later write
        // (see #2), so flag it now. Until tracking starts, other virtual
        // devices may still be loading, and this one could point at one of
        // them. `start` checks everything once they are all in.
        if self.tracking.load(Ordering::Acquire) {
            let refs = input_devices.iter().chain(&output_devices).cloned();
            for missing in self.missing(refs.collect()).await {
                warn_dangling(&device_id, &missing);
            }
        }

        // Add device. It queues on its id too, so it doesn't replace one of
        // that id while a write to it is in flight.
        {
            let (_turn, mut devices) = self.lock_for_write(|_| vec![device_id.clone()]).await;
            devices.insert(device_id.clone(), device);
        }

        // Update input mappings (physical -> virtual)
        {
            let mut input_map = self.input_mappings.write().await;
            for physical_id in input_devices {
                input_map
                    .entry(physical_id)
                    .or_insert_with(Vec::new)
                    .push(device_id.clone());
            }
        }

        // Update output mappings (virtual -> physical)
        {
            let mut output_map = self.output_mappings.write().await;
            output_map.insert(device_id.clone(), output_devices);
        }

        // Register virtual device in state store
        let device_type = {
            let devices = self.virtual_devices.read().await;
            let Some(virtual_device) = devices.get(&device_id) else {
                return Ok(());
            };
            let device_info = virtual_device.device_info();
            let device_type = format!("{:?}", device_info.device_type);
            let initial_state = virtual_device.current_state();
            self.state_store
                .add_device(device_info, initial_state)
                .await;
            device_type
        };

        // Announced the way the server announces a discovered device.
        self.publish_lifecycle(&device_id, EventType::DeviceAdded { device_type })
            .await;
        Ok(())
    }

    /// Publish a `DeviceAdded`/`DeviceRemoved` event for `device_id`.
    async fn publish_lifecycle(&self, device_id: &DeviceId, event_type: EventType) {
        self.event_bus
            .publish(DeviceEvent {
                timestamp: std::time::SystemTime::now(),
                device_id: device_id.clone(),
                event_type,
            })
            .await;
    }

    /// Every `(virtual device, device it reads or writes)` pair the store
    /// has no device for: a group's members, a scene's devices, a
    /// controller's button and targets. A dangling reference doesn't fail
    /// registration, only a later write (see #2). [`Self::start`] logs these.
    pub async fn dangling_references(&self) -> Vec<(DeviceId, DeviceId)> {
        let references: Vec<(DeviceId, Vec<DeviceId>)> = {
            let devices = self.virtual_devices.read().await;
            devices
                .values()
                .map(|device| {
                    let mut refs = device.input_devices();
                    refs.extend(device.output_devices());
                    (device.device_id().clone(), refs)
                })
                .collect()
        };

        let mut dangling = Vec::new();
        for (virtual_id, refs) in references {
            for missing in self.missing(refs).await {
                dangling.push((virtual_id.clone(), missing));
            }
        }
        dangling.sort();
        dangling
    }

    /// Every button that more than one button controller binds, with those
    /// controllers (sorted). Each press of such a button runs all of their
    /// actions. A stale `button_test.toml` next to `button_ctrl.toml` did
    /// that: the server loads every `virtual_devices/*.toml`, and both bound
    /// `switch_hallway` (#34 review, nit 5). [`Self::start`] logs these.
    pub async fn shared_buttons(&self) -> Vec<(DeviceId, Vec<DeviceId>)> {
        let mut bound: HashMap<DeviceId, Vec<DeviceId>> = HashMap::new();
        {
            let devices = self.virtual_devices.read().await;
            for device in devices.values() {
                if !matches!(device.device_type(), VirtualDeviceType::ButtonController) {
                    continue;
                }
                // A controller's only input is its button.
                for button in device.input_devices() {
                    bound
                        .entry(button)
                        .or_default()
                        .push(device.device_id().clone());
                }
            }
        }

        let mut shared: Vec<(DeviceId, Vec<DeviceId>)> = bound
            .into_iter()
            .filter(|(_, controllers)| controllers.len() > 1)
            .map(|(button, mut controllers)| {
                controllers.sort();
                (button, controllers)
            })
            .collect();
        shared.sort();
        shared
    }

    /// The ids in `refs` the store has no device for, each once.
    async fn missing(&self, refs: Vec<DeviceId>) -> Vec<DeviceId> {
        let mut seen = HashSet::new();
        let mut missing = Vec::new();
        for id in refs {
            if seen.insert(id.clone()) && self.state_store.get_device(&id).await.is_none() {
                missing.push(id);
            }
        }
        missing
    }

    /// Remove a virtual device. A `DeviceRemoved` event announces it, so
    /// clients drop it without a refetch (#16). Removing one the manager
    /// doesn't have does nothing, and announces nothing.
    ///
    /// It leaves the manager and the store under one hold of the
    /// `virtual_devices` lock (#34 review, finding 2). So a write that takes
    /// the lock, such as a button action, finds it in both or in neither.
    /// It used to leave the store only after letting go, and an action that
    /// ran in between took it for a physical light: it wrote the group's
    /// state back into the store and queued it for the gateway, under an id
    /// the hub doesn't know.
    pub async fn remove_virtual_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<(), VirtualDeviceError> {
        // Remove from devices, and from the store, in one hold. It queues on
        // its id first, so a write to it in flight (a scene's fade) commits
        // before it goes, as it did when that write held the
        // `virtual_devices` lock throughout (#58). It leaves the queue with
        // that lock, before the mappings are cleaned up.
        let (device, removed_from_store) = {
            let (_turn, mut devices) = self.lock_for_write(|_| vec![device_id.clone()]).await;
            let Some(device) = devices.remove(device_id) else {
                return Ok(());
            };
            (device, self.state_store.remove_device(device_id).await)
        };

        // Clean up input mappings
        {
            let mut input_map = self.input_mappings.write().await;
            for input_device_id in device.input_devices() {
                if let Some(virtual_ids) = input_map.get_mut(&input_device_id) {
                    virtual_ids.retain(|id| id != device_id);
                    if virtual_ids.is_empty() {
                        input_map.remove(&input_device_id);
                    }
                }
            }
        }

        // Clean up output mappings
        {
            let mut output_map = self.output_mappings.write().await;
            output_map.remove(device_id);
        }

        removed_from_store?;
        self.publish_lifecycle(device_id, EventType::DeviceRemoved)
            .await;
        Ok(())
    }

    /// Input tracking for one event from the bus. A device's `state` change
    /// (`AttributeChanged{attribute: "state"}`, the shape of every state
    /// echo) goes to [`Self::handle_device_state_change`]. Then every
    /// virtual device the event is an input of gets to react to it (see
    /// [`VirtualDevice::reactions`]): a button controller runs its action
    /// for a press. Anything else is ignored.
    ///
    /// [`Self::start`] runs this for every event on the bus. Call it
    /// directly to drive tracking yourself instead (the tests do, to control
    /// exactly when it catches up), and [`Self::resync`] after missing any.
    pub async fn handle_event(&self, event: &DeviceEvent) -> Result<(), VirtualDeviceError> {
        let tracked = self.track(event).await;
        self.react_to(event).await;
        tracked
    }

    /// The state-tracking half of [`Self::handle_event`].
    async fn track(&self, event: &DeviceEvent) -> Result<(), VirtualDeviceError> {
        let EventType::AttributeChanged {
            attribute,
            new_value,
            ..
        } = &event.event_type
        else {
            return Ok(());
        };
        if attribute != "state" {
            return Ok(());
        }
        let Ok(new_state) = serde_json::from_value::<DeviceStateValue>(new_value.clone()) else {
            return Ok(());
        };

        // Event timestamps are near-present wall-clock millis; this only
        // truncates past year ~292 million.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "wall-clock millis since epoch, nowhere near u64::MAX until year ~292 million"
        )]
        let timestamp_millis = event
            .timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let device_state = DeviceState {
            device_id: event.device_id.clone(),
            device_info: DeviceInfo {
                device_id: event.device_id.clone(),
                name: "Unknown".to_string(),
                device_type: DeviceType::Light,
                capabilities: vec![],
                device_groups: vec![],
                manufacturer: None,
                model: None,
                firmware_version: None,
                battery_powered: false,
                reachable: true,
                last_seen: timestamp_millis,
                custom_attributes: std::collections::HashMap::new(),
            },
            state: new_state,
            last_updated: timestamp_millis,
            last_synced_from_gateway: Some(timestamp_millis),
            last_synced_to_gateway: Some(timestamp_millis),
        };

        self.handle_device_state_change(&event.device_id, &device_state)
            .await
    }

    /// Handle a state change of `device_id`, an input of virtual devices.
    /// Each one whose current state doesn't already account for the input
    /// (see [`VirtualDevice::accounts_for`]) is re-derived, and echoed if
    /// that moved it.
    ///
    /// The input is judged as the store holds it now, not as `new_state`
    /// (the event) has it. That is only the fallback for a device the store
    /// doesn't have. Tracking can run behind the writes: by the time it gets
    /// to a member's echo, later writes may have replaced that state.
    /// Judging the stale one would re-derive the group from members that
    /// have moved on.
    pub async fn handle_device_state_change(
        &self,
        device_id: &DeviceId,
        new_state: &DeviceState,
    ) -> Result<(), VirtualDeviceError> {
        self.track_input(device_id, Some(new_state)).await
    }

    /// [`Self::handle_device_state_change`], with `fallback` as the input
    /// for a device the store doesn't have. With none, such an input is
    /// skipped ([`Self::resync`] has nothing else to go on).
    async fn track_input(
        &self,
        device_id: &DeviceId,
        fallback: Option<&DeviceState>,
    ) -> Result<(), VirtualDeviceError> {
        // Find virtual devices that depend on this physical device
        let virtual_device_ids = {
            let input_map = self.input_mappings.read().await;
            input_map.get(device_id).cloned().unwrap_or_default()
        };

        if virtual_device_ids.is_empty() {
            return Ok(());
        }

        // Take the lock before reading the input, so no virtual write is
        // half-done while we look.
        let mut devices = self.virtual_devices.write().await;
        let stored = self.state_store.get_device(device_id).await;
        let Some(input) = stored.as_ref().or(fallback) else {
            return Ok(());
        };

        // Notify all dependent virtual devices
        for virtual_id in virtual_device_ids {
            let Some(virtual_device) = devices.get_mut(&virtual_id) else {
                continue;
            };
            if virtual_device.accounts_for(device_id, &input.state) {
                // The input is where this device's own state puts it: the
                // echo of its own write, or a change it already reflects.
                // Re-deriving anyway is lossy (linear ranges).
                continue;
            }

            if let Err(e) = virtual_device.on_input_changed(device_id, input).await {
                tracing::warn!(
                    "Virtual device {} failed to handle input change: {}",
                    virtual_id,
                    e
                );
                continue;
            }

            // Update virtual device state in store, and echo it if it moved
            let new_virtual_state = virtual_device.current_state();
            if let Err(e) = self
                .store_state(&virtual_id, new_virtual_state, false)
                .await
            {
                tracing::error!("Failed to update virtual device state: {}", e);
            }
        }

        Ok(())
    }

    /// Make the writes the virtual devices that `event` is an input of ask
    /// for (see [`VirtualDevice::reactions`]): a button controller's action
    /// for a press. Each one is a write of its own (see
    /// [`Self::run_action`]). A failed one is logged and doesn't stop the
    /// others; the press has happened either way.
    ///
    /// Only the lookups happen under the locks. They're let go before any
    /// action runs, since each one takes the `virtual_devices` lock itself.
    async fn react_to(&self, event: &DeviceEvent) {
        let virtual_device_ids = {
            let input_map = self.input_mappings.read().await;
            input_map.get(&event.device_id).cloned().unwrap_or_default()
        };
        if virtual_device_ids.is_empty() {
            return;
        }

        let actions: Vec<(DeviceId, ButtonAction)> = {
            let devices = self.virtual_devices.read().await;
            virtual_device_ids
                .iter()
                .filter_map(|id| devices.get(id).map(|device| (id, device.reactions(event))))
                .flat_map(|(id, actions)| actions.into_iter().map(move |a| (id.clone(), a)))
                .collect()
        };

        for (virtual_id, action) in actions {
            match self.run_action(&action).await {
                Ok(()) => tracing::info!(
                    "✅ {} executed {:?} on {}",
                    virtual_id,
                    action.command,
                    action.target
                ),
                Err(e) => tracing::error!(
                    "❌ {} failed to execute {:?} on {}: {}",
                    virtual_id,
                    action.command,
                    action.target,
                    e
                ),
            }
        }
    }

    /// Make the write `action` asks for, starting from its target's state
    /// at the time. The target must be a light. A virtual one (a light
    /// group) is written the way [`Self::set_virtual_device_state`] writes
    /// it: the group fans out to its members, and they're committed through
    /// the sync engine and echoed. A physical one is written like a direct
    /// write.
    ///
    /// A target the manager doesn't have is physical only if the store
    /// doesn't mark it virtual, as the API tells them apart. A virtual one
    /// is `DeviceNotFound`: the manager is its only writer, and the gateway
    /// doesn't know it (#34 review, finding 2).
    ///
    /// This takes the `virtual_devices` lock and holds it to the end, so the
    /// state the action starts from is still the target's when the write
    /// lands (see the type's docs). So don't call it with that lock held.
    ///
    /// First it queues on the target and, for a virtual one, its outputs
    /// (every member any write to it can write; the action's state isn't
    /// known until its turn comes). So a press that shares a light with a
    /// scene's fade waits for the fade's commit, and lands after it, as a
    /// write that overlaps one does (see
    /// [`Self::set_virtual_device_state`]). Input tracking runs a press's
    /// actions in turn, so it waits with it: the presses and re-derivations
    /// behind it wait too, until the fade commits. That's as before #58,
    /// when tracking waited for every fade. A press that shares nothing with
    /// a write in flight doesn't wait.
    async fn run_action(&self, action: &ButtonAction) -> Result<(), VirtualDeviceError> {
        let target = &action.target;
        let (turn, mut devices) = self
            .lock_for_write(|devices| {
                let mut keys = devices
                    .get(target)
                    .map(|device| device.output_devices())
                    .unwrap_or_default();
                keys.push(target.clone());
                keys
            })
            .await;
        let current = if let Some(device) = devices.get(target) {
            device.current_state()
        } else {
            let stored = self
                .state_store
                .get_device(target)
                .await
                .ok_or_else(|| VirtualDeviceError::DeviceNotFound(target.clone()))?;
            if stored
                .device_info
                .device_groups
                .iter()
                .any(|g| g == "virtual")
            {
                return Err(VirtualDeviceError::DeviceNotFound(target.clone()));
            }
            stored.state
        };
        let DeviceStateValue::Light(light) = &current else {
            tracing::warn!(
                "⚠️ A button action can only control lights, {} is {:?}",
                target,
                current
            );
            return Ok(());
        };
        let new_state = DeviceStateValue::Light(action.apply(light.clone()));

        if devices.contains_key(target) {
            return self
                .write_virtual(&mut devices, &turn, target, new_state)
                .await;
        }
        self.commit_member_write(target, &current, &new_state, false)
            .await
    }

    /// Set virtual device state (called from API)
    ///
    /// Publishes a state echo for the virtual device and for every member
    /// whose state the write changed. The device only says what the write
    /// takes ([`VirtualDevice::plan_write`]). The manager commits each
    /// member through the attached sync engine (see
    /// [`Self::attach_sync_engine`]), the same way a direct write to it is,
    /// and the engine writes it into the store (#55). Without an engine the
    /// manager writes the store itself.
    ///
    /// A write can fail part-way (a missing member, as in #2). The members
    /// committed before the failure stay committed and echoed, and then the
    /// error is returned. A group keeps its old state on failure, which no
    /// longer accounts for those members, so input tracking re-derives it
    /// from them like after any outside change: the group ends up showing
    /// what happened.
    ///
    /// # Writes that overlap wait, the others don't (#58)
    ///
    /// A write whose plan takes time, a scene that fades or steps through a
    /// sequence, is planned without the `virtual_devices` lock, delays and
    /// all ([`VirtualDevice::plan_write_detached`]). The lock is taken
    /// only to look the device up and hand out the plan, and again to
    /// commit where the transition ends. That commit is still one hold of
    /// the lock: the members, the scene's own state and their echoes, with
    /// no other virtual write in between.
    ///
    /// What orders writes is a write queue. Every write joins it in the
    /// order it asks, with the devices it writes: its virtual device, and
    /// every member it may write ([`VirtualDevice::writes_to`]). A button
    /// action joins with its target (and a virtual target's outputs), and
    /// an add or a removal with the device itself. A write goes once every
    /// write that joined before it and shares a device with it is over,
    /// whether that one is running or still waiting itself, and it stays in
    /// the queue until it has committed, a scene's delays included. So:
    ///
    /// - **A write that overlaps a transition** (it shares a member with
    ///   it, or is to the same scene controller) waits for the
    ///   transition's commit, and lands after it. The newest write ends up
    ///   showing, as when every write waited on the `virtual_devices` lock.
    ///   An "all off" pressed during a ten-second fade turns the lights off
    ///   once the fade has committed, and they stay off.
    /// - **Writes that overlap each other** land in the order they asked,
    ///   even when they overlap only through a write that's still waiting.
    ///   A write to `[a, z]` that waits for a fade of `a` holds up a later
    ///   write to `z` alone, which then lands after it (#60 review,
    ///   finding 1).
    /// - **A write that overlaps nothing in flight** goes ahead at once. A
    ///   one-second fade used to hold up every virtual write, input
    ///   tracking, resync and button press for its whole second.
    /// - **A scene controller removed during its transition** goes once the
    ///   transition has committed, as before.
    /// - **A write dropped part-way** (a client that went away), while it
    ///   waits or during its transition, leaves the queue at once, and the
    ///   writes behind it go on. It commits nothing.
    /// - **A direct write to a light through the API** doesn't go through
    ///   the manager, so it lands at once, and a fade over that light
    ///   overwrites it when it commits. That's as it always was.
    ///
    /// Input tracking and resync don't queue. They only re-derive a virtual
    /// device from its inputs, under the `virtual_devices` lock, so they
    /// don't wait for a transition themselves. But tracking runs a press's
    /// actions in turn, and a press that overlaps a transition waits for
    /// it. Until the transition commits, the presses and re-derivations
    /// behind that press wait too. That's as before #58, when tracking
    /// waited for every transition.
    ///
    /// None of this can deadlock. A write waits only for writes that asked
    /// before it, so the earliest one in the queue never waits, and it
    /// waits in the queue before it takes the `virtual_devices` lock, which
    /// nothing holds while it waits in the queue. The queue holds only the
    /// writes in flight: each leaves it when it's over, however it ends.
    ///
    /// Cancelling a transition when a newer write to its devices comes in,
    /// instead of making that write wait, could be a later improvement. It
    /// would change what shows, so it's the owner's call, and it's not done
    /// here.
    ///
    /// Everything else is as it was: the same final states, the same
    /// delays before the commit, the same echoes, and the same error for a
    /// device the store doesn't have. A write that doesn't wait (a group
    /// write, an instant scene, a deactivation) is planned and committed
    /// under one hold of the `virtual_devices` lock.
    pub async fn set_virtual_device_state(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), VirtualDeviceError> {
        // Held until the write is committed (see above).
        let (turn, mut devices) = self
            .lock_for_write(|devices| Self::write_keys(devices, device_id, &new_state))
            .await;
        let Some(virtual_device) = devices.get(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };
        let Some(plan) = virtual_device.plan_write_detached(&new_state) else {
            // Held to the end (see the type's docs).
            return self
                .write_virtual(&mut devices, &turn, device_id, new_state)
                .await;
        };

        // A plan that waits runs without the `virtual_devices` lock (see
        // above), and takes it again only to commit.
        drop(devices);
        let planned = plan.await;
        let mut devices = self.virtual_devices.write().await;
        if !devices.contains_key(device_id) {
            // Can't happen: a removal queues on it, behind this write.
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        }
        self.commit_write(&mut devices, &turn, device_id, planned)
            .await
    }

    /// [`Self::set_virtual_device_state`], for a caller that already holds
    /// the `virtual_devices` lock and `turn`, its place in the write queue:
    /// `devices` is what the lock guards. The write is planned and committed
    /// in that one hold.
    async fn write_virtual(
        &self,
        devices: &mut HashMap<DeviceId, Box<dyn VirtualDevice>>,
        turn: &WriteTurn,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), VirtualDeviceError> {
        let Some(virtual_device) = devices.get(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };

        // What the write takes. Nothing is written yet, not even the store:
        // the member commits write it, each behind its protection window
        // (#55). The device writing its members into the store itself, for
        // the manager to commit after, left a gap a pull could revert them
        // in.
        let planned = virtual_device.plan_write(new_state).await;
        self.commit_write(devices, turn, device_id, planned).await
    }

    /// Commit `planned`, the write `device_id` planned (or the error its
    /// plan failed with): its members, in order, then its own new state,
    /// stored and echoed. `devices` is what the `virtual_devices` lock
    /// guards, which the caller holds, and which has `device_id`. `turn` is
    /// the write's place in the write queue, which the caller holds too.
    async fn commit_write(
        &self,
        devices: &mut HashMap<DeviceId, Box<dyn VirtualDevice>>,
        turn: &WriteTurn,
        device_id: &DeviceId,
        planned: Result<VirtualWrite, VirtualDeviceError>,
    ) -> Result<(), VirtualDeviceError> {
        let result = match planned {
            Ok(write) => {
                // A member the write wasn't queued on could be written while
                // another write to it is in flight: `writes_to` listed too
                // few (see there).
                debug_assert!(
                    turn.covers(write.members.iter().map(|(id, _)| id)),
                    "{device_id} planned a write outside its `writes_to`: {:?}",
                    write.members
                );
                self.commit_members(devices, write.members)
                    .await
                    .map(|()| write.state)
            }
            Err(e) => Err(e),
        };

        let Some(virtual_device) = devices.get_mut(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };
        match result {
            Ok(state) => {
                virtual_device.take_state(state);
                self.store_state(device_id, virtual_device.current_state(), true)
                    .await?;
                Ok(())
            }
            Err(e) => {
                // A device keeps its state on failure, but the store must
                // not be left behind it.
                let current_state = virtual_device.current_state();
                if let Err(store_error) = self.store_state(device_id, current_state, false).await {
                    tracing::error!("Failed to update virtual device state: {}", store_error);
                }
                Err(e)
            }
        }
    }

    /// Get virtual device state
    pub async fn get_virtual_device_state(
        &self,
        device_id: &DeviceId,
    ) -> Result<DeviceStateValue, VirtualDeviceError> {
        let devices = self.virtual_devices.read().await;

        if let Some(virtual_device) = devices.get(device_id) {
            Ok(virtual_device.current_state())
        } else {
            Err(VirtualDeviceError::DeviceNotFound(device_id.clone()))
        }
    }

    /// List all virtual devices
    pub async fn list_virtual_devices(&self) -> Vec<DeviceInfo> {
        let devices = self.virtual_devices.read().await;
        devices
            .values()
            .map(|device| device.device_info())
            .collect()
    }

    /// Get virtual device configuration
    pub async fn get_virtual_device_config(
        &self,
        device_id: &DeviceId,
    ) -> Result<VirtualDeviceConfig, VirtualDeviceError> {
        let devices = self.virtual_devices.read().await;

        if let Some(virtual_device) = devices.get(device_id) {
            Ok(virtual_device.config().clone())
        } else {
            Err(VirtualDeviceError::DeviceNotFound(device_id.clone()))
        }
    }

    /// Start the manager: input tracking runs [`Self::handle_event`] for
    /// every event on the bus, in a background task, and [`Self::resync`]
    /// whenever it has fallen behind the bus and missed some. It also logs
    /// every dangling reference (see [`Self::dangling_references`]), and
    /// every button more than one controller binds (see
    /// [`Self::shared_buttons`]).
    /// The server calls this once all virtual devices are loaded.
    pub async fn start(&self) -> Result<(), VirtualDeviceError> {
        let manager = Arc::new(self.clone());

        // Subscribe to state change events
        let mut event_receiver =
            LagAwareReceiver::new(self.event_bus.subscribe(), "virtual device tracking");

        self.tracking.store(true, Ordering::Release);
        for (virtual_id, missing) in self.dangling_references().await {
            warn_dangling(&virtual_id, &missing);
        }
        for (button, controllers) in self.shared_buttons().await {
            tracing::warn!(
                "⚠️ Button {} is bound by {} button controllers ({}): each press runs all of their actions",
                button,
                controllers.len(),
                controllers.join(", ")
            );
        }

        tokio::spawn(async move {
            // Falling behind the bus doesn't end tracking, and doesn't leave
            // the groups behind either (#15). It first handles the events
            // still buffered, a press among them included. Then, since the
            // events it missed may have moved members, it catches every
            // virtual device up from the store, once, at the bus's edge.
            loop {
                match event_receiver.recv().await {
                    Recv::Event(event) => {
                        if let Err(e) = manager.handle_event(&event).await {
                            tracing::error!(
                                "Failed to handle state change for virtual devices: {}",
                                e
                            );
                        }
                    }
                    Recv::Lagged(_) => manager.resync().await,
                    Recv::Closed => break,
                }
            }
        });

        Ok(())
    }

    /// Catch every virtual device up with its inputs as the store holds
    /// them now. Input tracking does this when it has fallen behind the bus
    /// (#15): the events it missed may have moved members, and without this
    /// their groups would stay stale until those members changed again.
    ///
    /// Each input goes through [`Self::handle_device_state_change`], the
    /// path an outside change of it takes. So a group that already accounts
    /// for its members (see [`VirtualDevice::accounts_for`]) is left alone,
    /// with the level it was set to, and one that doesn't is re-derived from
    /// them. With no member lit it goes off and keeps its level (#16).
    /// Re-deriving a group from members that haven't moved lands on the same
    /// state, and an unchanged state isn't echoed. So a group whose state
    /// changed is echoed once, however many of its members it has, and the
    /// others not at all.
    ///
    /// Physical inputs go first, so a group of groups is derived from inner
    /// groups that have caught up with their members. (Nested deeper, an
    /// outer group catches up from an inner one's echo, like after any
    /// change.) Only state is caught up: a press that was missed is gone,
    /// and running its action now, late, could undo whatever happened since.
    pub async fn resync(&self) {
        let mut inputs: Vec<DeviceId> = {
            let input_map = self.input_mappings.read().await;
            input_map.keys().cloned().collect()
        };
        {
            let devices = self.virtual_devices.read().await;
            inputs.sort_by_cached_key(|id| (devices.contains_key(id), id.clone()));
        }
        tracing::info!(
            "🔧 Catching virtual devices up with {} inputs after missing events",
            inputs.len()
        );

        for input in inputs {
            if let Err(e) = self.track_input(&input, None).await {
                tracing::error!("Failed to catch virtual devices up with {}: {}", input, e);
            }
        }
    }
}

impl Clone for VirtualDeviceManager {
    fn clone(&self) -> Self {
        Self {
            virtual_devices: Arc::clone(&self.virtual_devices),
            input_mappings: Arc::clone(&self.input_mappings),
            output_mappings: Arc::clone(&self.output_mappings),
            state_store: Arc::clone(&self.state_store),
            event_bus: Arc::clone(&self.event_bus),
            sync_engine: Arc::clone(&self.sync_engine),
            tracking: Arc::clone(&self.tracking),
            write_queue: Arc::clone(&self.write_queue),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Input tracking doesn't run on its own in most of these: [`pump`]
    //! feeds it the bus, so each test decides exactly when tracking catches
    //! up, and nothing waits on a clock.
    use super::*;
    use crate::button_controller::TestClock;
    use crate::{
        ButtonController, DummyGateway, LightGroup, LightGroupLinear, SceneController,
        VirtualWrite, DEFAULT_GROUP_LEVEL, ECHO_QUIET_PERIOD,
    };
    use std::time::Duration;
    use tokio::sync::broadcast::{self, error::TryRecvError};
    use tokio::sync::watch;
    use v1bectl_sync::{
        recv_lossy, ButtonPressType, Capability, Gateway, GatewayError, GatewayHealth, LightState,
        SceneState, SwitchState, SyncConfig, SyncStatus,
    };

    fn light_info(device_id: &str) -> DeviceInfo {
        DeviceInfo {
            device_id: device_id.to_string(),
            name: device_id.to_string(),
            device_type: DeviceType::Light,
            capabilities: vec![Capability::OnOff, Capability::Brightness],
            device_groups: vec![],
            manufacturer: None,
            model: None,
            firmware_version: None,
            battery_powered: false,
            reachable: true,
            last_seen: 0,
            custom_attributes: HashMap::new(),
        }
    }

    fn light(is_on: bool, brightness: u8) -> DeviceStateValue {
        DeviceStateValue::Light(LightState {
            is_on,
            brightness: Some(brightness),
            color_temp: Some(2700),
            rgb_color: None,
        })
    }

    /// A member light that's off.
    fn off() -> DeviceStateValue {
        light(false, 0)
    }

    /// What a group with none of its members on starts as: off, with a
    /// level to light them at (#16).
    fn group_start() -> DeviceStateValue {
        light(false, DEFAULT_GROUP_LEVEL)
    }

    /// A 1:1 `LightGroup` named `g` over `lights`, registered in `store`,
    /// with no sync engine attached and input tracking not started.
    async fn manager_with_group_in(
        store: Arc<StateStore>,
        lights: &[&str],
    ) -> (VirtualDeviceManager, Arc<EventBus>) {
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        manager
            .add_virtual_device(Box::new(group("g", lights, &store)))
            .await
            .expect("register");
        (manager, bus)
    }

    /// A 1:1 `LightGroup` named `device_id` over `lights`.
    fn group(device_id: &str, lights: &[&str], store: &Arc<StateStore>) -> LightGroup {
        let curves: serde_json::Map<String, serde_json::Value> = lights
            .iter()
            .map(|id| {
                (
                    id.to_string(),
                    serde_json::json!({ "breakpoints": [[0, 0], [100, 100]] }),
                )
            })
            .collect();
        let config = VirtualDeviceConfig {
            device_id: device_id.to_string(),
            device_type: VirtualDeviceType::LightGroup,
            name: device_id.to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({ "lights": lights, "brightness_curves": curves }),
        };
        LightGroup::new(config, store.clone()).expect("group")
    }

    async fn manager_with_group(
        lights: &[&str],
    ) -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
        let store = StateStore::new();
        let (manager, bus) = manager_with_group_in(store.clone(), lights).await;
        (manager, store, bus)
    }

    /// A `LightGroupLinear` named `device_id` over `lights`, each over the
    /// whole range, so a member's level is the group's.
    fn linear_group(device_id: &str, lights: &[&str], store: &Arc<StateStore>) -> LightGroupLinear {
        let members = lights
            .iter()
            .map(|id| (id.to_string(), id.to_string()))
            .collect();
        let ranges = lights.iter().map(|id| (id.to_string(), (0, 100))).collect();
        let config = VirtualDeviceConfig {
            device_id: device_id.to_string(),
            device_type: VirtualDeviceType::LightGroupLinear,
            name: device_id.to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        LightGroupLinear::new(config, members, ranges, store.clone()).expect("linear group")
    }

    /// Both group kinds; each has its own `calculate_group_state`.
    #[derive(Clone, Copy, Debug)]
    enum Kind {
        Curves,
        Linear,
    }

    const KINDS: [Kind; 2] = [Kind::Curves, Kind::Linear];

    /// Lights `a`, `b` and `c` in the store as `members` has them, then a
    /// 1:1 group `g` of `kind` over them, registered the way the server
    /// registers one at startup: after its members.
    async fn started_group(
        kind: Kind,
        members: [DeviceStateValue; 3],
    ) -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
        let store = StateStore::new();
        for (id, state) in ["a", "b", "c"].into_iter().zip(members) {
            store.add_device(light_info(id), state).await;
        }
        let lights = ["a", "b", "c"];
        let group: Box<dyn VirtualDevice> = match kind {
            Kind::Curves => Box::new(group("g", &lights, &store)),
            Kind::Linear => Box::new(linear_group("g", &lights, &store)),
        };
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        manager.add_virtual_device(group).await.expect("register");
        (manager, store, bus)
    }

    /// A plain `on`, as the API makes it of `{is_on: true}` (the widget's
    /// toggle): the group's stored state, switched on.
    async fn plain_on(manager: &VirtualDeviceManager, store: &StateStore, device_id: &str) {
        let DeviceStateValue::Light(mut state) = stored(store, device_id).await else {
            panic!("{device_id} isn't a light");
        };
        state.is_on = true;
        manager
            .set_virtual_device_state(&device_id.to_string(), DeviceStateValue::Light(state))
            .await
            .expect("plain on");
    }

    fn switch(is_pressed: bool) -> DeviceStateValue {
        switch_at(is_pressed, 85)
    }

    fn switch_at(is_pressed: bool, battery_level: u8) -> DeviceStateValue {
        DeviceStateValue::Switch(SwitchState {
            is_pressed,
            last_pressed: None,
            battery_level: Some(battery_level),
        })
    }

    /// A released switch `button` in `store`.
    async fn add_switch(store: &StateStore, button: &str) {
        let info = DeviceInfo {
            device_type: DeviceType::Switch,
            capabilities: vec![Capability::OnOff],
            ..light_info(button)
        };
        store.add_device(info, switch(false)).await;
    }

    /// A button controller `device_id` bound to `button`, that runs
    /// `press_on` on a press and `press_off` on a release.
    fn controller(
        device_id: &str,
        button: &str,
        press_on: serde_json::Value,
        press_off: serde_json::Value,
    ) -> ButtonController {
        let none = serde_json::json!([]);
        let actions = [press_on, press_off, none.clone(), none.clone(), none];
        controller_with(device_id, button, actions)
    }

    /// A button controller `device_id` bound to `button`, with `actions`:
    /// `[press_on, press_off, press_on_long, press_off_long,
    /// press_double]`, each `[]` for none.
    fn controller_with(
        device_id: &str,
        button: &str,
        actions: [serde_json::Value; 5],
    ) -> ButtonController {
        let config = VirtualDeviceConfig {
            device_id: device_id.to_string(),
            device_type: VirtualDeviceType::ButtonController,
            name: device_id.to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let [press_on, press_off, press_on_long, press_off_long, press_double] =
            actions.map(|value| value.as_array().cloned().expect("action"));
        ButtonController::new(
            config,
            button.to_string(),
            &press_on,
            &press_off,
            Some(&press_on_long),
            Some(&press_off_long),
            Some(&press_double),
        )
        .expect("controller")
    }

    /// A released switch `btn` in the store, and a button controller `ctrl`
    /// bound to it that runs `press_on` on a press and `press_off` on a
    /// release.
    async fn add_controller(
        manager: &VirtualDeviceManager,
        store: &StateStore,
        press_on: serde_json::Value,
        press_off: serde_json::Value,
    ) {
        add_switch(store, "btn").await;
        manager
            .add_virtual_device(Box::new(controller("ctrl", "btn", press_on, press_off)))
            .await
            .expect("register");
    }

    /// `btn` pressed (or released), as the sync engine reports it when the
    /// hub says so: stored, and echoed.
    async fn press(store: &StateStore, bus: &EventBus, is_pressed: bool) {
        press_switch(store, bus, "btn", is_pressed).await;
    }

    /// `button` pressed (or released), as the sync engine reports it when
    /// the hub says so: stored, and echoed.
    async fn press_switch(store: &StateStore, bus: &EventBus, button: &str, is_pressed: bool) {
        let button = button.to_string();
        let old = stored(store, &button).await;
        let new = switch(is_pressed);
        store
            .update_device_state(&button, new.clone())
            .await
            .unwrap();
        bus.publish(state_event(&button, Some(&old), &new)).await;
    }

    /// `button` reports `press_type`, as a hub reports a gesture it
    /// recognised over its event stream: a Dirigera remote's
    /// `clickPattern`, or the dummy's (#35).
    async fn report(bus: &EventBus, button: &str, press_type: ButtonPressType) {
        bus.publish(DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: button.to_string(),
            event_type: EventType::ButtonPressed {
                button_id: "main".to_string(),
                press_type,
            },
        })
        .await;
    }

    /// Input tracking, caught up: hands the manager every event published so
    /// far, in order, including the ones that handling publishes in turn.
    /// Returns them all.
    async fn pump(
        manager: &VirtualDeviceManager,
        rx: &mut broadcast::Receiver<DeviceEvent>,
    ) -> Vec<DeviceEvent> {
        let mut events = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => {
                    manager.handle_event(&event).await.expect("input tracking");
                    events.push(event);
                }
                Err(TryRecvError::Empty) => return events,
                Err(e) => panic!("event bus: {e}"),
            }
        }
    }

    fn echoes(events: &[DeviceEvent], device_id: &str) -> Vec<DeviceStateValue> {
        events
            .iter()
            .filter(|e| e.device_id == device_id)
            .filter_map(|e| match &e.event_type {
                EventType::AttributeChanged {
                    attribute,
                    new_value,
                    ..
                } if attribute == "state" => serde_json::from_value(new_value.clone()).ok(),
                _ => None,
            })
            .collect()
    }

    async fn stored(store: &StateStore, device_id: &str) -> DeviceStateValue {
        store
            .get_device(&device_id.to_string())
            .await
            .unwrap()
            .state
    }

    /// Without a sync engine the manager echoes the members itself: once
    /// each, next to the group's own echo.
    #[tokio::test]
    async fn group_write_without_sync_engine_echoes_group_and_members() {
        let (manager, store, bus) = manager_with_group(&["a", "b"]).await;
        for id in ["a", "b"] {
            store.add_device(light_info(id), off()).await;
        }
        let mut rx = bus.subscribe();

        let on = light(true, 60);
        manager
            .set_virtual_device_state(&"g".to_string(), on.clone())
            .await
            .expect("write");
        let events = pump(&manager, &mut rx).await;

        assert_eq!(stored(&store, "g").await, on);
        assert_eq!(echoes(&events, "g"), vec![on.clone()], "group echo");
        for id in ["a", "b"] {
            // 1:1 curves: members carry the group state verbatim.
            assert_eq!(stored(&store, id).await, on, "{id} state");
            assert_eq!(echoes(&events, id), vec![on.clone()], "{id} echo");
        }
    }

    /// With optimistic updates the sync engine echoes each member, and the
    /// manager must not echo it again. Without them the engine only queues
    /// the push and echoes nothing, so the echo is the manager's (#14
    /// review, finding 3). Either way: once per member, and pushed.
    #[tokio::test]
    async fn members_echo_once_with_and_without_optimistic_updates() {
        for optimistic_updates in [true, false] {
            let (manager, store, bus) = manager_with_group(&["a", "b"]).await;
            for id in ["a", "b"] {
                store.add_device(light_info(id), off()).await;
            }
            let engine = Arc::new(SyncEngine::new(
                store.clone(),
                bus.clone(),
                Arc::new(DummyGateway::new("basic_home")),
                Some(SyncConfig {
                    optimistic_updates,
                    ..SyncConfig::default()
                }),
            ));
            manager.attach_sync_engine(engine.clone());
            let mut rx = bus.subscribe();

            let on = light(true, 60);
            manager
                .set_virtual_device_state(&"g".to_string(), on.clone())
                .await
                .expect("write");
            let events = pump(&manager, &mut rx).await;

            assert_eq!(
                echoes(&events, "g"),
                vec![on.clone()],
                "group echo (optimistic_updates: {optimistic_updates})"
            );
            for id in ["a", "b"] {
                assert_eq!(stored(&store, id).await, on, "{id} state");
                assert_eq!(
                    echoes(&events, id),
                    vec![on.clone()],
                    "{id} must be echoed exactly once (optimistic_updates: {optimistic_updates})"
                );
                let status = engine.get_sync_status(&id.to_string()).await;
                assert!(
                    matches!(status, Some(SyncStatus::PendingSync { .. })),
                    "{id} never queued for the gateway: {status:?}"
                );
            }
        }
    }

    /// A write that fails part-way (a missing member, as in #2) commits and
    /// echoes the members it did change. The group keeps its old state,
    /// which doesn't account for them, so tracking re-derives it from the
    /// members as they are and echoes that. It must not claim the state it
    /// was asked for: a member after the failure never got it (#14 review,
    /// finding 5).
    #[tokio::test]
    async fn partial_group_write_re_derives_the_group() {
        // `LightGroup` writes its lights in order: `a`, then `missing`
        // fails, so `b` is never written.
        let (manager, store, bus) = manager_with_group(&["a", "missing", "b"]).await;
        for id in ["a", "b"] {
            store.add_device(light_info(id), light(true, 80)).await;
        }
        let mut rx = bus.subscribe();

        let asked = light(true, 20);
        let result = manager
            .set_virtual_device_state(&"g".to_string(), asked.clone())
            .await;
        assert!(result.is_err(), "a missing member must fail the write");
        let events = pump(&manager, &mut rx).await;

        assert_eq!(stored(&store, "a").await, asked);
        assert_eq!(echoes(&events, "a"), vec![asked], "changed member echo");
        assert_eq!(stored(&store, "b").await, light(true, 80), "b was written");
        assert!(echoes(&events, "b").is_empty(), "unchanged member echoed");
        // Re-derived from `a` at 20 and `b` at 80.
        let group = stored(&store, "g").await;
        assert_eq!(group, light(true, 50), "group must follow its members");
        assert_eq!(echoes(&events, "g"), vec![group.clone()], "group echo");
        assert_eq!(
            manager
                .get_virtual_device_state(&"g".to_string())
                .await
                .unwrap(),
            group,
            "the group's own state must match the store"
        );
    }

    /// A write that fails before it changed anything leaves the group where
    /// it was, in the store and in its own state, and echoes nothing (#14
    /// review, finding 5).
    #[tokio::test]
    async fn failed_group_write_that_changed_nothing_keeps_the_group() {
        // `missing` comes first, so the write fails before it reaches `a`.
        let (manager, store, bus) = manager_with_group(&["missing", "a"]).await;
        store.add_device(light_info("a"), off()).await;
        let mut rx = bus.subscribe();

        let result = manager
            .set_virtual_device_state(&"g".to_string(), light(true, 60))
            .await;
        assert!(result.is_err(), "a missing member must fail the write");
        let events = pump(&manager, &mut rx).await;

        assert!(
            events.is_empty(),
            "nothing changed, nothing to echo: {events:?}"
        );
        assert_eq!(stored(&store, "a").await, off());
        assert_eq!(stored(&store, "g").await, group_start());
        assert_eq!(
            manager
                .get_virtual_device_state(&"g".to_string())
                .await
                .unwrap(),
            group_start(),
            "the group's own state must match the store"
        );
    }

    /// #55: through the sync engine, a write that fails part-way stops at
    /// the member that fails, as it does without one. The members before it
    /// are committed through the engine. The missing one, and the ones after
    /// it, never reach the engine: the manager finds the missing one gone
    /// before it asks, so it queues no push for a device the store doesn't
    /// have.
    #[tokio::test]
    async fn partial_group_write_through_the_engine_stops_at_the_missing_member() {
        for optimistic_updates in [true, false] {
            let (manager, store, bus) = manager_with_group(&["a", "missing", "b"]).await;
            for id in ["a", "b"] {
                store.add_device(light_info(id), light(true, 80)).await;
            }
            let engine = Arc::new(SyncEngine::new(
                store.clone(),
                bus.clone(),
                Arc::new(DummyGateway::new("basic_home")),
                Some(SyncConfig {
                    optimistic_updates,
                    ..SyncConfig::default()
                }),
            ));
            manager.attach_sync_engine(engine.clone());
            let mut rx = bus.subscribe();

            let asked = light(true, 20);
            let result = manager
                .set_virtual_device_state(&"g".to_string(), asked.clone())
                .await;
            assert!(
                matches!(
                    &result,
                    Err(VirtualDeviceError::StateStore(StateError::DeviceNotFound(id)))
                        if id == "missing"
                ),
                "optimistic_updates: {optimistic_updates}: {result:?}"
            );
            let events = pump(&manager, &mut rx).await;

            assert_eq!(stored(&store, "a").await, asked);
            assert_eq!(echoes(&events, "a"), vec![asked.clone()], "a's echo");
            let status = engine.get_sync_status(&"a".to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "a never queued for the gateway: {status:?}"
            );
            for id in ["missing", "b"] {
                let status = engine.get_sync_status(&id.to_string()).await;
                assert!(status.is_none(), "{id} reached the engine: {status:?}");
            }
            assert_eq!(stored(&store, "b").await, light(true, 80), "b was written");
            assert!(echoes(&events, "b").is_empty(), "unchanged member echoed");
            // Re-derived from `a` at 20 and `b` at 80.
            assert_eq!(stored(&store, "g").await, light(true, 50), "group");
        }
    }

    /// #55: a scene hands its devices' states to the manager too, and the
    /// manager commits them through the sync engine: each stored, echoed
    /// once, and queued for the gateway. Then the scene is active.
    #[tokio::test]
    async fn scene_activation_commits_its_devices_through_the_engine() {
        let store = StateStore::new();
        for id in ["a", "b"] {
            store.add_device(light_info(id), off()).await;
        }
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let engine = attach_engine(&manager, &store, &bus);
        let on = light(true, 50);
        let scene = SceneController::new(
            VirtualDeviceConfig {
                device_id: "scene".to_string(),
                device_type: VirtualDeviceType::SceneController,
                name: "Scene".to_string(),
                description: None,
                enabled: true,
                config: serde_json::json!({ "scenes": { "evening": {
                    "name": "evening",
                    "device_states": { "a": on, "b": on },
                    "transition_type": "Instant",
                } } }),
            },
            store.clone(),
        )
        .expect("scene");
        manager
            .add_virtual_device(Box::new(scene))
            .await
            .expect("register");
        let mut rx = bus.subscribe();

        let evening = DeviceStateValue::Scene(SceneState {
            scene_name: "evening".to_string(),
            is_active: true,
        });
        manager
            .set_virtual_device_state(&"scene".to_string(), evening.clone())
            .await
            .expect("activate");
        let events = pump(&manager, &mut rx).await;

        for id in ["a", "b"] {
            assert_eq!(stored(&store, id).await, on, "{id} state");
            assert_eq!(echoes(&events, id), vec![on.clone()], "{id} echo");
            let status = engine.get_sync_status(&id.to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "{id} never queued for the gateway: {status:?}"
            );
        }
        assert_eq!(stored(&store, "scene").await, evening);
        assert_eq!(echoes(&events, "scene"), vec![evening], "scene echo");
    }

    /// Scene `name`, as a scene controller's config has it: it takes each
    /// of `devices` to its state with `transition` (a `TransitionType`).
    fn scene_config(
        name: &str,
        devices: &[(&str, DeviceStateValue)],
        transition: &serde_json::Value,
    ) -> (String, serde_json::Value) {
        let device_states: serde_json::Map<String, serde_json::Value> = devices
            .iter()
            .map(|(id, state)| ((*id).to_string(), serde_json::json!(state)))
            .collect();
        let scene = serde_json::json!({
            "name": name,
            "device_states": device_states,
            "transition_type": transition,
        });
        (name.to_string(), scene)
    }

    /// Scene controller `scene`, with `scenes` (see [`scene_config`]).
    fn scene_controller(
        store: &Arc<StateStore>,
        scenes: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> SceneController {
        let scenes: serde_json::Map<String, serde_json::Value> = scenes.into_iter().collect();
        SceneController::new(
            VirtualDeviceConfig {
                device_id: "scene".to_string(),
                device_type: VirtualDeviceType::SceneController,
                name: "Scene".to_string(),
                description: None,
                enabled: true,
                config: serde_json::json!({ "scenes": scenes }),
            },
            store.clone(),
        )
        .expect("scene")
    }

    /// Scene controller `scene`, with one scene, `evening` (see
    /// [`scene_config`]).
    fn scene(
        store: &Arc<StateStore>,
        devices: &[(&str, DeviceStateValue)],
        transition: &serde_json::Value,
    ) -> SceneController {
        scene_controller(store, [scene_config("evening", devices, transition)])
    }

    /// The scene `evening`, active: what activates it.
    fn evening() -> DeviceStateValue {
        DeviceStateValue::Scene(SceneState {
            scene_name: "evening".to_string(),
            is_active: true,
        })
    }

    /// A scene controller's state before any scene is activated.
    fn no_scene() -> DeviceStateValue {
        DeviceStateValue::Scene(SceneState {
            scene_name: "none".to_string(),
            is_active: false,
        })
    }

    /// #58: a fade commits where it ends, each device at its target, once
    /// its whole duration has passed (ten 100 ms steps for a second). Its
    /// steps are planned on the scene's own copy of the states (#55), so
    /// neither the store nor the bus sees one: each device is echoed once,
    /// at its target, the dimmed one and the one switched on alike.
    ///
    /// The clock is paused, so the delays pass at once and measure exactly.
    #[tokio::test(start_paused = true)]
    async fn a_fade_ends_at_its_targets_once_its_duration_has_passed() {
        let store = StateStore::new();
        store.add_device(light_info("a"), off()).await;
        store.add_device(light_info("b"), light(true, 90)).await;
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let targets = [("a", light(true, 80)), ("b", light(true, 20))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        manager
            .add_virtual_device(Box::new(scene(&store, &targets, &fade)))
            .await
            .expect("register");
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        manager
            .set_virtual_device_state(&"scene".to_string(), evening())
            .await
            .expect("activate");
        assert_eq!(started.elapsed(), Duration::from_secs(1), "fade time");
        let events = pump(&manager, &mut rx).await;

        for (id, target) in &targets {
            assert_eq!(&stored(&store, id).await, target, "{id} ends at its target");
            assert_eq!(echoes(&events, id), vec![target.clone()], "{id}'s echoes");
        }
        assert_eq!(stored(&store, "scene").await, evening());
        assert_eq!(echoes(&events, "scene"), vec![evening()], "scene echo");
    }

    /// #58: a sequence commits each of its devices at its target, once all
    /// of its delays have passed. (The order it takes them in, each after
    /// its own delay, is `scene_controller`'s test.)
    #[tokio::test(start_paused = true)]
    async fn a_sequence_ends_at_its_targets_once_its_delays_have_passed() {
        let store = StateStore::new();
        for id in ["a", "b", "c"] {
            store.add_device(light_info(id), off()).await;
        }
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let targets = [
            ("a", light(true, 10)),
            ("b", light(true, 20)),
            ("c", light(true, 30)),
        ];
        let sequence = serde_json::json!({ "Sequence": { "delays_ms": [100, 200, 300] } });
        manager
            .add_virtual_device(Box::new(scene(&store, &targets, &sequence)))
            .await
            .expect("register");
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        manager
            .set_virtual_device_state(&"scene".to_string(), evening())
            .await
            .expect("activate");
        assert_eq!(started.elapsed(), Duration::from_millis(600), "delays");
        let events = pump(&manager, &mut rx).await;

        for (id, target) in &targets {
            assert_eq!(&stored(&store, id).await, target, "{id} ends at its target");
            assert_eq!(echoes(&events, id), vec![target.clone()], "{id}'s echoes");
        }
        assert_eq!(stored(&store, "scene").await, evening());
        assert_eq!(echoes(&events, "scene"), vec![evening()], "scene echo");
    }

    /// #58: a fade over a device the store doesn't have fails at its first
    /// step, without sleeping through the rest, with the error a group's
    /// missing member gets. What the first step set the other device to is
    /// where it already was, so nothing is committed or echoed, and the
    /// scene isn't active.
    #[tokio::test(start_paused = true)]
    async fn a_fade_over_a_missing_device_fails_at_its_first_step_without_sleeping() {
        let store = StateStore::new();
        store.add_device(light_info("a"), off()).await;
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let targets = [("a", light(true, 80)), ("missing", light(true, 80))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        manager
            .add_virtual_device(Box::new(scene(&store, &targets, &fade)))
            .await
            .expect("register");
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        let result = manager
            .set_virtual_device_state(&"scene".to_string(), evening())
            .await;
        assert_eq!(started.elapsed(), Duration::ZERO, "it slept");
        assert!(
            matches!(
                &result,
                Err(VirtualDeviceError::StateStore(StateError::DeviceNotFound(id)))
                    if id == "missing"
            ),
            "{result:?}"
        );
        let events = pump(&manager, &mut rx).await;

        assert!(events.is_empty(), "nothing to echo: {events:?}");
        assert_eq!(stored(&store, "a").await, off());
        assert_eq!(stored(&store, "scene").await, no_scene());
        assert_eq!(
            manager
                .get_virtual_device_state(&"scene".to_string())
                .await
                .unwrap(),
            no_scene(),
            "the scene's own state must match the store"
        );
    }

    /// A one-second fade of `a` to 80, activated on its own task, and where
    /// its lights are when it ends.
    fn fade_of_a() -> (
        (String, serde_json::Value),
        [(&'static str, DeviceStateValue); 1],
    ) {
        let targets = [("a", light(true, 80))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        (scene_config("evening", &targets, &fade), targets)
    }

    fn activate(
        manager: &VirtualDeviceManager,
        scene_name: &str,
    ) -> tokio::task::JoinHandle<Result<(), VirtualDeviceError>> {
        let manager = manager.clone();
        let state = DeviceStateValue::Scene(SceneState {
            scene_name: scene_name.to_string(),
            is_active: true,
        });
        tokio::spawn(async move {
            manager
                .set_virtual_device_state(&"scene".to_string(), state)
                .await
        })
    }

    /// #58: a scene's transition doesn't hold the manager's lock while it
    /// waits. A write to an unrelated group, 100 ms into a one-second fade,
    /// lands at once, with the fade still running. It used to wait out the
    /// rest of the fade, about 900 ms, and input tracking, resync and
    /// button presses with it. The fade still ends at its target after its
    /// full second. (A paused clock: "at once" is exactly zero.)
    ///
    /// Only the fading scene's lights are held: the group's `c` is in
    /// another scene of the same controller, and that doesn't make the
    /// group write wait.
    #[tokio::test(start_paused = true)]
    async fn a_group_write_does_not_wait_for_a_fade() {
        let store = StateStore::new();
        for id in ["a", "c", "d"] {
            store.add_device(light_info(id), off()).await;
        }
        let (manager, _bus) = manager_with_group_in(store.clone(), &["c", "d"]).await;
        let (evening_config, targets) = fade_of_a();
        let morning_config = scene_config(
            "morning",
            &[("c", light(true, 30))],
            &serde_json::json!("Instant"),
        );
        manager
            .add_virtual_device(Box::new(scene_controller(
                &store,
                [evening_config, morning_config],
            )))
            .await
            .expect("register");

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let asked = tokio::time::Instant::now();
        manager
            .set_virtual_device_state(&"g".to_string(), light(true, 60))
            .await
            .expect("group write");
        assert_eq!(
            asked.elapsed(),
            Duration::ZERO,
            "the group write waited for the fade"
        );
        assert!(!fade.is_finished(), "the fade is over already");
        for id in ["c", "d"] {
            assert_eq!(stored(&store, id).await, light(true, 60), "{id}");
        }

        fade.await.expect("fade task").expect("fade");
        assert_eq!(started.elapsed(), Duration::from_secs(1), "fade time");
        for (id, target) in &targets {
            assert_eq!(&stored(&store, id).await, target, "{id} ends at its target");
        }
        assert_eq!(stored(&store, "scene").await, evening());
    }

    /// `f` on a task of its own, which returns when it finished.
    fn timed<F>(f: F) -> tokio::task::JoinHandle<(F::Output, tokio::time::Instant)>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send,
    {
        tokio::spawn(async move {
            let output = f.await;
            (output, tokio::time::Instant::now())
        })
    }

    /// A click of `button`, as a hub reports it.
    fn click(button: &str) -> DeviceEvent {
        DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: button.to_string(),
            event_type: EventType::ButtonPressed {
                button_id: "main".to_string(),
                press_type: ButtonPressType::SinglePress,
            },
        }
    }

    /// Lights `a` to `d`, off; group `g` over `a` and `c`; scene controller
    /// `scene` with `evening`, a one-second fade of `a` and `b` to 80, and
    /// `morning`, an instant scene of `d` at 30; and controller `ctrl`,
    /// whose `btn` click turns `a` off.
    async fn sunset_with_overlapping_writes(
    ) -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
        let store = StateStore::new();
        for id in ["a", "b", "c", "d"] {
            store.add_device(light_info(id), off()).await;
        }
        let (manager, bus) = manager_with_group_in(store.clone(), &["a", "c"]).await;
        let targets = [("a", light(true, 80)), ("b", light(true, 80))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        let evening_config = scene_config("evening", &targets, &fade);
        let morning_config = scene_config(
            "morning",
            &[("d", light(true, 30))],
            &serde_json::json!("Instant"),
        );
        manager
            .add_virtual_device(Box::new(scene_controller(
                &store,
                [evening_config, morning_config],
            )))
            .await
            .expect("register");
        let all_off = serde_json::json!(["off", "a"]);
        let controller = controller("ctrl", "btn", all_off, serde_json::json!([]));
        manager
            .add_virtual_device(Box::new(controller))
            .await
            .expect("register");
        (manager, store, bus)
    }

    /// #58: a write that overlaps a scene's fade waits for the fade's
    /// commit, and lands after it, so the newest write ends up showing, as
    /// on `main` (see [`VirtualDeviceManager::set_virtual_device_state`]).
    ///
    /// During a one-second fade of `a` and `b` to 80, each of these asks in
    /// turn, 100 ms apart, and overlaps it:
    ///
    /// - a write to group `g` over `a` and `c`, at 60 (it shares `a`);
    /// - `morning`, an instant scene of the same controller over `d` only
    ///   (it's the same scene controller);
    /// - a button press whose action turns `a` off: an "all off" in the
    ///   middle of a sunset fade.
    ///
    /// None of them lands during the fade, not even on `c` or `d`. Each
    /// lands once the fade has committed, in the order they asked. So `a`
    /// ends off, where the press put it: the fade doesn't turn it back on.
    #[tokio::test(start_paused = true)]
    async fn writes_that_overlap_a_fade_wait_for_it_and_the_newest_shows() {
        let (manager, store, bus) = sunset_with_overlapping_writes().await;
        let morning = DeviceStateValue::Scene(SceneState {
            scene_name: "morning".to_string(),
            is_active: true,
        });
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let group_write = timed({
            let manager = manager.clone();
            async move {
                manager
                    .set_virtual_device_state(&"g".to_string(), light(true, 60))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let morning_write = activate(&manager, "morning");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let press = timed({
            let manager = manager.clone();
            async move { manager.handle_event(&click("btn")).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 400 ms in: each of them waits for the fade.
        assert!(!group_write.is_finished(), "the group write didn't wait");
        assert!(!morning_write.is_finished(), "morning didn't wait");
        assert!(!press.is_finished(), "the press didn't wait");
        for id in ["a", "b", "c", "d"] {
            assert_eq!(
                stored(&store, id).await,
                off(),
                "{id} moved during the fade"
            );
        }

        fade.await.expect("fade task").expect("fade");
        let fade_committed = tokio::time::Instant::now();
        assert_eq!(fade_committed - started, Duration::from_secs(1), "fade");
        let (result, group_landed) = group_write.await.expect("group write task");
        result.expect("group write");
        morning_write.await.expect("morning task").expect("morning");
        let (result, press_landed) = press.await.expect("press task");
        result.expect("input tracking");
        assert_eq!(group_landed, fade_committed, "the group write's commit");
        assert_eq!(press_landed, fade_committed, "the press's commit");

        let events = pump(&manager, &mut rx).await;
        assert_eq!(
            echoes(&events, "a"),
            vec![light(true, 80), light(true, 60), light(false, 60)],
            "a's echoes: the fade, the group write, the press"
        );
        assert_eq!(echoes(&events, "c"), vec![light(true, 60)], "c's echoes");
        assert_eq!(echoes(&events, "d"), vec![light(true, 30)], "d's echoes");
        assert_eq!(
            echoes(&events, "scene"),
            vec![evening(), morning.clone()],
            "the scene's echoes: the fade, then morning"
        );
        assert_eq!(stored(&store, "a").await, light(false, 60), "a, off");
        assert_eq!(stored(&store, "b").await, light(true, 80), "b");
        assert_eq!(stored(&store, "scene").await, morning, "morning active");
        assert_eq!(
            manager
                .get_virtual_device_state(&"scene".to_string())
                .await
                .unwrap(),
            morning,
            "the scene's own state must match the store"
        );
    }

    /// #58: writes over the same devices in opposite member order can't
    /// deadlock: a write waits in the queue for the writes that asked
    /// before it, all its devices at once, whatever order its members come
    /// in.
    ///
    /// Group `g1` lists `a` then `b`, and `g2` lists `b` then `a`. Both ask
    /// during a fade over `a` and `b`, so both wait, and both can go when
    /// the fade is over. With per-device locks taken in member order, `g1`
    /// would get `a` and wait for `b` while `g2` got `b` and waited for
    /// `a`. Both finish, bounded by a timeout, in the order they asked, so
    /// `g2`'s level ends up showing.
    #[tokio::test(start_paused = true)]
    async fn overlapping_writes_in_opposite_member_order_do_not_deadlock() {
        let store = StateStore::new();
        for id in ["a", "b"] {
            store.add_device(light_info(id), off()).await;
        }
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        for (id, lights) in [("g1", ["a", "b"]), ("g2", ["b", "a"])] {
            manager
                .add_virtual_device(Box::new(group(id, &lights, &store)))
                .await
                .expect("register");
        }
        let targets = [("a", light(true, 80)), ("b", light(true, 80))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        manager
            .add_virtual_device(Box::new(scene(&store, &targets, &fade)))
            .await
            .expect("register");

        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let write = |id: &'static str, level| {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .set_virtual_device_state(&id.to_string(), light(true, level))
                    .await
            })
        };
        let (g1, g2) = (write("g1", 40), write("g2", 70));

        tokio::time::timeout(Duration::from_secs(10), async {
            fade.await.expect("fade task").expect("fade");
            g1.await.expect("g1 task").expect("g1");
            g2.await.expect("g2 task").expect("g2");
        })
        .await
        .expect("deadlocked");
        for id in ["a", "b"] {
            assert_eq!(stored(&store, id).await, light(true, 70), "{id}: g2 last");
        }
        assert_eq!(
            manager.write_queue.len(),
            0,
            "the queue holds only writes in flight"
        );
    }

    /// #58: a scene controller removed during its fade goes once the fade
    /// has committed, as when the fade held the manager's lock throughout.
    #[tokio::test(start_paused = true)]
    async fn a_scene_removed_during_its_fade_goes_once_the_fade_has_committed() {
        let store = StateStore::new();
        store.add_device(light_info("a"), off()).await;
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let (evening_config, targets) = fade_of_a();
        manager
            .add_virtual_device(Box::new(scene_controller(&store, [evening_config])))
            .await
            .expect("register");

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        manager
            .remove_virtual_device(&"scene".to_string())
            .await
            .expect("remove");
        assert_eq!(started.elapsed(), Duration::from_secs(1), "removal waited");
        assert!(fade.is_finished(), "removed before the fade committed");

        fade.await.expect("fade task").expect("fade");
        for (id, target) in &targets {
            assert_eq!(
                &stored(&store, id).await,
                target,
                "{id}: the fade committed"
            );
        }
        assert!(store.get_device(&"scene".to_string()).await.is_none());
        assert!(manager
            .get_virtual_device_state(&"scene".to_string())
            .await
            .is_err());
    }

    /// Lights `lights`, off; a 1:1 group for each of `groups`; and scene
    /// controller `scene`, whose `evening` fades `a` and `b` to 80 over a
    /// second.
    async fn fade_of_a_and_b_with(
        lights: &[&str],
        groups: &[(&str, &[&str])],
    ) -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
        let store = StateStore::new();
        for id in lights {
            store.add_device(light_info(id), off()).await;
        }
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        for (id, members) in groups {
            manager
                .add_virtual_device(Box::new(group(id, members, &store)))
                .await
                .expect("register");
        }
        let targets = [("a", light(true, 80)), ("b", light(true, 80))];
        let fade = serde_json::json!({ "Fade": { "duration_ms": 1000 } });
        manager
            .add_virtual_device(Box::new(scene(&store, &targets, &fade)))
            .await
            .expect("register");
        (manager, store, bus)
    }

    /// A write of `state` to `id` on a task of its own, which returns when
    /// it finished.
    fn spawn_write(
        manager: &VirtualDeviceManager,
        id: &'static str,
        state: DeviceStateValue,
    ) -> tokio::task::JoinHandle<(Result<(), VirtualDeviceError>, tokio::time::Instant)> {
        let manager = manager.clone();
        timed(async move {
            manager
                .set_virtual_device_state(&id.to_string(), state)
                .await
        })
    }

    /// #60 review, finding 1 (R1): a later write doesn't overtake an
    /// earlier one that waits, when they share a device the earlier one
    /// waits with.
    ///
    /// During a one-second fade of `a` and `b`, `g1` (over `a` and `z`)
    /// asks for 60, and waits for the fade on `a`. Then `g2` (over `z`
    /// alone) asks for 20. `z` is free, but `g1` is ahead of `g2` on it, so
    /// `g2` waits for `g1`. Both land once the fade has committed, `g1`
    /// first, and `z` ends at 20, the newest write, as on `main`. Per-device
    /// locks taken one at a time let `g2` land at once, and `g1` then
    /// overwrote it with 60.
    #[tokio::test(start_paused = true)]
    async fn a_later_write_does_not_overtake_an_earlier_waiting_one() {
        let (manager, store, bus) =
            fade_of_a_and_b_with(&["a", "b", "z"], &[("g1", &["a", "z"]), ("g2", &["z"])]).await;
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let g1 = spawn_write(&manager, "g1", light(true, 60));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let g2 = spawn_write(&manager, "g2", light(true, 20));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !g2.is_finished(),
            "g2 overtook g1, which waits for the fade"
        );
        assert_eq!(stored(&store, "z").await, off(), "z moved during the fade");

        fade.await.expect("fade task").expect("fade");
        let (result, g1_landed) = g1.await.expect("g1 task");
        result.expect("g1");
        let (result, g2_landed) = g2.await.expect("g2 task");
        result.expect("g2");
        assert_eq!(g1_landed - started, Duration::from_secs(1), "g1 landed");
        assert_eq!(g2_landed - started, Duration::from_secs(1), "g2 landed");

        let events = pump(&manager, &mut rx).await;
        assert_eq!(
            echoes(&events, "z"),
            vec![light(true, 60), light(true, 20)],
            "z's echoes: g1, then g2"
        );
        assert_eq!(
            stored(&store, "z").await,
            light(true, 20),
            "the newest shows"
        );
        assert_eq!(manager.write_queue.len(), 0, "writes left in the queue");
    }

    /// #60 review, finding 3 (mutant B): a button press on a group queues
    /// on the group's members too, so a press on a group that shares a
    /// light with a fade waits for the fade, and the press shows.
    ///
    /// Group `g` over `a` and `c` is on at 60. During a fade of `a` to 80,
    /// a click turns `g` off. It lands once the fade has committed, and
    /// `a` ends off.
    #[tokio::test(start_paused = true)]
    async fn a_press_on_a_group_waits_for_a_fade_over_its_members() {
        let (manager, store, bus) =
            fade_of_a_and_b_with(&["a", "b", "c"], &[("g", &["a", "c"])]).await;
        let all_off = serde_json::json!(["off", "g"]);
        let controller = controller("ctrl", "btn", all_off, serde_json::json!([]));
        manager
            .add_virtual_device(Box::new(controller))
            .await
            .expect("register");
        manager
            .set_virtual_device_state(&"g".to_string(), light(true, 60))
            .await
            .expect("g on");
        let mut rx = bus.subscribe();

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let press = timed({
            let manager = manager.clone();
            async move { manager.handle_event(&click("btn")).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!press.is_finished(), "the press didn't wait for the fade");

        fade.await.expect("fade task").expect("fade");
        let (result, landed) = press.await.expect("press task");
        result.expect("input tracking");
        assert_eq!(landed - started, Duration::from_secs(1), "the press landed");
        let events = pump(&manager, &mut rx).await;
        assert_eq!(
            echoes(&events, "a"),
            vec![light(true, 80), light(false, 0)],
            "a's echoes: the fade, then the press"
        );
        for id in ["a", "c"] {
            assert_eq!(stored(&store, id).await, light(false, 0), "{id} is off");
        }
    }

    /// #60 review, finding 3 (mutant C): a write whose devices change while
    /// it waits in the queue (an add or a removal ahead of it changed its
    /// target) joins again with the new devices, and waits for those too.
    ///
    /// The write names `a`, which another write holds, and `z` is held too.
    /// While it waits, what it names grows to `a` and `z`. When `a` is let
    /// go, the write must not go: it joins again, and waits for `z`.
    #[tokio::test(start_paused = true)]
    async fn a_write_whose_devices_change_while_it_waits_joins_again_with_them() {
        let manager = VirtualDeviceManager::new(StateStore::new(), Arc::new(EventBus::new(10)));
        let on_a = manager.write_queue.join(["a".to_string()]);
        let on_z = manager.write_queue.join(["z".to_string()]);
        let names = Arc::new(std::sync::Mutex::new(vec!["a".to_string()]));
        let write = tokio::spawn({
            let (manager, names) = (manager.clone(), Arc::clone(&names));
            async move {
                let (turn, devices) = manager
                    .lock_for_write(|_| names.lock().expect("names").clone())
                    .await;
                drop((devices, turn));
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!write.is_finished(), "it went before a was let go");

        names.lock().expect("names").push("z".to_string());
        drop(on_a);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!write.is_finished(), "it went without its turn on z");

        drop(on_z);
        tokio::time::timeout(Duration::from_secs(1), write)
            .await
            .expect("it never went")
            .expect("write task");
        assert_eq!(manager.write_queue.len(), 0, "writes left in the queue");
    }

    /// A write dropped while it waits in the queue (a client that went
    /// away) leaves the queue at once. A later write that waited for it
    /// goes, and the dropped write commits nothing.
    ///
    /// During a fade of `a`, `g1` (over `a` and `z`) waits for it, and `g2`
    /// (over `z`) waits for `g1`. `g1` is dropped: `g2` lands at once,
    /// without waiting for the fade.
    #[tokio::test(start_paused = true)]
    async fn a_write_dropped_while_it_waits_lets_later_writes_go() {
        let (manager, store, _bus) =
            fade_of_a_and_b_with(&["a", "b", "z"], &[("g1", &["a", "z"]), ("g2", &["z"])]).await;

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let g1 = spawn_write(&manager, "g1", light(true, 60));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let g2 = spawn_write(&manager, "g2", light(true, 20));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!g2.is_finished(), "g2 overtook g1");

        g1.abort();
        assert!(g1.await.expect_err("g1 finished").is_cancelled());
        let (result, g2_landed) = tokio::time::timeout(Duration::from_millis(1), g2)
            .await
            .expect("g2 still waits for the dropped write")
            .expect("g2 task");
        result.expect("g2");
        assert_eq!(g2_landed - started, Duration::from_millis(300), "g2 landed");
        assert_eq!(stored(&store, "z").await, light(true, 20), "z");

        fade.await.expect("fade task").expect("fade");
        assert_eq!(stored(&store, "a").await, light(true, 80), "a: the fade");
        assert_eq!(manager.write_queue.len(), 0, "writes left in the queue");
    }

    /// #60 re-check (nit): a write whose commit fails leaves the queue just
    /// as one that succeeds does. A mutant that skips that cleanup only on
    /// the error path survives the rest of this suite, since every other
    /// write here succeeds: only a write queued behind the failed one, and
    /// left waiting on a "done" signal it never sends, catches it (the
    /// reviewer's stress test deadlocked on it).
    ///
    /// `g` is a group over `a` and `missing`, and `missing` isn't in the
    /// store yet, so writing `g` fails partway (`a` commits, `missing`
    /// doesn't). A second write to `g` — it overlaps the first on `g`
    /// itself — must not wait on the failed write's queue entry: bounded by
    /// a timeout, it must land promptly once `missing` is there to commit.
    #[tokio::test(start_paused = true)]
    async fn a_failed_write_still_leaves_the_queue() {
        let (manager, store, _bus) = manager_with_group(&["a", "missing"]).await;
        store.add_device(light_info("a"), off()).await;

        let failed = manager
            .set_virtual_device_state(&"g".to_string(), light(true, 60))
            .await;
        assert!(failed.is_err(), "the missing member must fail the write");

        // Now that `missing` exists, an overlapping write must go through: a
        // failed write's queue entry left behind would hang it forever.
        store.add_device(light_info("missing"), off()).await;
        let overlapping = tokio::time::timeout(
            Duration::from_secs(1),
            manager.set_virtual_device_state(&"g".to_string(), light(true, 40)),
        )
        .await
        .expect("the failed write left its place in the queue");
        overlapping.expect("the overlapping write");

        assert_eq!(stored(&store, "g").await, light(true, 40), "g");
        assert_eq!(manager.write_queue.len(), 0, "writes left in the queue");
    }

    /// A scene activation dropped during its fade (a client that went away)
    /// leaves the queue at once. The write waiting for it goes, and the
    /// fade commits nothing.
    #[tokio::test(start_paused = true)]
    async fn a_fade_dropped_mid_transition_lets_later_writes_go() {
        let (manager, store, _bus) =
            fade_of_a_and_b_with(&["a", "b", "z"], &[("g1", &["a", "z"])]).await;

        let started = tokio::time::Instant::now();
        let fade = activate(&manager, "evening");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let g1 = spawn_write(&manager, "g1", light(true, 60));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!g1.is_finished(), "g1 didn't wait for the fade");

        fade.abort();
        assert!(fade.await.expect_err("the fade finished").is_cancelled());
        let (result, g1_landed) = tokio::time::timeout(Duration::from_millis(1), g1)
            .await
            .expect("g1 still waits for the dropped fade")
            .expect("g1 task");
        result.expect("g1");
        assert_eq!(g1_landed - started, Duration::from_millis(300), "g1 landed");

        // Well past where the fade would have ended: it committed nothing.
        tokio::time::sleep(Duration::from_secs(2)).await;
        for id in ["a", "z"] {
            assert_eq!(stored(&store, id).await, light(true, 60), "{id}: g1");
        }
        assert_eq!(stored(&store, "b").await, off(), "b: the fade committed");
        assert_eq!(stored(&store, "scene").await, no_scene(), "the scene");
        assert_eq!(manager.write_queue.len(), 0, "writes left in the queue");
    }

    /// Scene devices and controller targets are outputs, not inputs. A
    /// missing one is reported just like a missing group member (#14 review,
    /// nit).
    #[tokio::test]
    async fn dangling_references_include_outputs() {
        let (manager, store, _bus) = manager_with_group(&["a", "missing_light"]).await;
        store.add_device(light_info("a"), off()).await;
        let on = serde_json::to_value(light(true, 50)).unwrap();
        let scene = SceneController::new(
            VirtualDeviceConfig {
                device_id: "scene".to_string(),
                device_type: VirtualDeviceType::SceneController,
                name: "Scene".to_string(),
                description: None,
                enabled: true,
                config: serde_json::json!({ "scenes": { "evening": {
                    "name": "evening",
                    "device_states": { "a": on, "missing_scene_light": on },
                    "transition_type": "Instant",
                } } }),
            },
            store.clone(),
        )
        .expect("scene");
        manager
            .add_virtual_device(Box::new(scene))
            .await
            .expect("register");
        // #35 review, finding 4: every action of a controller is an output,
        // the long and double ones too.
        add_switch(&store, "btn").await;
        let actions = [
            serde_json::json!(["toggle", "a"]),
            serde_json::json!([]),
            serde_json::json!(["inc", "missing_long_light", 10]),
            serde_json::json!(["dec", "missing_long_release_light", 10]),
            serde_json::json!(["set", "missing_double_light", 100]),
        ];
        manager
            .add_virtual_device(Box::new(controller_with("ctrl", "btn", actions)))
            .await
            .expect("register");

        assert_eq!(
            manager.dangling_references().await,
            vec![
                ("ctrl".to_string(), "missing_double_light".to_string()),
                ("ctrl".to_string(), "missing_long_light".to_string()),
                ("ctrl".to_string(), "missing_long_release_light".to_string()),
                ("g".to_string(), "missing_light".to_string()),
                ("scene".to_string(), "missing_scene_light".to_string()),
            ]
        );
    }

    /// #16: a group takes its level from its members when it's registered,
    /// or the default one if none of them is on. Never 0: the widget's
    /// toggle sends a plain `on`, and at 0 that lights nothing.
    #[tokio::test]
    async fn group_starts_with_a_level() {
        for kind in KINDS {
            let (manager, store, _bus) =
                started_group(kind, [light(true, 40), light(true, 60), off()]).await;
            let seeded = light(true, 50);
            assert_eq!(
                stored(&store, "g").await,
                seeded,
                "{kind:?}: from its lit members"
            );
            assert_eq!(
                manager
                    .get_virtual_device_state(&"g".to_string())
                    .await
                    .unwrap(),
                seeded,
                "{kind:?}: the group's own state must match the store"
            );

            let (_manager, store, _bus) = started_group(kind, [off(), off(), off()]).await;
            assert_eq!(
                stored(&store, "g").await,
                group_start(),
                "{kind:?}: none lit"
            );
        }
    }

    /// #16, "plain on after start lights nothing": groups started at level
    /// 0, so a plain `on` fanned out 0% and turned every member off.
    #[tokio::test]
    async fn plain_on_after_start_lights_the_members() {
        for kind in KINDS {
            let (manager, store, _bus) = started_group(kind, [off(), off(), off()]).await;
            plain_on(&manager, &store, "g").await;
            for id in ["a", "b", "c"] {
                assert_eq!(
                    stored(&store, id).await,
                    light(true, DEFAULT_GROUP_LEVEL),
                    "{kind:?}: plain on must light {id}"
                );
            }
        }
    }

    /// #16 (#22 re-review): an outside off of every member (the hub app, a
    /// wall switch) re-derives the group to off, and it must keep its level.
    /// It went to `{off, 0}`, and the next plain `on` lit nothing.
    #[tokio::test]
    async fn outside_off_of_every_member_keeps_the_group_level() {
        for kind in KINDS {
            let (manager, store, bus) = started_group(kind, [off(), off(), off()]).await;
            let mut rx = bus.subscribe();
            manager
                .set_virtual_device_state(&"g".to_string(), light(true, 60))
                .await
                .expect("write");
            pump(&manager, &mut rx).await;

            // Stored and echoed, as the sync engine's GatewayWins does.
            for id in ["a", "b", "c"] {
                let old = stored(&store, id).await;
                store
                    .update_device_state(&id.to_string(), off())
                    .await
                    .unwrap();
                bus.publish(state_event(&id.to_string(), Some(&old), &off()))
                    .await;
            }
            let events = pump(&manager, &mut rx).await;
            assert_eq!(
                stored(&store, "g").await,
                light(false, 60),
                "{kind:?}: the group must go off and keep its level"
            );
            assert_eq!(
                echoes(&events, "g"),
                vec![light(false, 60)],
                "{kind:?}: one re-derived group echo"
            );

            plain_on(&manager, &store, "g").await;
            for id in ["a", "b", "c"] {
                assert_eq!(
                    stored(&store, id).await,
                    light(true, 60),
                    "{kind:?}: plain on must light {id} at the kept level"
                );
            }
        }
    }

    /// #16: `on` without a level restores the group's last one, and a level
    /// of 0 is off at the level it had.
    #[tokio::test]
    async fn on_without_a_level_restores_it_and_level_zero_keeps_it() {
        let without_level = |is_on| {
            DeviceStateValue::Light(LightState {
                is_on,
                brightness: None,
                color_temp: Some(2700),
                rgb_color: None,
            })
        };
        for kind in KINDS {
            let (manager, store, _bus) = started_group(kind, [off(), off(), off()]).await;
            let g = "g".to_string();
            for (asked, want, member) in [
                (light(true, 30), light(true, 30), light(true, 30)),
                (without_level(false), light(false, 30), off()),
                (without_level(true), light(true, 30), light(true, 30)),
                (light(true, 0), light(false, 30), off()),
                (without_level(true), light(true, 30), light(true, 30)),
            ] {
                manager
                    .set_virtual_device_state(&g, asked.clone())
                    .await
                    .expect("write");
                assert_eq!(stored(&store, "g").await, want, "{kind:?}: after {asked:?}");
                assert_eq!(
                    stored(&store, "a").await,
                    member,
                    "{kind:?}: a after {asked:?}"
                );
            }
        }
    }

    /// #16: a button press on a controller that targets a group must go
    /// through the group, the way an API write does: every member changes,
    /// is queued for the gateway and echoed once, and so is the group. The
    /// controller used to write the group's state straight into the store,
    /// so no member moved and nothing was echoed.
    #[tokio::test]
    async fn button_press_goes_through_the_group_to_its_members() {
        let (manager, store, bus) = started_group(Kind::Curves, [off(), off(), off()]).await;
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            bus.clone(),
            Arc::new(DummyGateway::new("basic_home")),
            None,
        ));
        manager.attach_sync_engine(engine.clone());
        let (press_on, press_off) = (
            serde_json::json!(["on", "g"]),
            serde_json::json!(["off", "g"]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        let mut rx = bus.subscribe();

        press(&store, &bus, true).await;
        let events = pump(&manager, &mut rx).await;
        // Started with nothing on, so `on` lights them at the default level.
        let on = light(true, DEFAULT_GROUP_LEVEL);
        for id in ["a", "b", "c"] {
            assert_eq!(stored(&store, id).await, on, "the press never reached {id}");
            assert_eq!(echoes(&events, id), vec![on.clone()], "{id} echo");
            let status = engine.get_sync_status(&id.to_string()).await;
            assert!(
                matches!(status, Some(SyncStatus::PendingSync { .. })),
                "{id} never queued for the gateway: {status:?}"
            );
        }
        assert_eq!(stored(&store, "g").await, on, "group after the press");
        assert_eq!(echoes(&events, "g"), vec![on.clone()], "group echo");

        press(&store, &bus, false).await;
        let events = pump(&manager, &mut rx).await;
        assert_eq!(
            stored(&store, "g").await,
            group_start(),
            "group after the release"
        );
        for id in ["a", "b", "c"] {
            assert_eq!(
                stored(&store, id).await,
                off(),
                "the release never reached {id}"
            );
            assert_eq!(echoes(&events, id), vec![off()], "{id} echo");
        }
    }

    /// A button action on a physical light is a direct write: the light
    /// changes, is queued for the gateway, and is echoed once.
    #[tokio::test]
    async fn button_press_on_a_light_is_a_direct_write() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        store.add_device(light_info("a"), off()).await;
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            bus.clone(),
            Arc::new(DummyGateway::new("basic_home")),
            None,
        ));
        manager.attach_sync_engine(engine.clone());
        let (press_on, press_off) = (
            serde_json::json!(["set", "a", 30]),
            serde_json::json!(["dec", "a", 10]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        let mut rx = bus.subscribe();

        press(&store, &bus, true).await;
        let events = pump(&manager, &mut rx).await;
        assert_eq!(stored(&store, "a").await, light(true, 30));
        assert_eq!(echoes(&events, "a"), vec![light(true, 30)], "a echo");
        let status = engine.get_sync_status(&"a".to_string()).await;
        assert!(
            matches!(status, Some(SyncStatus::PendingSync { .. })),
            "a never queued for the gateway: {status:?}"
        );

        press(&store, &bus, false).await;
        let events = pump(&manager, &mut rx).await;
        assert_eq!(stored(&store, "a").await, light(true, 20));
        assert_eq!(echoes(&events, "a"), vec![light(true, 20)], "a echo");
    }

    /// The same press with tracking running on its own, as in the server:
    /// the action takes the device lock that tracking has just let go of,
    /// and must not deadlock the tracking task. It keeps going afterwards
    /// too: the release gets through as well.
    #[tokio::test]
    async fn button_press_under_live_tracking_does_not_deadlock() {
        let (manager, store, bus) = started_group(Kind::Curves, [off(), off(), off()]).await;
        let (press_on, press_off) = (
            serde_json::json!(["on", "g"]),
            serde_json::json!(["off", "g"]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        manager.start().await.expect("start");
        let mut rx = bus.subscribe();

        for (is_pressed, want) in [
            (true, light(true, DEFAULT_GROUP_LEVEL)),
            (false, group_start()),
        ] {
            press(&store, &bus, is_pressed).await;
            // The timeout only bounds a failure; the group's echo ends the wait.
            tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(event) = recv_lossy(&mut rx, "test").await {
                    if echoes(&[event], "g") == vec![want.clone()] {
                        return;
                    }
                }
                panic!("event bus closed");
            })
            .await
            .unwrap_or_else(|_| panic!("tracking stalled: no group echo for pressed={is_pressed}"));
        }
        assert_eq!(
            stored(&store, "a").await,
            off(),
            "the release never reached a"
        );
    }

    /// #34 review, finding 1: a switch echo that isn't a press or a release
    /// runs no action. A battery tick (85 → 84, `is_pressed` still `false`)
    /// ran the release action, so with the shipped controller every battery
    /// tick of the remote turned its group off. The same `false` with no old
    /// value isn't a release either. A real press and release still run
    /// their actions after that.
    #[tokio::test]
    async fn battery_tick_of_the_button_leaves_the_group_alone() {
        let (manager, store, bus) = started_group(Kind::Curves, [off(), off(), off()]).await;
        let (press_on, press_off) = (
            serde_json::json!(["on", "g"]),
            serde_json::json!(["off", "g"]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        let mut rx = bus.subscribe();
        let on = light(true, 60);
        manager
            .set_virtual_device_state(&"g".to_string(), on.clone())
            .await
            .expect("write");
        pump(&manager, &mut rx).await;

        // The tick, as the sync engine's GatewayWins reports it: stored,
        // and echoed with the state before. Then an echo of it with no
        // state before.
        let btn = "btn".to_string();
        let tick = switch_at(false, 84);
        store.update_device_state(&btn, tick.clone()).await.unwrap();
        for (case, old) in [
            ("a battery tick", Some(switch(false))),
            ("no old value", None),
        ] {
            bus.publish(state_event(&btn, old.as_ref(), &tick)).await;
            let events = pump(&manager, &mut rx).await;
            assert_eq!(stored(&store, "g").await, on, "{case} moved the group");
            for id in ["a", "b", "c"] {
                assert_eq!(stored(&store, id).await, on, "{case} moved {id}");
            }
            assert_eq!(events.len(), 1, "{case} must be the only event: {events:?}");
        }

        press(&store, &bus, true).await;
        pump(&manager, &mut rx).await;
        assert_eq!(stored(&store, "g").await, on, "group after the press");
        press(&store, &bus, false).await;
        pump(&manager, &mut rx).await;
        assert_eq!(
            stored(&store, "g").await,
            light(false, 60),
            "the release must still turn the group off"
        );
    }

    /// #35: each gesture a hub reports whole (`ButtonPressed`) runs the
    /// actions [`ButtonController`]'s table gives it, in order. Light `a`
    /// starts on at 50, the press adds 20, the release takes 5 off, the
    /// long press sets 90 and the double press sets 30, so each echo of `a`
    /// shows which action ran:
    /// - a click is a press and a release: 70, then 65;
    /// - a double press runs `press_double` alone: 30;
    /// - a double press on a controller without a double action is two
    ///   clicks: 70, 65, 85, 80;
    /// - a long press is a press still held: `press_on_long` only, 90. The
    ///   long release (`off`) never runs: nothing reports it;
    /// - a long press on a controller without a long action is the press
    ///   edge: 70.
    #[tokio::test]
    async fn each_reported_gesture_runs_its_press_and_release_actions() {
        use ButtonPressType::{DoublePress, LongPress, SinglePress};
        let long = serde_json::json!(["set", "a", 90]);
        let double = serde_json::json!(["set", "a", 30]);
        let none = serde_json::json!([]);
        for (case, press_type, [press_on_long, press_double], want) in [
            ("a click", SinglePress, [&long, &double], vec![70, 65]),
            ("a double press", DoublePress, [&long, &double], vec![30]),
            (
                "a double press, no double action",
                DoublePress,
                [&long, &none],
                vec![70, 65, 85, 80],
            ),
            ("a long press", LongPress, [&long, &double], vec![90]),
            (
                "a long press, no long action",
                LongPress,
                [&none, &double],
                vec![70],
            ),
        ] {
            let store = StateStore::new();
            let bus = Arc::new(EventBus::new(100));
            store.add_device(light_info("a"), light(true, 50)).await;
            add_switch(&store, "btn").await;
            let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
            let actions = [
                serde_json::json!(["inc", "a", 20]),
                serde_json::json!(["dec", "a", 5]),
                press_on_long.clone(),
                serde_json::json!(["off", "a"]),
                press_double.clone(),
            ];
            manager
                .add_virtual_device(Box::new(controller_with("ctrl", "btn", actions)))
                .await
                .expect("register");
            let mut rx = bus.subscribe();

            report(&bus, "btn", press_type).await;
            let events = pump(&manager, &mut rx).await;
            let want: Vec<_> = want.into_iter().map(|level| light(true, level)).collect();
            assert_eq!(echoes(&events, "a"), want, "{case}: the actions run");
            assert_eq!(
                Some(&stored(&store, "a").await),
                want.last(),
                "{case}: where a ends"
            );
        }
    }

    /// #35: a `ButtonPressed` of another device runs nothing. Only the
    /// controller's own button's gestures do.
    #[tokio::test]
    async fn a_reported_gesture_of_another_button_runs_nothing() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        store.add_device(light_info("a"), light(true, 50)).await;
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let (press_on, press_off) = (
            serde_json::json!(["inc", "a", 20]),
            serde_json::json!(["dec", "a", 5]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        add_switch(&store, "other").await;
        let mut rx = bus.subscribe();

        report(&bus, "other", ButtonPressType::SinglePress).await;
        let events = pump(&manager, &mut rx).await;
        assert_eq!(stored(&store, "a").await, light(true, 50), "a moved");
        assert_eq!(events.len(), 1, "only the report: {events:?}");
    }

    /// #35, dedupe: a real remote can report one press both ways, as a
    /// `ButtonPressed` over the hub's event stream and as the `is_pressed`
    /// change the sync engine's pull echoes after it. It must fire once.
    /// Light `a` starts on at 50; the press adds 20 and the release takes
    /// 5 off, so a click moves it by 15 and each echo that ran would show.
    /// Two presses, each reported both ways, end at 80: 50 → 70 → 65, then
    /// 85 → 80. A battery tick of the remote runs nothing either. The
    /// controllers' clock doesn't move, so every echo is inside the quiet
    /// period after the gesture before it.
    ///
    /// And the dedupe is per button: a controller on `plain`, a switch that
    /// never reports a gesture, keeps running off its echoes (#34).
    #[tokio::test]
    async fn a_press_reported_both_ways_fires_once() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        for id in ["a", "b"] {
            store.add_device(light_info(id), light(true, 50)).await;
        }
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let clock = TestClock::new();
        for (id, button, target) in [("ctrl", "btn", "a"), ("ctrl_plain", "plain", "b")] {
            add_switch(&store, button).await;
            let (press_on, press_off) = (
                serde_json::json!(["inc", target, 20]),
                serde_json::json!(["dec", target, 5]),
            );
            let controller = clock.drive(controller(id, button, press_on, press_off));
            manager
                .add_virtual_device(Box::new(controller))
                .await
                .expect("register");
        }
        let mut rx = bus.subscribe();

        let mut echoed = Vec::new();
        for _ in 0..2 {
            // The hub's event first, then the pull catches the press.
            report(&bus, "btn", ButtonPressType::SinglePress).await;
            press(&store, &bus, true).await;
            press(&store, &bus, false).await;
            echoed.extend(echoes(&pump(&manager, &mut rx).await, "a"));
        }
        let btn = "btn".to_string();
        let tick = switch_at(false, 84);
        store.update_device_state(&btn, tick.clone()).await.unwrap();
        bus.publish(state_event(&btn, Some(&switch(false)), &tick))
            .await;
        echoed.extend(echoes(&pump(&manager, &mut rx).await, "a"));

        let want: Vec<_> = [70, 65, 85, 80].map(|level| light(true, level)).into();
        assert_eq!(echoed, want, "each press must run its click once");
        assert_eq!(stored(&store, "a").await, light(true, 80));

        press_switch(&store, &bus, "plain", true).await;
        press_switch(&store, &bus, "plain", false).await;
        let events = pump(&manager, &mut rx).await;
        assert_eq!(
            echoes(&events, "b"),
            vec![light(true, 70), light(true, 65)],
            "a button that never reported a gesture must still fire on its echoes"
        );
    }

    /// #35 review, finding 2: a button whose gestures stop coming (the
    /// hub's event stream dropped) works off its switch echoes again once
    /// the quiet period after its last gesture is over. It used to ignore
    /// them for good after its first gesture, so it was dead from then on.
    /// Light `a` starts on at 50; the press adds 10 and the release takes
    /// 5 off, so each echo that ran shows.
    #[tokio::test]
    async fn a_button_whose_gestures_stop_works_off_its_echoes_again() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        store.add_device(light_info("a"), light(true, 50)).await;
        add_switch(&store, "btn").await;
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let clock = TestClock::new();
        let (press_on, press_off) = (
            serde_json::json!(["inc", "a", 10]),
            serde_json::json!(["dec", "a", 5]),
        );
        let controller = clock.drive(controller("ctrl", "btn", press_on, press_off));
        manager
            .add_virtual_device(Box::new(controller))
            .await
            .expect("register");
        let mut rx = bus.subscribe();
        let levels = |events: Vec<DeviceEvent>| -> Vec<Option<u8>> {
            echoes(&events, "a")
                .into_iter()
                .map(|state| match state {
                    DeviceStateValue::Light(light) => light.brightness,
                    other => panic!("a echoed {other:?}"),
                })
                .collect()
        };

        report(&bus, "btn", ButtonPressType::SinglePress).await;
        press(&store, &bus, true).await;
        press(&store, &bus, false).await;
        let ran = levels(pump(&manager, &mut rx).await);
        assert_eq!(ran, [Some(60), Some(55)], "a click reported both ways");

        // The event stream drops: only the pull's echoes come from now on.
        for (case, by, want) in [
            (
                "just inside the window",
                ECHO_QUIET_PERIOD.saturating_sub(Duration::from_millis(1)),
                vec![],
            ),
            (
                "once it's over",
                Duration::from_millis(1),
                vec![Some(65), Some(60)],
            ),
            (
                "an hour on",
                Duration::from_hours(1),
                vec![Some(70), Some(65)],
            ),
        ] {
            clock.advance(by);
            press(&store, &bus, true).await;
            press(&store, &bus, false).await;
            let ran = levels(pump(&manager, &mut rx).await);
            assert_eq!(ran, want, "the echoes of a press, {case}");
        }

        // The stream is back: the next gesture opens a new window.
        report(&bus, "btn", ButtonPressType::SinglePress).await;
        press(&store, &bus, true).await;
        press(&store, &bus, false).await;
        let ran = levels(pump(&manager, &mut rx).await);
        assert_eq!(
            ran,
            [Some(75), Some(70)],
            "a click reported both ways again"
        );
        assert_eq!(stored(&store, "a").await, light(true, 70));
    }

    /// #35 review, finding 1: a controller that toggles on a click (as the
    /// shipped `button_ctrl.toml` does: `toggle` on the press, no release)
    /// flips its group with each click, through the group to its members.
    /// On at 60, a click switches the group and every member off, and the
    /// group keeps 60. All off, a click lights them at the group's level.
    /// Either way the second click gives back the start, members included.
    #[tokio::test]
    async fn a_toggle_click_flips_the_group_and_two_give_back_the_start() {
        let lit = light(true, 60);
        for kind in KINDS {
            for (case, members, flipped) in [
                (
                    "on at 60",
                    [lit.clone(), lit.clone(), lit.clone()],
                    light(false, 60),
                ),
                (
                    "all off",
                    [off(), off(), off()],
                    light(true, DEFAULT_GROUP_LEVEL),
                ),
            ] {
                let (manager, store, bus) = started_group(kind, members).await;
                let (press_on, press_off) =
                    (serde_json::json!(["toggle", "g"]), serde_json::json!([]));
                add_controller(&manager, &store, press_on, press_off).await;
                let mut start = Vec::new();
                for id in ["g", "a", "b", "c"] {
                    start.push(stored(&store, id).await);
                }
                let mut rx = bus.subscribe();

                report(&bus, "btn", ButtonPressType::SinglePress).await;
                let events = pump(&manager, &mut rx).await;
                assert_eq!(
                    echoes(&events, "g"),
                    vec![flipped.clone()],
                    "{kind:?}, {case}: one click, one write"
                );
                assert_eq!(stored(&store, "g").await, flipped, "{kind:?}, {case}");
                let DeviceStateValue::Light(group) = &flipped else {
                    unreachable!()
                };
                for id in ["a", "b", "c"] {
                    let DeviceStateValue::Light(member) = stored(&store, id).await else {
                        panic!("{id} isn't a light");
                    };
                    assert_eq!(
                        member.is_on, group.is_on,
                        "{kind:?}, {case}: {id} after a click: {member:?}"
                    );
                }

                report(&bus, "btn", ButtonPressType::SinglePress).await;
                pump(&manager, &mut rx).await;
                let mut end = Vec::new();
                for id in ["g", "a", "b", "c"] {
                    end.push(stored(&store, id).await);
                }
                assert_eq!(end, start, "{kind:?}, {case}: two clicks");
            }
        }
    }

    /// #35 review, finding 1: toggles don't race. Nine clicks of a toggling
    /// controller, handled all at once on four threads. Each toggle reads
    /// the group and writes it back under one hold of the manager's lock,
    /// so each starts from what the one before it left: the group's echoes
    /// alternate off, on, off, …, and it ends off (nine is odd) at its
    /// level, with every member off. A toggle that read the group and
    /// wrote it under two holds would lose a flip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rapid_toggle_clicks_through_the_manager_do_not_race() {
        const CLICKS: usize = 9;
        let lit = light(true, 60);
        let (manager, store, bus) =
            started_group(Kind::Curves, [lit.clone(), lit.clone(), lit]).await;
        let (press_on, press_off) = (serde_json::json!(["toggle", "g"]), serde_json::json!([]));
        add_controller(&manager, &store, press_on, press_off).await;
        let mut rx = bus.subscribe();
        let click = DeviceEvent {
            timestamp: std::time::SystemTime::now(),
            device_id: "btn".to_string(),
            event_type: EventType::ButtonPressed {
                button_id: "main".to_string(),
                press_type: ButtonPressType::SinglePress,
            },
        };

        let clicks: Vec<_> = (0..CLICKS)
            .map(|_| {
                let (manager, click) = (manager.clone(), click.clone());
                tokio::spawn(async move { manager.handle_event(&click).await })
            })
            .collect();
        for click in clicks {
            click.await.expect("click task").expect("click");
        }
        let events = pump(&manager, &mut rx).await;

        let group: Vec<bool> = echoes(&events, "g")
            .into_iter()
            .map(|state| {
                matches!(
                    state,
                    DeviceStateValue::Light(LightState { is_on: true, .. })
                )
            })
            .collect();
        let alternating: Vec<bool> = (0..CLICKS).map(|click| click % 2 == 1).collect();
        assert_eq!(group, alternating, "the group's echoes, one per click");
        assert_eq!(stored(&store, "g").await, light(false, 60));
        for id in ["a", "b", "c"] {
            assert_eq!(stored(&store, id).await, off(), "{id}");
        }
    }

    /// #34 review, nit 3: a member that's on at level 0 (the TUI's `-` can
    /// put a light there) isn't lit. A group whose members all read that is
    /// off, at its level, and a plain `on` lights them at that level. It
    /// came out `{on, 60}` with nothing lit.
    #[tokio::test]
    async fn members_on_at_level_0_leave_the_group_off_at_its_level() {
        for kind in KINDS {
            let (manager, store, bus) = started_group(kind, [off(), off(), off()]).await;
            let mut rx = bus.subscribe();
            manager
                .set_virtual_device_state(&"g".to_string(), light(true, 60))
                .await
                .expect("write");
            pump(&manager, &mut rx).await;

            // Stored and echoed, as the sync engine's GatewayWins does.
            let at_0 = light(true, 0);
            for id in ["a", "b", "c"] {
                let old = stored(&store, id).await;
                store
                    .update_device_state(&id.to_string(), at_0.clone())
                    .await
                    .unwrap();
                bus.publish(state_event(&id.to_string(), Some(&old), &at_0))
                    .await;
            }
            let events = pump(&manager, &mut rx).await;
            assert_eq!(
                stored(&store, "g").await,
                light(false, 60),
                "{kind:?}: members on at 0 must leave the group off at its level"
            );
            assert_eq!(
                echoes(&events, "g"),
                vec![light(false, 60)],
                "{kind:?}: one re-derived group echo"
            );

            plain_on(&manager, &store, "g").await;
            for id in ["a", "b", "c"] {
                assert_eq!(
                    stored(&store, id).await,
                    light(true, 60),
                    "{kind:?}: plain on must light {id} at the kept level"
                );
            }
        }
    }

    /// Log lines written while a test holds the subscriber, as text.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLogs {
        fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
            self.subscriber_at(tracing::Level::WARN)
        }

        fn subscriber_at(&self, level: tracing::Level) -> impl tracing::Subscriber + Send + Sync {
            let logs = self.clone();
            tracing_subscriber::fmt()
                .with_writer(move || logs.clone())
                .with_ansi(false)
                .with_max_level(level)
                .finish()
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("logs")).into_owned()
        }

        /// How many times `needle` was logged.
        fn count(&self, needle: &str) -> usize {
            self.text().matches(needle).count()
        }
    }

    /// #34 review, nit 5: two controllers on one button run two actions per
    /// press. A stale `button_test.toml` next to the new `button_ctrl.toml`
    /// did that, with no log line. `start` must warn about it. A button
    /// bound once isn't reported, and removing one of the two clears it.
    #[tokio::test]
    async fn start_warns_about_a_button_two_controllers_bind() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        store.add_device(light_info("a"), off()).await;
        for button in ["btn", "other"] {
            add_switch(&store, button).await;
        }
        for (id, button) in [
            ("ctrl_new", "btn"),
            ("ctrl_stale", "btn"),
            ("ctrl_other", "other"),
        ] {
            let (press_on, press_off) = (
                serde_json::json!(["on", "a"]),
                serde_json::json!(["off", "a"]),
            );
            manager
                .add_virtual_device(Box::new(controller(id, button, press_on, press_off)))
                .await
                .expect("register");
        }
        let both = vec!["ctrl_new".to_string(), "ctrl_stale".to_string()];
        assert_eq!(
            manager.shared_buttons().await,
            vec![("btn".to_string(), both)]
        );

        let logs = CapturedLogs::default();
        {
            let _default = tracing::subscriber::set_default(logs.subscriber());
            manager.start().await.expect("start");
        }
        let logs = logs.text();
        assert!(
            logs.contains("Button btn is bound by 2 button controllers (ctrl_new, ctrl_stale)"),
            "start must warn about btn: {logs:?}"
        );
        assert!(
            !logs.contains("Button other"),
            "other is bound once: {logs:?}"
        );

        manager
            .remove_virtual_device(&"ctrl_stale".to_string())
            .await
            .expect("remove");
        assert!(manager.shared_buttons().await.is_empty());
    }

    /// #16: adding a virtual device announces it with `DeviceAdded`, and
    /// removing it with `DeviceRemoved`, so clients see both without a
    /// refetch. A second remove announces nothing.
    #[tokio::test]
    async fn add_and_remove_announce_the_device() {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        let mut rx = bus.subscribe();
        let g = "g".to_string();
        let lifecycle = |events: Vec<DeviceEvent>| -> Vec<(DeviceId, EventType)> {
            events
                .into_iter()
                .map(|e| (e.device_id, e.event_type))
                .collect()
        };

        manager
            .add_virtual_device(Box::new(group("g", &["a"], &store)))
            .await
            .expect("register");
        assert!(store.get_device(&g).await.is_some(), "g is in the store");
        let added = EventType::DeviceAdded {
            device_type: "VirtualLightGroup".to_string(),
        };
        assert_eq!(
            lifecycle(pump(&manager, &mut rx).await),
            vec![(g.clone(), added)]
        );

        manager.remove_virtual_device(&g).await.expect("remove");
        assert!(store.get_device(&g).await.is_none(), "g left the store");
        assert_eq!(
            lifecycle(pump(&manager, &mut rx).await),
            vec![(g.clone(), EventType::DeviceRemoved)]
        );

        manager
            .remove_virtual_device(&g)
            .await
            .expect("remove again");
        assert!(
            pump(&manager, &mut rx).await.is_empty(),
            "nothing to announce"
        );
    }

    /// A sync engine on the dummy hub, attached to `manager`, that never
    /// runs: a write it takes only shows as its sync status.
    fn attach_engine(
        manager: &VirtualDeviceManager,
        store: &Arc<StateStore>,
        bus: &Arc<EventBus>,
    ) -> Arc<SyncEngine> {
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            bus.clone(),
            Arc::new(DummyGateway::new("basic_home")),
            None,
        ));
        manager.attach_sync_engine(engine.clone());
        engine
    }

    /// #34 review, finding 2: a button action that runs while its group is
    /// being removed must not write the group as a physical light. The
    /// removal took the group out of the manager, let go of the lock, and
    /// only then took it out of the store. An action in between found it in
    /// the store only, wrote the group's state back and queued it for the
    /// gateway under its virtual id.
    ///
    /// The test holds the removal after it lets go of the device lock (at
    /// the input mappings, the lock it takes next) and runs the action
    /// there. The group must have left the store with the manager, and the
    /// action must find nothing.
    #[tokio::test]
    async fn button_action_during_removal_never_writes_the_group_as_a_light() {
        let (manager, store, bus) = started_group(Kind::Curves, [off(), off(), off()]).await;
        let engine = attach_engine(&manager, &store, &bus);
        let g = "g".to_string();
        let on = ButtonAction::parse(&[serde_json::json!("on"), serde_json::json!("g")])
            .expect("action");

        let removal = {
            let held = manager.input_mappings.read().await;
            let removal = tokio::spawn({
                let (manager, g) = (manager.clone(), g.clone());
                async move { manager.remove_virtual_device(&g).await }
            });
            // The timeout only bounds a failure.
            tokio::time::timeout(Duration::from_secs(10), async {
                while manager.virtual_devices.read().await.contains_key(&g) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the removal never took g out of the manager");

            assert!(
                store.get_device(&g).await.is_none(),
                "g has left the manager but not the store"
            );
            let result = manager.run_action(&on).await;
            assert!(
                matches!(&result, Err(VirtualDeviceError::DeviceNotFound(id)) if *id == g),
                "the action must find no g: {result:?}"
            );
            drop(held);
            removal
        };
        removal.await.expect("removal task").expect("remove");

        assert!(
            store.get_device(&g).await.is_none(),
            "g is back in the store"
        );
        let status = engine.get_sync_status(&g).await;
        assert!(status.is_none(), "g was queued for the gateway: {status:?}");
        for id in ["a", "b", "c"] {
            assert_eq!(stored(&store, id).await, off(), "{id} moved");
        }
    }

    /// #34 review, finding 2: a button action's target that the store marks
    /// virtual, but the manager doesn't have, isn't a physical light, as the
    /// API tells them apart. The press finds nothing: it doesn't write the
    /// group's state back or queue it for the gateway. The group is taken
    /// out of the manager only, the first step of the old removal.
    #[tokio::test]
    async fn button_action_on_a_virtual_device_the_manager_lacks_writes_nothing() {
        let (manager, store, bus) = started_group(Kind::Curves, [off(), off(), off()]).await;
        let engine = attach_engine(&manager, &store, &bus);
        let (press_on, press_off) = (
            serde_json::json!(["on", "g"]),
            serde_json::json!(["off", "g"]),
        );
        add_controller(&manager, &store, press_on, press_off).await;
        let g = "g".to_string();
        manager.virtual_devices.write().await.remove(&g);
        let before = stored(&store, "g").await;
        let mut rx = bus.subscribe();

        press(&store, &bus, true).await;
        let events = pump(&manager, &mut rx).await;

        assert_eq!(
            stored(&store, "g").await,
            before,
            "g was written as a light"
        );
        assert!(echoes(&events, "g").is_empty(), "g was echoed");
        let status = engine.get_sync_status(&g).await;
        assert!(status.is_none(), "g was queued for the gateway: {status:?}");
        for id in ["a", "b", "c"] {
            assert_eq!(stored(&store, id).await, off(), "{id} moved");
        }
    }

    /// #15: input tracking must outlive falling behind the bus. More events
    /// for `a` are published than the bus keeps (1000) before tracking can
    /// work through them. The test holds the device lock meanwhile, so
    /// tracking can't get past an event for `a` even if it's already
    /// running. On the `current_thread` test runtime it may not have been
    /// polled at all yet. Either way it falls behind. A change to `b` must
    /// still re-derive `b`'s group `h`. Nothing about `a` can echo `h`, so
    /// that echo shows tracking got past the lag.
    #[tokio::test]
    async fn input_tracking_survives_falling_behind_the_bus() {
        let (manager, store, bus) = manager_with_group(&["a"]).await;
        manager
            .add_virtual_device(Box::new(group("h", &["b"], &store)))
            .await
            .expect("register");
        for id in ["a", "b"] {
            store.add_device(light_info(id), off()).await;
        }
        manager.start().await.expect("start");
        let mut rx = bus.subscribe();

        let echo_of_a = state_event(&"a".to_string(), None, &off());
        {
            // If tracking runs before we let go, it waits here, at its first
            // event for `a`.
            let _held = manager.virtual_devices.write().await;
            for _ in 0..1500 {
                bus.publish(echo_of_a.clone()).await;
            }
        }

        let on = light(true, 70);
        store
            .update_device_state(&"b".to_string(), on.clone())
            .await
            .unwrap();
        bus.publish(state_event(&"b".to_string(), None, &on)).await;

        // The timeout only bounds a failure; the echo ends the wait.
        let h_echo = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(event) = recv_lossy(&mut rx, "test").await {
                if event.device_id == "h" {
                    return event;
                }
            }
            panic!("event bus closed");
        })
        .await
        .expect("tracking died behind the bus: `h` was never re-derived");
        assert_eq!(echoes(&[h_echo], "h"), vec![on]);
    }

    /// `device_id` moved to `state` from outside (the hub app, a wall
    /// switch), as the sync engine reports it: stored, and echoed.
    async fn moved(store: &StateStore, bus: &EventBus, device_id: &str, state: DeviceStateValue) {
        let id = device_id.to_string();
        let old = stored(store, device_id).await;
        store.update_device_state(&id, state.clone()).await.unwrap();
        bus.publish(state_event(&id, Some(&old), &state)).await;
    }

    /// A `LightGroupLinear` named `device_id` over two lights, with ranges
    /// (80-100 and 0-50) that make re-deriving it lossy: set to 50, it
    /// reads back as 57.
    fn lossy_group(
        device_id: &str,
        [e, f]: [&str; 2],
        store: &Arc<StateStore>,
    ) -> LightGroupLinear {
        let config = VirtualDeviceConfig {
            device_id: device_id.to_string(),
            device_type: VirtualDeviceType::LightGroupLinear,
            name: device_id.to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        let members = [e, f].map(|id| (id.to_string(), id.to_string()));
        let ranges = HashMap::from([(e.to_string(), (80, 100)), (f.to_string(), (0, 50))]);
        LightGroupLinear::new(config, HashMap::from(members), ranges, store.clone())
            .expect("lossy group")
    }

    /// A bus small enough for a test to overflow: a subscriber lags once it
    /// is more than this many events behind.
    const RING: usize = 16;

    /// What input tracking logs when it falls behind the bus, and when it
    /// then catches the virtual devices up.
    const LAGGED: &str = "virtual device tracking subscriber lagged";
    const CAUGHT_UP: &str = "Catching virtual devices up";

    /// #15: once input tracking has fallen behind the bus, it catches every
    /// group up from the store. Members move while it's held at the device
    /// lock, and their echoes are lost behind three rings' worth of events.
    /// Nothing still buffered after the lag is about them.
    ///
    /// `g` (curves) must follow `a` to `{on, 40}`, and `h` (linear) go off
    /// with its level kept when `c` and `d` go off (#16), each echoed once
    /// and no more. `k`'s members didn't move, and it accounts for them
    /// (#22): re-deriving it anyway would be lossy (set to 50, its ranges
    /// read back as 57), so it must keep 50 and not echo.
    ///
    /// The last event buffered is a press of `btn`, whose controller turns
    /// `marker` on. Tracking handles it before it catches up (#47 review,
    /// finding 1). Catching up right after the lag left tracking a whole
    /// ring behind the bus, so the catch-up's own echoes lagged it again:
    /// two lags, two catch-ups, and the press only after both. It must be
    /// one lag and one catch-up. Then a change of `y` re-derives `z`: that
    /// echo shows tracking is past the catch-up, back on live events.
    #[tokio::test]
    async fn input_tracking_catches_the_groups_up_after_falling_behind() {
        let store = StateStore::new();
        for (id, state) in [
            ("a", light(true, 60)),
            ("b", light(true, 60)),
            ("c", light(true, 60)),
            ("d", light(true, 60)),
            ("e", off()),
            ("f", off()),
            ("marker", off()),
            ("y", off()),
        ] {
            store.add_device(light_info(id), state).await;
        }
        let bus = Arc::new(EventBus::with_capacity(100, RING));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        for device in [
            Box::new(group("g", &["a", "b"], &store)) as Box<dyn VirtualDevice>,
            Box::new(linear_group("h", &["c", "d"], &store)),
            Box::new(lossy_group("k", ["e", "f"], &store)),
            Box::new(group("z", &["y"], &store)),
        ] {
            manager.add_virtual_device(device).await.expect("register");
        }
        manager
            .set_virtual_device_state(&"k".to_string(), light(true, 50))
            .await
            .expect("k to 50");
        let (press_on, press_off) = (serde_json::json!(["on", "marker"]), serde_json::json!([]));
        add_controller(&manager, &store, press_on, press_off).await;
        assert_eq!(stored(&store, "g").await, light(true, 60), "g at start");
        assert_eq!(stored(&store, "h").await, light(true, 60), "h at start");
        assert_eq!(stored(&store, "k").await, light(true, 50), "k at start");
        // Tracking runs on this thread (the `current_thread` test runtime),
        // so this sees its logs.
        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(logs.subscriber_at(tracing::Level::INFO));
        manager.start().await.expect("start");

        let mut rx = {
            // Tracking gets no further than its first event, an echo of `e`,
            // before we let go. It may not even have been polled yet.
            let _held = manager.virtual_devices.write().await;
            let echo_of_e = state_event(&"e".to_string(), None, &stored(&store, "e").await);
            bus.publish(echo_of_e.clone()).await;
            moved(&store, &bus, "a", light(true, 20)).await;
            moved(&store, &bus, "c", off()).await;
            moved(&store, &bus, "d", off()).await;
            for _ in 0..3 * RING {
                bus.publish(echo_of_e.clone()).await;
            }
            press(&store, &bus, true).await;
            // Tracking needs the lock to echo anything, so this sees it all.
            bus.subscribe()
        };

        // The timeout only bounds a failure; `z`'s echo ends the wait.
        let events = tokio::time::timeout(Duration::from_secs(10), async {
            let mut events: Vec<DeviceEvent> = Vec::new();
            let seen = |events: &[DeviceEvent], id: &str| events.iter().any(|e| e.device_id == id);
            while !["marker", "g", "h"].iter().all(|id| seen(&events, id)) {
                events.push(rx.recv().await.expect("event bus"));
            }
            moved(&store, &bus, "y", light(true, 70)).await;
            while !seen(&events, "z") {
                events.push(rx.recv().await.expect("event bus"));
            }
            events
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "tracking never got past the lag, after {} catch-ups",
                logs.count(CAUGHT_UP)
            )
        });

        assert_eq!(logs.count(LAGGED), 1, "lagged exactly once");
        assert_eq!(logs.count(CAUGHT_UP), 1, "caught up exactly once");
        let first = |id: &str| events.iter().position(|e| e.device_id == id);
        assert!(
            first("marker") < first("g"),
            "the buffered press must run before the catch-up: {events:?}"
        );
        for (id, want, what) in [
            ("g", light(true, 40), "g must follow a"),
            ("h", light(false, 60), "h must go off and keep its level"),
        ] {
            assert_eq!(stored(&store, id).await, want, "{what}");
            assert_eq!(echoes(&events, id), vec![want], "{what}, echoed once");
        }
        assert_eq!(
            stored(&store, "k").await,
            light(true, 50),
            "k was re-derived"
        );
        assert!(echoes(&events, "k").is_empty(), "k was echoed");
        for id in ["g", "h", "k"] {
            assert_eq!(
                manager
                    .get_virtual_device_state(&id.to_string())
                    .await
                    .unwrap(),
                stored(&store, id).await,
                "{id}'s own state must match the store"
            );
        }
    }

    /// A virtual light over `input` that moves every time the manager hands
    /// it a change of its input, whether the input moved or not. So every
    /// catch-up echoes it, which is the #47 review's livelock mutant (every
    /// group echoed, even unchanged) without changing the manager.
    struct Restless {
        config: VirtualDeviceConfig,
        input: DeviceId,
        level: u8,
    }

    impl Restless {
        fn new(device_id: &str, input: &str) -> Self {
            let config = VirtualDeviceConfig {
                device_id: device_id.to_string(),
                device_type: VirtualDeviceType::LightGroup,
                name: device_id.to_string(),
                description: None,
                enabled: true,
                config: serde_json::json!({}),
            };
            Self {
                config,
                input: input.to_string(),
                level: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl VirtualDevice for Restless {
        fn device_id(&self) -> &DeviceId {
            &self.config.device_id
        }

        fn device_type(&self) -> VirtualDeviceType {
            VirtualDeviceType::LightGroup
        }

        fn config(&self) -> &VirtualDeviceConfig {
            &self.config
        }

        async fn plan_write(
            &self,
            _: DeviceStateValue,
        ) -> Result<VirtualWrite, VirtualDeviceError> {
            Ok(VirtualWrite {
                members: Vec::new(),
                state: self.current_state(),
            })
        }

        fn take_state(&mut self, _: DeviceStateValue) {}

        async fn on_input_changed(
            &mut self,
            _: &DeviceId,
            _: &DeviceState,
        ) -> Result<(), VirtualDeviceError> {
            self.level = self.level.wrapping_add(1);
            Ok(())
        }

        fn current_state(&self) -> DeviceStateValue {
            light(true, self.level)
        }

        fn input_devices(&self) -> Vec<DeviceId> {
            vec![self.input.clone()]
        }
    }

    /// #47 review, finding 1: a catch-up that echoes must not lag tracking
    /// again. `r` is echoed by every catch-up. Catching up right after a lag
    /// left tracking a whole ring behind the bus, so that echo was a new
    /// lag, and another catch-up, for good: the press buffered behind the
    /// lag never ran. Tracking has to handle what's buffered first, then
    /// catch up once, then go on with live events (the move of `a`).
    #[tokio::test]
    async fn a_catch_up_that_echoes_does_not_lag_tracking_again() {
        let store = StateStore::new();
        for id in ["a", "marker"] {
            store.add_device(light_info(id), off()).await;
        }
        let bus = Arc::new(EventBus::with_capacity(100, RING));
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        manager
            .add_virtual_device(Box::new(Restless::new("r", "a")))
            .await
            .expect("register");
        let (press_on, press_off) = (serde_json::json!(["on", "marker"]), serde_json::json!([]));
        add_controller(&manager, &store, press_on, press_off).await;
        // Tracking runs on this thread, so this sees its logs.
        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(logs.subscriber_at(tracing::Level::INFO));
        manager.start().await.expect("start");

        let mut rx = {
            let _held = manager.virtual_devices.write().await;
            for _ in 0..3 * RING {
                bus.publish(DeviceEvent {
                    timestamp: std::time::SystemTime::now(),
                    device_id: "noise".to_string(),
                    event_type: EventType::DeviceRemoved,
                })
                .await;
            }
            press(&store, &bus, true).await;
            bus.subscribe()
        };

        // The timeout only bounds a failure; `r`'s second echo ends the wait.
        let events = tokio::time::timeout(Duration::from_secs(10), async {
            let mut events: Vec<DeviceEvent> = Vec::new();
            let seen = |events: &[DeviceEvent], id: &str| events.iter().any(|e| e.device_id == id);
            while !(seen(&events, "marker") && seen(&events, "r")) {
                events.push(recv_lossy(&mut rx, "test").await.expect("event bus"));
            }
            moved(&store, &bus, "a", light(true, 30)).await;
            while echoes(&events, "r").len() < 2 {
                events.push(recv_lossy(&mut rx, "test").await.expect("event bus"));
            }
            events
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "tracking never got past the lag, after {} catch-ups",
                logs.count(CAUGHT_UP)
            )
        });

        assert_eq!(logs.count(LAGGED), 1, "lagged exactly once");
        assert_eq!(logs.count(CAUGHT_UP), 1, "caught up exactly once");
        assert_eq!(
            echoes(&events, "r"),
            vec![light(true, 1), light(true, 2)],
            "r: echoed by the catch-up, then by the move of a"
        );
        let marker = echoes(&events, "marker");
        assert!(
            matches!(marker.as_slice(), [DeviceStateValue::Light(l)] if l.is_on),
            "the press of btn must run, once: {marker:?}"
        );
    }

    /// The dummy hub, with the sync engine's reads held at a gate and its
    /// traffic counted. A test can then order its steps around the engine's
    /// workers instead of sleeping through them.
    struct GatedHub {
        inner: DummyGateway,
        /// Reads wait here until it's `true`.
        reads_open: watch::Sender<bool>,
        /// Reads that have started (before the gate), and reads answered.
        reads_started: watch::Sender<usize>,
        reads_done: watch::Sender<usize>,
        /// Writes answered.
        writes_done: watch::Sender<usize>,
    }

    impl GatedHub {
        fn new(inner: DummyGateway) -> Self {
            Self {
                inner,
                reads_open: watch::channel(false).0,
                reads_started: watch::channel(0).0,
                reads_done: watch::channel(0).0,
                writes_done: watch::channel(0).0,
            }
        }
    }

    #[async_trait::async_trait]
    impl Gateway for GatedHub {
        async fn discover_devices(&self) -> Result<Vec<DeviceInfo>, GatewayError> {
            self.inner.discover_devices().await
        }

        async fn get_device_state(
            &self,
            device_id: &DeviceId,
        ) -> Result<DeviceStateValue, GatewayError> {
            self.reads_started.send_modify(|n| *n += 1);
            let mut open = self.reads_open.subscribe();
            open.wait_for(|open| *open).await.expect("gate");
            let state = self.inner.get_device_state(device_id).await;
            self.reads_done.send_modify(|n| *n += 1);
            state
        }

        async fn set_device_state(
            &self,
            device_id: &DeviceId,
            state: DeviceStateValue,
        ) -> Result<(), GatewayError> {
            let result = self.inner.set_device_state(device_id, state).await;
            self.writes_done.send_modify(|n| *n += 1);
            result
        }

        async fn health_check(&self) -> Result<GatewayHealth, GatewayError> {
            self.inner.health_check().await
        }
    }

    /// Resolves once `counter` reaches `at_least`. The timeout only bounds
    /// a failure; nothing waits on it when things work.
    async fn reach(counter: &watch::Sender<usize>, at_least: usize, what: &str) {
        let mut rx = counter.subscribe();
        tokio::time::timeout(Duration::from_secs(20), rx.wait_for(|n| *n >= at_least))
            .await
            .unwrap_or_else(|_| panic!("{what}: still at {}", *counter.borrow()))
            .expect("hub");
    }

    /// The live-gateway half of #1: member writes must reach the hub, and
    /// the pulls after them must leave them alone. If they only hit the
    /// store, the pull worker (`GatewayWins`) reverts them.
    #[tokio::test]
    async fn virtual_member_writes_reach_the_gateway() {
        const LIGHTS: [&str; 3] = ["light_bedroom", "light_living_room", "light_kitchen"];
        let hub = Arc::new(GatedHub::new(DummyGateway::new("basic_home")));
        let store = StateStore::new();
        for info in hub.inner.discover_devices().await.expect("discover") {
            let state = hub
                .inner
                .get_device_state(&info.device_id)
                .await
                .expect("initial state");
            store.add_device(info, state).await;
        }
        let (manager, bus) = manager_with_group_in(store.clone(), &LIGHTS).await;
        // A short pull interval only makes the pulls come sooner. The gate,
        // not the interval, orders them against the write.
        let engine = Arc::new(SyncEngine::new(
            store.clone(),
            bus.clone(),
            hub.clone(),
            Some(SyncConfig {
                pull_interval: Duration::from_millis(100),
                ..SyncConfig::default()
            }),
        ));
        manager.attach_sync_engine(engine.clone());
        // Tracking runs as the server runs it, and sees the confirmations.
        manager.start().await.expect("start");
        let runner = tokio::spawn({
            let engine = engine.clone();
            async move { engine.start().await }
        });

        // The first pull starts at once and holds at the gate. It compares
        // the hub against the store as it was before the write, so it can't
        // revert what the write does. No other pull starts before it's done.
        reach(&hub.reads_started, 1, "first pull").await;
        let on = light(true, 60);
        manager
            .set_virtual_device_state(&"g".to_string(), on.clone())
            .await
            .expect("write");

        reach(&hub.writes_done, LIGHTS.len(), "member pushes").await;
        for id in LIGHTS {
            let on_hub = hub.inner.get_device_state(&id.to_string()).await;
            assert_eq!(
                on_hub.ok().as_ref(),
                Some(&on),
                "{id} never reached the gateway"
            );
        }

        // Let the pulls through: the held one (it confirms the pushes), a
        // full one after it, and the first read of the next, so the full one
        // has been handled.
        let cycle = store.device_count().await;
        hub.reads_open.send_replace(true);
        reach(&hub.reads_done, 2 * cycle + 1, "two pull cycles").await;
        for id in LIGHTS {
            assert_eq!(stored(&store, id).await, on, "{id} was reverted");
        }
        assert_eq!(stored(&store, "g").await, on, "group moved");

        engine.stop().await;
        runner
            .await
            .expect("sync engine task")
            .expect("sync engine");
    }
}
