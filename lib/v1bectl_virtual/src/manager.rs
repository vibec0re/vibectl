use crate::virtual_device::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use v1bectl_sync::*;

/// Virtual Device Manager - coordinates all virtual devices 🔥
///
/// A write to a virtual device ([`Self::set_virtual_device_state`]) fans out
/// to its members. Input tracking ([`Self::start`]) re-derives a virtual
/// device when one of its inputs changes from outside. Each of these runs
/// start to finish under the `virtual_devices` lock: the device's new state,
/// its members' commits, its store state and every echo. So they never
/// interleave. A second write can't land between a first write's fan-out
/// and its commits, and echoes go out in the order the store changed.
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
    /// members are only echoed.
    sync_engine: Arc<OnceLock<Arc<SyncEngine>>>,
    /// Set once [`Self::start`] has run (see [`Self::add_virtual_device`]).
    tracking: Arc<AtomicBool>,
}

/// A state echo for `device_id`, in the same `AttributeChanged{attribute:
/// "state"}` shape the sync engine publishes for a physical write. That's the
/// shape every client (TUI, widget, GTK, web) and this manager's own input
/// tracking decode.
///
/// `old_value` is the state the store held before, as in the engine's
/// GatewayWins and confirmation echoes. (The engine's optimistic echo sends
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
        }
    }

    /// Send member writes through `sync_engine`, like direct writes, so they
    /// reach the gateway. Without this the 2s pull worker (GatewayWins)
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

    /// Commit one member state that a virtual write already put in the store.
    ///
    /// A physical member goes through the sync engine when one is attached,
    /// the way a direct write to it does. That queues the gateway push. With
    /// optimistic updates on (the default) it also writes the store again
    /// (same value) and publishes the echo, so it is the member's only
    /// publisher.
    ///
    /// Otherwise the echo is ours: with no engine, for a virtual member (the
    /// gateway doesn't know it), and with optimistic updates off. In that
    /// mode a direct write is echoed once a pull confirms it. But the
    /// fan-out has already put this member's state in the store, so that
    /// pull finds nothing new and would never echo it.
    async fn commit_member_write(
        &self,
        member_id: &DeviceId,
        old_state: Option<&DeviceStateValue>,
        new_state: &DeviceStateValue,
        member_is_virtual: bool,
    ) {
        if let (Some(engine), false) = (self.sync_engine.get(), member_is_virtual) {
            match engine
                .apply_optimistic_update(member_id, new_state.clone())
                .await
            {
                Ok(()) if engine.config().optimistic_updates => return,
                Ok(()) => {}
                Err(e) => tracing::warn!(
                    "❌ Failed to sync member {} of a virtual write: {}",
                    member_id,
                    e
                ),
            }
        }
        self.publish_state(member_id, old_state, new_state).await;
    }

    /// Add a virtual device to the manager. It first takes its state from
    /// its inputs as the store holds them (see
    /// [`VirtualDevice::seed_from_inputs`]), so register a group after its
    /// members are in the store.
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

        // Add device
        {
            let mut devices = self.virtual_devices.write().await;
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
        let devices = self.virtual_devices.read().await;
        if let Some(virtual_device) = devices.get(&device_id) {
            let device_info = virtual_device.device_info();
            let initial_state = virtual_device.current_state();
            self.state_store
                .add_device(device_info, initial_state)
                .await;
        }

        Ok(())
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

    /// Remove a virtual device
    pub async fn remove_virtual_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<(), VirtualDeviceError> {
        // Remove from devices
        let removed_device = {
            let mut devices = self.virtual_devices.write().await;
            devices.remove(device_id)
        };

        if let Some(device) = removed_device {
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

            // Remove from state store
            self.state_store.remove_device(device_id).await?;
        }

        Ok(())
    }

    /// Input tracking for one event from the bus. A device's `state` change
    /// (`AttributeChanged{attribute: "state"}`, the shape of every state
    /// echo) goes to [`Self::handle_device_state_change`]; anything else is
    /// ignored. [`Self::start`] runs this for every event on the bus. Call
    /// it directly to drive tracking yourself instead (the tests do, to
    /// control exactly when it catches up).
    pub async fn handle_event(&self, event: &DeviceEvent) -> Result<(), VirtualDeviceError> {
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
        let input = stored.as_ref().unwrap_or(new_state);

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

    /// Set virtual device state (called from API)
    ///
    /// Publishes a state echo for the virtual device and for every member
    /// whose state the write changed. Members are committed through the
    /// attached sync engine (see [`Self::attach_sync_engine`]), the same way
    /// a direct write to them is.
    ///
    /// A write can fail part-way (a missing member, as in #2). The members
    /// it did change are still committed and echoed, and then the error is
    /// returned. A group keeps its old state on failure, which no longer
    /// accounts for those members, so input tracking re-derives it from
    /// them like after any outside change: the group ends up showing what
    /// happened.
    pub async fn set_virtual_device_state(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), VirtualDeviceError> {
        // Held to the end (see the type's docs).
        let mut devices = self.virtual_devices.write().await;
        let Some(virtual_device) = devices.get(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };

        // Snapshot what this write can touch, so we echo exactly what changed.
        let mut outputs = virtual_device.output_devices();
        let mut seen = HashSet::new();
        outputs.retain(|id| seen.insert(id.clone()));
        let virtual_outputs: HashSet<DeviceId> = outputs
            .iter()
            .filter(|id| devices.contains_key(*id))
            .cloned()
            .collect();
        let mut old_outputs = HashMap::new();
        for id in &outputs {
            if let Some(device) = self.state_store.get_device(id).await {
                old_outputs.insert(id.clone(), device.state);
            }
        }

        // Update virtual device (writes its members into the store)
        let Some(virtual_device) = devices.get_mut(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };
        let result = virtual_device.set_state(new_state).await;

        let mut changed = Vec::new();
        for id in outputs {
            let Some(device) = self.state_store.get_device(&id).await else {
                continue;
            };
            let old_state = old_outputs.remove(&id);
            if old_state.as_ref() != Some(&device.state) {
                changed.push((id, old_state, device.state));
            }
        }

        let current_state = virtual_device.current_state();

        // Whatever the write changed is in the store by now, even if it
        // failed part-way: commit and echo it.
        for (id, old_state, state) in &changed {
            self.commit_member_write(id, old_state.as_ref(), state, virtual_outputs.contains(id))
                .await;
        }

        match result {
            Ok(()) => {
                self.store_state(device_id, current_state, true).await?;
                Ok(())
            }
            Err(e) => {
                // The groups keep their state on failure, but a device that
                // moved before failing must not leave the store behind.
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
    /// every event on the bus, in a background task. It also logs every
    /// dangling reference (see [`Self::dangling_references`]). The server
    /// calls this once all virtual devices are loaded.
    pub async fn start(&self) -> Result<(), VirtualDeviceError> {
        let manager = Arc::new(self.clone());

        // Subscribe to state change events
        let mut event_receiver = self.event_bus.subscribe();

        self.tracking.store(true, Ordering::Release);
        for (virtual_id, missing) in self.dangling_references().await {
            warn_dangling(&virtual_id, &missing);
        }

        tokio::spawn(async move {
            // Falling behind the bus skips the events missed, it doesn't end
            // tracking (#15). Inputs are judged as the store holds them, so
            // the next event for a member catches its group up. A member
            // that doesn't change again stays unreconciled until it does.
            while let Some(event) = recv_lossy(&mut event_receiver, "virtual device tracking").await
            {
                if let Err(e) = manager.handle_event(&event).await {
                    tracing::error!("Failed to handle state change for virtual devices: {}", e);
                }
            }
        });

        Ok(())
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
        }
    }
}

#[cfg(test)]
mod tests {
    //! Input tracking doesn't run on its own in most of these: [`pump`]
    //! feeds it the bus, so each test decides exactly when tracking catches
    //! up, and nothing waits on a clock.
    use super::*;
    use crate::{DummyGateway, LightGroup, LightGroupLinear, SceneController, DEFAULT_GROUP_LEVEL};
    use std::time::Duration;
    use tokio::sync::broadcast::{self, error::TryRecvError};
    use tokio::sync::watch;

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

        assert_eq!(
            manager.dangling_references().await,
            vec![
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

    /// #15: input tracking must outlive falling behind the bus. Tracking is
    /// held at the device lock on an event for `a` while more events pile
    /// up behind it than the bus keeps (1000). A change to `b` must still
    /// re-derive `b`'s group `h`. Nothing about `a` can echo `h`, so that
    /// echo shows tracking got past the lag.
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
            // Tracking blocks here at its first event for `a`.
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
    /// store, the pull worker (GatewayWins) reverts them.
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
