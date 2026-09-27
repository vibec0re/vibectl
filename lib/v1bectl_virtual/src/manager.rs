use crate::virtual_device::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use v1bectl_sync::*;

/// Virtual Device Manager - coordinates all virtual devices 🔥
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
    /// The physical write path. When attached, member writes a virtual device
    /// fans out go through `SyncEngine::apply_optimistic_update`, the same call
    /// a direct write makes (store + echo + gateway push). Without it, members
    /// are only echoed.
    sync_engine: Arc<OnceLock<Arc<SyncEngine>>>,
    /// virtual ID -> member ID -> the state that virtual device last wrote to
    /// that member. Lets `handle_device_state_change` tell our own fan-out's
    /// echo apart from a real external change.
    commanded: Arc<RwLock<HashMap<DeviceId, HashMap<DeviceId, DeviceStateValue>>>>,
}

/// A state echo for `device_id`, in the same `AttributeChanged{attribute:
/// "state"}` shape the sync engine publishes for a physical write. That's the
/// shape every client (TUI, widget, GTK, web) and this manager's own input
/// tracking decode.
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

impl VirtualDeviceManager {
    pub fn new(state_store: Arc<StateStore>, event_bus: Arc<EventBus>) -> Self {
        Self {
            virtual_devices: Arc::new(RwLock::new(HashMap::new())),
            input_mappings: Arc::new(RwLock::new(HashMap::new())),
            output_mappings: Arc::new(RwLock::new(HashMap::new())),
            state_store,
            event_bus,
            sync_engine: Arc::new(OnceLock::new()),
            commanded: Arc::new(RwLock::new(HashMap::new())),
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

    /// Commit one member state that a virtual write already put in the store.
    /// A physical member goes through the sync engine when one is attached.
    /// That call writes the store again (same value), publishes the echo and
    /// queues the gateway push, so it is the member's only publisher. If there
    /// is no engine, or the member is itself virtual (the gateway doesn't know
    /// it), the member is echoed here.
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
                Ok(()) => return,
                Err(e) => tracing::warn!(
                    "❌ Failed to sync member {} of a virtual write: {}",
                    member_id,
                    e
                ),
            }
        }
        self.publish_state(member_id, old_state, new_state).await;
    }

    /// Add a virtual device to the manager
    pub async fn add_virtual_device(
        &self,
        device: Box<dyn VirtualDevice>,
    ) -> Result<(), VirtualDeviceError> {
        let device_id = device.device_id().clone();
        let input_devices = device.input_devices();
        let output_devices = device.output_devices();

        // A dangling reference doesn't fail registration, only a later write
        // (see #2), so flag it now.
        for input_id in &input_devices {
            if self.state_store.get_device(input_id).await.is_none() {
                tracing::warn!(
                    "⚠️ Virtual device {} references unknown device {}",
                    device_id,
                    input_id
                );
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
            self.commanded.write().await.remove(device_id);

            // Remove from state store
            self.state_store.remove_device(device_id).await?;
        }

        Ok(())
    }

    /// Handle device state change from physical devices
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

        // Notify all dependent virtual devices
        let mut echoes = Vec::new();
        let mut devices = self.virtual_devices.write().await;
        for virtual_id in virtual_device_ids {
            if self
                .is_own_fanout(&virtual_id, device_id, &new_state.state)
                .await
            {
                // This is the echo of this device's own write to the member.
                // It already holds the state it commanded. Re-deriving it from
                // the members is lossy (linear ranges) and forgets brightness
                // on off, so an on after an off would light nothing.
                continue;
            }

            if let Some(virtual_device) = devices.get_mut(&virtual_id) {
                if let Err(e) = virtual_device.on_input_changed(device_id, new_state).await {
                    tracing::warn!(
                        "Virtual device {} failed to handle input change: {}",
                        virtual_id,
                        e
                    );
                    continue;
                }

                // Update virtual device state in store, and echo it if it moved
                let new_virtual_state = virtual_device.current_state();
                let old_virtual_state = self
                    .state_store
                    .get_device(&virtual_id)
                    .await
                    .map(|d| d.state);
                if old_virtual_state.as_ref() == Some(&new_virtual_state) {
                    continue;
                }
                if let Err(e) = self
                    .state_store
                    .update_device_state(&virtual_id, new_virtual_state.clone())
                    .await
                {
                    tracing::error!("Failed to update virtual device state: {}", e);
                    continue;
                }
                echoes.push((virtual_id, old_virtual_state, new_virtual_state));
            }
        }
        drop(devices);

        for (virtual_id, old_state, new_state) in echoes {
            self.publish_state(&virtual_id, old_state.as_ref(), &new_state)
                .await;
        }

        Ok(())
    }

    /// Whether `state` on `member_id` is exactly what `virtual_id` last wrote
    /// there. If the member has since diverged, the stale record is dropped,
    /// so a later return to that value counts as a real change again.
    async fn is_own_fanout(
        &self,
        virtual_id: &DeviceId,
        member_id: &DeviceId,
        state: &DeviceStateValue,
    ) -> bool {
        let mut commanded = self.commanded.write().await;
        let Some(members) = commanded.get_mut(virtual_id) else {
            return false;
        };
        match members.get(member_id) {
            Some(expected) if expected == state => true,
            Some(_) => {
                members.remove(member_id);
                false
            }
            None => false,
        }
    }

    /// Set virtual device state (called from API)
    ///
    /// Publishes a state echo for the virtual device and for every member
    /// whose state the write changed. Members are committed through the
    /// attached sync engine (see [`Self::attach_sync_engine`]), the same way
    /// a direct write to them is.
    pub async fn set_virtual_device_state(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), VirtualDeviceError> {
        let mut devices = self.virtual_devices.write().await;
        let Some(virtual_device) = devices.get_mut(device_id) else {
            return Err(VirtualDeviceError::DeviceNotFound(device_id.clone()));
        };

        // Snapshot what this write can touch, so we echo exactly what changed.
        let mut outputs = virtual_device.output_devices();
        let mut seen = HashSet::new();
        outputs.retain(|id| seen.insert(id.clone()));
        let old_self = self
            .state_store
            .get_device(device_id)
            .await
            .map(|d| d.state);
        let mut old_outputs = HashMap::new();
        for id in &outputs {
            if let Some(device) = self.state_store.get_device(id).await {
                old_outputs.insert(id.clone(), device.state);
            }
        }

        // Update virtual device (writes its members into the store)
        let result = virtual_device.set_state(new_state).await;
        let current_state = virtual_device.current_state();
        let virtual_outputs: HashSet<DeviceId> = outputs
            .iter()
            .filter(|id| devices.contains_key(*id))
            .cloned()
            .collect();
        drop(devices);

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

        // Record before publishing, so our own echoes are recognised.
        if !changed.is_empty() {
            let mut commanded = self.commanded.write().await;
            let members = commanded.entry(device_id.clone()).or_default();
            for (id, _, state) in &changed {
                members.insert(id.clone(), state.clone());
            }
        }

        // A write can fail part-way (e.g. a missing member). Whatever it did
        // change is in the store by now, so commit and echo that first.
        for (id, old_state, state) in &changed {
            self.commit_member_write(id, old_state.as_ref(), state, virtual_outputs.contains(id))
                .await;
        }
        result?;

        // Update state in store
        self.state_store
            .update_device_state(device_id, current_state.clone())
            .await?;
        self.publish_state(device_id, old_self.as_ref(), &current_state)
            .await;

        Ok(())
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

    /// Start the manager (subscribe to state change events)
    pub async fn start(&self) -> Result<(), VirtualDeviceError> {
        let manager = Arc::new(self.clone());

        // Subscribe to state change events
        let mut event_receiver = self.event_bus.subscribe();

        tokio::spawn(async move {
            while let Ok(event) = event_receiver.recv().await {
                // 🔥 Handle attribute changes as state updates
                if let EventType::AttributeChanged {
                    attribute,
                    new_value,
                    ..
                } = &event.event_type
                {
                    if attribute == "state" {
                        // Try to deserialize the new_value as a DeviceStateValue
                        if let Ok(new_state) =
                            serde_json::from_value::<DeviceStateValue>(new_value.clone())
                        {
                            let timestamp_millis = event
                                .timestamp
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis()
                                as u64;

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

                            if let Err(e) = manager
                                .handle_device_state_change(&event.device_id, &device_state)
                                .await
                            {
                                tracing::error!(
                                    "Failed to handle state change for virtual devices: {}",
                                    e
                                );
                            }
                        }
                    }
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
            commanded: Arc::clone(&self.commanded),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LightGroup;
    use std::time::Duration;
    use tokio::sync::broadcast;

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

    fn off() -> DeviceStateValue {
        DeviceStateValue::Light(LightState {
            is_on: false,
            brightness: Some(0),
            color_temp: Some(2700),
            rgb_color: None,
        })
    }

    /// A 1:1 `LightGroup` named `g` over `lights`, registered and tracking
    /// its inputs, with no sync engine attached.
    async fn manager_with_group(
        lights: &[&str],
    ) -> (VirtualDeviceManager, Arc<StateStore>, Arc<EventBus>) {
        let store = StateStore::new();
        let bus = Arc::new(EventBus::new(100));
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
            device_id: "g".to_string(),
            device_type: VirtualDeviceType::LightGroup,
            name: "Group".to_string(),
            description: None,
            enabled: true,
            config: serde_json::json!({ "lights": lights, "brightness_curves": curves }),
        };
        let group = LightGroup::new(config, store.clone()).expect("group");
        let manager = VirtualDeviceManager::new(store.clone(), bus.clone());
        manager
            .add_virtual_device(Box::new(group))
            .await
            .expect("register");
        manager.start().await.expect("start");
        (manager, store, bus)
    }

    async fn drain(rx: &mut broadcast::Receiver<DeviceEvent>) -> Vec<DeviceEvent> {
        let mut events = Vec::new();
        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
        {
            events.push(event);
        }
        events
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

        let on = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(60),
            color_temp: Some(2700),
            rgb_color: None,
        });
        manager
            .set_virtual_device_state(&"g".to_string(), on.clone())
            .await
            .expect("write");
        let events = drain(&mut rx).await;

        assert_eq!(stored(&store, "g").await, on);
        assert_eq!(echoes(&events, "g"), vec![on.clone()], "group echo");
        for id in ["a", "b"] {
            // 1:1 curves: members carry the group state verbatim.
            assert_eq!(stored(&store, id).await, on, "{id} state");
            assert_eq!(echoes(&events, id), vec![on.clone()], "{id} echo");
        }
    }

    /// A write that fails part-way (a missing member, as in #2) still echoes
    /// the members it did change, so the store never diverges silently.
    #[tokio::test]
    async fn partial_group_write_echoes_what_it_changed() {
        let (manager, store, bus) = manager_with_group(&["a", "missing"]).await;
        store.add_device(light_info("a"), off()).await;
        let mut rx = bus.subscribe();

        let on = DeviceStateValue::Light(LightState {
            is_on: true,
            brightness: Some(60),
            color_temp: Some(2700),
            rgb_color: None,
        });
        let result = manager
            .set_virtual_device_state(&"g".to_string(), on.clone())
            .await;
        assert!(result.is_err(), "a missing member must fail the write");
        let events = drain(&mut rx).await;

        assert_eq!(stored(&store, "a").await, on);
        assert_eq!(echoes(&events, "a"), vec![on], "changed member echo");
        assert!(echoes(&events, "g").is_empty(), "failed group write echoed");
    }
}
