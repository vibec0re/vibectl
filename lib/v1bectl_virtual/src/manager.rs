use crate::virtual_device::*;
use std::collections::HashMap;
use std::sync::Arc;
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
}

impl VirtualDeviceManager {
    pub fn new(state_store: Arc<StateStore>, event_bus: Arc<EventBus>) -> Self {
        Self {
            virtual_devices: Arc::new(RwLock::new(HashMap::new())),
            input_mappings: Arc::new(RwLock::new(HashMap::new())),
            output_mappings: Arc::new(RwLock::new(HashMap::new())),
            state_store,
            event_bus,
        }
    }

    /// Add a virtual device to the manager
    pub async fn add_virtual_device(
        &self,
        device: Box<dyn VirtualDevice>,
    ) -> Result<(), VirtualDeviceError> {
        let device_id = device.device_id().clone();
        let input_devices = device.input_devices();
        let output_devices = device.output_devices();

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
        let mut devices = self.virtual_devices.write().await;
        for virtual_id in virtual_device_ids {
            if let Some(virtual_device) = devices.get_mut(&virtual_id) {
                if let Err(e) = virtual_device.on_input_changed(device_id, new_state).await {
                    tracing::warn!(
                        "Virtual device {} failed to handle input change: {}",
                        virtual_id,
                        e
                    );
                    continue;
                }

                // Update virtual device state in store
                let new_virtual_state = virtual_device.current_state();
                if let Err(e) = self
                    .state_store
                    .update_device_state(&virtual_id, new_virtual_state)
                    .await
                {
                    tracing::error!("Failed to update virtual device state: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Set virtual device state (called from API)
    pub async fn set_virtual_device_state(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), VirtualDeviceError> {
        let mut devices = self.virtual_devices.write().await;

        if let Some(virtual_device) = devices.get_mut(device_id) {
            // Update virtual device
            virtual_device.set_state(new_state).await?;

            // Update state in store
            let current_state = virtual_device.current_state();
            self.state_store
                .update_device_state(device_id, current_state)
                .await?;

            Ok(())
        } else {
            Err(VirtualDeviceError::DeviceNotFound(device_id.clone()))
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
        }
    }
}
