use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use v1bectl_state::*;

#[derive(Debug)]
pub struct StateStore {
    devices: RwLock<HashMap<DeviceId, DeviceState>>,
    device_groups: RwLock<HashMap<String, DeviceGroupInfo>>,
}

impl StateStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            devices: RwLock::new(HashMap::new()),
            device_groups: RwLock::new(HashMap::new()),
        })
    }

    // Device operations
    pub async fn add_device(&self, device_info: DeviceInfo, initial_state: DeviceStateValue) {
        let device_id = device_info.device_id.clone();
        let device_state = DeviceState {
            device_id: device_id.clone(),
            device_info,
            state: initial_state,
            last_updated: chrono::Utc::now().timestamp_millis() as u64,
            last_synced_to_gateway: None,
            last_synced_from_gateway: None,
        };

        let mut devices = self.devices.write().await;
        devices.insert(device_id.clone(), device_state);
        info!("Added device: {}", device_id);
    }

    pub async fn get_device(&self, device_id: &DeviceId) -> Option<DeviceState> {
        let devices = self.devices.read().await;
        devices.get(device_id).cloned()
    }

    pub async fn list_devices(&self) -> Vec<DeviceState> {
        let devices = self.devices.read().await;
        devices.values().cloned().collect()
    }

    pub async fn list_devices_by_type(&self, device_type: &DeviceType) -> Vec<DeviceState> {
        let devices = self.devices.read().await;
        devices
            .values()
            .filter(|d| &d.device_info.device_type == device_type)
            .cloned()
            .collect()
    }

    pub async fn list_devices_by_group(&self, group_name: &str) -> Vec<DeviceState> {
        let devices = self.devices.read().await;
        devices
            .values()
            .filter(|d| {
                d.device_info
                    .device_groups
                    .contains(&group_name.to_string())
            })
            .cloned()
            .collect()
    }

    pub async fn update_device_state(
        &self,
        device_id: &DeviceId,
        new_state: DeviceStateValue,
    ) -> Result<(), StateError> {
        let mut devices = self.devices.write().await;
        if let Some(device) = devices.get_mut(device_id) {
            device.state = new_state;
            device.last_updated = chrono::Utc::now().timestamp_millis() as u64;
            debug!("Updated device state: {}", device_id);
            Ok(())
        } else {
            warn!("Device not found for state update: {}", device_id);
            Err(StateError::DeviceNotFound(device_id.clone()))
        }
    }

    pub async fn remove_device(&self, device_id: &DeviceId) -> Result<DeviceState, StateError> {
        let mut devices = self.devices.write().await;
        devices
            .remove(device_id)
            .ok_or_else(|| StateError::DeviceNotFound(device_id.clone()))
    }

    // Device group operations
    pub async fn create_device_group(&self, group_info: DeviceGroupInfo) -> Result<(), StateError> {
        let mut groups = self.device_groups.write().await;
        if groups.contains_key(&group_info.group_name) {
            return Err(StateError::GroupAlreadyExists(group_info.group_name));
        }
        groups.insert(group_info.group_name.clone(), group_info);
        Ok(())
    }

    pub async fn list_device_groups(&self) -> Vec<DeviceGroupInfo> {
        let groups = self.device_groups.read().await;
        groups.values().cloned().collect()
    }

    pub async fn get_device_group(&self, group_name: &str) -> Option<DeviceGroupInfo> {
        let groups = self.device_groups.read().await;
        groups.get(group_name).cloned()
    }

    // Stats and health
    pub async fn device_count(&self) -> usize {
        let devices = self.devices.read().await;
        devices.len()
    }

    pub async fn reachable_device_count(&self) -> usize {
        let devices = self.devices.read().await;
        devices.values().filter(|d| d.device_info.reachable).count()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("Device not found: {0}")]
    DeviceNotFound(DeviceId),
    #[error("Device group already exists: {0}")]
    GroupAlreadyExists(String),
    #[error("Device group not found: {0}")]
    GroupNotFound(String),
}
