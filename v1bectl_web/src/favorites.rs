// 🔥 FAVORITES MANAGER - LOCAL STORAGE SAVE! 🔥

use gloo_storage::{LocalStorage, Storage};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const FAVORITES_KEY: &str = "v1bectl_favorites";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct FavoritesConfig {
    pub device_ids: HashSet<String>,
}

impl FavoritesConfig {
    // Load favorites from local storage
    pub fn load() -> Self {
        match LocalStorage::get::<FavoritesConfig>(FAVORITES_KEY) {
            Ok(config) => {
                log::info!(
                    "🔥 Loaded {} favorites from local storage!",
                    config.device_ids.len()
                );
                config
            }
            Err(e) => {
                log::warn!("No favorites found or error loading: {:?}", e);
                Self::default()
            }
        }
    }

    // Save favorites to local storage
    pub fn save(&self) -> Result<(), gloo_storage::errors::StorageError> {
        LocalStorage::set(FAVORITES_KEY, self)?;
        log::info!(
            "💾 Saved {} favorites to local storage!",
            self.device_ids.len()
        );
        Ok(())
    }

    // Add a favorite
    pub fn add_favorite(&mut self, device_id: String) -> bool {
        let added = self.device_ids.insert(device_id.clone());
        if added {
            log::info!("⭐ Added favorite: {}", device_id);
            let _ = self.save();
        }
        added
    }

    // Remove a favorite
    pub fn remove_favorite(&mut self, device_id: &str) -> bool {
        let removed = self.device_ids.remove(device_id);
        if removed {
            log::info!("❌ Removed favorite: {}", device_id);
            let _ = self.save();
        }
        removed
    }

    // Toggle favorite status
    pub fn toggle_favorite(&mut self, device_id: String) -> bool {
        if self.is_favorite(&device_id) {
            self.remove_favorite(&device_id);
            false
        } else {
            self.add_favorite(device_id);
            true
        }
    }

    // Check if device is favorite
    pub fn is_favorite(&self, device_id: &str) -> bool {
        self.device_ids.contains(device_id)
    }
}
