// 🔥 FAVORITES HOOK - SHARED STATE! 🔥

use crate::favorites::FavoritesConfig;
use std::rc::Rc;
use yew::prelude::*;

#[hook]
pub fn use_favorites() -> UseFavoritesHandle {
    let favorites = use_state(|| Rc::new(FavoritesConfig::load()));
    let show_offline = use_state(|| false); // 🔥 DEFAULT: Hide offline devices! CHOOOM REQUESTED! 💖

    let toggle_favorite = {
        let favorites = favorites.clone();
        Callback::from(move |device_id: String| {
            log::info!("🔥 Toggle favorite called for: {}", device_id);
            let mut new_favorites = (*favorites.as_ref()).clone();
            let is_fav = new_favorites.toggle_favorite(device_id.clone());
            log::info!("🔥 Device {} is now favorite: {}", device_id, is_fav);
            log::info!("🔥 Total favorites: {}", new_favorites.device_ids.len());
            favorites.set(Rc::new(new_favorites));
        })
    };

    let is_favorite = {
        let favorites = favorites.clone();
        Callback::from(move |device_id: String| -> bool { favorites.is_favorite(&device_id) })
    };

    let toggle_show_offline = {
        let show_offline = show_offline.clone();
        Callback::from(move |_| {
            let new_state = !*show_offline;
            log::info!("💖 Toggle offline devices: {}", new_state);
            show_offline.set(new_state);
        })
    };

    UseFavoritesHandle {
        favorites: favorites.clone(),
        toggle_favorite,
        is_favorite,
        show_offline: *show_offline,
        toggle_show_offline,
    }
}

#[derive(Clone)]
pub struct UseFavoritesHandle {
    pub favorites: UseStateHandle<Rc<FavoritesConfig>>,
    pub toggle_favorite: Callback<String>,
    pub is_favorite: Callback<String, bool>,
    pub show_offline: bool, // 🔥 Whether to show offline devices
    pub toggle_show_offline: Callback<()>, // 💖 Toggle the filter!
}
