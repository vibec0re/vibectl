// 🔥 FAVORITES CONTEXT - GLOBAL STATE! 🔥

use crate::favorites::FavoritesConfig;
use std::rc::Rc;
use yew::prelude::*;

#[derive(Clone, PartialEq)]
pub struct FavoritesContext {
    pub favorites: Rc<FavoritesConfig>,
    pub toggle_favorite: Callback<String>,
}

pub type FavoritesContextProvider = ContextProvider<FavoritesContext>;

#[derive(Properties, PartialEq)]
pub struct FavoritesProviderProps {
    pub children: Children,
}

#[function_component(FavoritesProvider)]
pub fn favorites_provider(props: &FavoritesProviderProps) -> Html {
    let favorites = use_state(|| Rc::new(FavoritesConfig::load()));

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

    let context = FavoritesContext {
        favorites: (*favorites).clone(),
        toggle_favorite,
    };

    html! {
        <FavoritesContextProvider context={context}>
            {props.children.clone()}
        </FavoritesContextProvider>
    }
}

#[hook]
pub fn use_favorites_context() -> FavoritesContext {
    use_context::<FavoritesContext>().expect("FavoritesContext not found")
}
