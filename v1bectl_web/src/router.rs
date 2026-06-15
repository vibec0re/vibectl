use yew::prelude::*;
use yew_router::prelude::*;

use crate::pages::{about::About, devices::Devices, home::Home};

// 🔥 VIBEC0RE ROUTES - PURE RUST NAVIGATION! 🔥
#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    #[at("/")]
    Home,
    #[at("/devices")]
    Devices,
    #[at("/about")]
    About,
    #[not_found]
    #[at("/404")]
    NotFound,
}

pub fn switch(routes: Route) -> Html {
    match routes {
        Route::Home => html! { <Home /> },
        Route::Devices => html! { <Devices /> },
        Route::About => html! { <About /> },
        Route::NotFound => html! {
            <div class="container-fluid min-vh-100 d-flex align-items-center justify-content-center bg-dark">
                <div class="text-center">
                    <h1 class="display-1 text-danger fw-bold glow">{"404"}</h1>
                    <p class="lead text-white">{"PAGE NOT FOUND!"}</p>
                    <Link<Route> to={Route::Home} classes="btn btn-vibec0re">
                        {"🏠 GO HOME"}
                    </Link<Route>>
                </div>
            </div>
        },
    }
}
