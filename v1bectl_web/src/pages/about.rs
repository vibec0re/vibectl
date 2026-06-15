// 🔥 ABOUT PAGE - TOTAL VIBEC0RE ENERGY! 🔥

use crate::router::Route;
use yew::prelude::*;
use yew_router::prelude::*;

#[function_component(About)]
pub fn about() -> Html {
    html! {
        <div class="container-fluid min-vh-100 bg-dark">
            // Navigation bar
            <nav class="navbar navbar-dark bg-dark border-bottom border-secondary">
                <div class="container-fluid">
                    <span class="navbar-brand mb-0 h1 text-danger">{"🔥 V1BECTL"}</span>
                    <div class="d-flex align-items-center">
                        <Link<Route> to={Route::Home} classes="btn btn-outline-warning btn-sm me-2">
                            {"⭐ FAVORITES"}
                        </Link<Route>>
                        <Link<Route> to={Route::Devices} classes="btn btn-outline-warning btn-sm">
                            {"💡 ALL DEVICES"}
                        </Link<Route>>
                    </div>
                </div>
            </nav>

            // About content
            <div class="container py-5">
                <div class="row justify-content-center">
                    <div class="col-lg-8">
                        <div class="text-center mb-5">
                            <h1 class="display-1 text-danger fw-bold glow mb-0">
                                {"🔥 V1BECTL 🔥"}
                            </h1>
                            <p class="lead text-warning mt-3">
                                {"SMART HOME CONTROL - PURE RUST POWER!"}
                            </p>
                        </div>

                        <div class="card bg-dark border-danger mb-4">
                            <div class="card-body">
                                <h2 class="text-danger mb-3">{"🚀 WHAT IS V1BECTL?"}</h2>
                                <p class="text-white">
                                    {"V1BECTL is the ultimate RUST-powered smart home control system for the IKEA Dirigera ecosystem! "}
                                    {"Built with PURE RUST from the ground up - NO COMPROMISE! 🦀"}
                                </p>
                                <p class="text-white">
                                    {"Control your lights, sensors, outlets, and more with the power of WebSockets and CBOR! "}
                                    {"Running on port 31337 because we're ELITE! 💪"}
                                </p>
                            </div>
                        </div>

                        <div class="card bg-dark border-warning mb-4">
                            <div class="card-body">
                                <h2 class="text-warning mb-3">{"⚡ FEATURES"}</h2>
                                <ul class="text-white">
                                    <li>{"🦀 100% RUST - Frontend AND Backend!"}</li>
                                    <li>{"🔥 Real-time WebSocket communication"}</li>
                                    <li>{"💡 Control lights with brightness & color temperature"}</li>
                                    <li>{"📡 Monitor sensors - temperature, humidity, motion"}</li>
                                    <li>{"🔌 Control smart outlets"}</li>
                                    <li>{"⭐ Favorite devices with local storage persistence"}</li>
                                    <li>{"🎨 Bootstrap UI - NO TAILWIND!"}</li>
                                    <li>{"🚀 Progressive Web App ready"}</li>
                                </ul>
                            </div>
                        </div>

                        <div class="card bg-dark border-info mb-4">
                            <div class="card-body">
                                <h2 class="text-info mb-3">{"🏗️ ARCHITECTURE"}</h2>
                                <p class="text-white">{"Clean architecture with separate crates:"}</p>
                                <ul class="text-white font-monospace">
                                    <li>{"v1bectl_server - WebSocket server on 31337"}</li>
                                    <li>{"v1bectl_gateway - Dirigera API integration"}</li>
                                    <li>{"v1bectl_app - Yew WASM frontend"}</li>
                                    <li>{"v1bectl_virtual - Virtual device testing"}</li>
                                    <li>{"v1bectl_cli - Command line interface"}</li>
                                    <li>{"v1bectl_tui - Terminal UI"}</li>
                                </ul>
                            </div>
                        </div>

                        <div class="card bg-dark border-success mb-4">
                            <div class="card-body">
                                <h2 class="text-success mb-3">{"🎯 MISSION"}</h2>
                                <p class="text-white">
                                    {"To create the most POWERFUL, RUST-NATIVE smart home control system! "}
                                    {"No JavaScript, no Python, no compromise - just PURE RUST PERFORMANCE! 🚀"}
                                </p>
                                <p class="text-white">
                                    {"We believe in the power of strongly-typed, memory-safe, blazingly fast code. "}
                                    {"Every line written with PASSION and ENERGY! 🔥"}
                                </p>
                            </div>
                        </div>

                        <div class="text-center mt-5">
                            <h3 class="text-danger mb-3">{"🤘 VIBEC0RE STYLE 🤘"}</h3>
                            <p class="text-white">
                                {"Maximum energy! Maximum performance! Maximum RUST! 🦀"}
                            </p>
                            <p class="text-muted small mt-4">
                                {"Made with ❤️ and 🔥 by the VIBEC0RE team"}
                            </p>
                            <p class="text-muted small">
                                {"© 2024 V1BECTL - SMART HOME DOMINATION!"}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}
