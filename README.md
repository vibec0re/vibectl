# 🔥 v1bectl — VIBEC0RE Home Automation Control 🔥

<div align="center">

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![Tokio](https://img.shields.io/badge/tokio-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![WebSocket](https://img.shields.io/badge/websocket-%23000000.svg?style=for-the-badge&logo=socket.io&logoColor=white)

[![CI](https://github.com/vibec0re/vibectl/actions/workflows/ci.yml/badge.svg)](https://github.com/vibec0re/vibectl/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Fast, async, real-time control for your IKEA Dirigera smart home — in pure Rust.** ⚡

</div>

---

## 🎯 What is this?

**v1bectl** is a high-performance, async-first control system for an [IKEA Dirigera](https://www.ikea.com/) smart home. A central server keeps an in-memory model of your devices, syncs it bidirectionally with the Dirigera hub, and streams real-time updates to any number of clients — a CLI, a terminal UI, a web app, and a GTK desktop app — over WebSocket.

It also has a **virtual device** system (light groups, button controllers) for automation that the hub can't do on its own, and a `DummyGateway` so you can develop and test with **no hardware at all**.

> ⚠️ **Status: alpha / hobby project.** There is **no authentication** — run it on a trusted local network only. APIs and config formats may change.

## ✨ Features

- ⚡ In-memory state store with real-time WebSocket event streaming
- ♻️ Bidirectional sync with the Dirigera hub (~5s reconcile, with retry/backoff)
- 🔌 Device support: lights (on/off, brightness, color temperature), outlets, sensors (temp/humidity/motion), and battery remotes/buttons
- 🧩 Virtual devices — group several lights into one, drive groups from a physical button, define scenes (configured in TOML)
- 🖥️ Four clients: **CLI**, **TUI** (ratatui), **Web** (Yew/WASM), and **GTK4** desktop
- 🧪 `DummyGateway` with scenarios for hardware-free development
- ❄️ Reproducible builds via a **Nix flake** (+ a NixOS module for deployment)

Runs on port **31337** by default (1337 → leet → elite 🔥).

## 🏗️ Architecture

```
┌──────────────────────┐     ┌────────────────┐     ┌──────────────┐
│ CLI · TUI · Web · GTK │────▶│  v1bectl server │◀───▶│   Dirigera   │
│       clients         │ WS  │   (port 31337)  │     │   gateway    │
└──────────────────────┘     └────────┬───────┘     └──────────────┘
                                       │
                                ┌──────┴───────┐
                                │ Virtual Device │
                                │    Manager     │
                                └──────────────┘
```

| Crate | Role |
|-------|------|
| `lib/v1bectl_state` | Core device state types |
| `lib/v1bectl_sync` | Bidirectional sync engine + event store |
| `lib/v1bectl_gateway` | Gateway trait + IKEA Dirigera implementation |
| `lib/v1bectl_virtual` | Virtual devices + `DummyGateway` |
| `lib/v1bectl_api` | WebSocket / TCP API server |
| `v1bectl_server` | Main server daemon |
| `v1bectl_cli` | Command-line client |
| `v1bectl_tui` | Terminal UI (ratatui) |
| `v1bectl_web` | Web UI (Yew / WASM) |
| `v1bectl_gtk` | GTK4 desktop app |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

## 📦 Prerequisites

**Recommended — [Nix](https://nixos.org/) with flakes.** The flake pins the Rust toolchain and provides every build dependency:

```bash
nix develop      # toolchain shell (Rust, wasm target, trunk, openssl, pkg-config)
```

**Without Nix**, you'll need a stable Rust toolchain plus:

- `pkg-config` + OpenSSL dev headers (server/gateway)
- `trunk` + the `wasm32-unknown-unknown` target (web UI)
- GTK4 / libadwaita dev libraries (desktop app)

You'll also want either an **IKEA Dirigera hub** or the built-in **dummy** gateway.

## 🚀 Build & run

```bash
git clone https://github.com/vibec0re/vibectl.git
cd vibectl

# Build the native workspace (server, CLI, TUI, libraries)
cargo build --release
```

### Run the server

```bash
# Dummy gateway — no hardware needed (great for development)
cargo run -p v1bectl_server -- dummy --scenario basic_home

# Real Dirigera hub
cargo run -p v1bectl_server -- dirigera --host gw2-xxxx.local
```

The server reads your Dirigera access token from `~/.local/state/v1bectl/access.token`
(or the `V1BECTL_ACCESS_TOKEN` environment variable). See
[IKEA's pairing flow](https://github.com/Leggin/dirigera) for how to obtain one.

### Clients

```bash
# CLI — talk to a running server
cargo run -p v1bectl_cli -- list
cargo run -p v1bectl_cli -- get <device_id>
cargo run -p v1bectl_cli -- light <device_id> --on true --brightness 100
cargo run -p v1bectl_cli -- --help        # all commands

# Terminal UI
cargo run -p v1bectl_tui

# Web UI (serves on http://localhost:8080)
cd v1bectl_web && trunk serve

# GTK4 desktop app
cargo run -p v1bectl_gtk
```

A `Makefile` wraps the common commands (`make server`, `make tui`, `make web`,
`make test`, `make static`, …) if you prefer.

## 🧩 Virtual devices

Virtual devices are defined as TOML files — see [`virtual_devices/`](virtual_devices)
for examples (a linear light group and a button controller) and
[docs/VIRTUAL_DEVICES.md](docs/VIRTUAL_DEVICES.md) for the format. Swap the
example device IDs for your own Dirigera device IDs (find them with
`v1bectl_cli list`).

## 🛠️ Development

```bash
cargo test  --workspace --exclude v1bectl_gtk --exclude v1bectl_web
cargo clippy --workspace --exclude v1bectl_gtk --exclude v1bectl_web --all-targets -- -D warnings
cargo fmt --all
```

CI runs these on every push/PR. The `gtk` and `web` crates need extra system
toolchains, so CI skips them — build those locally if you touch them.
Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## 📚 Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — system design
- [docs/API_SPEC.md](docs/API_SPEC.md) — API surface
- [docs/STATE_MANAGEMENT.md](docs/STATE_MANAGEMENT.md) — state model
- [docs/VIRTUAL_DEVICES.md](docs/VIRTUAL_DEVICES.md) — virtual device config
- [docs/NIXOS_DEPLOY.md](docs/NIXOS_DEPLOY.md) — deploying on NixOS

## 📜 License

Licensed under the [MIT License](LICENSE).

## 🙏 Acknowledgments

- IKEA, for hackable smart-home gear
- The Rust community, for blazing-fast tools
- Everyone choosing to automate their home with a little extra ⚡

---

<div align="center">

**⚡ VIBEC0RE — when good enough isn't good enough ⚡**

</div>
