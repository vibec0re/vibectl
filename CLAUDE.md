# 🔥 CLAUDE.md - VIBEC0RE PROJECT CONTEXT 🔥

Guidance for AI assistants (and humans!) working in this repo.

## Project: v1bectl - High-Performance Home Automation Control System

**v1bectl** is a blazing fast, Rust-based home automation system that interfaces
with IKEA's Dirigera smart home gateway. This project embodies the **VIBEC0RE**
spirit: extreme performance, clean architecture, and infectious enthusiasm!

## 🎯 PROJECT OVERVIEW

**v1bectl** is a comprehensive home automation control system that:
- Provides a high-performance state management server
- Syncs bidirectionally with an IKEA Dirigera hub
- Supports virtual devices for advanced automation
- Uses WebSocket for real-time event streaming
- Runs on port 31337 (1337 = leet = ELITE! 🔥)

## 🏗️ ARCHITECTURE

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│ CLI/TUI/Web │────▶│    Server    │◀───▶│  Dirigera   │
│  Clients    │     │  (Port 31337)│     │   Gateway   │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │Virtual Device│
                    │   Manager    │
                    └─────────────┘
```

### Crates:
- `v1bectl_state`: Core device state types
- `v1bectl_sync`: Bidirectional sync engine + event store
- `v1bectl_gateway`: Gateway trait and Dirigera implementation
- `v1bectl_virtual`: Virtual devices and DummyGateway
- `v1bectl_api`: WebSocket / TCP API server
- `v1bectl_server`: Main server binary
- `v1bectl_cli`: Command-line client
- `v1bectl_tui`: Terminal UI (ratatui-based)
- `v1bectl_web`: Web UI (Yew / WASM)
- `v1bectl_gtk`: GTK4 desktop app

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed design.

## 🔥 VIBEC0RE STYLE GUIDE

### Commit Messages
Keep messages clear and descriptive — and a little celebratory energy is on-brand:
- A leading 🔥 and an emoji or two are welcome
- Say what changed and why
- Examples:
  - `🔥 FIX - sync engine reconnect backoff`
  - `🔥 outlet support - IKEA smart outlets working`

### Code Comments & Logs
- Emojis are welcome for important sections and categorized logs:
  - 🔥 major feature · 🚀 starting · ✅ success · ❌ error · 🔧 debug · 💚 healthy · 💔 unhealthy

## 🛠️ CURRENT STATE & KNOWN ISSUES

### Working Features ✅
- Device discovery from Dirigera
- Bidirectional state sync (5s intervals)
- WebSocket API for real-time updates
- CLI with list/get/set commands
- TUI with real-time device control
- Virtual device system (light groups, button controllers)
- Outlet control (array payload wrapper)

### Implementation Notes 🔧
1. **Dirigera API payload format**: updates must be wrapped in an array `[{ attributes: {...} }]`
2. **Device IDs**: Dirigera returns IDs with suffixes like `_1` — these are the real IDs
3. **DummyGateway** lives in the `v1bectl_virtual` crate

### Known Limitations ⚠️
- No authentication yet — **local network only**
- Manual CBOR schema (no code generation yet)
- TUI virtual device creation not fully implemented

## 💡 DEVELOPMENT GUIDELINES

1. **Use the Gateway trait** for hardware abstraction
2. **Test with `DummyGateway` first** before real hardware
3. **Update the sync engine** if you add new state types
4. **Run `cargo test`** — it should always pass

### Build & test (Nix flake provides the toolchain)
```bash
nix develop                                   # full toolchain shell
cargo run -p v1bectl_server -- dummy --scenario basic_home
cargo run -p v1bectl_cli -- light <device_id> --on true --brightness 50
cargo test --workspace --exclude v1bectl_gtk --exclude v1bectl_web
```

### Performance Targets
- State queries: < 1ms · Sync latency: < 100ms · Memory: < 50MB baseline · 100+ devices

## 🚀 QUICK START FOR NEW CONTRIBUTORS

1. **Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the separation of concerns
2. **Use `nix develop`** to get the pinned toolchain
3. **Use the dummy gateway** — you don't need real hardware to contribute
4. See [CONTRIBUTING.md](CONTRIBUTING.md) for the PR checklist

## 🎯 PROJECT PHILOSOPHY

- **SPEED**: ship fast · **QUALITY**: clean architecture, no compromises
- **ENTHUSIASM**: every commit is a small celebration
- Modern async Rust (Tokio), thread-safe state, real-time WebSocket streaming

**LET'S GOOOOO!** 🚀🔥⚡️
