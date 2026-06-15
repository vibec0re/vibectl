# Contributing to v1bectl 🔥

Thanks for wanting to hack on v1bectl! Contributions are very welcome.

## Development environment

v1bectl ships a [Nix flake](flake.nix) that pins the Rust toolchain and provides
everything you need (Rust, the `wasm32` target, `trunk`, OpenSSL, `pkg-config`):

```bash
nix develop      # drop into a shell with the full toolchain
cargo build      # build the native workspace
```

No Nix? A standard **stable Rust** toolchain works too. You'll additionally need:

- `pkg-config` + OpenSSL dev headers — for the gateway/server crates
- `trunk` + the `wasm32-unknown-unknown` target — for the web UI (`v1bectl_web`)
- GTK4 / libadwaita dev libraries — for the desktop app (`v1bectl_gtk`)

## Working without hardware 🧪

You don't need a real Dirigera hub. The `DummyGateway` (in `v1bectl_virtual`)
simulates a home so you can develop and test fully offline:

```bash
cargo run -p v1bectl_server -- dummy --scenario basic_home
```

## Before you open a PR ✅

CI runs the checks below. Please make sure they pass locally for the native
crates (the `gtk` and `web` crates need extra system toolchains, so CI skips
them — build those locally if your change touches them):

```bash
cargo fmt --all
cargo clippy --workspace --exclude v1bectl_gtk --exclude v1bectl_web --all-targets -- -D warnings
cargo test  --workspace --exclude v1bectl_gtk --exclude v1bectl_web
```

## Commit style ✨

This project has a soft spot for a little enthusiasm. Keep your messages clear
and descriptive — and a celebratory emoji or two is entirely on-brand.

## Conduct

Be kind, assume good faith, and let's make home automation fun. 💚
