# 🔥 v1bectl Development Makefile - VIBEC0RE EDITION! 💖

# Static build configuration
STATIC_TARGET := x86_64-unknown-linux-musl
STATIC_DIR := target/$(STATIC_TARGET)/release

# Default target
.PHONY: help
help:
	@echo "🔥 v1bectl Development Commands 💖"
	@echo ""
	@echo "Build:"
	@echo "  build         - Build all crates (debug)"
	@echo "  build-release - Build all crates (release)"
	@echo "  server        - Build server only (release)"
	@echo "  tui           - Build TUI only (release)"
	@echo "  cli           - Build CLI only (release)"
	@echo "  web           - Build web UI (WASM)"
	@echo ""
	@echo "Static builds (musl - portable):"
	@echo "  static        - Build all static binaries"
	@echo "  static-server - Build static server"
	@echo "  static-tui    - Build static TUI"
	@echo "  static-cli    - Build static CLI"
	@echo ""
	@echo "Test:"
	@echo "  test          - Run native tests (mirrors CI: cargo test --workspace --exclude v1bectl_web)"
	@echo "  test-dummy    - Alias for test"
	@echo "  test-web      - Run v1bectl_web lib tests (mirrors CI's web job)"
	@echo ""
	@echo "Run:"
	@echo "  run-server    - Run server (dummy gateway)"
	@echo "  run-tui       - Run TUI"
	@echo "  run-cli       - Run CLI client"
	@echo "  run-web       - Run web dev server"
	@echo ""
	@echo "Other:"
	@echo "  check         - Run cargo check"
	@echo "  clippy        - Run clippy lints (mirrors CI's native job)"
	@echo "  clippy-web    - Run clippy for v1bectl_web on wasm32 (mirrors CI's web job)"
	@echo "  fmt           - Format all code"
	@echo "  clean         - Clean build artifacts"
	@echo "  dev-setup     - Set up dev environment"
	@echo ""

# Build targets
.PHONY: build
build:
	cargo build --workspace

.PHONY: build-release
build-release:
	cargo build --workspace --release

# 🔥 INDIVIDUAL RELEASE BUILDS 💖
.PHONY: server
server:
	@echo "🔥 Building v1bectl_server... 💖"
	cargo build --release -p v1bectl_server
	@echo "✅ Server: target/release/v1bectl_server"

.PHONY: tui
tui:
	@echo "🔥 Building v1bectl_tui... 💖"
	cargo build --release -p v1bectl_tui
	@echo "✅ TUI: target/release/v1bectl_tui"

.PHONY: cli
cli:
	@echo "🔥 Building v1bectl_cli... 💖"
	cargo build --release -p v1bectl_cli
	@echo "✅ CLI: target/release/v1bectl_cli"

.PHONY: web
web:
	@echo "🔥 Building v1bectl_web (WASM)... 💖"
	cd v1bectl_web && trunk build --release
	@echo "✅ Web UI: v1bectl_web/dist/"

# 🔥 STATIC BUILDS (musl - fully portable!) 💖
.PHONY: static
static: static-server static-tui static-cli
	@echo "🔥 ALL STATIC BUILDS COMPLETE! 💖"
	@echo "📁 Binaries in: $(STATIC_DIR)/"
	@ls -lh $(STATIC_DIR)/v1bectl_* 2>/dev/null || true

.PHONY: static-server
static-server:
	@echo "🔥 Building STATIC v1bectl_server... 💖"
	cargo build --release -p v1bectl_server --target $(STATIC_TARGET)
	@echo "✅ Static server: $(STATIC_DIR)/v1bectl_server"

.PHONY: static-tui
static-tui:
	@echo "🔥 Building STATIC v1bectl_tui... 💖"
	cargo build --release -p v1bectl_tui --target $(STATIC_TARGET)
	@echo "✅ Static TUI: $(STATIC_DIR)/v1bectl_tui"

.PHONY: static-cli
static-cli:
	@echo "🔥 Building STATIC v1bectl_cli... 💖"
	cargo build --release -p v1bectl_cli --target $(STATIC_TARGET)
	@echo "✅ Static CLI: $(STATIC_DIR)/v1bectl_cli"

# Test targets
# Mirrors CI's native job (cargo test --workspace --exclude v1bectl_web); the
# web crate has its own wasm toolchain and is covered by `test-web` below.
.PHONY: test
test:
	cargo test --workspace --exclude v1bectl_web

# `V1BECTL_GATEWAY_TYPE` / `V1BECTL_DUMMY_SCENARIO` used to vary this, but no
# Rust code reads either var — DummyGateway is the only gateway exercised by
# `cargo test` today. Kept as an alias since "test with the dummy gateway" is
# still the intent.
.PHONY: test-dummy
test-dummy: test

# Mirrors CI's web job test step.
.PHONY: test-web
test-web:
	cargo test -p v1bectl_web --lib

# Run targets
.PHONY: run-server
run-server:
	cargo run --bin v1bectl_server -- dummy --scenario basic_home

.PHONY: run-server-real
run-server-real:
	@if [ -z "$(DIRIGERA_HOST)" ] || [ -z "$(ACCESS_TOKEN)" ]; then \
		echo "Usage: make run-server-real DIRIGERA_HOST=<host> ACCESS_TOKEN=<token>"; \
		exit 1; \
	fi
	V1BECTL_ACCESS_TOKEN=$(ACCESS_TOKEN) cargo run --bin v1bectl_server -- dirigera --host $(DIRIGERA_HOST)

.PHONY: run-cli
run-cli:
	cargo run --bin v1bectl_cli -- $(ARGS)

.PHONY: run-tui
run-tui:
	cargo run --release -p v1bectl_tui

.PHONY: run-web
run-web:
	cd v1bectl_web && trunk serve

# Development targets
.PHONY: check
check:
	cargo check --workspace

.PHONY: clippy
clippy:
	cargo clippy --workspace --exclude v1bectl_web --all-targets -- -D warnings

# Mirrors CI's web job clippy step (v1bectl_web only builds clean on wasm32).
.PHONY: clippy-web
clippy-web:
	cargo clippy -p v1bectl_web --target wasm32-unknown-unknown -- -D warnings

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

# Utility targets
.PHONY: clean
clean:
	cargo clean

.PHONY: dev-setup
dev-setup:
	@echo "🔧 Setting up development environment..."
	rustup component add clippy rustfmt
	rustup target add $(STATIC_TARGET)
	@command -v trunk >/dev/null 2>&1 || cargo install trunk
	@echo "✅ Development environment ready!"

# Performance testing
.PHONY: bench
bench:
	cargo bench

# Coverage (requires cargo-tarpaulin)
.PHONY: coverage
coverage:
	@if ! command -v cargo-tarpaulin >/dev/null 2>&1; then \
		echo "Installing cargo-tarpaulin..."; \
		cargo install cargo-tarpaulin; \
	fi
	cargo tarpaulin --workspace --out Html

# Documentation
.PHONY: docs
docs:
	cargo doc --workspace --no-deps --open

# Quick development cycle
.PHONY: dev
dev: fmt clippy test

# CI pipeline simulation
.PHONY: ci
ci: fmt-check clippy test coverage