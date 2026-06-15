# v1bectl Architecture Overview

## System Components

### Core Server (`v1bectl-server`)
- **Purpose**: Central state management and device coordination
- **Language**: Rust
- **Responsibilities**:
  - Collect and maintain device states from Dirigera gateway
  - Provide async RPC API for state changes
  - Handle state synchronization bidirectionally
  - Manage virtual device logic
  - WebSocket subscriptions for real-time updates

### CLI Client (`v1bectl-cli`)
- **Purpose**: Testing and manual control interface
- **Language**: Rust
- **Responsibilities**:
  - Send commands to server
  - Display device states
  - Subscribe to state changes
  - Interactive testing of API functions

### Gateway Integration Layer
- **Real Gateway**: Dirigera API at `gw2-xxxx.local` (your hub's mDNS hostname)
- **Dummy Gateway**: Mock implementation for testing
- **Responsibilities**:
  - Abstract gateway communication
  - Handle authentication (access tokens)
  - Translate between v1bectl and gateway protocols

## Data Flow

```
[Dirigera Gateway] <--> [Gateway Layer] <--> [State Manager] <--> [RPC API]
                                                    |
                                              [Virtual Devices]
                                                    |
                                              [WebSocket Subscriptions] <--> [Clients]
```

## Key Design Principles

1. **State Synchronization**: Bidirectional sync with conflict resolution
2. **Real-time Updates**: WebSocket subscriptions for immediate state changes  
3. **Testing First**: Dummy gateway enables development without hardware dependency
4. **Binary protocol**: Compact CBOR encoding for client/server messages
5. **Extensible**: Virtual device system allows complex automation scenarios

## Module Structure

```
v1bectl/
├── v1bectl_server/           # Main server binary
├── v1bectl_cli/              # CLI client binary  
├── lib/                      # Shared libraries
│   ├── v1bectl_state/        # Core device state types
│   ├── v1bectl_sync/         # Bidirectional sync engine + event store
│   ├── v1bectl_gateway/      # Gateway trait + Dirigera implementation
│   ├── v1bectl_virtual/      # Virtual devices + DummyGateway
│   └── v1bectl_api/          # WebSocket/TCP API server
├── v1bectl_tui/              # Terminal UI (ratatui)
├── v1bectl_web/              # Web UI (Yew/WASM)
└── v1bectl_gtk/              # GTK4 desktop app
```

## Communication Protocols

- **Transport**: TCP + WebSocket
- **Encoding**: CBOR (Compact Binary Object Representation)
- **Authentication**: None (local network assumption)
- **Correlation**: Request/response correlation IDs for async operations
