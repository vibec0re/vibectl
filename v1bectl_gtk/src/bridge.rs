// 🔥 BRIDGE TYPES - CROSS-THREAD COMMUNICATION!!! 🚀

use v1bectl_sync::*;

/// Messages sent from Tokio background thread → GTK main thread
#[derive(Debug, Clone)]
pub enum GtkMsg {
    /// Initial device list received after discovery completes
    DevicesDiscovered(Vec<DeviceState>),

    /// A single device's state changed (from subscription event or server push)
    DeviceStateChanged {
        device_id: String,
        new_state: DeviceStateValue,
    },

    /// Connection status to the server changed
    ConnectionStatus(ConnectionState),
}

/// Commands sent from GTK main thread → Tokio background thread
#[derive(Debug, Clone)]
pub enum ServerCmd {
    /// Set light state (brightness, color temperature, RGB, on/off)
    SetLightState {
        device_id: String,
        is_on: Option<bool>,
        brightness: Option<u8>,
        color_temp: Option<u16>,
    },

    /// Set outlet power state
    SetOutletState { device_id: String, is_on: bool },

    /// Force a re-discovery of all devices on the gateway
    Rediscover,
}

/// Connection state to the v1bectl server
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Actively attempting to connect
    Connecting,
    /// Connected and receiving events
    Connected,
    /// Not connected (graceful disconnect or startup)
    Disconnected,
    /// Reconnecting after a network error (includes attempt count)
    Reconnecting(u32),
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Connecting => write!(f, "Connecting..."),
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Reconnecting(attempt) => {
                write!(f, "Reconnecting... (attempt {})", attempt)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_display() {
        assert_eq!(ConnectionState::Connecting.to_string(), "Connecting...");
        assert_eq!(ConnectionState::Connected.to_string(), "Connected");
        assert_eq!(
            ConnectionState::Reconnecting(5).to_string(),
            "Reconnecting... (attempt 5)"
        );
    }

    #[test]
    fn test_server_cmd_variants() {
        let cmd = ServerCmd::SetLightState {
            device_id: "light-1".to_string(),
            is_on: Some(true),
            brightness: Some(50),
            color_temp: None,
        };

        // Just verify it can be constructed and cloned
        let _ = cmd.clone();
    }
}
