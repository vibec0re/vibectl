// TCP/WebSocket server - placeholder
pub struct ApiServer {
    // kept: placeholder server stub; `port` is stored for the upcoming
    // implementation and isn't read yet.
    #[allow(dead_code)]
    port: u16,
}

impl ApiServer {
    #[must_use]
    pub fn new(port: u16) -> Self {
        Self { port }
    }
}
