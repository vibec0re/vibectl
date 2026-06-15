use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};
use v1bectl_state::*;

pub struct EventBus {
    sender: broadcast::Sender<DeviceEvent>,
    event_history: Arc<RwLock<VecDeque<DeviceEvent>>>,
    max_history: usize,
}

impl EventBus {
    pub fn new(max_history: usize) -> Self {
        let (sender, _) = broadcast::channel(1000);
        Self {
            sender,
            event_history: Arc::new(RwLock::new(VecDeque::with_capacity(max_history))),
            max_history,
        }
    }

    pub async fn publish(&self, event: DeviceEvent) {
        // Store in history
        {
            let mut history = self.event_history.write().await;
            if history.len() >= self.max_history {
                history.pop_front();
            }
            history.push_back(event.clone());
        }

        // Broadcast to subscribers
        if self.sender.send(event.clone()).is_err() {
            debug!("No subscribers for event: {:?}", event.event_type);
        } else {
            info!(
                "Published event: {:?} for device {}",
                event.event_type, event.device_id
            );
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DeviceEvent> {
        self.sender.subscribe()
    }

    pub async fn get_recent_events(&self, limit: Option<usize>) -> Vec<DeviceEvent> {
        let history = self.event_history.read().await;
        let take_count = limit.unwrap_or(history.len()).min(history.len());
        history.iter().rev().take(take_count).cloned().collect()
    }

    pub async fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
