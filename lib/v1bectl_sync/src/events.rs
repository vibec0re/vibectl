use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};
use v1bectl_state::*;

/// Receive from a broadcast channel while tolerating [`broadcast::error::RecvError::Lagged`].
///
/// A bare `while let Ok(event) = rx.recv().await` treats `Lagged` the same as
/// `Closed` and exits the loop for good, silently, the first time the
/// subscriber falls behind the channel's capacity. `recv_lossy` instead logs
/// a warning naming the subscriber and how many events were skipped, then
/// keeps waiting for the next event. It only gives up (returning `None`)
/// once the channel is actually closed.
pub async fn recv_lossy<T: Clone>(rx: &mut broadcast::Receiver<T>, name: &str) -> Option<T> {
    loop {
        match rx.recv().await {
            Ok(event) => return Some(event),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("⚠️ {name} subscriber lagged, skipped {n} events");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn test_event(n: u32) -> DeviceEvent {
        DeviceEvent {
            timestamp: SystemTime::now(),
            device_id: format!("device_{n}"),
            event_type: EventType::DeviceRemoved,
        }
    }

    /// A plain `while let Ok(event) = rx.recv().await` treats `Lagged` like
    /// `Closed` and gives up for good the first time a subscriber falls
    /// behind. `recv_lossy` must instead skip past the lag (with a warning)
    /// and keep delivering events, only stopping once the channel actually
    /// closes.
    #[tokio::test]
    async fn recv_lossy_survives_lag_and_ends_on_close() {
        let (tx, mut rx) = broadcast::channel(2);

        // Overflow the capacity-2 channel: only the last 2 of these 5 sends
        // survive in the buffer, so the receiver is guaranteed to be lagged
        // the first time it calls recv().
        for n in 0..5 {
            tx.send(test_event(n)).expect("receiver still subscribed");
        }

        // recv_lossy must swallow the Lagged error internally and hand back
        // the oldest event that's still actually in the buffer.
        let after_lag = recv_lossy(&mut rx, "test subscriber")
            .await
            .expect("expected an event after skipping the lag, got None (Closed)");
        assert_eq!(after_lag.device_id, "device_3");

        let next = recv_lossy(&mut rx, "test subscriber")
            .await
            .expect("expected the next buffered event");
        assert_eq!(next.device_id, "device_4");

        // A later, non-lagged send must still come through normally.
        tx.send(test_event(5)).expect("receiver still subscribed");
        let later = recv_lossy(&mut rx, "test subscriber")
            .await
            .expect("expected the post-lag event to still be delivered");
        assert_eq!(later.device_id, "device_5");

        // Only Closed should end the loop.
        drop(tx);
        assert!(
            recv_lossy(&mut rx, "test subscriber").await.is_none(),
            "expected None once the sender is dropped (Closed)"
        );
    }
}
