use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};
use v1bectl_state::DeviceEvent;

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
            Err(RecvError::Lagged(n)) => warn_lagged(name, n),
            Err(RecvError::Closed) => return None,
        }
    }
}

/// The one log line for a subscriber `name` that fell behind its channel
/// and missed `skipped` events.
fn warn_lagged(name: &str, skipped: u64) {
    warn!("⚠️ {name} subscriber lagged, skipped {skipped} events");
}

/// What [`LagAwareReceiver::recv`] got from a broadcast channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recv<T> {
    /// The next event.
    Event(T),
    /// The subscriber fell behind the channel's capacity, and this many
    /// events were dropped before it got to them. The events that were
    /// still buffered have been handed back already, so this is the time to
    /// catch up on what the dropped ones changed.
    Lagged(u64),
    /// Every sender is gone, so no more events will come.
    Closed,
}

/// A subscription to a broadcast channel for a subscriber that has to know
/// when it missed events, so it can catch up on what they changed (#15).
///
/// [`Self::recv`] reports a lag as [`Recv::Lagged`], and logs it the way
/// [`recv_lossy`] does, but only after it has handed back the events still
/// buffered. Tokio moves a lagged receiver to the oldest event the channel
/// still holds, a whole ring behind the newest one. A subscriber that caught
/// up right there (a resync from the store) would lag again on anything
/// published meanwhile, its own echoes included, and catch up again, over
/// and over (#47 review). Once the buffer is drained, the receiver is at the
/// channel's edge. So a lag costs one catch-up, and no event older than the
/// catch-up comes after it.
///
/// Falling behind again while draining (the subscriber is still slower than
/// the channel) is part of the same lag: its events are added to the count,
/// and the drain goes on from the oldest event left. The drain ends at the
/// edge, or after a ring's worth of events without falling behind again. So
/// under a burst that doesn't let up, a subscriber that keeps pace but can't
/// get to the edge still catches up, once per ring's worth.
#[derive(Debug)]
pub struct LagAwareReceiver<T> {
    rx: broadcast::Receiver<T>,
    name: &'static str,
    /// Set while draining after a lag.
    catching_up: Option<CatchUp>,
}

/// A [`LagAwareReceiver`]'s drain after a lag.
#[derive(Debug)]
struct CatchUp {
    /// The events dropped so far.
    skipped: u64,
    /// How many more buffered events it hands back before it reports the
    /// lag: what the channel buffered when the receiver last fell behind.
    left: usize,
}

impl<T: Clone> LagAwareReceiver<T> {
    /// Wrap `rx`, the subscription of the subscriber `name` (as the logs
    /// call it).
    #[must_use]
    pub fn new(rx: broadcast::Receiver<T>, name: &'static str) -> Self {
        Self {
            rx,
            name,
            catching_up: None,
        }
    }

    /// The next event, or a lag once the events still buffered after it
    /// have been handed back, or the end of the channel.
    pub async fn recv(&mut self) -> Recv<T> {
        loop {
            let Some(catch_up) = &mut self.catching_up else {
                match self.rx.recv().await {
                    Ok(event) => return Recv::Event(event),
                    Err(RecvError::Lagged(n)) => {
                        // Right after a lag, the receiver is a whole ring
                        // behind: this is what the channel still buffers.
                        let left = self.rx.len();
                        self.catching_up = Some(CatchUp { skipped: n, left });
                        continue;
                    }
                    Err(RecvError::Closed) => return Recv::Closed,
                }
            };
            if catch_up.left > 0 {
                match self.rx.try_recv() {
                    Ok(event) => {
                        catch_up.left -= 1;
                        return Recv::Event(event);
                    }
                    Err(TryRecvError::Lagged(n)) => {
                        catch_up.skipped = catch_up.skipped.saturating_add(n);
                        catch_up.left = self.rx.len();
                        continue;
                    }
                    // At the channel's edge (or it closed, which the next
                    // receive reports).
                    Err(TryRecvError::Empty | TryRecvError::Closed) => {}
                }
            }
            // The lag is reported here either at the channel's edge or with
            // the budget spent. Spent is the weaker of the two (#47
            // re-check): a subscriber that kept pace for a whole ring
            // without reaching the edge may still be almost a ring behind.
            // Then whatever its catch-up publishes (the manager's group
            // echoes, say) pushes the oldest buffered events out, and it
            // lags, and catches up, once more. That's bounded: a ring's
            // worth of events is consumed for every catch-up, and it takes
            // a burst that keeps pace for a whole ring (1024 events on the
            // server's bus) to get here. An event lost that way is lost the
            // way any lag loses one.
            let skipped = catch_up.skipped;
            self.catching_up = None;
            warn_lagged(self.name, skipped);
            return Recv::Lagged(skipped);
        }
    }
}

pub struct EventBus {
    sender: broadcast::Sender<DeviceEvent>,
    event_history: Arc<RwLock<VecDeque<DeviceEvent>>>,
    max_history: usize,
}

impl EventBus {
    /// A bus that keeps the last `max_history` events, and lets a
    /// subscriber fall 1000 events behind before it lags (see
    /// [`Self::with_capacity`]).
    #[must_use]
    pub fn new(max_history: usize) -> Self {
        Self::with_capacity(max_history, 1000)
    }

    /// A bus that keeps the last `max_history` events, and lets a
    /// subscriber fall `capacity` events behind (rounded up to a power of
    /// two) before it lags and misses the oldest ones. A test can use a
    /// small one to make a subscriber lag.
    ///
    /// # Panics
    ///
    /// If `capacity` is 0, or larger than `usize::MAX / 2`: tokio's
    /// broadcast channel can't hold either.
    #[must_use]
    pub fn with_capacity(max_history: usize, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
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

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<DeviceEvent> {
        self.sender.subscribe()
    }

    pub async fn get_recent_events(&self, limit: Option<usize>) -> Vec<DeviceEvent> {
        let history = self.event_history.read().await;
        let take_count = limit.unwrap_or(history.len()).min(history.len());
        history.iter().rev().take(take_count).cloned().collect()
    }

    // No callers anywhere in the workspace await this (or call it at all),
    // so dropping `async` is a pure signature simplification, not a
    // behaviour change.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use v1bectl_state::EventType;

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

    /// What a [`LagAwareReceiver`] got, with an event as its device id.
    async fn next(rx: &mut LagAwareReceiver<DeviceEvent>) -> Recv<String> {
        match rx.recv().await {
            Recv::Event(event) => Recv::Event(event.device_id),
            Recv::Lagged(n) => Recv::Lagged(n),
            Recv::Closed => Recv::Closed,
        }
    }

    fn device(n: u32) -> Recv<String> {
        Recv::Event(format!("device_{n}"))
    }

    /// A lag is reported once, with how many events were dropped, but only
    /// after the events still buffered: the receiver is then at the
    /// channel's edge, so a catch-up there doesn't lag it again (#15, #47
    /// review).
    #[tokio::test]
    async fn lag_aware_receiver_drains_the_buffer_then_reports_the_lag() {
        let (tx, rx) = broadcast::channel(2);
        let mut rx = LagAwareReceiver::new(rx, "test subscriber");
        for n in 0..5 {
            tx.send(test_event(n)).expect("receiver still subscribed");
        }

        assert_eq!(next(&mut rx).await, device(3));
        assert_eq!(next(&mut rx).await, device(4));
        assert_eq!(next(&mut rx).await, Recv::Lagged(3));

        // A catch-up's own publishes are the next events, not another lag.
        tx.send(test_event(5)).expect("receiver still subscribed");
        tx.send(test_event(6)).expect("receiver still subscribed");
        assert_eq!(next(&mut rx).await, device(5));
        assert_eq!(next(&mut rx).await, device(6));

        drop(tx);
        assert_eq!(next(&mut rx).await, Recv::Closed);
    }

    /// Falling behind again while draining is the same lag: the events
    /// dropped are added to it, the drain goes on to the edge, and the lag
    /// is still reported once.
    #[tokio::test]
    async fn lag_aware_receiver_counts_a_lag_while_draining_into_the_same_one() {
        let (tx, rx) = broadcast::channel(2);
        let mut rx = LagAwareReceiver::new(rx, "test subscriber");
        for n in 0..5 {
            tx.send(test_event(n)).expect("receiver still subscribed");
        }
        assert_eq!(next(&mut rx).await, device(3));

        // 4 and 5 are overwritten before it gets to them.
        for n in 5..8 {
            tx.send(test_event(n)).expect("receiver still subscribed");
        }
        assert_eq!(next(&mut rx).await, device(6));
        assert_eq!(next(&mut rx).await, device(7));
        assert_eq!(next(&mut rx).await, Recv::Lagged(3 + 2));
        tx.send(test_event(8)).expect("receiver still subscribed");
        assert_eq!(next(&mut rx).await, device(8));
    }

    /// Under a burst that doesn't let up, the drain can't get to the edge.
    /// It still reports the lag after a ring's worth of events without
    /// falling behind again, so the subscriber catches up anyway.
    #[tokio::test]
    async fn lag_aware_receiver_reports_the_lag_after_a_ring_of_events() {
        let (tx, rx) = broadcast::channel(4);
        let mut rx = LagAwareReceiver::new(rx, "test subscriber");
        for n in 0..6 {
            tx.send(test_event(n)).expect("receiver still subscribed");
        }
        assert_eq!(next(&mut rx).await, device(2));
        for n in 3..6 {
            // Another event for every one it takes.
            tx.send(test_event(n + 3))
                .expect("receiver still subscribed");
            assert_eq!(next(&mut rx).await, device(n));
        }
        assert_eq!(next(&mut rx).await, Recv::Lagged(2));
        for n in 6..9 {
            assert_eq!(next(&mut rx).await, device(n));
        }
    }
}
