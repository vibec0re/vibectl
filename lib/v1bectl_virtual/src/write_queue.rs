//! 🚦 The virtual device manager's write queue (#58).
//!
//! Every manager write joins one queue, in the order it asks, with the set
//! of devices it writes (its virtual device and every member it may
//! write). It goes once every write that joined before it and shares a
//! device with it is over, whether that write is running or still waiting
//! itself. So a write waits for an earlier one it overlaps, and for
//! everything that earlier one waits for in turn, and two writes that
//! overlap always land in the order they asked, even through a chain of
//! writes they don't directly share a device with. A write that overlaps
//! nothing earlier goes at once.
//!
//! A write waits only for writes that joined before it, so the waits can't
//! form a cycle: the earliest write in the queue never waits. The queue
//! holds only the writes in flight (waiting or running), since each leaves
//! it when it's over, however it ends: done, failed, panicked, or dropped
//! part-way (a client that went away).
//!
//! This replaces per-device locks taken one at a time (#60 review, finding
//! 1). A write blocked on one of its devices held none of the others, so a
//! later write that shared only those went first, and the older write then
//! landed last.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;
use v1bectl_sync::DeviceId;

/// The writes in flight, in the order they asked.
#[derive(Default)]
pub(crate) struct WriteQueue {
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    /// The ticket the next write gets.
    next_ticket: u64,
    /// In ticket order.
    queue: VecDeque<Entry>,
}

/// A write in the queue.
struct Entry {
    ticket: u64,
    devices: BTreeSet<DeviceId>,
    /// Turns `true` (or closes) once the write is over.
    done: watch::Receiver<bool>,
}

impl WriteQueue {
    /// The queue. It's only ever held to join or leave, never across an
    /// `.await`, so a poisoned one still holds good entries.
    fn entries(&self) -> MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Join the queue with a write to `devices`. In one step, under the
    /// queue's lock, it notes every earlier write that shares a device with
    /// it (running or still waiting), and takes its place behind them. Wait
    /// for them with [`WriteTurn::ready`].
    pub(crate) fn join(self: &Arc<Self>, devices: impl IntoIterator<Item = DeviceId>) -> WriteTurn {
        let devices: BTreeSet<DeviceId> = devices.into_iter().collect();
        let (done, done_rx) = watch::channel(false);
        let mut entries = self.entries();
        let ticket = entries.next_ticket;
        entries.next_ticket += 1;
        let ahead = entries
            .queue
            .iter()
            .filter(|earlier| !earlier.devices.is_disjoint(&devices))
            .map(|earlier| earlier.done.clone())
            .collect();
        entries.queue.push_back(Entry {
            ticket,
            devices: devices.clone(),
            done: done_rx,
        });
        drop(entries);
        WriteTurn {
            queue: Arc::clone(self),
            ticket,
            devices,
            ahead,
            done,
        }
    }

    /// How many writes are in flight.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries().queue.len()
    }
}

/// A write's place in the [`WriteQueue`]. Dropping it, however the write
/// ends, takes the write out of the queue and lets the writes waiting for
/// it go on.
pub(crate) struct WriteTurn {
    queue: Arc<WriteQueue>,
    ticket: u64,
    devices: BTreeSet<DeviceId>,
    /// The earlier writes it waits for.
    ahead: Vec<watch::Receiver<bool>>,
    done: watch::Sender<bool>,
}

impl WriteTurn {
    /// Wait until every earlier write that shares a device with this one is
    /// over. It's cancel-safe: dropped part-way, it forgets only the writes
    /// it has seen end, so waiting again picks up where it left off. (And
    /// the turn still leaves the queue when it's dropped.)
    pub(crate) async fn ready(&mut self) {
        while let Some(earlier) = self.ahead.last_mut() {
            // `Ok`: it's over. `Err`: its turn was dropped before it could
            // say so, which means it's over too. A `watch` keeps its value,
            // so a write that ended before this looks is seen as over.
            let _ = earlier.wait_for(|&done| done).await;
            self.ahead.pop();
        }
    }

    /// Whether it was queued with every device in `ids`.
    pub(crate) fn covers<'a>(&self, ids: impl IntoIterator<Item = &'a DeviceId>) -> bool {
        ids.into_iter().all(|id| self.devices.contains(id))
    }
}

impl Drop for WriteTurn {
    fn drop(&mut self) {
        self.queue
            .entries()
            .queue
            .retain(|entry| entry.ticket != self.ticket);
        self.done.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ids(ids: &[&str]) -> Vec<DeviceId> {
        ids.iter().map(ToString::to_string).collect()
    }

    /// Whether `turn` becomes ready within a second (of the paused clock).
    async fn ready_soon(turn: &mut WriteTurn) -> bool {
        tokio::time::timeout(Duration::from_secs(1), turn.ready())
            .await
            .is_ok()
    }

    /// A turn waits for an earlier one it shares a device with, not for one
    /// it doesn't. The queue is empty once they're all dropped.
    #[tokio::test(start_paused = true)]
    async fn a_turn_waits_only_for_earlier_overlapping_ones() {
        let queue = Arc::new(WriteQueue::default());
        let mut first = queue.join(ids(&["a", "b"]));
        assert!(ready_soon(&mut first).await, "nothing is ahead of it");
        let mut unrelated = queue.join(ids(&["c"]));
        assert!(ready_soon(&mut unrelated).await, "it shares nothing");
        assert!(unrelated.covers(&ids(&["c"])));
        assert!(!unrelated.covers(&ids(&["a"])));

        let mut overlapping = queue.join(ids(&["b", "c"]));
        assert!(
            !ready_soon(&mut overlapping).await,
            "it went before b and c"
        );
        drop(first);
        assert!(!ready_soon(&mut overlapping).await, "it went before c");
        drop(unrelated);
        assert!(ready_soon(&mut overlapping).await, "it never went");
        drop(overlapping);
        assert_eq!(queue.len(), 0, "the queue holds only writes in flight");
    }

    /// #60 review, finding 1: a turn that's waiting holds up a later one
    /// that shares a device with it, even when that device is free. The
    /// later one goes after it, not before.
    #[tokio::test(start_paused = true)]
    async fn a_waiting_turn_holds_up_later_overlapping_ones() {
        let queue = Arc::new(WriteQueue::default());
        let mut fade = queue.join(ids(&["a"]));
        assert!(ready_soon(&mut fade).await);
        let mut older = queue.join(ids(&["a", "z"]));
        let mut newer = queue.join(ids(&["z"]));
        assert!(!ready_soon(&mut older).await, "it went before the fade");
        assert!(!ready_soon(&mut newer).await, "it overtook the older write");

        drop(fade);
        assert!(ready_soon(&mut older).await, "it never went");
        assert!(!ready_soon(&mut newer).await, "it overtook the older write");
        drop(older);
        assert!(ready_soon(&mut newer).await, "it never went");
    }

    /// A turn dropped while it waits (a client that went away) lets the
    /// later turns waiting for it go.
    #[tokio::test(start_paused = true)]
    async fn a_turn_dropped_while_it_waits_lets_later_ones_go() {
        let queue = Arc::new(WriteQueue::default());
        let mut fade = queue.join(ids(&["a"]));
        assert!(ready_soon(&mut fade).await);
        let mut older = queue.join(ids(&["a", "z"]));
        let mut newer = queue.join(ids(&["z"]));
        assert!(!ready_soon(&mut older).await);
        drop(older);
        assert!(ready_soon(&mut newer).await, "the dropped turn held it up");
        drop((fade, newer));
        assert_eq!(queue.len(), 0, "the queue holds only writes in flight");
    }
}
