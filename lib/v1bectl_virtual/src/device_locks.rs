//! 🔐 Per-device write locks for the virtual device manager (#58).
//!
//! Each manager write holds the lock of every device it writes (the virtual
//! device and its members) from before it plans to after it commits, a
//! scene's delays included. So two writes that share a device are
//! serialized, in the order they asked, and two that don't run side by
//! side. See `VirtualDeviceManager::set_virtual_device_state` for why.
//!
//! A set of locks is always taken in one order, sorted by device id, so two
//! writes can't each hold a lock the other waits for.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::OwnedMutexGuard;
use v1bectl_sync::DeviceId;

/// One lock per device id, made when it's first asked for, and dropped
/// again once nothing holds or waits for it. So the map holds only the
/// devices a write is on right now.
#[derive(Default)]
pub(crate) struct DeviceLocks {
    locks: Mutex<HashMap<DeviceId, Arc<tokio::sync::Mutex<()>>>>,
}

impl DeviceLocks {
    /// The map. It's only ever held to look a lock up, add or drop one, and
    /// never across an `.await`, so a poisoned one still holds a good map.
    fn map(&self) -> MutexGuard<'_, HashMap<DeviceId, Arc<tokio::sync::Mutex<()>>>> {
        self.locks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take the lock of each device in `ids` (each once, in sorted order),
    /// waiting for any another write holds. Each lock is handed out in the
    /// order it was asked for (tokio's `Mutex` is fair).
    ///
    /// Dropping the future part-way lets go of the locks it has taken.
    pub(crate) async fn lock(
        self: &Arc<Self>,
        ids: impl IntoIterator<Item = DeviceId>,
    ) -> DeviceLockSet {
        let mut ids: Vec<DeviceId> = ids.into_iter().collect();
        ids.sort();
        ids.dedup();

        let mut set = DeviceLockSet {
            locks: Arc::clone(self),
            held: Vec::with_capacity(ids.len()),
        };
        for id in ids {
            let lock = Arc::clone(self.map().entry(id.clone()).or_default());
            let guard = lock.lock_owned().await;
            set.held.push((id, guard));
        }
        set
    }

    /// How many devices have a lock now: held, or waited for.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map().len()
    }
}

/// The locks one write holds (see [`DeviceLocks::lock`]). Dropping it lets
/// them go.
pub(crate) struct DeviceLockSet {
    locks: Arc<DeviceLocks>,
    /// Sorted by id.
    held: Vec<(DeviceId, OwnedMutexGuard<()>)>,
}

impl DeviceLockSet {
    /// Whether it holds the lock of every device in `ids`.
    pub(crate) fn covers<'a>(&self, ids: impl IntoIterator<Item = &'a DeviceId>) -> bool {
        ids.into_iter()
            .all(|id| self.held.binary_search_by(|(held, _)| held.cmp(id)).is_ok())
    }
}

impl Drop for DeviceLockSet {
    fn drop(&mut self) {
        let mut map = self.locks.map();
        for (id, guard) in self.held.drain(..) {
            drop(guard);
            // Nothing holds or waits for it once only the map has it: a
            // waiter keeps its own handle until it has taken the lock. The
            // map is held here, so no one can take a new handle meanwhile.
            if map
                .get(&id)
                .is_some_and(|lock| Arc::strong_count(lock) == 1)
            {
                map.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ids(ids: &[&str]) -> Vec<DeviceId> {
        ids.iter().map(ToString::to_string).collect()
    }

    /// Sorted, each once, and dropped from the map when let go.
    #[tokio::test]
    async fn a_set_holds_each_device_once_and_is_pruned_when_dropped() {
        let locks = Arc::new(DeviceLocks::default());
        let set = locks.lock(ids(&["b", "a", "b"])).await;
        assert!(set.covers(&ids(&["a", "b"])));
        assert!(!set.covers(&ids(&["a", "c"])));
        assert_eq!(locks.len(), 2);
        drop(set);
        assert_eq!(locks.len(), 0, "idle locks must not pile up");
    }

    /// A set waits for a device another set holds, and not for one it
    /// doesn't. The waiter's lock stays in the map until it's done.
    #[tokio::test(start_paused = true)]
    async fn a_set_waits_only_for_a_device_another_holds() {
        let locks = Arc::new(DeviceLocks::default());
        let first = locks.lock(ids(&["a", "b"])).await;

        let unrelated = tokio::time::timeout(Duration::from_secs(1), locks.lock(ids(&["c"])))
            .await
            .expect("an unrelated device must not wait");
        drop(unrelated);

        let waiter = tokio::spawn({
            let locks = Arc::clone(&locks);
            async move { drop(locks.lock(ids(&["b", "c"])).await) }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished(), "it took b while another set held it");
        assert!(locks.len() >= 2);

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("it never got b")
            .expect("waiter task");
        assert_eq!(locks.len(), 0, "idle locks must not pile up");
    }

    /// A set dropped while it waits lets go of what it had taken.
    #[tokio::test(start_paused = true)]
    async fn a_set_dropped_while_it_waits_lets_go() {
        let locks = Arc::new(DeviceLocks::default());
        let holder = locks.lock(ids(&["b"])).await;
        // Takes `a`, then waits for `b`.
        let waiting = tokio::time::timeout(Duration::from_secs(1), locks.lock(ids(&["a", "b"])));
        assert!(waiting.await.is_err(), "b was taken twice");
        tokio::time::timeout(Duration::from_secs(1), locks.lock(ids(&["a"])))
            .await
            .expect("a was never let go");
        drop(holder);
        assert_eq!(locks.len(), 0, "idle locks must not pile up");
    }
}
