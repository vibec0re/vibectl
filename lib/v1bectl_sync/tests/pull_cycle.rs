//! 🔍 A pull cycle goes on past a device whose handling fails (#32, #50
//! review).
//!
//! Since #45 a `ServerWins` push no longer fails the pull, so the one error
//! left in handling a device is its store write failing: the device was
//! removed between the conflict check and the write. That gap has no await
//! on the hub in it, so the test makes it happen with a tracing subscriber
//! instead. It holds the pull worker's thread at the first "Resolving
//! conflict" event, the test removes that device from the store, and lets
//! go. The write fails, and the rest of the cycle must still reconcile the
//! other devices.
//!
//! The engine's pull interval is an hour, so the cycle at its start is the
//! only one: a cycle that stopped at the error would leave the others
//! unreconciled for good. The subscriber is process-global, so this file
//! holds this one test.

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::*;
use tokio::sync::watch;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use v1bectl_sync::{EventBus, EventType, StateStore, SyncConfig, SyncEngine};

const CONFLICT: &str = "Resolving conflict for device: ";
const HANDLE_FAILED: &str = "Pull: handling the gateway state of";
const IDS: [&str; 3] = ["a", "b", "c"];

/// Where the choreography is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for the first conflict.
    Armed,
    /// The pull worker is held at the conflict of this device.
    Held(String),
    /// Let go.
    Released,
}

struct Hook {
    phase: watch::Sender<Phase>,
    released: Mutex<bool>,
    wake: Condvar,
    /// The pull logged a device's handling as failed.
    handle_failed: Mutex<bool>,
}

impl Hook {
    fn release(&self) {
        self.phase.send_replace(Phase::Released);
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

/// Collects an event's `message` field.
struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

struct HookSubscriber(Arc<Hook>);

impl Subscriber for HookSubscriber {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let hook = &self.0;
        let mut message = Message(String::new());
        event.record(&mut message);
        if message.0.contains(HANDLE_FAILED) {
            *hook.handle_failed.lock().unwrap() = true;
        }
        let armed = *hook.phase.borrow() == Phase::Armed;
        if let (true, Some(device)) = (armed, message.0.strip_prefix(CONFLICT)) {
            hook.phase.send_replace(Phase::Held(device.to_string()));
            // Block this worker thread (and so the pull) until the test
            // lets go.
            let mut released = hook.released.lock().unwrap();
            while !*released {
                released = hook.wake.wait(released).unwrap();
            }
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_whose_handling_fails_does_not_end_the_pull_cycle() {
    let hook = Arc::new(Hook {
        phase: watch::channel(Phase::Armed).0,
        released: Mutex::new(false),
        wake: Condvar::new(),
        handle_failed: Mutex::new(false),
    });
    tracing::subscriber::set_global_default(HookSubscriber(hook.clone()))
        .expect("the only subscriber in this test binary");

    // Someone switched every light on at the wall before the engine starts.
    let hub = TestHub::new(OnSet::Apply, false);
    let store = StateStore::new();
    for id in IDS {
        store.add_device(light_info(id), off()).await;
        hub.report(id, on());
    }
    let bus = Arc::new(EventBus::new(100));
    let mut rx = bus.subscribe();
    let engine = SyncEngine::new(
        store.clone(),
        bus.clone(),
        hub.clone(),
        Some(SyncConfig {
            pull_interval: Duration::from_hours(1),
            ..SyncConfig::default()
        }),
    );
    let runner = tokio::spawn({
        let engine = engine.clone();
        async move { engine.start().await }
    });

    // The cycle at start reaches its first conflict, and is held there.
    let mut phase = hook.phase.subscribe();
    let held = tokio::time::timeout(WAIT, phase.wait_for(|p| matches!(p, Phase::Held(_))))
        .await
        .map(|p| p.expect("hook").clone());
    let removed = match &held {
        Ok(Phase::Held(device)) => {
            store.remove_device(device).await.expect("remove it");
            Some(device.clone())
        }
        _ => None,
    };
    // Let go before asserting anything: a panic with the pull worker's
    // thread still held would hang the runtime's shutdown.
    hook.release();
    let removed = removed.expect("the pull never reached a conflict");

    // The removed device's store write fails. The others must still be
    // reconciled by this cycle: their store takes the hub's `on`, echoed.
    let mut left: HashSet<String> = IDS
        .iter()
        .map(ToString::to_string)
        .filter(|id| *id != removed)
        .collect();
    let want = json(&on());
    let reconciled = tokio::time::timeout(WAIT, async {
        while !left.is_empty() {
            let event = rx.recv().await.expect("event bus");
            if let EventType::AttributeChanged { new_value, .. } = event.event_type {
                if new_value == want {
                    left.remove(&event.device_id);
                }
            }
        }
    })
    .await;
    assert!(
        reconciled.is_ok(),
        "{left:?} never reconciled: {removed}'s failure ended the pull cycle"
    );
    assert!(
        *hook.handle_failed.lock().unwrap(),
        "{removed}: its handling didn't fail, so this didn't test the error path"
    );
    for id in IDS.iter().filter(|id| **id != removed) {
        assert_eq!(
            store
                .get_device(&id.to_string())
                .await
                .expect("in store")
                .state,
            on(),
            "{id} in the store"
        );
    }

    engine.stop().await;
    runner.await.expect("engine task").expect("engine");
}
