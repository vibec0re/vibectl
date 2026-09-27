//! 🔒 Two simultaneous writes to one device must leave the store and the
//! queued push on the same (the later) value (#32).
//!
//! `apply_optimistic_update` used to write the store, echo, and only then
//! queue the push, as separate steps. A second write that ran completely in
//! between (its own store write, echo and push) left the store on the second
//! value and the buffer, the pending confirmation and so the hub on the
//! first.
//!
//! The test makes that interleaving happen instead of hoping for it. A
//! tracing subscriber holds the first write's thread at the first event the
//! event bus logs, which is its echo: after its store write, before its
//! push was queued. The second write then runs on the other worker thread,
//! either to completion (the old code: nothing stopped it) or until it logs
//! that it's waiting for the write lock the first one holds (the fix). That
//! line comes before the lock is tried, so the test then checks the second
//! write really is locked out: no echo of it for `LOCKED_OUT`, with the
//! first one still held. Only then is the first write let go. The engine
//! starts after both writes, so its first drain pushes exactly what the
//! buffer ended on.
//!
//! The subscriber is process-global, so this file holds this one test.

mod common;

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::*;
use tokio::sync::watch;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use v1bectl_sync::{EventBus, StateStore, SyncEngine};

/// How long the second write must stay without an echo while the first one
/// is held inside its critical section. A write that isn't locked out echoes
/// within microseconds of logging its wait.
const LOCKED_OUT: Duration = Duration::from_millis(200);

/// Where the choreography is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for the first write's echo.
    Armed,
    /// The first write is held at its echo.
    FirstHeld,
    /// With the first write held, the second one is about to wait for the
    /// write lock.
    SecondAtLock,
}

struct Hook {
    phase: watch::Sender<Phase>,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Hook {
    fn release(&self) {
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
        let phase = *hook.phase.borrow();
        match phase {
            Phase::Armed if event.metadata().target() == "v1bectl_sync::events" => {
                hook.phase.send_replace(Phase::FirstHeld);
                // Block this worker thread (and so the first write) until the
                // test lets go.
                let mut released = hook.released.lock().unwrap();
                while !*released {
                    released = hook.wake.wait(released).unwrap();
                }
            }
            Phase::FirstHeld => {
                let mut message = Message(String::new());
                event.record(&mut message);
                if message.0.contains("waiting for the write lock") {
                    hook.phase.send_replace(Phase::SecondAtLock);
                }
            }
            _ => {}
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_writes_leave_store_and_push_on_the_same_value() {
    let hook = Arc::new(Hook {
        phase: watch::channel(Phase::Armed).0,
        released: Mutex::new(false),
        wake: Condvar::new(),
    });
    tracing::subscriber::set_global_default(HookSubscriber(hook.clone()))
        .expect("the only subscriber in this test binary");

    let id = "a".to_string();
    let hub = TestHub::new(OnSet::Apply, false);
    let store = StateStore::new();
    store.add_device(light_info(&id), off()).await;
    hub.report(&id, off());
    let bus = Arc::new(EventBus::new(100));
    let mut rx = bus.subscribe();
    let engine = SyncEngine::new(store.clone(), bus.clone(), hub.clone(), None);
    let (first, second) = (light(true, 31), light(true, 72));

    let write = |state| {
        let (engine, id) = (engine.clone(), id.clone());
        tokio::spawn(async move { engine.apply_optimistic_update(&id, state).await })
    };
    let mut phase = hook.phase.subscribe();

    let first_write = write(first.clone());
    tokio::time::timeout(WAIT, phase.wait_for(|p| *p == Phase::FirstHeld))
        .await
        .expect("the first write never echoed")
        .expect("hook");

    // The first write's echo went out just before the hook held it.
    let first_echo = rx.try_recv().map(|event| event.device_id);

    let mut second_write = write(second.clone());
    let second_done = tokio::time::timeout(WAIT, async {
        tokio::select! {
            done = &mut second_write => Some(done),
            _ = phase.wait_for(|p| *p == Phase::SecondAtLock) => None,
        }
    })
    .await;
    // Logging that it waits for the lock doesn't prove the second write
    // waits: the line comes before the lock is tried. While the first write
    // is held inside its critical section, the second must not get any
    // further, so no echo of it may come. (Bounded: it can only let a
    // broken build pass on a very slow machine, never fail a correct one.)
    let echoed_meanwhile = match &second_done {
        Ok(Some(_)) => Some("it ran to completion".to_string()),
        Ok(None) => tokio::time::timeout(LOCKED_OUT, rx.recv())
            .await
            .ok()
            .map(|event| format!("it echoed {event:?}")),
        Err(_) => None,
    };

    // Let go before asserting anything: a panic with the first write's
    // thread still held would hang the runtime's shutdown.
    hook.release();
    assert_eq!(first_echo, Ok(id.clone()), "the first write's echo");
    let second_done =
        second_done.expect("the second write neither finished nor reached the write lock");
    assert_eq!(
        echoed_meanwhile, None,
        "the second write got past the write lock while the first held it"
    );
    first_write
        .await
        .expect("first write task")
        .expect("first write");
    match second_done {
        Some(done) => done,
        None => second_write.await,
    }
    .expect("second write task")
    .expect("second write");

    // Both writes are in. Start the engine: its first drain pushes what the
    // buffer (and the pending confirmation) ended on.
    let runner = tokio::spawn({
        let engine = engine.clone();
        async move { engine.start().await }
    });
    hub.wait("the push", |log| !log.sets_started.is_empty())
        .await;

    let stored = store.get_device(&id).await.expect("device").state;
    assert_eq!(stored, second, "the store holds the later write");
    assert_eq!(
        hub.log().sets_for(&id),
        vec![stored.clone()],
        "the store and the queued push disagree after two simultaneous writes"
    );
    let echoes = drain_events(&mut rx);
    assert_eq!(
        echoes.last().map(|(_, _, new)| new.clone()),
        Some(json(&stored)),
        "the last echo disagrees with the store: {echoes:?}"
    );

    engine.stop().await;
    runner.await.expect("engine task").expect("engine");
}
