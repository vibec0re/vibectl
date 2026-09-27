//! 🔥 Integration tests for the sync engine's push, pull and retry workers
//! (#32).
//!
//! The engine runs for real, against the shared rig's `TestHub` (see
//! `common`). Steps are ordered on the hub's traffic log, or, where the
//! engine takes a retry out of its queue without touching the hub, on its
//! retry queue (`Rig::wait_for_retry_queue`). Where a test is about an
//! interval, it measures a lower bound only: a tokio interval never fires
//! early, so the bound holds however slow the machine is, and the 20 s
//! waits only turn a hang into a failure. Where a test checks that a PATCH
//! does *not* go out, it waits a bounded time (`TestHub::within`), which a
//! slow machine can only turn into a false pass, never a false failure.

mod common;

use std::time::{Duration, Instant};

use common::*;
use v1bectl_sync::{ConflictResolution, SyncConfig};

// ---------------------------------------------------------------------
// Worker intervals come from the config
// ---------------------------------------------------------------------

/// The buffer worker drains on `push_interval`, not on a hardcoded 50 ms.
///
/// Its first tick fires when the engine starts, before the write (the rig
/// waits for the first pull cycle, which runs in the same poll). So the push
/// can only go out on a later tick, `push_interval` after the start or more.
#[tokio::test]
async fn buffer_worker_drains_on_push_interval() {
    let push_interval = Duration::from_millis(600);
    let started = Instant::now();
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Apply, false),
        Pulls::Periodic,
        SyncConfig {
            push_interval,
            ..SyncConfig::default()
        },
    )
    .await;

    rig.write("a", on()).await;
    rig.hub
        .wait("the push", |log| !log.sets_started.is_empty())
        .await;
    let pushed = started.elapsed();
    assert!(
        pushed >= push_interval,
        "pushed {pushed:?} after the engine started, before the first \
         push_interval ({push_interval:?}) tick: the buffer worker ignores it"
    );

    rig.shutdown().await;
}

/// The retry worker checks for due retries on `retry_interval`, not on a
/// hardcoded 1 s. Its first tick fires at the start, with nothing to retry,
/// so the first retry comes at a later tick.
#[tokio::test]
async fn retry_worker_runs_on_retry_interval() {
    let retry_interval = Duration::from_millis(1250);
    let started = Instant::now();
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::Periodic,
        SyncConfig {
            retry_interval,
            base_retry_delay: Duration::from_millis(1),
            ..SyncConfig::default()
        },
    )
    .await;

    rig.write("a", on()).await;
    rig.hub
        .wait("the push and its first retry", |log| {
            log.sets_started.len() >= 2
        })
        .await;
    let retried = started.elapsed();
    assert!(
        retried >= retry_interval,
        "first retry {retried:?} after the engine started, before the first \
         retry_interval ({retry_interval:?}) tick: the retry worker ignores it"
    );

    rig.shutdown().await;
}

// ---------------------------------------------------------------------
// The pull cycle
// ---------------------------------------------------------------------

/// One device's error doesn't cost the other devices their pull.
///
/// With `ServerWins`, a pull that finds the hub disagreeing pushes the
/// store's value back, and this hub fails every PATCH, so handling every
/// device errors. A cycle that stopped at the first error would read the
/// same first device (the store's iteration order doesn't change) and never
/// get to the others.
#[tokio::test]
async fn a_device_error_does_not_end_the_pull_cycle() {
    const IDS: [&str; 3] = ["a", "b", "c"];
    let rig = Rig::new(
        &IDS,
        TestHub::new(OnSet::Fail, false),
        Pulls::Periodic,
        SyncConfig {
            conflict_resolution: ConflictResolution::ServerWins,
            ..SyncConfig::default()
        },
    )
    .await;

    // Someone switched every light on at the wall.
    for id in IDS {
        rig.hub.report(id, on());
    }
    let before = rig.hub.log();
    rig.full_pull_cycle().await;
    rig.full_pull_cycle().await;

    let log = rig.hub.log();
    for id in IDS {
        assert!(
            log.reads_of(id) > before.reads_of(id),
            "{id} was never pulled again: another device's error ended the cycle \
             (reads {:?})",
            log.reads
        );
        assert!(
            log.sets_for(id).len() > before.sets_for(id).len(),
            "{id}: its conflict was never handled (no ServerWins push)"
        );
    }

    rig.shutdown().await;
}

// ---------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------

/// Fast retries: due 1 ms after a failure, checked every 5 ms.
fn fast_retries() -> SyncConfig {
    SyncConfig {
        base_retry_delay: Duration::from_millis(1),
        max_retry_delay: Duration::from_millis(1),
        retry_interval: Duration::from_millis(5),
        max_retry_attempts: 100,
        ..SyncConfig::default()
    }
}

/// A retry sends the device's latest value, read when it's queued again,
/// not the value that failed (#32). Resending that one can land after a
/// newer write and overwrite it on the hub.
///
/// Every PATCH waits at the gate. The older write's push fails, and so does
/// its first retry. The newer write is made while that retry is held, and
/// its push goes out right after the retry failed. The second retry comes
/// due while the newer push is held, so it's queued again with the value it
/// reads then, and goes out after the newer push. The 50 ms retry delay only
/// has to outlast the moment between the failure and the newer push going
/// out: the buffer worker sends it at once, without waiting for a tick.
#[tokio::test]
async fn a_retry_sends_the_latest_value_not_the_failed_one() {
    let delay = Duration::from_millis(50);
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, true),
        Pulls::OnDemand,
        SyncConfig {
            base_retry_delay: delay,
            max_retry_delay: delay,
            ..fast_retries()
        },
    )
    .await;
    let (older, newer) = (light(true, 30), light(true, 70));

    rig.write("a", older.clone()).await;
    rig.hub
        .wait("the older push at the gate", |log| {
            log.sets_started.len() == 1
        })
        .await;
    rig.hub.release(1);
    rig.hub
        .wait("its first retry at the gate", |log| {
            log.sets_started.len() == 2
        })
        .await;

    rig.write("a", newer.clone()).await;
    rig.hub.release(1);
    rig.hub
        .wait("the newer push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    rig.wait_for_retry_queue("the second retry queued again", 0)
        .await;

    // The hub recovers and answers everything: the newer push, then the
    // retry. It ends on the newer write.
    rig.hub.set_on_set(OnSet::Apply);
    rig.hub.open_gate();
    rig.hub
        .wait("the retry after the newer push answered", |log| {
            log.sets_started.len() == 4 && log.sets_done == 4
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![older.clone(), older.clone(), newer.clone(), newer.clone()],
        "PATCHes: the retry after the newer write resent the older value"
    );
    assert_eq!(rig.hub.reported("a"), Some(newer.clone()), "on the hub");
    assert_eq!(rig.stored("a").await, newer, "in the store");

    rig.shutdown().await;
}

/// A retry and a newer write of the same device are never in flight at
/// once (#32 review). The retry worker used to send a retry to the gateway
/// itself while the buffer worker pushed a newer write. A hub that answered
/// the newer PATCH first then ended on the retry's older value, while the
/// store and the pending confirmation held the newer one; once the window
/// ran out, the pull took the hub's value, and the write was reverted and
/// never pushed again. A due retry now goes back through the sync buffer,
/// so the newer write waits until the retry is answered.
///
/// The retry is held at the gate while the newer write comes in, and the
/// hub then answers the newest PATCH it holds first.
#[tokio::test]
async fn a_retry_in_flight_is_not_overtaken_by_a_newer_write() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, true),
        Pulls::OnDemand,
        fast_retries(),
    )
    .await;
    let (older, newer) = (light(true, 30), light(true, 70));

    rig.write("a", older.clone()).await;
    rig.hub
        .wait("the older push at the gate", |log| {
            log.sets_started.len() == 1
        })
        .await;
    rig.hub.release(1);
    rig.hub
        .wait("its retry at the gate", |log| log.sets_started.len() == 2)
        .await;

    // The hub is back, and the user writes again while the retry is held.
    // Its push must wait for the retry (60 buffer ticks here).
    rig.hub.set_on_set(OnSet::Apply);
    rig.write("a", newer.clone()).await;
    let overtaken = rig
        .hub
        .within(Duration::from_millis(300), |log| {
            log.sets_started.len() == 3
        })
        .await;

    // The hub answers the newest PATCH it holds first, then the rest.
    rig.hub.release_newest();
    rig.hub
        .wait("the newest PATCH answered", |log| log.sets_done >= 2)
        .await;
    rig.hub.open_gate();
    rig.hub
        .wait("the newer write's push answered", |log| {
            log.sets_for("a").contains(&newer) && log.sets_done == log.sets_started.len()
        })
        .await;
    assert_eq!(
        rig.hub.reported("a"),
        Some(newer.clone()),
        "the hub ended on the older value: the retry landed after the newer write"
    );
    assert!(
        !overtaken,
        "the newer write's PATCH went out while the retry's was in flight: {:?}",
        rig.hub.log()
    );
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![older.clone(), older, newer.clone()],
        "PATCHes"
    );
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, newer, "in the store");

    rig.shutdown().await;
}

/// The retry count is per write, not per device (#32 review): a new write
/// to a device with failures behind it gets the whole `max_retry_attempts`.
/// It used to take over the older write's count, and was given up early.
///
/// The older write fails twice, and its second retry waits out its 200 ms
/// delay. The newer write, made meanwhile, must still be pushed three times.
#[tokio::test]
async fn a_new_write_gets_its_own_retry_budget() {
    let delay = Duration::from_millis(200);
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::OnDemand,
        SyncConfig {
            max_retry_attempts: 3,
            base_retry_delay: delay,
            max_retry_delay: delay,
            ..fast_retries()
        },
    )
    .await;
    let (older, newer) = (light(true, 30), light(true, 70));

    rig.write("a", older.clone()).await;
    rig.hub
        .wait("the older write's push and first retry", |log| {
            log.sets_done >= 2
        })
        .await;
    rig.write("a", newer.clone()).await;
    rig.hub
        .wait(
            "the newer write's push and two retries (max_retry_attempts: 3)",
            |log| log.sets_for("a").iter().filter(|s| **s == newer).count() >= 3,
        )
        .await;

    rig.shutdown().await;
}

/// `max_retry_attempts` counts the first failure too (#32 review): at 1, a
/// failed push is given up at once. The first failure used to skip the cap,
/// so it always queued one retry.
///
/// The batch's first push fails. Once the buffer worker sends the second
/// one, it's done with the first, so a retry would have been queued by then
/// (and, with a 60 s delay, would still be waiting).
#[tokio::test]
async fn max_retry_attempts_counts_the_first_failure() {
    let delay = Duration::from_secs(60);
    let rig = Rig::new(
        &["h", "a", "b"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig {
            max_retry_attempts: 1,
            base_retry_delay: delay,
            max_retry_delay: delay,
            ..SyncConfig::default()
        },
    )
    .await;

    let first = rig.write_one_batch("h", &[("a", on()), ("b", on())]).await;
    rig.hub.set_on_set(OnSet::Fail);
    rig.hub.release(1);
    rig.hub
        .wait("the batch's second push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    assert_eq!(
        rig.engine.get_sync_stats().await.retry_queue_size,
        0,
        "{first}'s failed push was queued for a retry at max_retry_attempts: 1"
    );

    rig.shutdown().await;
}

/// Retries go straight to the gateway and don't restart the protection
/// window: only a write does, and its push going out. With a hub that keeps
/// failing, the write is reverted once the window from its push is up,
/// while the retries are still going, and the retries after that send what
/// the store now holds (the hub's value): the write was abandoned.
#[tokio::test]
async fn retries_do_not_extend_the_protection_window() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::Periodic,
        SyncConfig {
            protection_window: Duration::from_millis(300),
            max_retry_attempts: 100_000,
            ..fast_retries()
        },
    )
    .await;
    let mut rx = rig.bus.subscribe();

    rig.write("a", on()).await;
    // If every retry restarted the window, it would only run out once the
    // retries stop, long after this wait gives up.
    wait_for_echo(&mut rx, "a", &off()).await;
    // The retries go on, now with the store's value. (A retry that read its
    // value just before the revert may still send `on` first.)
    let at_revert = rig.hub.log().sets_started.len();
    rig.hub
        .wait("a retry of the store's value after the revert", |log| {
            log.sets_started[at_revert..]
                .iter()
                .any(|(id, state)| id == "a" && *state == off())
        })
        .await;

    rig.shutdown().await;
}

/// A push that keeps failing is given up after `max_retry_attempts`
/// failures. Its retry entry used to stay in the queue, due at every tick,
/// so it was retried forever.
///
/// `b` fails the same way after `a` gave up. Its second retry comes on a
/// later retry tick than its first, and the tick of its first would also
/// have retried a still-due `a`, so by then that retry is in the log.
#[tokio::test]
async fn a_failing_push_is_given_up_after_max_retry_attempts() {
    let rig = Rig::new(
        &["a", "b"],
        TestHub::new(OnSet::Fail, false),
        Pulls::OnDemand,
        SyncConfig {
            max_retry_attempts: 3,
            ..fast_retries()
        },
    )
    .await;

    rig.write("a", on()).await;
    rig.hub
        .wait("a: the push and two retries", |log| {
            log.sets_for("a").len() >= 3
        })
        .await;
    rig.write("b", on()).await;
    rig.hub
        .wait("b: the push and two retries", |log| {
            log.sets_for("b").len() >= 3
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a").len(),
        3,
        "a was retried after max_retry_attempts (3) failures"
    );

    rig.shutdown().await;
}
