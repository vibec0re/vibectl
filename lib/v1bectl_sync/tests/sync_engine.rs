//! 🔥 Integration tests for the sync engine's push, pull and retry workers
//! (#32).
//!
//! The engine runs for real, against the shared rig's `TestHub` (see
//! `common`). Steps are ordered on the hub's traffic log. Where a test is
//! about an interval, it measures a lower bound only: a tokio interval never
//! fires early, so the bound holds however slow the machine is, and the
//! 20 s waits only turn a hang into a failure.

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

/// A retry sends the device's latest value, read when it goes out, not the
/// value that failed (#32). Resending that one can land after a newer write
/// and overwrite it on the hub.
///
/// Every PATCH waits at the gate, so the order is fixed: the older write's
/// push fails, its first retry is held, the newer write's push is held
/// behind it, the retry fails, and the next retry is the only thing that
/// can PATCH (the buffer worker is held on the newer push).
#[tokio::test]
async fn a_retry_sends_the_latest_value_not_the_failed_one() {
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
        .wait("its first retry at the gate", |log| {
            log.sets_started.len() == 2
        })
        .await;

    rig.write("a", newer.clone()).await;
    rig.hub
        .wait("the newer push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;

    rig.hub.release(1);
    rig.hub
        .wait("the next retry at the gate", |log| {
            log.sets_started.len() == 4
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![older.clone(), older.clone(), newer.clone(), newer.clone()],
        "PATCHes: the retry after the newer write resent the older value"
    );

    // The hub recovers and answers everything it holds. It ends on the
    // newer write.
    rig.hub.set_on_set(OnSet::Apply);
    rig.hub.open_gate();
    rig.hub
        .wait("all four answered", |log| log.sets_done >= 4)
        .await;
    assert_eq!(rig.hub.reported("a"), Some(newer.clone()), "on the hub");
    assert_eq!(rig.stored("a").await, newer, "in the store");

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
