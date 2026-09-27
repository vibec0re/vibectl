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
