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
use v1bectl_sync::{
    ConflictResolution, SyncConfig, SyncPriority, SyncStatus, SyncTask, SyncTaskType,
};

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

/// Every device's conflict is handled, however its push fares.
///
/// With `ServerWins`, a pull that finds the hub disagreeing queues the
/// store's value for the gateway, and this hub fails every PATCH. The pull
/// used to push it itself, so a failed push was an error of the pull; a
/// cycle that stopped at the first error read the same first device every
/// time (the store's iteration order doesn't change) and never got to the
/// others (#32). The push now goes out from the sync buffer (#45), and its
/// failure is the buffer worker's to retry. The cycle still logs and skips a
/// device whose handling fails, but a failed `ServerWins` push no longer
/// counts as one.
#[tokio::test]
async fn server_wins_handles_every_conflicting_device() {
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
    rig.hub
        .wait("every device's ServerWins push, twice", |log| {
            IDS.iter().all(|id| log.sets_for(id).len() >= 2)
        })
        .await;
    for id in IDS {
        assert_eq!(
            rig.hub.log().sets_for(id)[..2],
            [off(), off()],
            "{id}: the ServerWins pushes send the store's value"
        );
    }

    rig.shutdown().await;
}

/// A pull's `ServerWins` push goes out from the sync buffer (#45), so it
/// can't be in flight while the buffer worker pushes a newer write of the
/// same device. The pull used to push it itself, and a hub that answered the
/// newer PATCH first ended on the store's older value.
///
/// Someone flips `a` at the wall. The pull finds the hub disagreeing and the
/// server wins: the store's `off` goes out, and is held at the gate. The
/// user then writes `a`. Its push must wait for the held one (6 buffer ticks
/// here), and the hub, which answers the newest PATCH it holds first, ends
/// on the user's write.
#[tokio::test]
async fn a_server_wins_push_is_not_overtaken_by_a_newer_write() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Apply, true),
        Pulls::Periodic,
        SyncConfig {
            conflict_resolution: ConflictResolution::ServerWins,
            ..SyncConfig::default()
        },
    )
    .await;
    let written = light(true, 70);

    rig.hub.report("a", on());
    rig.hub
        .wait("the ServerWins push at the gate", |log| {
            log.sets_started.len() == 1
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![off()],
        "the ServerWins push"
    );

    rig.write("a", written.clone()).await;
    let overtaken = rig
        .hub
        .within(Duration::from_millis(300), |log| log.sets_started.len() > 1)
        .await;

    // The hub answers the newest PATCH it holds first, then the rest.
    rig.hub.release_newest();
    rig.hub
        .wait("the newest PATCH answered", |log| log.sets_done >= 1)
        .await;
    rig.hub.open_gate();
    rig.hub
        .wait("the write's push answered", |log| {
            log.sets_for("a").contains(&written) && log.sets_done == log.sets_started.len()
        })
        .await;
    assert_eq!(
        rig.hub.reported("a"),
        Some(written.clone()),
        "the hub ended on the ServerWins value: it landed after the user's write"
    );
    assert!(
        !overtaken,
        "the write's PATCH went out while the ServerWins push was in flight: {:?}",
        rig.hub.log()
    );
    rig.full_pull_cycle().await;
    assert_eq!(rig.stored("a").await, written, "in the store");

    rig.shutdown().await;
}

/// A device removed from the store takes its sync status with it (#45), as
/// it does its pending confirmation (#32). The status used to stay for good,
/// and `get_sync_stats` kept counting it.
#[tokio::test]
async fn a_removed_device_takes_its_sync_status_with_it() {
    let rig = Rig::new(
        &["a", "b"],
        TestHub::new(OnSet::Apply, false),
        Pulls::Periodic,
        SyncConfig::default(),
    )
    .await;
    let (a, b) = ("a".to_string(), "b".to_string());

    rig.write(&a, on()).await;
    rig.hub
        .wait("a's push", |log| !log.sets_for(&a).is_empty())
        .await;
    // The buffer worker pushes one device at a time: once it pushes `b`,
    // it's done with `a`, status and all.
    rig.write(&b, on()).await;
    rig.hub
        .wait("b's push", |log| !log.sets_for(&b).is_empty())
        .await;
    assert!(
        matches!(
            rig.engine.get_sync_status(&a).await,
            Some(SyncStatus::InSync { .. })
        ),
        "a's status after its push"
    );

    rig.store.remove_device(&a).await.expect("remove a");
    rig.full_pull_cycle().await;
    assert!(
        rig.engine.get_sync_status(&a).await.is_none(),
        "the removed device's sync status outlived it"
    );
    assert!(
        rig.engine.get_sync_status(&b).await.is_some(),
        "b's sync status"
    );

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

/// A retry never resends a value older than a write made since (#32):
/// landing after the newer write, it would overwrite it on the hub. Nor, now,
/// does it send anything else once a newer write is made (#45): the newer
/// write's own push and retries carry the device's latest value.
///
/// Every PATCH waits at the gate. The older write's push fails, and so does
/// its first retry. The newer write is made while that retry is held, and
/// its push goes out right after the retry failed. The second retry comes
/// due while the newer push is held. It used to go out with the older value
/// (#32), and then with the newer one, a duplicate of the push in flight
/// (#45); now it's dropped. The 50 ms retry delay only has to outlast the
/// moment between the failure and the newer push going out: the buffer
/// worker sends it at once, without waiting for a tick.
#[tokio::test]
async fn a_retry_of_an_older_write_is_dropped_once_a_newer_one_is_pushed() {
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
    rig.wait_for_retry_queue("the second retry taken out", 0)
        .await;

    // The hub recovers and answers the newer push. Nothing may follow it.
    rig.hub.set_on_set(OnSet::Apply);
    rig.hub.open_gate();
    rig.hub
        .wait("the newer push answered", |log| log.sets_done >= 3)
        .await;
    let resent = rig
        .hub
        .within(Duration::from_millis(300), |log| log.sets_started.len() > 3)
        .await;
    assert!(
        !resent,
        "the older write's retry went out after the newer write: {:?}",
        rig.hub.log().sets_for("a")
    );
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![older.clone(), older, newer.clone()],
        "PATCHes"
    );
    assert_eq!(rig.hub.reported("a"), Some(newer.clone()), "on the hub");
    assert_eq!(rig.stored("a").await, newer, "in the store");

    rig.shutdown().await;
}

/// What goes out first, and fails, in [`a_superseded_retry_leaves_a_switch_change`].
#[derive(Clone, Copy, Debug)]
enum FirstPush {
    /// A push queued with `queue_sync` below `Critical`: not a user write.
    Queued,
    /// A user write.
    UserWrite,
}

/// A push of `a` fails, and its retry waits. The user then writes `a`, and
/// the retry comes due while that write's PATCH is in flight. The write
/// lands, the hub confirms it, and someone flips the switch at the wall.
/// The retry must not undo that: the user's write superseded it, and owns
/// the device's pushes (#45).
///
/// The retry used to be queued again with the write's value (the pending
/// entry's), whatever push it retried. That duplicate PATCH went out once
/// the write had landed, and put the write back on the hub over the switch
/// change (`FirstPush::UserWrite`). A queued push's retry went on with the
/// user's value as if it were its own, past the write's confirmation or
/// abandonment (`FirstPush::Queued`).
///
/// The retry delay (500 ms) only has to outlast the moment between the
/// first push failing and the write's push going out (at the next 5 ms
/// buffer tick); the "test timing" assertion says if it didn't.
async fn a_superseded_retry_leaves_a_switch_change(first: FirstPush) {
    let delay = Duration::from_millis(500);
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
    let (older, written, flipped) = (light(true, 30), light(true, 70), light(false, 40));

    match first {
        FirstPush::Queued => {
            rig.engine
                .queue_sync(SyncTask {
                    device_id: "a".to_string(),
                    task_type: SyncTaskType::PushToGateway {
                        new_state: older.clone(),
                    },
                    created_at: Instant::now(),
                    priority: SyncPriority::Normal,
                })
                .await;
        }
        FirstPush::UserWrite => rig.write("a", older.clone()).await,
    }
    rig.hub
        .wait("the first push at the gate", |log| {
            log.sets_started.len() == 1
        })
        .await;
    rig.hub.release(1);
    rig.wait_for_retry_queue("its retry queued", 1).await;

    // The user writes while the retry waits, and the write's push is held.
    rig.hub.set_on_set(OnSet::Apply);
    rig.write("a", written.clone()).await;
    rig.hub
        .wait("the write's push at the gate", |log| {
            log.sets_started.len() == 2
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![older, written.clone()],
        "{first:?}, test timing: the retry came due before the write was pushed"
    );
    // The retry comes due while that PATCH is in flight.
    rig.wait_for_retry_queue("the retry taken out", 0).await;

    // The write lands, and the hub confirms it. Then the switch is flipped.
    rig.hub.release(1);
    rig.hub
        .wait("the write landed", |log| log.sets_done >= 2)
        .await;
    rig.pull("a").await;
    rig.hub.report("a", flipped.clone());
    rig.pull("a").await;
    assert_eq!(
        rig.stored("a").await,
        flipped,
        "{first:?}: the switch change"
    );

    let resent = rig
        .hub
        .within(Duration::from_millis(300), |log| log.sets_started.len() > 2)
        .await;
    rig.hub.open_gate();
    rig.hub
        .wait("every PATCH answered", |log| {
            log.sets_done == log.sets_started.len()
        })
        .await;
    assert!(
        !resent,
        "{first:?}: the superseded retry went out after the write had landed: {:?}",
        rig.hub.log().sets_for("a")
    );
    assert_eq!(
        rig.hub.reported("a"),
        Some(flipped),
        "{first:?}: the switch change on the hub"
    );

    rig.shutdown().await;
}

#[tokio::test]
async fn a_superseded_writes_retry_leaves_a_switch_change() {
    a_superseded_retry_leaves_a_switch_change(FirstPush::UserWrite).await;
}

#[tokio::test]
async fn a_queued_pushs_retry_does_not_carry_a_user_write_on() {
    a_superseded_retry_leaves_a_switch_change(FirstPush::Queued).await;
}

/// A retry is checked once more right before it goes out (#45). One that
/// was current when it was queued again, but whose write ran out its window
/// while it waited in the sync buffer, is dropped then. It used to be
/// pushed anyway: the pull had already reverted the write, the hub then took
/// the abandoned value after all, and the next pull brought it back into
/// the store.
///
/// `a`'s push fails, and its retry comes due while the buffer worker is held
/// on `h`'s PATCH, so it waits in the buffer. That's well inside `a`'s
/// window (200 ms after the failure, of 800 ms); the window then runs out and
/// the pull reverts `a`, and only then is `h` let through.
#[tokio::test]
async fn a_retry_whose_write_ran_out_its_window_while_it_waited_is_dropped() {
    let delay = Duration::from_millis(200);
    let rig = Rig::new(
        &["a", "h"],
        TestHub::new(OnSet::Fail, true),
        Pulls::Periodic,
        SyncConfig {
            protection_window: Duration::from_millis(800),
            base_retry_delay: delay,
            max_retry_delay: delay,
            ..fast_retries()
        },
    )
    .await;
    let mut rx = rig.bus.subscribe();

    rig.write("a", on()).await;
    rig.hub
        .wait("a's push at the gate", |log| log.sets_started.len() == 1)
        .await;
    rig.write("h", on()).await;
    rig.hub.release(1);
    rig.hub
        .wait("h's push at the gate", |log| log.sets_started.len() == 2)
        .await;
    assert_eq!(
        rig.hub.log().sets_for("h"),
        vec![on()],
        "test timing: a's retry went out before h's push"
    );
    // a's retry comes due, and goes into the buffer behind h.
    rig.wait_for_retry_queue("a's retry taken out", 0).await;
    // a's window runs out, and the pull reverts it.
    wait_for_echo(&mut rx, "a", &off()).await;

    rig.hub.set_on_set(OnSet::Apply);
    rig.hub.release(1);
    let resent = rig
        .hub
        .within(Duration::from_millis(300), |log| {
            log.sets_for("a").len() > 1
        })
        .await;
    rig.hub.open_gate();
    rig.hub
        .wait("every PATCH answered", |log| {
            log.sets_done == log.sets_started.len()
        })
        .await;
    assert!(
        !resent,
        "the retry of a write whose window had run out went out: {:?}",
        rig.hub.log().sets_for("a")
    );
    rig.full_pull_cycle().await;
    assert_eq!(rig.hub.reported("a"), Some(off()), "a on the hub");
    assert_eq!(rig.stored("a").await, off(), "a in the store");

    rig.shutdown().await;
}

/// Retries are keyed per device and kind of task (#45). A queued pull of a
/// device with a push retry waiting must not cancel it when it succeeds,
/// nor replace it when it fails. They used to share one entry per device.
///
/// The push retry waits out a minute, so it's still waiting at the end.
#[tokio::test]
async fn a_queued_pull_neither_cancels_nor_replaces_a_push_retry() {
    let delay = Duration::from_mins(1);
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::OnDemand,
        SyncConfig {
            base_retry_delay: delay,
            max_retry_delay: delay,
            ..SyncConfig::default()
        },
    )
    .await;

    rig.write("a", on()).await;
    rig.wait_for_retry_queue("the failed push's retry", 1).await;

    rig.pull("a").await;
    assert_eq!(
        rig.engine.get_sync_stats().await.retry_queue_size,
        1,
        "a successful pull of a cancelled its push retry"
    );

    rig.hub.fail_reads_of("a");
    rig.pull("a").await;
    assert_eq!(
        rig.engine.get_sync_stats().await.retry_queue_size,
        2,
        "a failed pull of a replaced its push retry, instead of waiting next to it"
    );

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

/// A retry that comes due while a newer write of the device waits in the
/// sync buffer is dropped: the write supersedes it (#32 review). The write
/// goes out once, and as a write: its push marks the pending entry pushed,
/// so the hub's confirmation clears it and a switch change right after is
/// taken.
///
/// The batch's first push is held while the newer write comes in, then
/// fails; its retry comes due while the buffer worker is held on the
/// batch's second push, with the newer write still in the buffer.
#[tokio::test]
async fn a_buffered_write_supersedes_a_due_retry() {
    let rig = Rig::new(
        &["h", "a", "b"],
        TestHub::new(OnSet::Apply, true),
        Pulls::Periodic,
        SyncConfig {
            protection_window: Duration::from_mins(1),
            ..fast_retries()
        },
    )
    .await;
    let (older, newer) = (light(true, 30), light(true, 70));

    let first = rig
        .write_one_batch("h", &[("a", older.clone()), ("b", older.clone())])
        .await;
    rig.write(&first, newer.clone()).await;
    rig.hub.set_on_set(OnSet::Fail);
    rig.hub.release(1);
    rig.hub
        .wait("the batch's second push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    rig.hub.set_on_set(OnSet::Apply);
    // The retry comes due and is taken out of the queue. While the buffer
    // worker is held, the only way a PATCH of `first` can go out is a retry
    // sent although the newer write is buffered.
    let retry_sent = tokio::select! {
        () = rig.wait_for_retry_queue("the retry taken out", 0) => false,
        () = rig.hub.wait("a retry PATCH", |log| log.sets_for(&first).len() > 1) => true,
    };
    assert!(
        !retry_sent,
        "{first}: the retry went out although the newer write was buffered: {:?}",
        rig.hub.log()
    );

    rig.hub.open_gate();
    rig.hub
        .wait("the newer write's push landed", |log| {
            log.sets_for(&first).contains(&newer) && log.sets_done == log.sets_started.len()
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for(&first),
        vec![older, newer.clone()],
        "{first}: PATCHes"
    );
    rig.full_pull_cycle().await;
    let flipped = light(false, 40);
    rig.hub.report(&first, flipped.clone());
    rig.full_pull_cycle().await;
    assert_eq!(
        rig.stored(&first).await,
        flipped,
        "{first}: the switch change after the hub confirmed the newer write \
         was ignored: its push went out as a retry, not as a write"
    );

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
    let delay = Duration::from_mins(1);
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

/// Retries don't restart the protection window: only a write's own push
/// does. With a hub that keeps failing, the write is reverted once the
/// window from its push is up, while the retries are still going. Then the
/// write is abandoned: its retries stop. They used to go on with the
/// store's value, which is the hub's own by then (#32 review).
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
    // No retry of the store's value comes, and at most one of `on`: a
    // retry queued again just before the revert. (Bounded: 6 buffer ticks
    // and 60 retry ticks.)
    let at_revert = rig.hub.log().sets_started.len();
    let store_value_retried = rig
        .hub
        .within(Duration::from_millis(300), |log| {
            log.sets_started[at_revert..]
                .iter()
                .any(|(id, state)| id == "a" && *state == off())
        })
        .await;
    let after_revert = rig.hub.log().sets_started[at_revert..].to_vec();
    assert!(
        !store_value_retried && after_revert.len() <= 1,
        "the abandoned write was still retried after the revert: {after_revert:?}"
    );

    rig.shutdown().await;
}

/// A push queued with `queue_sync` below `Critical` never had a pending
/// confirmation, so there's no newer user value to send instead: its retry
/// resends its own value (#32 review). It used to send the store's, which
/// such a push never wrote.
#[tokio::test]
async fn a_queued_push_is_retried_with_its_own_value() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, true),
        Pulls::OnDemand,
        fast_retries(),
    )
    .await;

    rig.engine
        .queue_sync(SyncTask {
            device_id: "a".to_string(),
            task_type: SyncTaskType::PushToGateway { new_state: on() },
            created_at: Instant::now(),
            priority: SyncPriority::Normal,
        })
        .await;
    rig.hub
        .wait("the push at the gate", |log| log.sets_started.len() == 1)
        .await;
    rig.hub.release(1);
    rig.hub
        .wait("its retry at the gate", |log| log.sets_started.len() == 2)
        .await;
    assert_eq!(
        rig.hub.log().sets_for("a"),
        vec![on(), on()],
        "PATCHes: the retry didn't resend the queued value"
    );

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
