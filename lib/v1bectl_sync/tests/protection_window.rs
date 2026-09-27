//! 🛡️ Integration tests for the protection window of user writes (#23).
//!
//! A user (`SyncPriority::Critical`) write goes into the store at once and
//! is pushed to the gateway later, from the sync buffer. Until the hub
//! confirms it, a pull that still reports the old value must leave the store
//! alone. The pending confirmation used to be armed only when the push went
//! out, so a pull that ran while a write was still queued (behind the other
//! members of a group write, say) reverted it.
//!
//! The engine runs for real, against the shared rig's `TestHub` (see
//! `common`): a fake gateway whose PATCHes can be held at a gate and whose
//! traffic is logged. The tests order their steps on that log, never on
//! sleeps.

mod common;

use std::time::{Duration, Instant};

use common::*;
use v1bectl_sync::SyncConfig;

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The #23 regression, as a group write makes it: three member writes in
/// one burst, and the first PATCH takes its time. The other two wait in the
/// buffer behind it, and the pull that runs meanwhile still reads the old
/// value for all three. None of them may be reverted.
#[tokio::test]
async fn queued_writes_survive_a_pull_while_the_first_push_is_held() {
    const IDS: [&str; 3] = ["a", "b", "c"];
    let rig = Rig::new(
        &IDS,
        TestHub::new(OnSet::Apply, true),
        Pulls::Periodic,
        SyncConfig::default(),
    )
    .await;

    for id in IDS {
        rig.write(id, on()).await;
    }
    rig.hub
        .wait("first PATCH at the gate", |log| {
            !log.sets_started.is_empty()
        })
        .await;

    // A whole pull cycle while that PATCH is held: the hub still reports
    // `off` for every device, and at most one push is out.
    rig.full_pull_cycle().await;
    let log = rig.hub.log();
    assert_eq!(log.sets_started.len(), 1, "one PATCH out: {log:?}");
    assert_eq!(log.sets_done, 0, "the PATCH is still held: {log:?}");
    for id in IDS {
        assert_eq!(rig.hub.reported(id), Some(off()), "{id} on the hub");
    }
    let held = &log.sets_started[0].0;
    for id in IDS {
        assert_eq!(
            rig.stored(id).await,
            on(),
            "{id} was reverted by a pull while its write was {} (#23)",
            if id == held {
                "in flight"
            } else {
                "still queued"
            }
        );
    }

    // Let the pushes land. The hub reports the new value, and the store
    // keeps it.
    rig.hub.open_gate();
    rig.hub
        .wait("all three pushes", |log| log.sets_done >= IDS.len())
        .await;
    rig.full_pull_cycle().await;
    for id in IDS {
        assert_eq!(rig.hub.reported(id), Some(on()), "{id} on the hub");
        assert_eq!(rig.stored(id).await, on(), "{id} in the store");
    }

    rig.shutdown().await;
}

/// The whole lifecycle of a pending confirmation: armed at the write (a
/// pull before the push is ignored), cleared when the hub confirms the
/// value (the store keeps it and echoes the confirmation), and gone after
/// that (the next external change is taken at once, not ignored for the
/// rest of the window).
#[tokio::test]
async fn hub_confirmation_clears_the_pending_entry() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();

    rig.write("a", on()).await;
    rig.hub
        .wait("PATCH at the gate", |log| !log.sets_started.is_empty())
        .await;
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "pull before the push landed");

    rig.hub.release(1);
    rig.hub.wait("push", |log| log.sets_done >= 1).await;
    drain_events(&mut rx);
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "after the hub confirmed");
    assert_eq!(
        drain_events(&mut rx),
        vec![("a".to_string(), json(&on()), json(&on()))],
        "the confirmation (old: the expected value, new: the hub's)"
    );

    // Someone flips the physical switch right after. With the pending
    // entry still there, this pull would be ignored as "not our change".
    let flipped = light(false, 40);
    rig.hub.report("a", flipped.clone());
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, flipped, "external change after it");

    rig.shutdown().await;
}

/// The window is bounded. With the hub still reporting the old value once
/// it's up, GatewayWins reverts the write, whether the push went through
/// (and the device never changed) or failed. The store isn't left
/// "protected" with a value the hub never got.
#[tokio::test]
async fn expired_window_lets_the_gateway_win() {
    let window = Duration::from_millis(150);
    for on_set in [OnSet::Ignore, OnSet::Fail] {
        let rig = Rig::new(
            &["a"],
            TestHub::new(on_set, false),
            Pulls::Periodic,
            SyncConfig {
                protection_window: window,
                ..SyncConfig::default()
            },
        )
        .await;
        let mut rx = rig.bus.subscribe();

        let written = Instant::now();
        rig.write("a", on()).await;
        wait_for_echo(&mut rx, "a", &off()).await;
        let reverted = written.elapsed();
        assert!(
            reverted >= window,
            "{on_set:?}: reverted after {reverted:?}, inside the {window:?} window"
        );
        assert_eq!(rig.stored("a").await, off(), "{on_set:?}: store");
        rig.hub.wait("push", |log| log.sets_done >= 1).await;

        rig.shutdown().await;
    }
}

/// Two writes to one device while it's still queued. The buffer sends one
/// PATCH, with the second value, and the pending entry expects the second
/// value too. The hub reporting the first one isn't a confirmation; if it
/// were taken as one, the store would drop the user's latest write.
#[tokio::test]
async fn coalesced_writes_expect_the_latest_value() {
    let rig = Rig::new(
        &["x", "a"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();

    // Keep the buffer worker busy on another device, so both writes to `a`
    // stay queued.
    rig.write("x", on()).await;
    rig.hub
        .wait("x at the gate", |log| !log.sets_started.is_empty())
        .await;
    let (first, second) = (light(true, 30), light(true, 70));
    rig.write("a", first.clone()).await;
    rig.write("a", second.clone()).await;

    rig.hub.report("a", first.clone());
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, second, "hub reports the first write");

    rig.hub.open_gate();
    rig.hub.wait("both pushes", |log| log.sets_done >= 2).await;
    assert_eq!(rig.hub.log().sets_for("a"), vec![second.clone()], "PATCHes");
    drain_events(&mut rx);
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, second, "after the hub confirmed");
    assert_eq!(
        drain_events(&mut rx),
        vec![("a".to_string(), json(&second), json(&second))],
        "the confirmation, expecting the second write"
    );

    rig.shutdown().await;
}

/// A newer write that comes in after the buffer drained the older one. The
/// older push still goes out, and restarting the window for it must not
/// move the expectation back to the older value: the hub confirming that
/// older value would then revert the newer write.
#[tokio::test]
async fn stale_push_keeps_the_newer_expectation() {
    let rig = Rig::new(
        &["x", "a", "b"],
        TestHub::new(OnSet::Apply, true),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;
    let mut rx = rig.bus.subscribe();
    let (older, newer) = (light(true, 30), light(true, 70));

    // Hold the buffer worker on `x` while `a` and `b` are written, so the
    // next drain takes both. Its first PATCH is held too, and the other
    // device's value waits in the drained batch.
    rig.write("x", on()).await;
    rig.hub
        .wait("x at the gate", |log| log.sets_started.len() == 1)
        .await;
    rig.write("a", older.clone()).await;
    rig.write("b", older.clone()).await;
    rig.hub.release(1);
    rig.hub
        .wait("a or b at the gate", |log| log.sets_started.len() == 2)
        .await;
    let held = rig.hub.log().sets_started[1].0.clone();
    let stale = if held == "a" { "b" } else { "a" };

    // `stale`'s older value is already out of the buffer. A newer write
    // re-arms its pending entry and goes into the buffer.
    rig.write(stale, newer.clone()).await;

    // The older push goes out (its window restarts) and is held at the gate.
    // The hub already shows it while it's still answering the PATCH. This is
    // the pull that would take the older value for a confirmation if the
    // restart had moved the expectation back to it.
    rig.hub.release(1);
    rig.hub
        .wait("the older push at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for(stale),
        vec![older.clone()],
        "PATCHes"
    );
    rig.hub.report(stale, older.clone());
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale}: the hub showing the older push reverted the newer write"
    );

    // The older push lands. The newer one goes out and is held: the hub
    // still reports the older value.
    rig.hub.release(1);
    rig.hub
        .wait("the newer push at the gate", |log| {
            log.sets_started.len() == 4
        })
        .await;
    assert_eq!(
        rig.hub.log().sets_for(stale),
        vec![older.clone(), newer.clone()],
        "{stale}: PATCHes"
    );
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale}: the hub confirming the older push reverted the newer write"
    );

    rig.hub.open_gate();
    rig.hub.wait("all pushes", |log| log.sets_done >= 4).await;
    drain_events(&mut rx);
    rig.pull(stale).await;
    assert_eq!(
        rig.stored(stale).await,
        newer,
        "{stale} after the hub confirmed"
    );
    assert_eq!(
        drain_events(&mut rx),
        vec![(stale.to_string(), json(&newer), json(&newer))],
        "the confirmation, expecting the newer write"
    );

    rig.shutdown().await;
}

/// A failed push leaves the pending entry in place. The retry comes well
/// inside the window, so a pull in between must not revert the write
/// (a transient failure would flicker the UI). `expired_window_lets_the_gateway_win`
/// shows the entry still runs out.
#[tokio::test]
async fn failed_push_stays_protected_within_the_window() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Fail, false),
        Pulls::OnDemand,
        SyncConfig::default(),
    )
    .await;

    rig.write("a", on()).await;
    rig.hub.wait("failed push", |log| log.sets_done >= 1).await;
    rig.pull("a").await;
    assert_eq!(rig.stored("a").await, on(), "pull right after the failure");

    rig.shutdown().await;
}
