//! 🛡️ Integration tests for the protection window of user writes (#23).
//!
//! A user write (`apply_optimistic_update`) goes into the store at once and
//! is pushed to the gateway later, from the sync buffer. Until the hub
//! confirms it, a pull that still reports the old value must leave the store
//! alone. The pending confirmation used to be armed only when the push went
//! out, so a pull that ran while a write was still queued (behind the other
//! members of a group write, say) reverted it.
//!
//! The engine runs for real, against the shared rig's `TestHub` (see
//! `common`): a fake gateway whose `PATCHes` can be held at a gate and whose
//! traffic is logged. The tests order their steps on that log, never on
//! sleeps, except `push_time_restart_covers_a_queue_delay`, whose subject is
//! elapsed time.

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

/// The same lifecycle through the periodic pull (#32). With optimistic
/// updates the store shows the write before the hub does, so once the push
/// lands, the pull finds the store and the hub equal and has nothing to
/// reconcile. It must still take that as the confirmation: a physical
/// switch right after must be taken, not ignored for the rest of the
/// window. (The window is long, so it can't run out during the test.)
#[tokio::test]
async fn periodic_pull_confirmation_clears_the_pending_entry() {
    let rig = Rig::new(
        &["a"],
        TestHub::new(OnSet::Apply, false),
        Pulls::Periodic,
        SyncConfig {
            protection_window: Duration::from_mins(1),
            ..SyncConfig::default()
        },
    )
    .await;

    rig.write("a", on()).await;
    rig.hub.wait("push", |log| log.sets_done >= 1).await;
    // A whole cycle that finds `on` in the store and on the hub.
    rig.full_pull_cycle().await;
    assert_eq!(rig.stored("a").await, on(), "after the hub confirmed");

    let flipped = light(false, 40);
    rig.hub.report("a", flipped.clone());
    rig.full_pull_cycle().await;
    assert_eq!(
        rig.stored("a").await,
        flipped,
        "the switch change after the confirmation was ignored: the pending \
         entry outlived the periodic pull's confirmation"
    );

    rig.shutdown().await;
}

/// The window is bounded. With the hub still reporting the old value once
/// it's up, `GatewayWins` reverts the write, whether the push went through
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

/// The window restarts when the push goes out. A write that waited in the
/// buffer for most of its window, whose push is then slow to be answered,
/// must not be reverted by a pull that lands after the write's original
/// window but inside the restarted one.
///
/// From the #31 review. The window is time, so this is the one test here
/// that sleeps: the two sleeps are the queue delay and the slow answer, and
/// the two timing asserts turn a machine too slow for them into a clear
/// "test timing" failure rather than a false pass or fail.
#[tokio::test]
async fn push_time_restart_covers_a_queue_delay() {
    let window = Duration::from_secs(1);
    let rig = Rig::new(
        &["a", "b"],
        TestHub::new(OnSet::Apply, true),
        Pulls::Periodic,
        SyncConfig {
            protection_window: window,
            ..SyncConfig::default()
        },
    )
    .await;

    let written = Instant::now();
    rig.write("a", on()).await;
    rig.write("b", on()).await;
    rig.hub
        .wait("first PATCH at the gate", |log| log.sets_started.len() == 1)
        .await;
    let held = rig.hub.log().sets_started[0].0.clone();
    let queued = if held == "a" { "b" } else { "a" };

    // The queued write waits behind the held PATCH for 60% of its window.
    tokio::time::sleep(window * 6 / 10).await;
    rig.hub.release(1);
    rig.hub
        .wait("queued PATCH at the gate", |log| {
            log.sets_started.len() == 2
        })
        .await;
    let pushed_at = Instant::now();

    // Its push is out but not answered. Wait until the window measured from
    // the write is over, while the one measured from the push isn't.
    tokio::time::sleep(window * 6 / 10).await;
    assert!(
        written.elapsed() > window,
        "test timing: the original window is not over yet"
    );
    assert!(
        pushed_at.elapsed() < window,
        "test timing: the restarted window already ran out"
    );
    rig.full_pull_cycle().await;
    assert_eq!(
        rig.stored(queued).await,
        on(),
        "{queued}: reverted by a pull inside the restarted window (push-time restart missing)"
    );

    rig.hub.open_gate();
    rig.hub.wait("both pushes", |log| log.sets_done >= 2).await;
    rig.shutdown().await;
}

/// Which device of a two-write batch the toggle-back tests toggle back.
#[derive(Clone, Copy, Debug)]
enum ToggledBack {
    /// The device whose `on` PATCH the batch sends first: the toggle-back is
    /// made while that PATCH is in flight.
    HeldFirst,
    /// The device whose `on` PATCH waits behind the other one: the
    /// toggle-back is made while its stale `on` is out of the buffer but not
    /// sent yet.
    Behind,
}

/// A pull of `id` the way `pulls` makes them: a whole periodic cycle, or
/// a queued `PullFromGateway` of `id`.
async fn pull_now(rig: &Rig, pulls: Pulls, id: &str) {
    match pulls {
        Pulls::Periodic => rig.full_pull_cycle().await,
        Pulls::OnDemand => rig.pull(id).await,
    }
}

/// From the #32 review: a pull may only take a value for a write's
/// confirmation once that value's push went out. The user toggles a light
/// on and back off while the `on` PATCH is in flight (a double click, a
/// slider snapped back). The hub still reports `off`, which is also what
/// the toggle-back expects, but only by coincidence: the `on` PATCH is
/// about to move the hub. The toggle-back must stay protected until its own
/// push lands, whichever order the batch sends its `PATCHes` in, and the UI
/// must never show it `on` again.
///
/// `Pulls::Periodic` takes the periodic pull's path (`clear_confirmed`, the
/// store and the hub agree); `Pulls::OnDemand` a queued pull's
/// (`handle_gateway_state_change`).
async fn toggle_back_during_an_in_flight_patch(pulls: Pulls, toggled_back: ToggledBack) {
    let rig = Rig::new(
        &["h", "a", "b"],
        TestHub::new(OnSet::Apply, true),
        pulls,
        SyncConfig::default(),
    )
    .await;
    let first = rig.write_one_batch("h", &[("a", on()), ("b", on())]).await;
    let behind = if first == "a" { "b" } else { "a" };
    let toggled = match toggled_back {
        ToggledBack::HeldFirst => first.as_str(),
        ToggledBack::Behind => behind,
    };
    let case = format!("{pulls:?}, {toggled_back:?}: {toggled}");
    let mut rx = rig.bus.subscribe();

    // Toggle back before the hub answered: it still reports `off`.
    rig.write(toggled, off()).await;
    pull_now(&rig, pulls, toggled).await;
    assert_eq!(
        rig.stored(toggled).await,
        off(),
        "{case}: before the `on` PATCH landed"
    );

    // The batch's `on` PATCHes land, but not the toggle-back's own push.
    rig.hub.release(1);
    rig.hub
        .wait("the batch's second PATCH at the gate", |log| {
            log.sets_started.len() == 3
        })
        .await;
    if let ToggledBack::Behind = toggled_back {
        assert_eq!(
            rig.hub.log().sets_started[2],
            (toggled.to_string(), on()),
            "{case}: the stale `on` goes out"
        );
        rig.hub.release(1);
        rig.hub
            .wait("the stale `on` landed", |log| log.sets_done >= 3)
            .await;
    }
    // Held first: `toggled`'s `on` landed; the buffer worker is held on
    // `behind`, and the toggle-back still waits in the buffer. Behind: its
    // stale `on` landed; the toggle-back's push may be out, but not landed.
    assert_eq!(rig.hub.reported(toggled), Some(on()), "{case}: the hub");
    pull_now(&rig, pulls, toggled).await;
    assert_eq!(
        rig.stored(toggled).await,
        off(),
        "{case}: the toggle-back was reverted: the pull that found the hub \
         still on the old value took it for the confirmation"
    );

    // The toggle-back's push lands, and that is its confirmation.
    rig.hub.open_gate();
    rig.hub
        .wait("the toggle-back's push landed", |log| {
            log.sets_for(toggled).last() == Some(&off()) && log.sets_done == log.sets_started.len()
        })
        .await;
    pull_now(&rig, pulls, toggled).await;
    assert_eq!(rig.hub.reported(toggled), Some(off()), "{case}: the hub");
    assert_eq!(rig.stored(toggled).await, off(), "{case}: the store");
    let shown: Vec<Value> = drain_events(&mut rx)
        .into_iter()
        .filter(|(id, _, _)| id == toggled)
        .map(|(_, _, new)| new)
        .collect();
    assert!(
        shown.iter().all(|new| *new == json(&off())),
        "{case}: the UI showed the toggled-back light on again: {shown:?}"
    );

    rig.shutdown().await;
}

#[tokio::test]
async fn toggle_back_during_its_own_in_flight_patch_is_not_reverted() {
    toggle_back_during_an_in_flight_patch(Pulls::Periodic, ToggledBack::HeldFirst).await;
}

#[tokio::test]
async fn toggle_back_behind_another_devices_patch_is_not_reverted() {
    toggle_back_during_an_in_flight_patch(Pulls::Periodic, ToggledBack::Behind).await;
}

#[tokio::test]
async fn queued_pull_toggle_back_during_its_own_in_flight_patch_is_not_reverted() {
    toggle_back_during_an_in_flight_patch(Pulls::OnDemand, ToggledBack::HeldFirst).await;
}

#[tokio::test]
async fn queued_pull_toggle_back_behind_another_devices_patch_is_not_reverted() {
    toggle_back_during_an_in_flight_patch(Pulls::OnDemand, ToggledBack::Behind).await;
}

/// A device removed from the store takes its pending confirmation with it
/// (#32). Nothing else clears it: only a pull of the device does, and pulls
/// only read devices in the store. If the device comes back under the same
/// id, a stale entry would ignore its first real changes for the rest of
/// that window.
///
/// `b` stays in the store so the pull cycles have something to read.
#[tokio::test]
async fn a_removed_device_takes_its_pending_confirmation_with_it() {
    let rig = Rig::new(
        &["a", "b"],
        TestHub::new(OnSet::Ignore, false),
        Pulls::Periodic,
        SyncConfig {
            protection_window: Duration::from_mins(1),
            ..SyncConfig::default()
        },
    )
    .await;

    // A write the hub never confirms: its entry would last the whole window.
    rig.write("a", on()).await;
    rig.hub.wait("push", |log| log.sets_done >= 1).await;
    rig.store
        .remove_device(&"a".to_string())
        .await
        .expect("remove a");
    rig.full_pull_cycle().await;

    // `a` comes back (rediscovered, say), and its switch is flipped.
    rig.store.add_device(light_info("a"), off()).await;
    let flipped = light(true, 40);
    rig.hub.report("a", flipped.clone());
    rig.full_pull_cycle().await;
    assert_eq!(
        rig.stored("a").await,
        flipped,
        "a pending entry from before the removal ignored the change"
    );

    rig.shutdown().await;
}
