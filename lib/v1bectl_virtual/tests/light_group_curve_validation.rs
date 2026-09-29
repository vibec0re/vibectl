//! #71: `LightGroup::new` checks a member's brightness curve the way
//! `v1bectl_server` checks a TOML `min`/`max` pair before it builds one
//! (#67): every breakpoint must be a brightness, 0..=100, and a curve needs
//! at least two of them to interpolate between. Before, the API's
//! `CreateVirtualDevice` took any curve straight from the client — set to
//! 60, `[[0, 0], [100, 200]]` planned its member at 120.
//!
//! Every curve that was valid before stays valid: rising, falling (`min >
//! max`), a dip, or a plateau.

use v1bectl_sync::StateStore;
use v1bectl_virtual::{LightGroup, VirtualDeviceConfig, VirtualDeviceType};

const LIGHT: &str = "light";

/// A single-member `LightGroup` config over `breakpoints`, built the way
/// `v1bectl_server` and the API's `CreateVirtualDevice` build one.
fn config_over(breakpoints: &[[u8; 2]]) -> VirtualDeviceConfig {
    VirtualDeviceConfig {
        device_id: "g".to_string(),
        device_type: VirtualDeviceType::LightGroup,
        name: "g".to_string(),
        description: None,
        enabled: true,
        config: serde_json::json!({
            "lights": [LIGHT],
            "brightness_curves": { LIGHT: { "breakpoints": breakpoints } },
        }),
    }
}

/// #71: the API's example — set to 60, this plans the member at 120.
#[test]
fn a_breakpoint_over_100_is_rejected() {
    let store = StateStore::new();
    let Err(err) = LightGroup::new(config_over(&[[0, 0], [100, 200]]), store) else {
        panic!("[[0, 0], [100, 200]] was accepted");
    };
    assert!(
        err.to_string().contains("out of range"),
        "unexpected message: {err}"
    );
}

/// The group-brightness side of a breakpoint is checked too, not just the
/// device-brightness side.
#[test]
fn a_group_level_over_100_is_rejected() {
    let store = StateStore::new();
    let Err(err) = LightGroup::new(config_over(&[[0, 0], [150, 100]]), store) else {
        panic!("[[0, 0], [150, 100]] was accepted");
    };
    assert!(
        err.to_string().contains("out of range"),
        "unexpected message: {err}"
    );
}

/// A curve needs at least two breakpoints to interpolate between.
#[test]
fn a_single_breakpoint_is_rejected() {
    let store = StateStore::new();
    let Err(err) = LightGroup::new(config_over(&[[50, 50]]), store) else {
        panic!("a single breakpoint was accepted");
    };
    assert!(
        err.to_string().contains("at least 2 breakpoints"),
        "unexpected message: {err}"
    );
}

/// An empty curve is rejected the same way.
#[test]
fn an_empty_curve_is_rejected() {
    let store = StateStore::new();
    let Err(err) = LightGroup::new(config_over(&[]), store) else {
        panic!("an empty curve was accepted");
    };
    assert!(
        err.to_string().contains("at least 2 breakpoints"),
        "unexpected message: {err}"
    );
}

/// Every shape of curve that was valid before #71 stays valid: 1:1, a TOML
/// `min`/`max` pair, a falling range (`min > max`), a dip, and a plateau.
#[test]
fn every_currently_valid_curve_still_builds() {
    let curves: [(&str, &[[u8; 2]]); 6] = [
        ("identity", &[[0, 0], [100, 100]]),
        ("toml range", &[[0, 10], [100, 60]]),
        ("falling", &[[0, 80], [100, 20]]),
        ("dip", &[[0, 100], [50, 20], [100, 100]]),
        ("plateau", &[[0, 0], [30, 40], [60, 40], [100, 100]]),
        ("flat", &[[0, 50], [100, 50]]),
    ];
    for (name, breakpoints) in curves {
        let store = StateStore::new();
        LightGroup::new(config_over(breakpoints), store)
            .unwrap_or_else(|e| panic!("{name} {breakpoints:?} was rejected: {e}"));
    }
}
