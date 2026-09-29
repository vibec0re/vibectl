//! #58: the "Configuration System" section of `docs/VIRTUAL_DEVICES.md` once
//! showed JSON the loader never accepted. Every ` ```toml ` block in that doc
//! now claims to be a real `VirtualDeviceTomlConfig` — this pins that they
//! all still parse as one, so the doc can't rot back into fiction.

use std::path::PathBuf;
use v1bectl_virtual::VirtualDeviceTomlConfig;

fn docs_file() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/VIRTUAL_DEVICES.md");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every ` ```toml ` ... ` ``` ` fenced block in `doc`, in order, body only.
fn toml_blocks(doc: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut lines = doc.lines();
    while lines.by_ref().any(|line| line.trim() == "```toml") {
        let mut block = String::new();
        for line in lines.by_ref() {
            if line.trim() == "```" {
                break;
            }
            block.push_str(line);
            block.push('\n');
        }
        blocks.push(block);
    }
    blocks
}

/// The doc's own examples, parsed the way `load_virtual_devices_from_dir`
/// parses a shipped `virtual_devices/*.toml` file.
#[test]
fn every_toml_example_in_the_docs_parses() {
    let blocks = toml_blocks(&docs_file());
    assert!(
        !blocks.is_empty(),
        "no ```toml block found in docs/VIRTUAL_DEVICES.md"
    );
    for (i, block) in blocks.iter().enumerate() {
        toml::from_str::<VirtualDeviceTomlConfig>(block)
            .unwrap_or_else(|e| panic!("block {} of {}: {e}\n{block}", i + 1, blocks.len()));
    }
}

/// The doc claims one example per `VirtualDeviceTomlConfig` variant: if a
/// new type is ever added without a doc example (or one is renamed), this
/// says so directly instead of leaving it to be noticed by hand.
#[test]
fn every_config_type_has_a_doc_example() {
    let blocks = toml_blocks(&docs_file());
    let configs: Vec<VirtualDeviceTomlConfig> = blocks
        .iter()
        .map(|block| toml::from_str(block).expect("parses (see the other test)"))
        .collect();

    let kind = |c: &VirtualDeviceTomlConfig| match c {
        VirtualDeviceTomlConfig::LightGroup(_) => "light_group",
        VirtualDeviceTomlConfig::LightGroupLinear(_) => "light_group_linear",
        VirtualDeviceTomlConfig::ButtonController(_) => "button_controller",
        VirtualDeviceTomlConfig::SceneController(_) => "scene_controller",
    };
    let mut kinds: Vec<&str> = configs.iter().map(kind).collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(
        kinds,
        vec![
            "button_controller",
            "light_group",
            "light_group_linear",
            "scene_controller",
        ],
        "docs/VIRTUAL_DEVICES.md should show one example per config type"
    );
}
