// 🔥 CYBER SWITCH - NEON TOGGLE! 💖

use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct CyberSwitchProps {
    pub checked: bool,
    pub on_toggle: Callback<bool>,
    #[prop_or_default]
    pub disabled: bool,
}

#[function_component(CyberSwitch)]
pub fn cyber_switch(props: &CyberSwitchProps) -> Html {
    let on_change = {
        let on_toggle = props.on_toggle.clone();
        let checked = props.checked;
        Callback::from(move |_: Event| {
            on_toggle.emit(!checked);
        })
    };

    html! {
        <label class="cyber-switch">
            <input
                type="checkbox"
                checked={props.checked}
                onchange={on_change}
                disabled={props.disabled}
            />
            <span class="switch-track">
                <span class="switch-thumb"></span>
            </span>
        </label>
    }
}
