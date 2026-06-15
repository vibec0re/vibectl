// 🔥 OUTLET CONTROL - CYBER POWER SWITCH! 💖

use super::CyberSwitch;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct OutletControlProps {
    pub name: String,
    pub is_on: bool,
    pub on_toggle: Callback<bool>,
}

#[function_component(OutletControl)]
pub fn outlet_control(props: &OutletControlProps) -> Html {
    html! {
        <div class="outlet-control">
            <div>
                <span class="outlet-name">{&props.name}</span>
                <span class={classes!("outlet-status", if props.is_on { "on" } else { "" })}>
                    {if props.is_on { "ON" } else { "OFF" }}
                </span>
            </div>
            <CyberSwitch
                checked={props.is_on}
                on_toggle={props.on_toggle.clone()}
            />
        </div>
    }
}
