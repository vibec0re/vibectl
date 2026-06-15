// 🔥 LIGHT CONTROL - CYBER DIMMER! 💖

use super::CyberSlider;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct LightControlProps {
    pub name: String,
    pub is_on: bool,
    pub brightness: u8,
    pub on_toggle: Callback<bool>,
    pub on_brightness: Callback<u8>,
    #[prop_or(true)]
    pub show_switch: bool,
    #[prop_or(true)]
    pub show_slider: bool,
}

#[function_component(LightControl)]
pub fn light_control(props: &LightControlProps) -> Html {
    let on_click = {
        let on_toggle = props.on_toggle.clone();
        let is_on = props.is_on;
        Callback::from(move |_: MouseEvent| {
            on_toggle.emit(!is_on);
        })
    };

    let toggle_class = if props.is_on {
        "light-toggle on"
    } else {
        "light-toggle off"
    };

    // 🔥 ADD CLASS FOR NO-SLIDER VARIANT! 💖
    let container_class = if props.show_slider {
        "light-control"
    } else {
        "light-control no-slider"
    };

    html! {
        <div class={container_class}>
            // Toggle button FIRST
            {if props.show_switch {
                html! {
                    <button class={toggle_class} onclick={on_click}>
                        {&props.name}
                    </button>
                }
            } else {
                html! { <span class="light-name">{&props.name}</span> }
            }}

            // Slider SECOND
            {if props.show_slider {
                html! {
                    <div class="light-slider">
                        <CyberSlider
                            value={props.brightness}
                            on_change={props.on_brightness.clone()}
                            disabled={!props.is_on}
                        />
                    </div>
                }
            } else {
                html! {}
            }}
        </div>
    }
}
