// 🔥 CYBER SLIDER - SMOOTH BRIGHTNESS CONTROL! 💖

use wasm_bindgen::JsCast;
use web_sys::HtmlInputElement;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct CyberSliderProps {
    pub value: u8,
    pub on_change: Callback<u8>,
    #[prop_or(0)]
    pub min: u8,
    #[prop_or(100)]
    pub max: u8,
    #[prop_or_default]
    pub disabled: bool,
}

#[function_component(CyberSlider)]
pub fn cyber_slider(props: &CyberSliderProps) -> Html {
    let on_input = {
        let on_change = props.on_change.clone();
        Callback::from(move |e: InputEvent| {
            if let Some(target) = e.target() {
                if let Ok(input) = target.dyn_into::<HtmlInputElement>() {
                    if let Ok(value) = input.value().parse::<u8>() {
                        on_change.emit(value);
                    }
                }
            }
        })
    };

    // Calculate percentage for CSS custom property
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "value is clamped to [min, max] by the slider itself, so the fraction is in \
                  [0.0, 1.0] and the result fits in [0, 100] before it ever reaches this cast"
    )]
    let percent = if props.max > props.min {
        (f32::from(props.value - props.min) / f32::from(props.max - props.min) * 100.0) as u32
    } else {
        0
    };

    let style = format!("--percent: {percent}%");

    html! {
        <input
            type="range"
            class="cyber-slider filled"
            min={props.min.to_string()}
            max={props.max.to_string()}
            value={props.value.to_string()}
            oninput={on_input}
            disabled={props.disabled}
            style={style}
        />
    }
}
