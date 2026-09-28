// 🔥 SENSOR DISPLAY - CYBER TEMP & HUMIDITY! 💖

use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SensorDisplayProps {
    #[prop_or_default]
    pub temperature: Option<f32>,
    #[prop_or_default]
    pub humidity: Option<f32>,
    #[prop_or(true)]
    pub show_temp: bool,
    #[prop_or(true)]
    pub show_humidity: bool,
}

#[function_component(SensorDisplay)]
pub fn sensor_display(props: &SensorDisplayProps) -> Html {
    html! {
        <div class="sensor-display">
            {if props.show_temp {
                html! {
                    <div class="sensor-value">
                        <span class="icon">{"🌡️"}</span>
                        <span class="value">
                            {props.temperature.map_or_else(|| "--".to_string(), |t| format!("{t:.1}"))}
                        </span>
                        <span class="unit">{"°C"}</span>
                    </div>
                }
            } else {
                html! {}
            }}

            {if props.show_temp && props.show_humidity {
                html! { <span class="separator">{"::"}</span> }
            } else {
                html! {}
            }}

            {if props.show_humidity {
                html! {
                    <div class="sensor-value">
                        <span class="icon">{"💧"}</span>
                        <span class="value">
                            {props.humidity.map_or_else(|| "--".to_string(), |h| format!("{h:.0}"))}
                        </span>
                        <span class="unit">{"%"}</span>
                    </div>
                }
            } else {
                html! {}
            }}
        </div>
    }
}
