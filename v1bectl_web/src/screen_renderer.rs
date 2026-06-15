// 🔥 SCREEN RENDERER - DYNAMIC KDL-DRIVEN UI! 💖

use std::collections::HashMap;
use yew::prelude::*;

use crate::components::{LightControl, OutletControl, SensorDisplay};
use crate::screens::{DeviceRef, Element, Group, Screen};
use crate::websocket_reconnect::{ApiRequest, DeviceState, DeviceStateValue};

#[derive(Properties, PartialEq)]
pub struct ScreenRendererProps {
    pub screen: Screen,
    pub devices: Vec<DeviceState>,
    pub on_request: Callback<ApiRequest>,
}

#[function_component(ScreenRenderer)]
pub fn screen_renderer(props: &ScreenRendererProps) -> Html {
    // Build device lookup maps
    let devices_by_id: HashMap<String, &DeviceState> = props
        .devices
        .iter()
        .map(|d| (d.device_info.device_id.clone(), d))
        .collect();

    let devices_by_name: HashMap<String, &DeviceState> = props
        .devices
        .iter()
        .map(|d| (d.device_info.name.clone(), d))
        .collect();

    html! {
        <div class="screen">
            {for props.screen.groups.iter().map(|group| {
                render_group(group, &devices_by_id, &devices_by_name, &props.on_request)
            })}
        </div>
    }
}

fn render_group(
    group: &Group,
    devices_by_id: &HashMap<String, &DeviceState>,
    devices_by_name: &HashMap<String, &DeviceState>,
    on_request: &Callback<ApiRequest>,
) -> Html {
    html! {
        <div class="group">
            {for group.elements.iter().map(|element| {
                render_element(element, devices_by_id, devices_by_name, on_request)
            })}
        </div>
    }
}

fn render_element(
    element: &Element,
    devices_by_id: &HashMap<String, &DeviceState>,
    devices_by_name: &HashMap<String, &DeviceState>,
    on_request: &Callback<ApiRequest>,
) -> Html {
    match element {
        Element::Sensor {
            device_ref,
            show_temp,
            show_humidity,
        } => {
            let device = resolve_device(device_ref, devices_by_id, devices_by_name);

            let (temperature, humidity) = device
                .map(|d| extract_sensor_values(&d.state))
                .unwrap_or((None, None));

            html! {
                <SensorDisplay
                    temperature={temperature}
                    humidity={humidity}
                    show_temp={*show_temp}
                    show_humidity={*show_humidity}
                />
            }
        }

        Element::Light {
            name,
            device_ref,
            show_switch,
            show_slider,
        } => {
            let device = resolve_device(device_ref, devices_by_id, devices_by_name);
            let device_id = get_device_id(device_ref, device);

            let (is_on, brightness) = device
                .map(|d| extract_light_values(&d.state))
                .unwrap_or((false, 0));

            let on_toggle = {
                let on_request = on_request.clone();
                let device_id = device_id.clone();
                Callback::from(move |new_state: bool| {
                    if let Some(id) = &device_id {
                        on_request.emit(ApiRequest::SetLightState {
                            device_id: id.clone(),
                            is_on: Some(new_state),
                            brightness: None,
                            color_temp: None,
                            rgb_color: None,
                        });
                    }
                })
            };

            let on_brightness = {
                let on_request = on_request.clone();
                let device_id = device_id.clone();
                Callback::from(move |new_brightness: u8| {
                    if let Some(id) = &device_id {
                        on_request.emit(ApiRequest::SetLightState {
                            device_id: id.clone(),
                            is_on: None,
                            brightness: Some(new_brightness),
                            color_temp: None,
                            rgb_color: None,
                        });
                    }
                })
            };

            html! {
                <LightControl
                    name={name.clone()}
                    is_on={is_on}
                    brightness={brightness}
                    on_toggle={on_toggle}
                    on_brightness={on_brightness}
                    show_switch={*show_switch}
                    show_slider={*show_slider}
                />
            }
        }

        Element::Outlet { name, device_ref } => {
            let device = resolve_device(device_ref, devices_by_id, devices_by_name);
            let device_id = get_device_id(device_ref, device);

            let is_on = device
                .map(|d| extract_outlet_value(&d.state))
                .unwrap_or(false);

            let on_toggle = {
                let on_request = on_request.clone();
                let device_id = device_id.clone();
                Callback::from(move |new_state: bool| {
                    if let Some(id) = &device_id {
                        on_request.emit(ApiRequest::SetOutletState {
                            device_id: id.clone(),
                            is_on: new_state,
                        });
                    }
                })
            };

            html! {
                <OutletControl
                    name={name.clone()}
                    is_on={is_on}
                    on_toggle={on_toggle}
                />
            }
        }

        Element::Text { template } => {
            // TODO: Template substitution
            html! {
                <div class="text-element">{template}</div>
            }
        }

        // 🔥 BLOCK - HORIZONTAL GROUPING! 💖
        Element::Block { elements } => {
            html! {
                <div class="block-horizontal">
                    {for elements.iter().map(|el| {
                        render_element(el, devices_by_id, devices_by_name, on_request)
                    })}
                </div>
            }
        }
    }
}

fn resolve_device<'a>(
    device_ref: &DeviceRef,
    devices_by_id: &'a HashMap<String, &'a DeviceState>,
    devices_by_name: &'a HashMap<String, &'a DeviceState>,
) -> Option<&'a DeviceState> {
    match device_ref {
        DeviceRef::ById(id) => devices_by_id.get(id).copied(),
        DeviceRef::ByName(name) => devices_by_name.get(name).copied(),
    }
}

fn get_device_id(device_ref: &DeviceRef, device: Option<&DeviceState>) -> Option<String> {
    match device_ref {
        DeviceRef::ById(id) => Some(id.clone()),
        DeviceRef::ByName(_) => device.map(|d| d.device_info.device_id.clone()),
    }
}

fn extract_sensor_values(state: &DeviceStateValue) -> (Option<f32>, Option<f32>) {
    match state {
        DeviceStateValue::Sensor(s) => (s.temperature, s.humidity),
        _ => (None, None),
    }
}

fn extract_light_values(state: &DeviceStateValue) -> (bool, u8) {
    match state {
        DeviceStateValue::Light(s) => (s.is_on, s.brightness.unwrap_or(0)),
        _ => (false, 0),
    }
}

fn extract_outlet_value(state: &DeviceStateValue) -> bool {
    match state {
        DeviceStateValue::Outlet(s) => s.is_on,
        _ => false,
    }
}

// 🔥 SCREEN DOTS NAVIGATION! 💖
#[derive(Properties, PartialEq)]
pub struct ScreenDotsProps {
    pub total: usize,
    pub current: usize,
    pub on_select: Callback<usize>,
}

#[function_component(ScreenDots)]
pub fn screen_dots(props: &ScreenDotsProps) -> Html {
    html! {
        <div class="screen-dots">
            {for (0..props.total).map(|i| {
                let on_select = props.on_select.clone();
                let onclick = Callback::from(move |_| on_select.emit(i));
                let class = if i == props.current { "dot active" } else { "dot" };
                html! {
                    <div class={class} onclick={onclick}></div>
                }
            })}
        </div>
    }
}
