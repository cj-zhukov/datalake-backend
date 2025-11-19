use leptos::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Select,
    Download,
}

impl AsRef<str> for Mode {
    fn as_ref(&self) -> &str {
        match self {
            Mode::Select => "Select",
            Mode::Download => "Download",
        }
    }
}

#[component]
pub fn OperationPanel(
    mode: ReadSignal<Mode>,
    set_mode: WriteSignal<Mode>,
    send_request: impl Fn(web_sys::MouseEvent) + 'static + Clone,
    is_loading: ReadSignal<bool>,
    modes: Vec<Mode>, // which one button to show
) -> impl IntoView {
    view! {
        <div style="display: flex; justify-content: space-between; align-items: center; width: 600px;">
            <select
                style="font-size: 1rem; padding: 0.5rem;"
                on:change=move |ev| {
                    let selected = event_target_value(&ev);
                    if selected == "Select" {
                        set_mode.set(Mode::Select);
                    } else {
                        set_mode.set(Mode::Download);
                    }
                }
            >
                // <option value="Select" selected={move || mode.get() == Mode::Select}>"Select"</option>
                // <option value="Download" selected={move || mode.get() == Mode::Download}>"Download"</option>
                
                <For
                    each=move || modes.clone()
                    key=|m| *m as i32
                    children=move |m| {
                        let value = m.as_ref().to_string();
                        view! {
                            <option
                                value=value.clone()
                                selected={move || mode.get() == m}
                            >
                                { value.clone() }
                            </option>
                        }
                    }
                />
            </select>

            <button
                style="font-size: 1rem; padding: 0.5rem 1rem;"
                on:click=send_request
                disabled=move || is_loading.get()
            >
                "Send Query"
            </button>

        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_mode() {
        assert_eq!(Mode::Select.as_ref(), "Select");
        assert_eq!(Mode::Download.as_ref(), "Download");

        let (mode, set_mode) = signal(Mode::Select);
        set_mode.set(Mode::Select);
        assert_eq!(mode.get(), Mode::Select);

        let (mode, set_mode) = signal(Mode::Select);
        set_mode.set(Mode::Download);
        assert_eq!(mode.get(), Mode::Download);
    }
}