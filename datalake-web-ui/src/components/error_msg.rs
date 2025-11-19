use leptos::prelude::*;

#[component]
pub fn ErrorMessage(error: ReadSignal<Option<String>>) -> impl IntoView {
    view! {
        <Show when=move || error.get().is_some()>
            <div style="color: red; font-weight: bold; text-align: center; font-size: 2.0rem;">
                {move || error.get().unwrap_or_default()}
            </div>
        </Show>
    }
}