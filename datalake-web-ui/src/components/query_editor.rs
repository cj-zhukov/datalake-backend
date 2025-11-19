use leptos::prelude::*;

#[component]
pub fn QueryEditor(
    query: ReadSignal<String>,
    set_query: WriteSignal<String>,
) -> impl IntoView {
    view! {
        <textarea
            style="width: 800px; height: 150px; font-size: 1rem;"
            on:input=move |ev| set_query.set(event_target_value(&ev))
        >
            { move || query.get() }
        </textarea>
    }
}
