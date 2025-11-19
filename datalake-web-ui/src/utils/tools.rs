use leptos::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::window;

#[component]
pub fn DataTable(records: Vec<Vec<String>>) -> impl IntoView {
    let header: Vec<String> = records.get(0).cloned().unwrap_or_default();
    let rows_source = {
        move || records.iter().skip(1).cloned().collect::<Vec<_>>()
    };

    view! {
        <table class="min-w-full border border-gray-300">
            <thead>
                <tr>
                    <For
                        each=move || header.clone()
                        key=|h| h.clone()
                        children=move |col| view! {
                            <th class="px-2 py-1 border-b">{col}</th>
                        }
                    />
                </tr>
            </thead>
            <tbody>
                <For
                    each=rows_source
                    key=|row| row.join(",")
                    children=move |row| view! {
                        <tr>
                            <For
                                each=move || row.clone()
                                key=|cell| cell.clone()
                                children=move |cell| view! {
                                    <td class="px-2 py-1 border-b">{cell}</td>
                                }
                            />
                        </tr>
                    }
                />
            </tbody>
        </table>
    }
}

pub async fn write_to_clipboard(text: &str) -> Result<(), JsValue> {
    let window = window().ok_or_else(|| JsValue::from_str("No global `window` exists"))?;
    let navigator = window.navigator();
    let clipboard = navigator.clipboard();
    let promise = clipboard.write_text(text);
    JsFuture::from(promise).await?;
    Ok(())
}
