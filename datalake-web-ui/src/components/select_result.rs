use leptos::{prelude::*, reactive::spawn_local};
use serde_json::Value;
use gloo_net::http::Request;
use leptos::logging::log;

#[component]
pub fn SelectResult(
    url: ReadSignal<Option<String>>,
    set_is_loading_global: WriteSignal<bool>,
) -> impl IntoView {
    let (rows, set_rows) = signal(Vec::<serde_json::Map<String, Value>>::new());
    let (error, set_error) = signal(None::<String>);
    let (loading, set_loading) = signal(false);

    Effect::new(move |_| {
        if let Some(url) = url.get() {
            log!("Fetching table-data NDJSON: {}", url);

            set_loading.set(true);
            set_error.set(None);

            spawn_local(async move {
                let text = match Request::get(&url).send().await {
                    Ok(resp) => match resp.text().await {
                        Ok(t) => {
                            log!("RAW RESPONSE: {:?}", t);
                            t
                        }
                        Err(e) => {
                            set_error.set(Some(format!("Failed reading body: {e}")));
                            set_loading.set(false);
                            set_is_loading_global.set(false);   
                            return;
                        }
                    },
                    Err(e) => {
                        set_error.set(Some(format!("Network error: {e}")));
                        set_loading.set(false);
                        set_is_loading_global.set(false);      
                        return;
                    }
                };

                // Parse NDJSON lines
                let mut parsed_rows = Vec::new();
                for (i, line) in text.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(Value::Object(obj)) => {
                            parsed_rows.push(obj);
                        }
                        Ok(_) => {
                            set_error.set(Some(format!(
                                "Line {} is not a JSON object",
                                i + 1
                            )));
                            set_loading.set(false);
                            set_is_loading_global.set(false);   
                            return;
                        }
                        Err(e) => {
                            set_error.set(Some(format!(
                                "JSON parse error on line {}: {}",
                                i + 1,
                                e
                            )));
                            set_loading.set(false);
                            set_is_loading_global.set(false);   
                            return;
                        }
                    }
                }

                set_rows.set(parsed_rows);
                set_loading.set(false);
                set_is_loading_global.set(false);
            });
        }
    });

    // Extract headers from the first row
    let headers = move || {
        rows.get()
            .first()
            .map(|row| row.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };

    view! {
        <div style="
            width: 100%;
            max-width: 1200px;
            margin: 2rem auto;
            text-align: center;
        ">

            <Show when=move || loading.get()>
                <p>"Loading table..."</p>
            </Show>

            <Show when=move || error.get().is_some()>
                <div style="color: red; margin-bottom: 1rem;">
                    {move || error.get().unwrap()}
                </div>
            </Show>

            <Show when=move || !rows.get().is_empty()>
                <div style="
                    display: flex;
                    justify-content: center;
                    width: 100%;
                ">
                    <table style="
                        border-collapse: collapse;
                        margin: 0 auto;        /* <-- centers table */
                        font-size: 14px;
                        border: 1px solid #ccc;
                    ">
                        <thead style="background: #f3f3f3;">
                            <tr>
                                {move || headers().into_iter().map(|h| view! {
                                    <th style="padding: 8px; border: 1px solid #ccc;">
                                        {h}
                                    </th>
                                }).collect_view()}
                            </tr>
                        </thead>

                        <tbody>
                            {move || rows.get().into_iter().map(|row| {
                                let header_list = headers();
                                view! {
                                    <tr>
                                        {header_list.into_iter().map(|col| {
                                            let val = row.get(&col)
                                                .map(|v| v.to_string())
                                                .unwrap_or_default();
                                            view! {
                                                <td style="padding: 6px; border: 1px solid #ddd;">
                                                    {val}
                                                </td>
                                            }
                                        }).collect_view()}
                                    </tr>
                                }
                            }).collect_view()}
                        </tbody>
                    </table>
                </div>
            </Show>
        </div>
    }
}
