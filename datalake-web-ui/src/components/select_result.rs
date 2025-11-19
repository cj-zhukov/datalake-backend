use leptos::{prelude::*, reactive::spawn_local};
use serde_json::Value;
use gloo_net::http::Request;
use leptos::logging::log;

#[component]
pub fn SelectResult(url: ReadSignal<Option<String>>) -> impl IntoView {
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
                            return;
                        }
                    },
                    Err(e) => {
                        set_error.set(Some(format!("Network error: {e}")));
                        set_loading.set(false);
                        return;
                    }
                };

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
                            return;
                        }
                        Err(e) => {
                            set_error.set(Some(format!(
                                "JSON parse error on line {}: {}",
                                i + 1,
                                e
                            )));
                            set_loading.set(false);
                            return;
                        }
                    }
                }

                set_rows.set(parsed_rows);
                set_loading.set(false);
            });
        }
    });

    // Headers from the first object
    let headers = move || {
        rows.get()
            .first()
            .map(|row| row.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };

    view! {
        <div style="width: 100%; max-width: 1200px; margin-top: 2rem;">

            <Show when=move || loading.get()>
                <p>"Loading table..."</p>
            </Show>

            <Show when=move || error.get().is_some()>
                <div style="color: red;">{move || error.get().unwrap()}</div>
            </Show>

            <Show when=move || !rows.get().is_empty()>
                <table style="
                    border-collapse: collapse;
                    width: 100%;
                    font-size: 14px;
                    border: 1px solid #ccc;
                ">
                    <thead style="background: #f3f3f3;">
                        <tr>
                            {move || headers().into_iter().map(|h| view! {
                                <th style="padding: 8px; border: 1px solid #ccc;">{h}</th>
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
            </Show>

        </div>
    }
}

// #[component]
// pub fn SelectResult(url: ReadSignal<Option<String>>) -> impl IntoView {
//     // Internal state
//     let (rows, set_rows) = signal(Vec::<serde_json::Map<String, Value>>::new());
//     let (error, set_error) = signal(None::<String>);
//     let (loading, set_loading) = signal(false);

//     // Whenever URL changes → fetch JSON
//     create_effect(move |_| {
//         if let Some(url) = url.get() {
//             log!("Fetching table-data JSON: {}", url);
//             set_loading.set(true);
//             set_error.set(None);

//             spawn_local(async move {
//                 match Request::get(&url).send().await {
//                     Ok(resp) => match resp.json::<Value>().await {
//                         Ok(value) => {
//                             // Expect array of objects (rows)
//                             let arr = value.as_array().unwrap();
//                             let mapped: Vec<_> = arr
//                                 .iter()
//                                 .filter_map(|v| v.as_object().cloned())
//                                 .collect();

//                             set_rows.set(mapped);
//                             set_loading.set(false);
//                         }
//                         Err(e) => {
//                             set_error.set(Some(format!("JSON Parse error: {e}")));
//                             set_loading.set(false);
//                         }
//                     },
//                     Err(e) => {
//                         set_error.set(Some(format!("Network error: {e}")));
//                         set_loading.set(false);
//                     }
//                 }
//             });
//         }
//     });

//     // Extract headers dynamically
//     let headers = move || {
//         rows.get()
//             .first()
//             .map(|row| row.keys().cloned().collect::<Vec<_>>())
//             .unwrap_or_default()
//     };

//     view! {
//         <div style="width: 100%; max-width: 1200px; margin-top: 2rem;">
//             <Show when=move || loading.get()>
//                 <p>"Loading table..."</p>
//             </Show>

//             <Show when=move || error.get().is_some()>
//                 <div style="color: red;">
//                     {move || error.get().unwrap()}
//                 </div>
//             </Show>

//             <Show when=move || !rows.get().is_empty()>
//                 <table style="
//                     border-collapse: collapse; 
//                     width: 100%; 
//                     font-size: 14px; 
//                     border: 1px solid #ccc;
//                 ">
//                     <thead style="background: #f3f3f3;">
//                         <tr>
//                             {move || headers().into_iter().map(|h| {
//                                 view! {
//                                     <th style="padding: 8px; border: 1px solid #ccc;">
//                                         {h}
//                                     </th>
//                                 }
//                             }).collect_view()}
//                         </tr>
//                     </thead>

//                     <tbody>
//                         {move || rows.get().into_iter().map(|row| {
//                             let headers_local = headers();
//                             view! {
//                                 <tr>
//                                     {headers_local.into_iter().map(|col| {
//                                         let val = row.get(&col)
//                                             .map(|v| v.to_string())
//                                             .unwrap_or_default();
//                                         view! {
//                                             <td style="padding: 6px; border: 1px solid #ddd;">
//                                                 {val}
//                                             </td>
//                                         }
//                                     }).collect_view()}
//                                 </tr>
//                             }
//                         }).collect_view()}
//                     </tbody>
//                 </table>
//             </Show>
//         </div>
//     }
// }
