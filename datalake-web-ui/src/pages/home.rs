use crate::components::*;
use crate::utils::constraints::*;

use gloo_net::http::Request;
use gloo_timers::future::TimeoutFuture;
use leptos::{logging::log, prelude::*, task::spawn_local};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use web_sys::{HtmlAnchorElement, wasm_bindgen::JsCast};

#[derive(Debug, Serialize)]
struct ApiRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    pub _result_parquet: Value, //#TODO
    pub result_json: Value,
}

#[component]
pub fn Home() -> impl IntoView {
    let (query, set_query) = signal("select * from 's3://bucket/path-to-data/' limit 1000".to_string());
    let (mode, set_mode) = signal(Mode::Select); // mode
    let (is_loading, set_is_loading) = signal(false); // spinner
    let (_result, set_result) = signal(None::<String>); // select result tabular format from select opeation
    let (url, set_url) = signal(None::<String>); 
    let (error, set_error) = signal(None::<String>); // error msg

    let send_request = move |_| {
        let current_mode = mode.get();
        spawn_local(async move {
            let payload = ApiRequest {
                query: query.get_untracked(),
            };
            let endpoint = match current_mode {
                Mode::Select => format!("{URL}query"),
                Mode::Download => format!("{URL}query"),
            };
            log!(
                "Sending payload: {:?} to: {:?} with mode: {}",
                payload,
                endpoint,
                current_mode.as_ref()
            );
            set_is_loading.set(true);
            set_error.set(None);

            let response = match Request::post(&endpoint)
                .header("Content-Type", "application/json")
                .json(&payload)
            {
                Ok(req) => match req.send().await {
                    Ok(req) => req,
                    Err(e) => {
                        set_result.set(None);
                        set_error.set(Some(format!("Network error: {e}")));
                        set_is_loading.set(false);
                        return;
                    }
                },
                Err(e) => {
                    set_result.set(None);
                    set_error.set(Some(format!("Failed to build request: {e}")));
                    set_is_loading.set(false);
                    return;
                }
            };

            if !response.ok() {
                let msg = match response.status() {
                    400 => "Invalid user input".to_string(),
                    404 => "No data found".to_string(),
                    500 => "Internal server error".to_string(),
                    _ => format!("Error {} occurred", response.status()),
                };
                set_result.set(None);
                set_error.set(Some(msg));
                set_is_loading.set(false);
                return;
            }

            match current_mode {
                Mode::Select => {
                    match response.json::<ApiResponse>().await {
                        Ok(resp) => {
                            let presigned_url = resp
                                .result_json
                                .as_str()
                                .expect("result_json is not a string")
                                .to_string();

                            log!("Presigned url received: {}", presigned_url);
                            log!("Start polling");

                            spawn_local(async move {
                                loop {
                                    let Ok(resp) = Request::get(&presigned_url).send().await else {
                                        log!("Still waiting...");
                                        TimeoutFuture::new(1000).await;
                                        continue;
                                    };

                                    if resp.ok() {
                                        log!("Presigned URL is ready");
                                        set_url.set(Some(presigned_url));
                                        break;
                                    }
                                }
                            });
                        },
                        Err(e) => {
                            set_result.set(None);
                            set_error.set(Some(format!("Failed to parse response: {e}")));
                        }
                    }
                }
                _ => unimplemented!()
                // Mode::Download => match response.json::<ApiResponse>().await {
                //     Ok(resp) => {
                //         let presigned_url = resp.result_parquet.to_string();
                //         log!("Presigned url received: {}", presigned_url);
                //         log!("Start polling");
                //         spawn_local(async move {
                //             loop {
                //                 let Ok(resp) = Request::get(&presigned_url).send().await else {
                //                     log!("Still waiting...");
                //                     TimeoutFuture::new(1000).await;
                //                     continue;
                //                 };
                //                 if resp.ok() {
                //                     log!("Presigned URL is ready");
                //                     set_url.set(Some(presigned_url));
                //                     set_is_loading.set(false);
                //                     break;
                //                 }
                //             }
                //         });
                //     }
                //     Err(e) => {
                //         set_result.set(None);
                //         set_error.set(Some(format!("Failed to parse response: {e}")));
                //         set_is_loading.set(false);
                //     }
                // },
            }
        });
    };

    let download_data = move |_| {
        if let Some(url) = url.get_untracked() {
            if let Some(document) = window().document() {
                match document.create_element("a") {
                    Ok(element) => {
                        if let Ok(anchor) = element.dyn_into::<HtmlAnchorElement>() {
                            anchor.set_href(&url);
                            anchor.set_download(ZIP_NAME);
                            if anchor.set_attribute("download", ZIP_NAME).is_err() {
                                set_error.set(Some("Failed to set download attribute".to_string()));
                                return;
                            }
                            match document.body() {
                                Some(body) => {
                                    if body.append_child(&anchor).is_err() {
                                        set_error.set(Some(
                                            "Failed to append anchor to body".to_string(),
                                        ));
                                        return;
                                    }
                                    anchor.click();
                                    if body.remove_child(&anchor).is_err() {
                                        set_error.set(Some(
                                            "Failed to remove anchor from body".to_string(),
                                        ));
                                    }
                                }
                                None => {
                                    set_error.set(Some("No <body> found in document".to_string()))
                                }
                            }
                        } else {
                            set_error.set(Some("Failed to cast element to anchor".to_string()));
                        }
                    }
                    Err(_e) => set_error.set(Some("Failed to create anchor element".to_string())),
                };
            } else {
                set_error.set(Some("Document not available".to_string()));
            }
        } else {
            set_error.set(Some("Document not available".to_string()));
        }
    };

    view! {
        // spinner
        <Spinner visible=is_loading />
        
        <div style="display: flex; flex-direction: column; align-items: center; gap: 1rem;">
            <h1>"Datalake Web UI"</h1>

            // input query form
            <QueryEditor query=query set_query=set_query />

            // error msg
            <ErrorMessage error=error />

            // operation type
            <OperationPanel
                mode=mode
                set_mode=set_mode
                send_request=send_request
                is_loading=is_loading
                modes=vec![Mode::Select, Mode::Download]
            />
            
            // select result
            <Show when=move || mode.get() == Mode::Select>
                <SelectResult url=url set_is_loading_global=set_is_loading />
            </Show>

            // download result
            <Show when=move || mode.get() == Mode::Download>
                <DownloadResult download_data=download_data url=url />
            </Show>
        </div>
    }
}
