use gloo_timers::future::TimeoutFuture;
use leptos::{prelude::*, task::spawn_local};

use crate::utils::constraints::QUERY_EXAMPLES;
use crate::utils::tools::write_to_clipboard;

#[component]
fn CopyButton(#[prop(into)] text_to_copy: String) -> impl IntoView {
    let (copied, set_copy) = signal(false);

    let on_click = move |_| {
        let text = text_to_copy.clone();
        spawn_local(async move {
            if write_to_clipboard(&text).await.is_ok() {
                set_copy.set(true);
                TimeoutFuture::new(2000).await;
                set_copy.set(false);
            }
        });
    };

    view! {
        <button on:click=on_click>
            {move || if copied.get() { "Copied!" } else { "Copy" }}
        </button>
    }
}

#[component]
pub fn Docs() -> impl IntoView {
    view! {
        <div style="padding: 2rem; max-width: 800px; margin: 0 auto;">
            <h1>"📘 Documentation"</h1>

        // query Examples
            <section>
            <h2>"Example Queries"</h2>
            {
            view! {
                <>
                    {QUERY_EXAMPLES.to_vec().into_iter().map(|(title, query)| view! {
                        <div style="margin-bottom: 1.5rem; background: #f9f9f9; border: 1px solid #ddd; border-radius: 12px; padding: 1.25rem; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                            <h3>{title}</h3>
                            <pre style="background: #f0f0f0; padding: 1rem; border-radius: 8px; overflow-x: auto;"><code>{query}</code></pre>
                            <CopyButton text_to_copy=query.to_string() />
                        </div>
                    }).collect::<Vec<_>>()}
                </>
            }
        }
        </section>

        // Object Store Table Schema
        <section>
            <h2>"Object Store Schema"</h2>
            <table style="width: 100%; border-collapse: collapse; border: 1px solid #ccc;">
                <thead>
                    <tr>
                        <th style="border: 1px solid #ccc; padding: 0.5rem;">Column</th>
                        <th style="border: 1px solid #ccc; padding: 0.5rem;">Type</th>
                        <th style="border: 1px solid #ccc; padding: 0.5rem;">Description</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">file_name</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">name of the file</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">file_type</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">extension of the file</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">file_size</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">INTEGER</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">size of the file in KB</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">file_path</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">path to the file in S3</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">file_url</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">url of the file in S3</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">order_id</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">case name or order name</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">study</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">HMC | AH | angie_restorative_data_collection_study_US | 32clinic | Hadassah_scans</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">dt</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">date and time of the case</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">scanner_type</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">5D | NG</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">data_type</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">TEXT</td>
                        <td style="border: 1px solid #ccc; padding: 0.5rem;">rnd (default) | vnv</td>
                    </tr>
                </tbody>
            </table>
        </section>
        </div>
    }
}
