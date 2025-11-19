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
        
        </div>
    }
}
