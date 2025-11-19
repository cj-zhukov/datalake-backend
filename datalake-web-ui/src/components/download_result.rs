use leptos::prelude::*;

#[component]
pub fn DownloadResult(
    download_data: impl Fn(web_sys::MouseEvent) + 'static + Clone,
    url: ReadSignal<Option<String>>,
) -> impl IntoView {
    let (clicked, set_clicked) = signal(false);

    // Reset `clicked` when new URL is available
    Effect::new(move |_| {
        if url.get().is_some() {
            set_clicked.set(false);
        }
    });

    let on_click = {
        let download_data = download_data.clone();
        move |ev: web_sys::MouseEvent| {
            if !clicked.get() {
                set_clicked.set(true);
                download_data(ev);
            }
        }
    };

    view! {
        <div style="width: 600px; text-align: center;">
            <button
                style="font-size: 1rem; padding: 0.5rem 1rem;"
                on:click=on_click
                disabled=move || url.get().is_none() || clicked.get()
            >
                "Download Data"
            </button>
        </div>
    }
}
