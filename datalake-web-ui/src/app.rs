use crate::pages::*;

use leptos::prelude::*;
use leptos_router::{
    components::{A, Route, Router, Routes},
    path,
};

#[component]
pub fn App() -> impl IntoView {
    view! {
        <Router>
            <nav style="margin-bottom: 1rem;">
                <span style="margin-right: 1rem;">
                    <A href="/">"Home"</A>
                </span>
                <span style="margin-right: 1rem;">
                    <A href="/docs">"Docs"</A>
                </span>
            </nav>
            <main>
                <Routes transition=true fallback=|| "This page could not be found.">
                    <Route path=path!("") view=Home />
                    <Route path=path!("docs") view=Docs />
                </Routes>
            </main>
        </Router>
    }
}
