use leptos::mount::mount_to_body;
use leptos::prelude::*;

use datalake_web_ui::app::App;

fn main() {
    mount_to_body(App);
}