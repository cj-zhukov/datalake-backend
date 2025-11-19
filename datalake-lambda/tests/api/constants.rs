use std::{env as std_env, sync::LazyLock};

use dotenvy::dotenv;

pub mod env {
    pub const ADDRESS_URL_ENV_VAR: &str = "ADDRESS_URL";
}

pub static ADDRESS: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::ADDRESS_URL_ENV_VAR).expect("ADDRESS_URL_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("ADDRESS_URL_ENV_VAR must not be empty.");
    }
    secret
});