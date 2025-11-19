use std::{env as std_env, sync::LazyLock};

use dotenvy::dotenv;

pub mod env {
    pub const TABLE_PATH_ENV_VAR: &str = "TABLE_PATH";
    pub const TABLE_NAME_ENV_VAR: &str = "TABLE_NAME";
    pub const REQ_ID_ENV_VAR: &str = "REQUEST_ID"; 
    pub const QUERY_ENV_VAR: &str = "QUERY";
}

// pub const BUCKET_DATA: &str = "test-data-platform-eu-central-1";
// pub const PREFIX_DATA: &str = "dev/data-lake/backend-fusion/data/";
pub const BUCKET_TARGET: &str = "test-data-platform-eu-central-1";
pub const PREFIX_TARGET: &str = "dev/data-lake/backend-fusion/result/";
pub const REGION: &str = "eu-central-1";
pub const TABLE: &str = "images";
pub const URL: &str = "localhost:50050";
pub const CHUNK_SIZE: u64 = 10_000_000; // 10 MiB
pub const PARALLEL_THRESHOLD: u64 = 300_000_000; // 300 MiB
pub const CHUNKS_WORKERS: usize = 10; // max workers chunks for file
pub const MAX_ATTEMPTS: usize = 5;
pub const AWS_MAX_RETRIES: u32 = 10;
pub const MAX_CHUNKS: u64 = 10_000; // 10 GiB
pub const CHUNKS_MAX_RETRY: u64 = 5; // max retry for chunk

pub static TABLE_PATH: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::TABLE_PATH_ENV_VAR)
        .expect("TABLE_PATH_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("TABLE_PATH_ENV_VAR must not be empty.");
    }
    secret
});

pub static TABLE_NAME: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::TABLE_NAME_ENV_VAR)
        .expect("TABLE_NAME_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("TABLE_NAME_ENV_VAR must not be empty.");
    }
    secret
});

pub static REQUEST_ID: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::REQ_ID_ENV_VAR)
        .expect("REQUEST_ID must be set.");
    if secret.is_empty() {
        panic!("REQUEST_ID must not be empty.");
    }
    secret
});

pub static QUERY: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::QUERY_ENV_VAR)
        .expect("QUERY_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("QUERY_ENV_VAR must not be empty.");
    }
    secret
});
