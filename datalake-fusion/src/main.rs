use std::time::Instant;

use awscreds::Credentials;
use color_eyre::Result;
use ballista::extension::SessionContextExt;
use ballista_core::object_store::state_with_s3_support;
use datafusion::prelude::SessionContext;

use datalake_fusion::handler;
use datalake_fusion::utils::constants::*;

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    dbg!("initing state");
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap_or_default();
    let aws_secret_access_key = creds.secret_key.unwrap_or_default();
    let aws_session_token = creds.security_token.unwrap_or_default();
    let state = state_with_s3_support()?;
    let url = format!("df://{URL}");
    dbg!(&url);
    let ctx = SessionContext::remote_with_state(&url, state).await?;
    ctx.sql(&format!("SET s3.access_key_id = '{aws_access_key_id}'")).await?;
    ctx.sql(&format!("SET s3.secret_access_key = '{aws_secret_access_key}'")).await?;
    ctx.sql(&format!("SET s3.session_token = '{aws_session_token}'")).await?;
    let table_path = TABLE_PATH.to_string();
    dbg!(&table_path);
    let table_name = TABLE_NAME.to_string();
    dbg!(&table_name);
    let query = QUERY.to_string();
    dbg!(&query);
    let request_id = REQUEST_ID.to_string();
    dbg!(&request_id);
    dbg!("starting handler");
    handler(ctx, table_path, table_name, request_id, query).await?;
    dbg!("finishing handler, elapsed: {:.2?}", now.elapsed());
    Ok(())
}
