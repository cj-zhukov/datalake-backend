use std::sync::Arc;

use lambda_runtime::{Error, run, service_fn};

use datalake_lambda::{
    AppState,
    error::init_error_handler,
    handler,
    utils::{aws::get_aws_client, constants::REGION, tracing::init_tracing},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_error_handler()?;
    init_tracing();

    let client = get_aws_client(REGION.to_string()).await;
    let app_state = Arc::new(AppState { client });

    run(service_fn(|event| async {
        handler(event, app_state.clone()).await.map_err(|err| {
            tracing::error!(?err, "lambda handler failed");
            err
        })
    }))
    .await?;

    Ok(())
}
