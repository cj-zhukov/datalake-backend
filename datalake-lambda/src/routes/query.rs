use std::time::Duration;

use aws_sdk_s3::{Client, presigning::PresigningConfig};
use serde::{Deserialize, Serialize};

use crate::{
    ApiResponse, ApiResponseKind,
    error::ApiError,
    utils::{
        aws::{get_ecs_client, run_ecs_task},
        constants::*,
    },
};

#[derive(Deserialize, Serialize, Debug)]
pub struct QueryResponse {
    pub result_parquet: String, // parqet url 
    pub result_json: String, // json url (for visualization for web ui) 
}

#[tracing::instrument(level = "info", name = "query", skip(client))]
pub async fn post_query(
    client: &Client,
    request_id: &str,
    query: &str,
    table_path: &str,
    table_name: &str,
) -> Result<ApiResponse, ApiError> {
    // prepare parquet file
    let key_parquet = format!("{DATA_PREFIX}{request_id}.parquet"); 
    tracing::info!("creating presigned object for key: {}", key_parquet);
    let get_object_request1 = client
        .get_object()
        .bucket(DATA_BUCKET)
        .key(&key_parquet)
        .response_content_type("application/parquet") // for browser
        .response_content_disposition("attachment; filename=\"download.parquet\""); // for browser

    let presigning_config1 = PresigningConfig::builder()
        .expires_in(Duration::from_secs(PRESIGNED_TIMEOUT))
        .build()
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let presigned_url1 = get_object_request1
        .presigned(presigning_config1.clone())
        .await
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    // prepare json file
    let key_json = format!("{DATA_PREFIX}{request_id}.json"); 
    tracing::info!("creating presigned object for key: {}", key_json);
    let get_object_request2 = client
        .get_object()
        .bucket(DATA_BUCKET)
        .key(&key_json)
        .response_content_type("application/json") // for browser
        .response_content_disposition("attachment; filename=\"download.json\""); // for browser

    let presigning_config2 = PresigningConfig::builder()
        .expires_in(Duration::from_secs(PRESIGNED_TIMEOUT))
        .build()
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let presigned_url2 = get_object_request2
        .presigned(presigning_config2.clone())
        .await
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let resp = QueryResponse {
        result_parquet: presigned_url1.uri().to_string(),
        result_json: presigned_url2.uri().to_string(),
    };
    let body = serde_json::to_string(&resp)?;

    // pass request_id & query to ecs task and start the task
    let ecs_client = get_ecs_client(REGION.to_string()).await;
    let subnets = SUBNETS.iter().map(|x| x.to_string()).collect();
    let security_groups = SECURITY_GROUPS.iter().map(|x| x.to_string()).collect();
    let _output = run_ecs_task(
        &ecs_client,
        CLUSTER,
        TASK_NAME,
        CONTAINER_NAME,
        Some(subnets),
        Some(security_groups),
        request_id,
        query,
        table_path,
        table_name,
    )
    .await
    .map_err(|e| ApiError::UnexpectedError(e.into()))?; //#TODO process output of ecs
    tracing::info!("starting ecs task");

    let response = ApiResponseKind::Ok(Some(body)).try_into()?;

    Ok(response)
}
