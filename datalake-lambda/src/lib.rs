pub mod error;
pub mod routes;
pub mod utils;

use std::time::Instant;
use std::{collections::HashMap, sync::Arc};

use aws_sdk_s3::Client;
use http::Response;
use lambda_runtime::LambdaEvent;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::routes::query;
use crate::routes::route::ApiRoute;
use crate::utils::pathparser::ParseredTablePath;
use crate::utils::queryparser::{prepare_query, replace_table_name};

pub enum ApiResponseKind {
    Ok(Option<String>),
    NotFound,
    BadRequest,
}

#[derive(Deserialize, Debug)]
pub struct ApiRequest {
    #[serde(rename = "httpMethod")]
    pub method: String,
    pub path: String,
    pub body: String,
    #[serde(rename = "requestContext")]
    pub request_context: RequestContext,
}

#[derive(Deserialize, Debug)]
pub struct RequestContext {
    pub identity: Identity,
}

#[derive(Deserialize, Debug)]
pub struct Identity {
    #[serde(rename = "sourceIp")]
    pub source_ip: Option<String>,
    #[serde(rename = "userAgent")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse {
    #[serde(rename = "statusCode")]
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

impl ApiResponse {
    fn new(response: Response<Option<String>>) -> Self {
        let status = response.status().as_u16();
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Access-Control-Allow-Origin".to_string(), "*".to_string());
        headers.insert("Access-Control-Allow-Headers".to_string(), "*".to_string());
        headers.insert(
            "Access-Control-Allow-Methods".to_string(),
            "POST, GET, OPTIONS".to_string(),
        );
        let body = response.body().to_owned();
        Self {
            status,
            headers,
            body,
        }
    }
}

impl TryFrom<ApiResponseKind> for ApiResponse {
    type Error = ApiError;

    fn try_from(kind: ApiResponseKind) -> Result<Self, Self::Error> {
        let response = match kind {
            ApiResponseKind::NotFound => Response::builder().status(404).body(None)?,
            ApiResponseKind::BadRequest => Response::builder().status(400).body(None)?,
            ApiResponseKind::Ok(body) => Response::builder().status(200).body(body)?,
        };
        Ok(ApiResponse::new(response))
    }
}

#[derive(Deserialize, Debug)]
struct Query {
    pub query: String,
}

pub struct AppState {
    pub client: Client,
}

pub async fn handler(
    event: LambdaEvent<ApiRequest>,
    state: Arc<AppState>,
) -> Result<ApiResponse, ApiError> {
    let start = Instant::now();
    let (request, context) = event.into_parts();
    let method = request.method;
    let path = request.path;
    let body = request.body;
    let request_id = context.request_id;
    let user_ip = request.request_context.identity.source_ip;
    let user_agent = request.request_context.identity.user_agent;
    tracing::info!({ user_ip, user_agent, path, method, query = %body }, "starting handler");

    let route: ApiRoute = match (method.as_str(), path.as_str()).try_into() {
        Ok(route) => route,
        Err(e) => {
            tracing::error!("{e}, query: {body}");
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    let (query, table_path) = match serde_json::from_str::<Query>(&body) {
        Ok(query) => {
            match prepare_query(&query.query) {
                Ok(query) => (query.query, query.table_name),
                Err(e) => {
                    tracing::error!("{e}, query: {body}");
                    return ApiResponseKind::BadRequest.try_into();
                }
            }
        }
        Err(e) => {
            tracing::error!("{e}, query: {body}");
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    let table_path = match ParseredTablePath::new(&table_path) {
        Ok(name) => name,
        Err(e) => {
            tracing::error!("{e}, query: {body}");
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    // #TODO check if s3 path contains parquet files?

    let table_name = match &table_path.extract_table_name() {
        Ok(name) => name.to_string(),
        Err(e) => {
            tracing::error!("{e}, query: {body}");
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    // replace s3 path in query with table name
    let query = replace_table_name(&query, &table_name);

    tracing::info!({ query, table_name, table_path = %table_path.as_ref() }, "processing query");

    let response = match route {
        ApiRoute::QueryPost => {
            query::post_query(&state.client, &request_id, &query, table_path.as_ref(), &table_name).await?
        }
    };

    let exec_time = start.elapsed().as_secs();
    tracing::info!({ duration = %exec_time }, "finishing handler");
    Ok(response)
}
