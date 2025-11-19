use crate::constants::ADDRESS;
use crate::helpers::TestApp;

use datalake_lambda::routes::query::QueryResponse;

#[tokio::test]
async fn should_return_200_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from 's3://path-to-data-exists' limit 10"), // valid query and path
    });
    let response = app.post_query(&input).await;
    assert_eq!(response.status().as_u16(), 200);

    let response = response
        .json::<QueryResponse>()
        .await
        .expect("Could not deserialize response body to Response");
    assert!(!response.result_parquet.is_empty());
    assert!(!response.result_json.is_empty());
}

#[tokio::test]
async fn should_return_400_if_invalid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("foo bar baz"), // query is not valid
    });
    let response = app.post_query(&input).await;
    assert_eq!(response.status().as_u16(), 400);
}

#[tokio::test]
async fn should_return_400_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from 's3://path-to-data-doesn't-exist' limit 10"), // invalid path
    });
    let response = app.post_query(&input).await;
    assert_eq!(response.status().as_u16(), 400); //#TODO maybe check if path-to-data exists and return 404 instead of client error?
}
