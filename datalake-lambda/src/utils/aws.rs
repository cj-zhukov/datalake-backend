use std::time::Duration;

use aws_config::{BehaviorVersion, Region, retry::RetryConfig, timeout::TimeoutConfig};
use aws_sdk_ecs::Client as ECSClient;
use aws_sdk_ecs::operation::run_task::RunTaskOutput;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
    NetworkConfiguration, TaskOverride,
};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::{Client, operation::get_object::GetObjectOutput};

use crate::utils::error::UtilsError;

pub async fn get_aws_client(region: String) -> Client {
    let region = Region::new(region);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let timeout = TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(60 * 5))
        .operation_attempt_timeout(Duration::from_secs(60 * 5))
        .connect_timeout(Duration::from_secs(60 * 5))
        .build();
    let config_builder = Builder::from(&sdk_config)
        .timeout_config(timeout)
        .retry_config(RetryConfig::standard().with_max_attempts(10));
    let config = config_builder.build();
    Client::from_conf(config)
}

pub async fn get_ecs_client(region: String) -> ECSClient {
    let region = Region::new(region);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    ECSClient::new(&sdk_config)
}

pub async fn get_aws_object(
    client: Client,
    bucket: &str,
    key: &str,
) -> Result<GetObjectOutput, UtilsError> {
    let req = client.get_object().bucket(bucket).key(key);

    let res = req.send().await?;

    Ok(res)
}

pub async fn run_ecs_task(
    client: &ECSClient,
    cluster: &str,
    task_definition: &str,
    container: &str,
    subnets: Option<Vec<String>>,
    security_groups: Option<Vec<String>>,
    request_id: &str,
    query: &str,
    table_path: &str,
    table_name: &str,
) -> Result<RunTaskOutput, UtilsError> {
    let env_vars = vec![
        KeyValuePair::builder()
            .name("REQUEST_ID")
            .value(request_id)
            .build(),
        KeyValuePair::builder()
            .name("QUERY")
            .value(query)
            .build(),
        KeyValuePair::builder()
            .name("TABLE_PATH")
            .value(table_path)
            .build(),
        KeyValuePair::builder()
            .name("TABLE_NAME")
            .value(table_name)
            .build(),
    ];
    let overrides = TaskOverride::builder()
        .container_overrides(
            ContainerOverride::builder()
                .name(container)
                .set_environment(Some(env_vars))
                .build(),
        )
        .build();

    let network_configuration = NetworkConfiguration::builder()
        .awsvpc_configuration(
            AwsVpcConfiguration::builder()
                .set_subnets(subnets)
                .set_security_groups(security_groups)
                .assign_public_ip(AssignPublicIp::Disabled)
                .build()?,
        )
        .build();

    let run_task_builder = client.run_task();
    let run_task_builder = run_task_builder
        .cluster(cluster)
        .task_definition(task_definition)
        .launch_type(LaunchType::Fargate)
        .network_configuration(network_configuration)
        .overrides(overrides);

    let output = run_task_builder.send().await?;
    Ok(output)
}

// pub async fn write_df_to_s3(
//     client: &Client,
//     bucket: &str,
//     key: &str,
//     df: DataFrame,
// ) -> Result<(), UtilsError> {
//     let mut buf = vec![];
//     let schema = Schema::from(df.schema());
//     let mut stream = df.execute_stream().await?;
//     let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
//     while let Some(batch) = stream.next().await.transpose()? {
//         writer.write(&batch).await?;
//     }
//     writer.close().await?;

//     let _resp = client
//         .put_object()
//         .bucket(bucket)
//         .key(key)
//         .body(buf.into())
//         .send()
//         .await?;
//     Ok(())
// }
