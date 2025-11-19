use std::sync::Arc;

use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    Client, config::Builder,primitives::ByteStream, types::{CompletedMultipartUpload, CompletedPart}
};
use bytes::Bytes;
use color_eyre::Result;
use datafusion::{arrow::datatypes::Schema, parquet::arrow::AsyncArrowWriter, prelude::DataFrame};
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tokio::task::JoinSet;

use crate::utils::constants::*;

pub async fn get_aws_client(region: String) -> Client {
    let region = Region::new(region);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let config_builder = Builder::from(&sdk_config)
        .retry_config(RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES));
    let config = config_builder.build();
    Client::from_conf(config)
}

/// Write dataframe to aws s3 by chunk
pub async fn write_df_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    df: DataFrame,
) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res
        .upload_id()
        .unwrap();
        // .ok_or_else(|| UtilsError::UnexpectedError(Report::msg("missing upload_id")))?;

    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let mut stream = ByteStream::from(buf);
    let mut part_number = 1;

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(ByteStream::from(bytes))
            .part_number(part_number)
            .send()
            .await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;

    Ok(())
}

/// Write big dataframe to aws s3 by chunk
pub async fn write_big_df_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    df: DataFrame,
    max_workers: usize,
) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res
        .upload_id()
        .unwrap();
        // .ok_or_else(|| UtilsError::UnexpectedError(Report::msg("missing upload_id")))?;

    let parts: Vec<(usize, Bytes)> = buf
        .chunks(CHUNK_SIZE as usize)
        .enumerate()
        .map(|(i, chunk)| (i + 1, Bytes::copy_from_slice(chunk)))
        .collect();

    let semaphore = Arc::new(Semaphore::new(max_workers));
    let mut tasks = JoinSet::new();

    for (part_number, chunk) in parts {
        let client = client.clone();
        let upload_id = upload_id.to_string();
        let key = key.to_string();
        let bucket = bucket.to_string();
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await?;
            // .map_err(|e| UtilsError::UnexpectedError(e.into()))?;

        tasks.spawn(async move {
            let res = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id.clone())
                .part_number(part_number as i32)
                .body(ByteStream::from(chunk.clone()))
                .send()
                .await?;

            drop(permit);

            Ok::<CompletedPart, aws_sdk_s3::Error>(
                CompletedPart::builder()
                    .e_tag(res.e_tag().unwrap_or_default())
                    .part_number(part_number as i32)
                    .build(),
            )
        });
    }

    let mut completed_parts = Vec::new();
    while let Some(res) = tasks.join_next().await {
        let part = res??;
            // .map_err(|e| UtilsError::UnexpectedError(e.into()))?
            // .map_err(|e| UtilsError::UnexpectedError(e.into()))?;
        completed_parts.push(part);
    }

    completed_parts.sort_by_key(|part| part.part_number());
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    Ok(())
}