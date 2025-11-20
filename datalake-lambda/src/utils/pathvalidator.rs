use aws_sdk_s3::Client;

use crate::utils::{error::UtilsError, pathparser::ParseredTablePath};

pub async fn path_validator(path: &ParseredTablePath, client: &Client) -> Result<bool, UtilsError> {
    let bucket = &path.bucket;
    if client.head_bucket().bucket(bucket).send().await.is_err() {
        return Ok(false);
    }

    let Some(prefix) = &path.prefix else {
        return Ok(true);
    };

    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send()
        .await?;

    let objs = resp.contents();
    Ok(!objs.is_empty())
}
