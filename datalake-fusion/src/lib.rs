pub mod utils;

use color_eyre::Result;
use datafusion::{dataframe::DataFrameWriteOptions, prelude::SessionContext};

use crate::utils::constants::*;

pub async fn handler(
    ctx: SessionContext,
    table_path: String, 
    table_name: String,
    request_id: String, 
    query: String,
) -> Result<()> {
    dbg!("registering data path");
    ctx.register_parquet(
        table_name,
        table_path,
        Default::default(),
    )
    .await?;

    let write_dir_path = &format!("s3://{BUCKET_TARGET}/{PREFIX_TARGET}{request_id}.parquet");
    let write_dir_path2 = &format!("s3://{BUCKET_TARGET}/{PREFIX_TARGET}{request_id}.json");

    dbg!("running task");
    let df = ctx.sql(&query).await?;
    df.clone().write_json(write_dir_path2, DataFrameWriteOptions::default(), None).await?;    
    df.write_parquet(write_dir_path, Default::default(), Default::default()).await?;

    Ok(())
}