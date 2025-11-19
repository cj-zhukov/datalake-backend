pub const URL: &str = "path-to-api"; // #TODO set as env var
pub const ZIP_NAME: &str = "download.parquet";
pub const QUERY_EXAMPLES: &[(&str, &str)] = &[
    ("Basic Usage #1", 
    r#"
    select * from 's3://bucket/path-to-data/'"#),
    ("Basic Usage #2", 
    r#"
    select * from 's3://bucket/path-to-data/' 
    where data_type = 'foo' 
    limit 10"#),
    ("Files from 2021 (by date range)", 
    r#"
    select * from 's3://bucket/path-to-data/'  
    where cast(dt AS date) between '2021-01-01' and '2021-12-01' 
    limit 10"#),
    ("Filter by file_type and order_id", 
    r#"
    select * from 's3://bucket/path-to-data/'  
    where file_type in ('foo', 'bar', 'baz') 
    and or_id = 'foo-id' 
    limit 10"#),
    ("Exclude by file name", 
    r#"
    select * from 's3://bucket/path-to-data/'  
    where file_type not in ('foo', 'bar', 'baz') 
    and or_id = 'foo-id' 
    limit 10"#),
    ("Biggest foo files from 2022", 
    r#"
    select * from 's3://bucket/path-to-data/'  
    where cast(dt AS date) between '2021-01-01' and '2021-12-01' 
    and file_type = 'foo'
    order by file_size desc 
    limit 10"#),
];
