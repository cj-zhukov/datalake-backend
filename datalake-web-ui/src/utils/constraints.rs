pub const URL: &str = "https://wg4w0o8cad.execute-api.eu-central-1.amazonaws.com/test/";
pub const ZIP_NAME: &str = "download.parquet";
pub const QUERY_EXAMPLES: &[(&str, &str)] = &[
    ("Basic Usage #1", 
    r#"
    select * from object_store"#),
    ("Basic Usage #2", 
    r#"
    select * from object_store 
    where data_type = 'rnd' 
    limit 10"#),
    ("Files from 2021 (by prefix)", 
    r#"
    select * from object_store 
    where dt like '2021%' 
    limit 10"#),
    ("Files from 2021 (by date range)", 
    r#"
    select * from object_store 
    where cast(dt AS date) between '2021-01-01' and '2021-12-01' 
    limit 10"#),
    ("Filter by file_type and order_id", 
    r#"
    select * from object_store 
    where file_type in ('itr', 'niri', '3dm') 
    and order_id = '2024-3-28T10-38-34-025' 
    limit 10"#),
    ("Exclude by file name", 
    r#"
    select * from object_store 
    where file_name not in ('foo', 'bar', 'baz') 
    and order_id = '2024-3-28T10-38-34-025' 
    limit 10"#),
    ("Biggest niri files from 2022", 
    r#"
    select * from object_store 
    where dt like '2022%' 
    and file_type = 'niri'
    order by file_size desc 
    limit 10"#),
];
