use thiserror::Error;
use url::Url;

#[derive(Debug, Error, PartialEq)]
pub enum PathParserError {
    #[error("Invalid S3 path: must start with s3://")]
    InvalidScheme,

    #[error("Invalid S3 path: missing bucket name")]
    MissingBucket,

    #[error("Invalid S3 path: missing table name")]
    MissingTableName,

    #[error("Failed to parse S3 path: {0}")]
    ParseError(String),
}

#[derive(Debug)]
pub struct ParseredTablePath(Url);

impl ParseredTablePath {
    pub fn new(path: &str) -> Result<Self, PathParserError> {
        let cleaned = path.trim_matches(&['\'', '"'][..]).trim();
        let url = Url::parse(cleaned)
            .map_err(|e| PathParserError::ParseError(e.to_string()))?;

        if url.scheme() != "s3" {
            return Err(PathParserError::InvalidScheme);
        }
        if url.host_str().is_none() {
            return Err(PathParserError::MissingBucket);
        }
        Ok(Self(url))
    }
}

impl AsRef<str> for ParseredTablePath {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl ParseredTablePath {
    pub fn extract_table_name(&self) -> Result<String, PathParserError> {
        let url = &self.0;
        let key = url.path().trim_matches('/');
        let bucket = url.host_str().ok_or(PathParserError::MissingBucket)?;
        let table_name = if key.is_empty() {
            // no path -> use bucket name instead
            bucket
        } else {
            key.split('/').next_back().ok_or(PathParserError::MissingTableName)?
        };
        Ok(table_name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("'s3://bucket/path-to-data/'", Ok("s3://bucket/path-to-data/"))]
    #[case("'s3://bucket/path-to-data'", Ok("s3://bucket/path-to-data"))]
    #[case("s3://bucket/path-to-data/", Ok("s3://bucket/path-to-data/"))]
    #[case("s3://bucket/path-to-data", Ok("s3://bucket/path-to-data"))]
    #[case("s3://path-to-data/", Ok("s3://path-to-data/"))]
    #[case("s3://path-to-data", Ok("s3://path-to-data"))]
    #[case("s3://", Err(PathParserError::MissingBucket))]
    #[case("s3:/", Err(PathParserError::MissingBucket))]
    #[case("s3:", Err(PathParserError::MissingBucket))]
    #[case("s3", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    #[case("", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    #[case("foo", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    fn validate_table_path_test(
        #[case] input: &str,
        #[case] expected: Result<&str, PathParserError>,
    ) {
        let result = ParseredTablePath::new(input);

        match (result, expected) {
            (Ok(parsed), Ok(expected_url)) => {
                assert_eq!(parsed.as_ref(), expected_url);
            }
            (Err(err), Err(expected_err)) => {
                assert_eq!(err, expected_err);
            }
            (other_result, other_expected) => {
                panic!("Mismatch: got {:?}, expected {:?}", other_result, other_expected);
            }
        }
    }

    #[rstest]
    #[case("'s3://bucket/path-to-data/'", Ok("path-to-data".to_string()))]
    #[case("'s3://bucket/path-to-data'", Ok("path-to-data".to_string()))]
    #[case("s3://bucket/path-to-data/", Ok("path-to-data".to_string()))]
    #[case("s3://bucket/path-to-data", Ok("path-to-data".to_string()))]
    #[case("s3://path-to-data/", Ok("path-to-data".to_string()))]
    #[case("s3://path-to-data", Ok("path-to-data".to_string()))]
    #[case("s3://", Err(PathParserError::MissingBucket))]
    #[case("s3:/", Err(PathParserError::MissingBucket))]
    #[case("s3:", Err(PathParserError::MissingBucket))]
    #[case("s3", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    #[case("", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    #[case("foo", Err(PathParserError::ParseError("relative URL without a base".to_string())))]
    fn extract_table_name_test(
        #[case] input: &str,
        #[case] expected: Result<String, PathParserError>,
    ) {
        let validated = ParseredTablePath::new(input);
        let result = match validated {
            Ok(valid) => valid.extract_table_name(),
            Err(e) => Err(e),
        };
        assert_eq!(result, expected);
    }
}
