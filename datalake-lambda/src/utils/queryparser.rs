use sqlparser::ast::{Expr, LimitClause, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use thiserror::Error;

use crate::utils::constants::MAX_ROWS;

#[derive(Debug, Error, PartialEq)]
pub enum QueryParserError {
    #[error("SQL parse error")]
    SqlParseError(#[from] ParserError),

    #[error("Invalid query: doesn't contain table name")]
    InvalidTableName,

    #[error("Select query type not found")]
    SelectQueryNotFound,

    #[error("Unsupported query type")]
    UnsupportedQueryType,
}

#[derive(Debug, PartialEq)]
pub struct QueryParsered {
    pub query: String,
    pub table_name: String,
}

/// validate the query
pub fn prepare_query(query: &str) -> Result<QueryParsered, QueryParserError> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, query)?;
    let query_stmt = ast
        .get_mut(0)
        .ok_or(QueryParserError::UnsupportedQueryType)?;
    let Statement::Query(_query) = query_stmt else {
        return Err(QueryParserError::UnsupportedQueryType);
    };
    let res = prepare_query_worker(&mut ast)?;
    Ok(res)
}

/// validate the query,
/// prepare the query by adding limit if not exists,
/// checkening table name
fn prepare_query_worker(ast: &mut [Statement]) -> Result<QueryParsered, QueryParserError> {
    if let Some(Statement::Query(query)) = ast.get_mut(0) {
        let table_name = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(from_table) = select.from.first() {
                    match &from_table.relation {
                        TableFactor::Table { name, .. } => Some(name.to_string()),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        let table_name = table_name.ok_or(QueryParserError::InvalidTableName)?;

        if let SetExpr::Select(_select) = &mut *query.body {
            // query contains limit
            if query.limit_clause.is_none() {
                query.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number(MAX_ROWS.to_string(), false).into(),
                    )),
                    offset: None,
                    limit_by: vec![],
                })
            };

            Ok(QueryParsered {
                query: ast[0].to_string(),
                table_name,
            })
        } else {
            Err(QueryParserError::SelectQueryNotFound)
        }
    } else {
        Err(QueryParserError::UnsupportedQueryType)
    }
}

pub fn replace_table_name(query: &str, table_name: &str) -> String {
    if let Some(start) = query.find("s3://") {
        let before = &query[..start];
        let quote_char = before.chars().rev().find(|&c| c == '\'' || c == '"');
        let quote_pos = quote_char
            .and_then(|q| before.rfind(q))
            .unwrap_or(0);

        let prefix = &query[..quote_pos];
        let rest = &query[start..];
        let end_quote_idx = rest
            .find(['\'', '"'])
            .map(|idx| start + idx + 1)
            .unwrap_or(query.len());

        let suffix = &query[end_quote_idx..];

        format!("{prefix}{table_name}{suffix}")
    } else {
        query.to_string()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("select * from foo", Ok(QueryParsered{ query: "SELECT * FROM foo LIMIT 1000".to_string(), table_name: "foo".to_string() }))]
    #[case("select * from 's3://bucket/path-to-data/'", Ok(QueryParsered{ query: "SELECT * FROM 's3://bucket/path-to-data/' LIMIT 1000".to_string(), table_name: "'s3://bucket/path-to-data/'".to_string() }))]
    #[case("select * from 's3://path-to-data'", Ok(QueryParsered{ query: "SELECT * FROM 's3://path-to-data' LIMIT 1000".to_string(), table_name: "'s3://path-to-data'".to_string() }))]
    #[case("select * from foo limit 10", Ok(QueryParsered{ query: "SELECT * FROM foo LIMIT 10".to_string(), table_name: "foo".to_string() }))]
    #[case("select * from", Err(QueryParserError::SqlParseError(ParserError::ParserError("Expected: identifier, found: EOF".to_string()))))]
    #[case("delete from foo", Err(QueryParserError::UnsupportedQueryType))]
    #[case(
        "update foo set data_type = 'foo' where data_type = 'rnd'",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "insert into foo(file_name) values('foo')",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case("foo bar baz", Err(QueryParserError::SqlParseError(ParserError::ParserError("Expected: an SQL statement, found: foo at Line: 1, Column: 1".to_string()))))]
    fn prepare_query_test(
        #[case] input: &str,
        #[case] expected: Result<QueryParsered, QueryParserError>,
    ) {
        assert_eq!(expected, prepare_query(input));
    }


    #[rstest]
    #[case(("select * from 's3://bucket/path/images/'", "images"), "select * from images")]
    #[case(("select * from foo", "images"), "select * from foo")]
    fn replace_table_path_test(
        #[case] input: (&str, &str),
        #[case] expected: &str,
    ) {
        assert_eq!(expected, replace_table_name(input.0, input.1));
    }
}
