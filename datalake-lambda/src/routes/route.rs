#[derive(Debug, PartialEq)]
pub enum ApiRoute {
    QueryPost,
}

impl TryFrom<(&str, &str)> for ApiRoute {
    type Error = String;

    fn try_from((method, path): (&str, &str)) -> Result<Self, Self::Error> {
        match (method, path) {
            ("POST", "/query") => Ok(ApiRoute::QueryPost),
            _ => Err(format!(
                "unsupported resource method: {method}, path: {path}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[test]
    #[case(("POST", "/query"), Ok(ApiRoute::QueryPost))]
    #[case(("foo", "/foo"), Err(format!("unsupported resource method: foo, path: /foo")))]
    #[case(("", "/"), Err(format!("unsupported resource method: , path: /")))]
    fn test_api_route(#[case] input: (&str, &str), #[case] expected: Result<ApiRoute, String>) {
        let res = input.try_into();
        assert_eq!(res, expected);
    }
}
