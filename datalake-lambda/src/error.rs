use color_eyre::eyre::{Report, Result};
use http::Error as HttpError;
use lambda_runtime::Diagnostic;
use serde_json::Error as SerdeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Http error")]
    HttpError(#[from] HttpError),

    #[error("Serde error")]
    SerdeError(#[from] SerdeError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}

impl From<ApiError> for Diagnostic {
    fn from(value: ApiError) -> Diagnostic {
        Diagnostic {
            error_type: "API Error".to_string(),
            error_message: value.to_string(),
        }
    }
}

pub fn init_error_handler() -> Result<()> {
    color_eyre::install()?;
    Ok(())
}
