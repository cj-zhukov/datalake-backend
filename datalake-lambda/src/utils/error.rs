use aws_sdk_ecs::operation::run_task::RunTaskError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use aws_smithy_types::error::operation::BuildError;
use color_eyre::eyre::Report;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("Io error")]
    IoError(#[from] IoError),

    #[error("AWS Sdk error")]
    SdkError(#[from] SdkError<GetObjectError>),

    #[error("AWS ListObjectsV2Error error")]
    ListObjectsV2Error(#[from] SdkError<ListObjectsV2Error>),

    #[error("ECS Run Task Sdk error")]
    EcsRunTaskError(#[from] SdkError<RunTaskError>),

    #[error("AWSSmithy error")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("Build error")]
    AWSBuildError(#[from] BuildError),

    #[error("AWS PutObjectError error")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}
