use deadpool::managed::{BuildError, PoolError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DgraphError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Connection pool error: {0}")]
    Pool(#[from] BuildError),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Pool run error: {0}")]
    PoolRunError(#[from] PoolError<tonic::Status>),
}

pub type Result<T> = std::result::Result<T, DgraphError>; 