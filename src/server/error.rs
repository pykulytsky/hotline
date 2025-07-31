use thiserror::Error;

use crate::handshake::HandshakeError;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Server handshake error: {0}")]
    HandshakeError(#[from] HandshakeError),

    #[error("Server connection error")]
    ConnectionError,

    #[error("Server IO error: {0}")]
    IoError(#[from] tokio::io::Error),
}
