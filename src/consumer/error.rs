use thiserror::Error;

use crate::{handshake::HandshakeError, message::codec::MessageEncodingError};

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("Producer connection error: {0}")]
    ConnectionError(#[from] tokio::io::Error),

    #[error("Producer handshake error: {0}")]
    HandshakeError(#[from] HandshakeError),

    #[error("Failed to consume message: {0}")]
    MessageError(#[from] MessageEncodingError),
}
