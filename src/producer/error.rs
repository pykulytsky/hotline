use thiserror::Error;

use crate::{handshake::HandshakeError, message::codec::MessageEncodingError};

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("Consumer connection error: {0}")]
    ConnectionError(#[from] tokio::io::Error),

    #[error("Consumer handshake error: {0}")]
    HandshakeError(#[from] HandshakeError),

    #[error("Failed to produce message: {0}")]
    MessageError(#[from] MessageEncodingError),
}
