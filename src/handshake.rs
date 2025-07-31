use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

pub mod codec;

#[derive(Debug, Clone)]
pub enum ClientType {
    Producer,
    Consumer,
}

impl ClientType {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            ClientType::Producer => b"producer".as_slice(),
            ClientType::Consumer => b"consumer".as_slice(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Handshake {
    pub client_type: ClientType,
}

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("Received invalid client type during handshake")]
    InvalidClientType,

    #[error("Unexpected IO error during handshake: {0}")]
    UnexpectedIoError(#[from] std::io::Error),

    #[error("Error encountered during handshake")]
    ServerError,
}

impl Handshake {
    pub const CONSUMER: Self = Self {
        client_type: ClientType::Consumer,
    };
    pub const PRODUCER: Self = Self {
        client_type: ClientType::Producer,
    };

    pub fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put(self.client_type.as_bytes());
        bytes.freeze()
    }

    pub fn parse(bytes: BytesMut) -> Result<Self, HandshakeError> {
        let bytes = bytes.freeze();
        let client_type = match bytes.as_ref() {
            b"producer" => ClientType::Producer,
            b"consumer" => ClientType::Consumer,
            _ => return Err(HandshakeError::InvalidClientType),
        };
        Ok(Self { client_type })
    }
}
