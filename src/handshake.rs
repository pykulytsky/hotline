use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub mod codec;

#[derive(Debug, Clone, PartialEq)]
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
    pub key: Option<String>,
}

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("Received invalid client type during handshake")]
    InvalidClientType,

    #[error("Unexpected IO error during handshake: {0}")]
    UnexpectedIoError(#[from] std::io::Error),

    #[error("Error encountered during handshake")]
    ServerError,

    #[error("Failed to parse string")]
    StringParsing(#[from] std::str::Utf8Error),
}

impl Handshake {
    pub const PRODUCER: Self = Self {
        client_type: ClientType::Producer,
        key: None,
    };

    pub fn new_consumer<Key: ToString>(key: Key) -> Self {
        Self {
            client_type: ClientType::Consumer,
            key: Some(key.to_string()),
        }
    }

    pub fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put(self.client_type.as_bytes());
        if let Some(key) = &self.key {
            bytes.put(key.as_bytes());
        }
        bytes.freeze()
    }

    pub fn parse(bytes: BytesMut) -> Result<Self, HandshakeError> {
        let mut bytes = bytes.freeze();
        let client_type_bytes = bytes.get_u64().to_be_bytes();
        let client_type = match client_type_bytes.as_ref() {
            b"producer" => ClientType::Producer,
            b"consumer" => ClientType::Consumer,
            _ => return Err(HandshakeError::InvalidClientType),
        };
        let key = if bytes.is_empty() {
            None
        } else {
            Some(std::str::from_utf8(bytes.as_ref())?.to_string())
        };
        Ok(Self { client_type, key })
    }
}
