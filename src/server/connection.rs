use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::{
    handshake::{ClientType, codec::HandshakeCodec},
    message::codec::MessageCodec,
    server::error::ServerError,
};

#[derive(Debug)]
pub struct Connection {
    pub(crate) client_type: ClientType,
    pub(crate) transport: Framed<TcpStream, MessageCodec>,
    pub(crate) key: Option<String>,
}

impl Connection {
    pub async fn initalize(tcp: TcpStream) -> Result<Self, ServerError> {
        let mut transport = Framed::new(tcp, HandshakeCodec::new());
        let handshake = transport
            .next()
            .await
            .ok_or(ServerError::ConnectionError)??;
        transport.send(handshake.clone()).await?;
        let transport = Framed::new(transport.into_inner(), MessageCodec::new());
        Ok(Self {
            client_type: handshake.client_type,
            key: handshake.key,
            transport,
        })
    }

    pub fn is_producer(&self) -> bool {
        self.client_type == ClientType::Producer
    }

    pub fn is_consumer(&self) -> bool {
        self.client_type == ClientType::Consumer
    }
}
