use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::{
    handshake::{Handshake, HandshakeError, codec::HandshakeCodec},
    message::{Message, codec::MessageCodec},
    producer::error::ProducerError,
};
pub mod error;

#[derive(Debug)]
pub struct Producer {
    transport: Framed<TcpStream, MessageCodec>,
}

impl Producer {
    pub async fn connect<Addr: ToSocketAddrs>(addr: Addr) -> Result<Self, ProducerError> {
        let tcp = TcpStream::connect(addr).await?;
        let mut transport = Framed::new(tcp, HandshakeCodec::new());
        transport.send(Handshake::PRODUCER).await?;
        // TODO: add timeout
        transport
            .next()
            .await
            .ok_or(HandshakeError::ServerError)??;
        let transport = Framed::new(transport.into_inner(), MessageCodec::new());
        Ok(Self { transport })
    }

    pub async fn produce<B: Into<Bytes>, Key: ToString>(
        &mut self,
        body: B,
        key: Key,
    ) -> Result<(), ProducerError> {
        Ok(self.transport.send(Message::new(body, key)).await?)
    }
}
