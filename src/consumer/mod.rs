use futures::SinkExt;
use futures::StreamExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::{
    consumer::error::ConsumerError,
    handshake::{Handshake, HandshakeError, codec::HandshakeCodec},
    message::codec::MessageCodec,
};

pub mod error;
pub mod stream;

#[derive(Debug)]
pub struct Consumer {
    transport: Framed<TcpStream, MessageCodec>,
}

impl Consumer {
    pub async fn connect<Addr: ToSocketAddrs, Key: ToString>(
        addr: Addr,
        key: Key,
    ) -> Result<Self, ConsumerError> {
        let tcp = TcpStream::connect(addr).await?;
        let mut transport = Framed::new(tcp, HandshakeCodec::new());
        let consumer_handshake = Handshake::new_consumer(key);
        transport.send(consumer_handshake).await?;
        // TODO: add timeout
        transport
            .next()
            .await
            .ok_or(HandshakeError::ServerError)??;
        let transport = Framed::new(transport.into_inner(), MessageCodec::new());
        Ok(Self { transport })
    }
}
