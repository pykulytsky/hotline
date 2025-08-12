use std::pin::Pin;

use futures::SinkExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;

use crate::message::Message;
use crate::server::ack::Ack;
use crate::{
    consumer::error::ConsumerError,
    handshake::{Handshake, HandshakeError, codec::HandshakeCodec},
    message::codec::MessageCodec,
    server::ack::AckCodec,
};

pub mod error;
pub mod stream;

pub struct Consumer {
    read: FramedRead<OwnedReadHalf, MessageCodec>,
    write: FramedWrite<OwnedWriteHalf, AckCodec>,
    autoack: bool,
    pub pending_ack: Option<Pin<BoxFuture<'static, ()>>>,
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
        let (read, write) = transport.into_inner().into_split();
        let read = FramedRead::new(read, MessageCodec::new());
        let write = FramedWrite::new(write, AckCodec::new());
        Ok(Self {
            read,
            write,
            autoack: false,
            pending_ack: None,
        })
    }

    pub fn autoack(&mut self) -> &mut Self {
        self.autoack = true;
        self
    }

    pub async fn ack(&mut self, msg: &Message) -> Result<(), ConsumerError> {
        unsafe {
            self.write.send(Ack::new(msg.id.assume_init())).await?;
        }
        Ok(())
    }
}
