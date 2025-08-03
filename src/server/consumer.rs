use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    message::codec::MessageCodec,
    server::{ack::AckCodec, connection::Connection},
};

pub type AckHandler = FramedRead<OwnedReadHalf, AckCodec>;
pub type ConsumerMessagehandler = FramedWrite<OwnedWriteHalf, MessageCodec>;

#[derive(Debug)]
pub struct Consumer {
    pub(crate) ack: AckHandler,
    pub(crate) message: ConsumerMessagehandler,
}

impl Consumer {
    pub fn into_split(self) -> (AckHandler, ConsumerMessagehandler) {
        let Self { ack, message } = self;
        (ack, message)
    }
}

impl From<Connection> for Consumer {
    fn from(connection: Connection) -> Self {
        let (read_half, write_half) = connection.transport.into_inner().into_split();
        let ack = FramedRead::new(read_half, AckCodec::new());
        let message = FramedWrite::new(write_half, MessageCodec::new());
        Self { ack, message }
    }
}
