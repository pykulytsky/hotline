use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::message::Message;

#[derive(Debug, Error)]
pub enum MessageEncodingError {
    #[error("Unexpected IO error: {0}")]
    UnexpectedIoError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct MessageCodec {
    length_delimited_codec: LengthDelimitedCodec,
}

impl MessageCodec {
    pub fn new() -> Self {
        Self {
            length_delimited_codec: LengthDelimitedCodec::new(),
        }
    }
}

impl Decoder for MessageCodec {
    type Item = Message;

    type Error = MessageEncodingError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.length_delimited_codec.decode(src)? {
            let message = Message::parse(frame);
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = MessageEncodingError;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let bytes = item.as_bytes();
        Ok(self.length_delimited_codec.encode(bytes, dst)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Message;

    use super::MessageCodec;
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::FramedRead;
    use tokio_util::codec::FramedWrite;

    #[tokio::test]
    async fn single_message() {
        let mut buffer = Vec::new();
        let mut transport = FramedWrite::new(&mut buffer, MessageCodec::new());
        transport.send(Message::new("1")).await.unwrap();
        transport.send(Message::new("test")).await.unwrap();
        transport
            .send(Message::new([1, 2, 3].as_slice()))
            .await
            .unwrap();
        let mut client = FramedRead::new(buffer.as_slice(), MessageCodec::new());
        let msg1 = client.next().await.unwrap().unwrap();
        let msg2 = client.next().await.unwrap().unwrap();
        let msg3 = client.next().await.unwrap().unwrap();
        assert!(client.next().await.is_none());
        assert!(&msg1.body == b"1".as_slice());
        assert!(&msg2.body == b"test".as_slice());
        assert!(&msg3.body == [1, 2, 3].as_slice());
    }
}
