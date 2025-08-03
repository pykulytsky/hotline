use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

#[derive(Debug, Clone)]
pub struct Ack {
    pub(crate) message_id: u64,
}

impl Ack {
    pub fn new(message_id: u64) -> Self {
        Self { message_id }
    }
}

#[derive(Debug)]
pub struct AckCodec {
    length_delimited_codec: LengthDelimitedCodec,
}

impl AckCodec {
    pub fn new() -> Self {
        Self {
            length_delimited_codec: LengthDelimitedCodec::new(),
        }
    }

    pub fn encode(&mut self, data: Ack, dst: &mut BytesMut) -> Result<(), tokio::io::Error> {
        let mut bytes = BytesMut::new();
        bytes.put(b"ack".as_slice());
        bytes.put_u64(data.message_id);
        self.length_delimited_codec.encode(bytes.freeze(), dst)
    }

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Ack>, tokio::io::Error> {
        if let Some(mut frame) = self.length_delimited_codec.decode(src)? {
            frame.advance(3); // TODO: check if it is ack
            let message_id = frame.get_u64();
            Ok(Some(Ack::new(message_id)))
        } else {
            return Ok(None);
        }
    }
}

impl Encoder<Ack> for AckCodec {
    type Error = tokio::io::Error;

    fn encode(&mut self, item: Ack, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        self.encode(item, dst)
    }
}

impl Decoder for AckCodec {
    type Item = Ack;

    type Error = tokio::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}
