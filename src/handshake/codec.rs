use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::handshake::{Handshake, HandshakeError};

pub struct HandshakeCodec {
    length_delimited_codec: LengthDelimitedCodec,
}

impl HandshakeCodec {
    pub fn new() -> Self {
        Self {
            length_delimited_codec: LengthDelimitedCodec::new(),
        }
    }
}

impl Decoder for HandshakeCodec {
    type Item = Handshake;

    type Error = HandshakeError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.length_delimited_codec.decode(src)? {
            Ok(Some(Handshake::parse(frame)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<Handshake> for HandshakeCodec {
    type Error = HandshakeError;

    fn encode(&mut self, item: Handshake, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let bytes = item.as_bytes();
        Ok(self.length_delimited_codec.encode(bytes, dst)?)
    }
}
