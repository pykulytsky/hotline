use std::pin::Pin;

use futures::Stream;

use crate::{
    consumer::Consumer,
    message::{Message, codec::MessageEncodingError},
};

impl Stream for Consumer {
    type Item = Result<Message, MessageEncodingError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.transport).poll_next(cx)
    }
}
