use futures::StreamExt;

pub mod consumer;
mod handshake;
pub mod message;
pub mod producer;
mod time;

pub async fn test() {
    let message = message::Message::new("test");
    message.as_bytes();
    let mut consumer = consumer::Consumer::connect("addr").await.unwrap();
    while let Some(Ok(msg)) = consumer.next().await {
        println!("{:?}", msg.data());
    }
}
