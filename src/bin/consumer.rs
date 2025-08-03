use futures::StreamExt;
use hotline::consumer::Consumer;

#[tokio::main]
async fn main() {
    let mut consumer = Consumer::connect("localhost:6969").await.unwrap();
    while let Some(Ok(msg)) = consumer.next().await {
        println!(
            "Received message: {:?}",
            String::from_utf8_lossy(msg.data())
        );
    }
}
