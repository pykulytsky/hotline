use futures::StreamExt;
use hotline::consumer::Consumer;

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    args.next();
    args.next();
    let key = args.next().unwrap_or("*".to_string());

    let mut consumer = Consumer::connect("localhost:6969", key).await.unwrap();
    while let Some(Ok(msg)) = consumer.next().await {
        println!(
            "Received message: {:?}",
            String::from_utf8_lossy(msg.data())
        );
    }
}
