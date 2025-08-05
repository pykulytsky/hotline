use std::io::Write;

use hotline::producer::Producer;

#[tokio::main]
async fn main() {
    let mut producer = Producer::connect("localhost:6969").await.unwrap();
    let mut buf = String::with_capacity(100);
    loop {
        print!("Enter a message: ");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut buf).unwrap();
        let line = buf.clone();
        let (key, msg) = line.split_once(" ").unwrap();
        producer
            .produce(msg.to_string(), key.to_string())
            .await
            .unwrap();
        buf.clear();
    }
}
