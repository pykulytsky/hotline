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
        producer.produce(buf.clone()).await.unwrap();
        buf.clear();
    }
}
