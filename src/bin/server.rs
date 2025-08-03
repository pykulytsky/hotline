use hotline::server::Server;

#[tokio::main]
async fn main() {
    let server = Server::new();
    server.listen("localhost:6969").await.unwrap();
}
