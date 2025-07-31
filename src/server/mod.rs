use tokio::net::{TcpListener, ToSocketAddrs};

use crate::server::{connection::Connection, error::ServerError};

pub mod connection;
pub mod error;

#[derive(Debug)]
pub struct Server {}

impl Server {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn listen<Addr: ToSocketAddrs>(addr: Addr) -> Result<(), ServerError> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            match listener.accept().await {
                Ok((tcp, _addr)) => {
                    tokio::spawn(async move {
                        let connection = Connection::initalize(tcp).await.unwrap();
                        dbg!(&connection);
                        loop {}
                    });
                }
                Err(err) => {
                    eprintln!("{err}");
                }
            }
        }
    }
}
