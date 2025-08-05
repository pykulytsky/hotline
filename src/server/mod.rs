use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

use crate::{
    message::Message,
    server::{bus::MessageBus, connection::Connection, error::ServerError, queue::Queue},
};

pub mod ack;
mod bus;
pub mod connection;
mod consumer;
pub mod error;
mod queue;

#[derive(Debug)]
pub struct Server {
    producers_tx: UnboundedSender<Connection>,
    new_queue_rx: UnboundedReceiver<(String, UnboundedReceiver<Message>)>,
    new_queue_tx: UnboundedSender<String>,
    queue_channels: Arc<RwLock<HashMap<String, UnboundedSender<Connection>>>>,
    bus: MessageBus,
}

impl Server {
    pub fn new() -> Self {
        let (producers_tx, producers_rx) = unbounded_channel();
        let (new_queue_tx, new_queue_rx) = unbounded_channel();
        let (new_consumer_queue_tx, new_consumer_queue_rx) = unbounded_channel();
        let bus = MessageBus::new(producers_rx, new_queue_tx, new_consumer_queue_rx);
        let queue_channels = Arc::new(RwLock::new(HashMap::new()));
        Self {
            producers_tx,
            new_queue_rx,
            new_queue_tx: new_consumer_queue_tx,
            queue_channels,
            bus,
        }
    }

    pub async fn listen<Addr: ToSocketAddrs>(self, addr: Addr) -> Result<(), ServerError> {
        let span = tracing::info_span!("server");
        let _guard = span.enter();
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(
            "Listening on {}",
            listener.local_addr().unwrap().to_string()
        );
        tokio::spawn(self.bus.start());

        let mut new_queue_rx = self.new_queue_rx;
        let queue_channels = self.queue_channels.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!("new_queues");
            let _guard = span.enter();
            while let Some((key, queue_rx)) = new_queue_rx.recv().await {
                tracing::info!("Received request to create new queue {}", &key);
                let (consumers_tx, consumers_rx) = unbounded_channel();
                let queue = Queue::new(key.clone(), consumers_rx, queue_rx);
                queue.start().await;
                queue_channels.write().await.insert(key, consumers_tx);
            }
        });

        let producers_tx = self.producers_tx.clone();
        let new_queue_tx = self.new_queue_tx;
        loop {
            let queue_channels = self.queue_channels.clone();
            let producers_tx = producers_tx.clone();
            let new_queue_tx = new_queue_tx.clone();
            match listener.accept().await {
                Ok((tcp, addr)) => {
                    tokio::spawn(async move {
                        let span = tracing::info_span!("connections");
                        let _guard = span.enter();
                        let connection = Connection::initalize(tcp).await.unwrap();
                        if connection.is_consumer() {
                            tracing::info!("New consumer connected {}", addr);
                            let key = connection.key.clone().unwrap_or("*".to_string());
                            loop {
                                let mut queue_channels = queue_channels.write().await;
                                match queue_channels.get_mut(&key) {
                                    Some(queue) => {
                                        queue.send(connection).unwrap();
                                        break;
                                    }
                                    None => {
                                        new_queue_tx.send(key.clone()).unwrap();
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                        } else {
                            tracing::info!("New producer connected {}", addr);
                            producers_tx.send(connection).unwrap();
                        }
                    });
                }
                Err(err) => {
                    eprintln!("{err}");
                }
            }
        }
    }
}
