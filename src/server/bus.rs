use futures::StreamExt;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use thiserror::Error;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{
    RwLock,
    mpsc::{UnboundedReceiver, UnboundedSender},
};

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn generate_id() -> u64 {
    ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

use crate::{message::Message, server::connection::Connection};

pub type Queues = Arc<RwLock<HashMap<String, UnboundedSender<Message>>>>;
pub type NewQueueSender = UnboundedSender<(String, UnboundedReceiver<Message>)>;
pub type NewQueueReceiver = UnboundedReceiver<String>;

#[derive(Debug, Error)]
pub enum MessageBusError {
    #[error("Failed to create a queue")]
    FailedToCreateQueue,

    #[error("Failed to route a message to a queue {0}")]
    FailedToRouteMessage(String),
}

#[derive(Debug)]
pub struct MessageBus {
    producers_rx: UnboundedReceiver<Connection>,
    queues: Queues,
    new_queue_rx: NewQueueReceiver,
    new_queue_tx: NewQueueSender,
}

impl MessageBus {
    pub fn new(
        producers_rx: UnboundedReceiver<Connection>,
        new_queue_tx: NewQueueSender,
        new_queue_rx: NewQueueReceiver,
    ) -> Self {
        let queues = Arc::new(RwLock::new(HashMap::new()));
        Self {
            producers_rx,
            queues,
            new_queue_rx,
            new_queue_tx,
        }
    }

    pub async fn start(mut self) {
        let span = tracing::info_span!("message_bus");
        let _guard = span.enter();
        tracing::info!("Starting message bus");
        let queues = self.queues.clone();
        let new_queue_tx = self.new_queue_tx.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!("new_queues");
            let _guard = span.enter();
            while let Some(key) = self.new_queue_rx.recv().await {
                tracing::info!("Registering new queue: {}", &key);
                let (new_channel_tx, new_channel_rx) = unbounded_channel();
                let mut queues = queues.write().await;
                queues.insert(key.clone(), new_channel_tx.clone());
                tracing::info!("Successfully registered new queue: {}", &key);
                new_queue_tx
                    .send((key, new_channel_rx))
                    .map_err(|_| MessageBusError::FailedToCreateQueue)
                    .unwrap();
            }
        });

        while let Some(mut producer) = self.producers_rx.recv().await {
            let span = tracing::info_span!("producers");
            let _guard = span.enter();
            tracing::info!("Registered new producer");
            let queues = self.queues.clone();
            let new_queue_tx = self.new_queue_tx.clone();
            tokio::spawn(async move {
                let span = tracing::info_span!("producer_messages");
                let _guard = span.enter();
                tracing::info!("Strated listening for messages from producer");
                while let Some(Ok(msg)) = producer.transport.next().await {
                    tracing::info!("Received message from producer");
                    let _ = process_producer_message(msg, &queues, &new_queue_tx).await;
                }
            });
        }
        tracing::info!("Message bus has successfully stopped");
    }
}

async fn process_producer_message(
    mut message: Message,
    queues: &Queues,
    new_queue_tx: &NewQueueSender,
) -> Result<(), MessageBusError> {
    message.set_id(generate_id());
    let key = message.key.clone();
    let new_message = match queues.write().await.entry(key.clone()) {
        Occupied(existing_queue) => existing_queue.get().clone(),
        Vacant(new_queue) => {
            let (new_channel_tx, new_channel_rx) = unbounded_channel();
            new_queue.insert(new_channel_tx.clone());
            new_queue_tx
                .send((key.clone(), new_channel_rx))
                .map_err(|_| MessageBusError::FailedToCreateQueue)?;
            new_channel_tx
        }
    };
    new_message
        .send(message.clone())
        .map_err(|_| MessageBusError::FailedToRouteMessage(key))
}
