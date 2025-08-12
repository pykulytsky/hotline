use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::{
    net::tcp::OwnedWriteHalf,
    sync::{RwLock, RwLockWriteGuard, mpsc::UnboundedReceiver},
};
use tokio_util::codec::FramedWrite;

use crate::{
    message::{Message, codec::MessageCodec},
    server::{
        connection::Connection,
        consumer::{AckHandler, Consumer, ConsumerMessagehandler},
    },
};

#[derive(Debug)]
pub struct Queue {
    key: String,
    _queue: Arc<RwLock<VecDeque<Message>>>,
    new_consumer: UnboundedReceiver<Connection>,
    new_message: UnboundedReceiver<Message>,
    consumers: Arc<RwLock<Vec<ConsumerMessagehandler>>>, // TODO: this needs to be changed to be able to consume multiple queues
    // instead store channel and spawn consumer thread to receive messages
    ack_handlers: Arc<RwLock<Vec<AckHandler>>>,
}

impl Queue {
    pub fn new(
        key: String,
        new_consumer: UnboundedReceiver<Connection>,
        new_message: UnboundedReceiver<Message>,
    ) -> Self {
        let _queue = Arc::new(RwLock::new(VecDeque::with_capacity(256)));
        let consumers = Arc::new(RwLock::new(Vec::new()));
        let ack_handlers = Arc::new(RwLock::new(Vec::new()));
        Self {
            key,
            _queue,
            new_consumer,
            new_message,
            consumers,
            ack_handlers,
        }
    }

    pub async fn start(self) {
        let span = tracing::info_span!("queue", key = &self.key);
        let _guard = span.enter();
        tracing::info!("Started a queue");
        let last_consumer = Arc::new(AtomicUsize::new(0));
        let mut new_consumer = self.new_consumer;
        let mut new_message = self.new_message;
        let consumers = self.consumers.clone();
        let ack_handlers = self.ack_handlers.clone();
        let queue = self._queue.clone();
        tokio::spawn(async move {
            while let Some(connection) = new_consumer.recv().await {
                tracing::info!("Binding new consumer");
                let (ack, message) = Consumer::from(connection).into_split();
                consumers.write().await.push(message);
                ack_handlers.write().await.push(ack);
            }
        });
        let consumers = self.consumers.clone();
        tokio::spawn(async move {
            while let Some(message) = new_message.recv().await {
                handle_new_message(message, last_consumer.clone(), &queue, &consumers).await;
            }
        });

        let ack_handlers = self.ack_handlers.clone();
        let queue = self._queue.clone();
        tokio::spawn(async move {
            loop {
                handle_ack(&ack_handlers, &queue).await;
            }
        });
    }
}

fn select_consumer_round_robin<'c>(
    consumers: &'c mut RwLockWriteGuard<'_, Vec<FramedWrite<OwnedWriteHalf, MessageCodec>>>,
    last_index: Arc<AtomicUsize>,
) -> Option<&'c mut FramedWrite<OwnedWriteHalf, MessageCodec>> {
    if consumers.is_empty() {
        return None;
    }

    if last_index.load(std::sync::atomic::Ordering::Acquire) >= consumers.len() {
        last_index.store(0, std::sync::atomic::Ordering::Release);
    }

    let consumer = consumers.get_mut(last_index.load(std::sync::atomic::Ordering::Acquire));
    last_index.fetch_add(1, std::sync::atomic::Ordering::Release);
    consumer
}

async fn handle_ack(
    ack_handlers: &Arc<RwLock<Vec<AckHandler>>>,
    queue: &Arc<RwLock<VecDeque<Message>>>,
) {
    let mut ack_handlers = ack_handlers.write().await;
    if ack_handlers.is_empty() {
        return;
    }
    let ack_handlers = ack_handlers.iter_mut().map(|ack| ack.next());
    let res = tokio::time::timeout(
        Duration::from_millis(100),
        futures::future::select_all(ack_handlers),
    )
    .await;
    if let Ok((Some(Ok(ack)), idx, _)) = res {
        tracing::info!("Received ack from consumer {}", idx);
        let message_index = queue
            .read()
            .await
            .iter()
            .position(|msg| msg.id() == ack.message_id);
        if let Some(index) = message_index {
            queue.write().await.remove(index);
        }
    }
}

async fn handle_new_message(
    message: Message,
    last_consumer: Arc<AtomicUsize>,
    queue: &Arc<RwLock<VecDeque<Message>>>,
    consumers: &Arc<RwLock<Vec<ConsumerMessagehandler>>>,
) {
    queue.write().await.push_back(message.clone());
    let mut consumers = consumers.write().await;
    let consumer = select_consumer_round_robin(&mut consumers, last_consumer);
    if let Some(consumer) = consumer {
        consumer.send(message).await.unwrap();
    }
}
