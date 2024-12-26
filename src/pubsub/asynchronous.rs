use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::pubsub::PubSub;

/// Error types for the asynchronous flavors 
#[derive(Debug)]
pub enum Error {
    /// We are not connected to the broker
    Disconnected,

    /// An error occurred while enqueuing
    EnqueueFailure,

    /// No items were dequeued
    DequeueEmpty,
}

/// Non-blocking enqueue variant: check mapping capacity
/// and wait for len(items) slots to be available
pub struct EnqueueBulkFuture<'a, S, B> {
    pub(crate) pubsub: &'a mut PubSub,
    pub(crate) items: &'a [(S, B)],
}

impl<'a, S, B> Future for EnqueueBulkFuture<'a, S, B> 
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let enqueue_fut_mut = &mut *self;
        let tx_mapping = match enqueue_fut_mut.pubsub.tx_mapping.as_mut() {
            Some(m) => m,
            None => return Poll::Ready(Err(Error::Disconnected)),
        };

        let expected_count = enqueue_fut_mut.items.len();
        if tx_mapping.capacity() < expected_count {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let enqueued = tx_mapping.enqueue_bulk_bytes(&enqueue_fut_mut.items);
        
        if enqueued != expected_count {
            Poll::Ready(Err(Error::EnqueueFailure))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// Non-blocking dequeue variant: check pending messages
/// and wait for "count" items to be available
pub struct DequeueBulkFuture<'a> {
    pub(crate) pubsub: &'a mut PubSub,
    pub(crate) count: usize,
}

impl<'a> Future for DequeueBulkFuture<'a> {
    type Output = Result<Vec<(String, Vec<u8>)>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let dequeue_fut_mut = &mut *self;
        let rx_mapping = match dequeue_fut_mut.pubsub.rx_mapping.as_mut() {
            Some(m) => m,
            None => return Poll::Ready(Err(Error::Disconnected)),
        };
        
        let pending = rx_mapping.pending();
        if pending < dequeue_fut_mut.count {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        if dequeue_fut_mut.pubsub.dequeue_bufs.len() < dequeue_fut_mut.count {
            dequeue_fut_mut.pubsub.dequeue_bufs.resize_with(
                dequeue_fut_mut.count, 
                || (String::new(), Vec::new())
            );
        }

        let dequeued = rx_mapping.dequeue_bulk_bytes(
            &mut dequeue_fut_mut.pubsub.dequeue_bufs[..dequeue_fut_mut.count], 
            true
        );

        if dequeued == 0 {
            Poll::Ready(Err(Error::DequeueEmpty))
        } else {
            let messages = dequeue_fut_mut.pubsub.dequeue_bufs[..dequeued]
                .iter()
                .map(|(topic, data)| (topic.clone(), data.clone()))
                .collect();
            Poll::Ready(Ok(messages))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pubsub::test_helpers::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_async_enqueue() {
        let ctx = TestContext::new("/tmp/llmq-async-enqueue.sock");
        let (mut publisher, mut subscriber) = ctx.connect_clients();

        subscriber.add_subscription("async-topic");
        
        let messages = [
            ("async-topic", b"Message 1".to_vec()),
            ("async-topic", b"Message 2".to_vec()),
            ("async-topic", b"Message 3".to_vec()),
        ];

        let result = publisher.enqueue_bulk_bytes_async(&messages).await;
        assert!(result.is_ok(), "Async enqueue should succeed");

        tokio::time::sleep(Duration::from_millis(10)).await;

        for (i, expected) in messages.iter().enumerate() {
            if let Some((topic, msg)) = subscriber.dequeue_bytes() {
                assert_eq!(topic, expected.0);
                assert_eq!(msg, &expected.1);
            } else {
                panic!("Failed to receive message {}", i);
            }
        }

        assert!(subscriber.dequeue_bytes().is_none(), "Received unexpected extra message");
    }

    #[tokio::test]
    async fn test_async_bulk_dequeue() {
        let ctx = TestContext::new("/tmp/llmq-async-bulk-dequeue.sock");
        let (mut publisher, mut subscriber) = ctx.connect_clients();

        subscriber.add_subscription("async-topic");

        let messages = [
            ("async-topic", b"Message 1".to_vec()),
            ("async-topic", b"Message 2".to_vec()),
            ("async-topic", b"Message 3".to_vec()),
        ];

        for (topic, data) in &messages {
            publisher.enqueue_bytes(topic, data);
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        let received = subscriber.dequeue_bulk_bytes_async(3).await.unwrap();
        
        assert_eq!(received.len(), messages.len(), "Should receive all messages");
        for (i, (topic, data)) in received.iter().enumerate() {
            assert_eq!(topic, "async-topic");
            assert_eq!(data, &messages[i].1);
        }
    }

    #[tokio::test]
    async fn test_async_bulk_dequeue_wait_then_publish() {
        let ctx = TestContext::new("/tmp/llmq-async-bulk-dequeue-wait.sock");
        let (mut publisher, mut subscriber) = ctx.connect_clients();

        subscriber.add_subscription("async-topic");

        let dequeue_task = tokio::spawn({
            let mut subscriber = subscriber;
            async move {
                subscriber.dequeue_bulk_bytes_async(3).await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let messages = [
            ("async-topic", b"Message 1".to_vec()),
            ("async-topic", b"Message 2".to_vec()),
            ("async-topic", b"Message 3".to_vec()),
        ];

        for (topic, data) in &messages {
            publisher.enqueue_bytes(topic, data);
        }

        let received = dequeue_task.await.unwrap().unwrap();
        
        assert_eq!(received.len(), messages.len(), "Should receive all messages");
        for (i, (topic, data)) in received.iter().enumerate() {
            assert_eq!(topic, "async-topic");
            assert_eq!(data, &messages[i].1);
        }
    }
}