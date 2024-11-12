//! LLMQ messaging client
//! 
//! This module contains the [`PubSub`] struct, which is used to publish and
//! receive messages from the broker.
//! 
//! In general, prefer to use deep buffers for ring buffer sizes, as this will
//! prevent data from being lost in the case of a slow consumer.
//! 
//! [`PubSub`]: crate::pubsub::PubSub

use std::collections::HashSet;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::adapter::serde::{
    BytesToType,
    Header,
    TypeToBytes,
    HEADER_SIZE,
};
use crate::protocol::control::{Response, Request};
use crate::queue::Mapping;
use crate::queue::Error as MappingError;

/// Error type for the [`PubSub`]` struct
/// 
/// [`PubSub`]: crate::pubsub::PubSub
#[derive(Debug)]
pub enum Error {
    /// We are not connected to the broker
    Disconnected,

    /// An IO error occurred
    Io(std::io::Error),

    /// A codec error occurred (serialization or deserialization)
    Codec(bincode::Error),

    /// Unexpected type of response from broker
    Unexpected(Response),

    /// Error with the mapping
    Mapping(MappingError),
}

/// LLMQ messaging client. Can be used to both publish and receive messages
/// from the [`Broker`].
/// 
/// [`Broker`]: crate::broker::Broker
pub struct PubSub {
    /// Number of rx slots in the ring buffer
    rx_slots: usize,

    /// Number of tx slots in the ring buffer
    tx_slots: usize,

    /// Path to the broker's UNIX control socket
    unix_path: String,

    /// What we think our subscriptions currently are. Used to handle
    /// reconnections and if subscriptions are added before the connection has
    /// finalized
    subscriptions: HashSet<String>,

    /// UNIX stream to the broker
    connection: Option<UnixStream>,

    /// Attached mapping to the rx rings set up by the broker
    rx_mapping: Option<Mapping>,

    /// Attached mapping to the tx rings set up by the broker
    tx_mapping: Option<Mapping>,

    /// Reusable buffer for serialization
    serialize_buf: Vec<u8>,

    /// Reusable buffer for dequeue payloads
    dequeue_buf: Vec<u8>,

    /// Reusable string for dequeue topics
    dequeue_topic: String,
}

/// Default UNIX socket path the broker control socket listens on
pub const DEFAULT_UNIX_PATH: &str = "/tmp/llmq.sock";

/// Default number of rx slots to allocate
pub const DEFAULT_RX_SLOTS: usize = 16 * 1024;

/// Default number of tx slots to allocate
pub const DEFAULT_TX_SLOTS: usize = 16 * 1024;

impl Default for PubSub {
    fn default() -> Self {
        Self::new(DEFAULT_UNIX_PATH, DEFAULT_RX_SLOTS, DEFAULT_TX_SLOTS)
    }
}

impl PubSub {
    /// Create a new PubSub instance
    /// 
    /// We connect to the broker at the given path, and use the provided
    /// sizes for the rx and tx ring buffers.
    /// 
    /// Consider using the default values if you are unsure what to use,
    /// either by [`PubSub::default`] or [`DEFAULT_UNIX_PATH`],
    /// [`DEFAULT_RX_SLOTS`], and [`DEFAULT_TX_SLOTS`].
    /// 
    /// # Examples
    /// 
    /// ```
    /// let pubsub = PubSub::default();
    /// 
    /// pubsub.add_subscription("topic1");
    /// pubsub.connect();
    /// ```
    /// 
    /// Note that we can add subscriptions before and after connecting without
    /// any issues.
    /// 
    /// ['PubSub::default`]: PubSub::default
    /// [`DEFAULT_UNIX_PATH`]: crate::pubsub::DEFAULT_UNIX_PATH
    /// [`DEFAULT_RX_SLOTS`]: crate::pubsub::DEFAULT_RX_SLOTS
    /// [`DEFAULT_TX_SLOTS`]: crate::pubsub::DEFAULT_TX_SLOTS
    pub fn new<S>(path: S, rx_count: usize, tx_count: usize) -> Self
    where
        S: Into<String>,
    {
        Self {
            rx_slots: rx_count,
            tx_slots: tx_count,
            unix_path: path.into(),
            subscriptions: Default::default(),
            connection: None,
            rx_mapping: None,
            tx_mapping: None,
            serialize_buf: Vec::new(),
            dequeue_buf: Vec::new(),
            dequeue_topic: String::new(),
        }
    }

    /// Adds a new subscription that we can receive messages for.
    /// 
    /// If topic is already subscribed to, or if rx slots is full, this does
    /// nothing
    pub fn add_subscription<S>(&mut self, topic: S)
    where
        S: Into<String>,
    {
        let topic_str = topic.into();
        if self.subscriptions.contains(&topic_str) || self.connection.is_none() {
            return;
        }

        let add_sub_resp = self.send_control_message(
            Request::AddSubscription(topic_str)
        );
        match add_sub_resp {
            Ok(Response::AddSubscription(subs)) => {
                self.subscriptions = subs;
            },
            Ok(resp) => {
                eprintln!("Unexpected response: {:?}", resp);
                return;
            },
            Err(e) => {
                eprintln!("Failed to add subscription: {:?}", e);
                return;
            },
        }
    }

    /// Removes a subscription that we no longer want to receive messages for.
    /// 
    /// If topic is not subscribed to, this does nothing
    pub fn del_subscription<S>(&mut self, topic: S)
    where
        S: Into<String>,
    {
        let topic_str = topic.into();
        if !self.subscriptions.contains(&topic_str) || self.connection.is_none() {
            return;
        }

        let del_sub_resp = self.send_control_message(
            Request::RemoveSubscription(topic_str)
        );
        match del_sub_resp {
            Ok(Response::RemoveSubscription(subs)) => {
                self.subscriptions = subs;
            },
            Ok(resp) => {
                eprintln!("Unexpected response: {:?}", resp);
                return;
            },
            Err(e) => {
                eprintln!("Failed to del subscription: {:?}", e);
                return;
            },
        }
    }

    /// Gets the topics we are subscribed to
    pub fn subscriptions(&self) -> &HashSet<String> {
        &self.subscriptions
    }

    /// Establishes a connection to the broker. Does nothing if we are already
    /// connected, otherwise blocks until we can establish a connection and
    /// exchange the initial handshake.
    pub fn connect(&mut self) -> Result<(), Error> {
        if self.connection.is_some() {
            // we are already connected, nothing to do
            return Ok(());
        }

        self.connection = Some(UnixStream::connect(&self.unix_path)?);

        let setup_resp = self.send_control_message(
            Request::Setup(self.rx_slots, self.tx_slots)
        )?;
        match setup_resp {
            // received a setup response, let's stand up the mapping
            Response::Setup(rx_path, tx_path) => {
                self.rx_mapping = Some(Mapping::new_attach(rx_path)?);
                self.tx_mapping = Some(Mapping::new_attach(tx_path)?);
            },

            // we got an unexpected response, disconnect
            _ => {
                self.connection = None;
                return Err(Error::Unexpected(setup_resp));
            },
        }

        // we iterate over a cloned list of subscriptions so that we don't
        // run into issues with the borrow checker and invoking the mutable
        // send_control_message (and we need to clone the strings for that
        // invocation anyway...)
        for topic in self.subscriptions.clone().into_iter() {
            self.send_control_message(Request::AddSubscription(topic))?;
        }

        Ok(())
    }

    /// Enqueue a message to be sent to the broker under the given topic
    pub fn enqueue_bytes<S>(&mut self, topic: S, buf: &[u8])
    where
        S: AsRef<str>,
    {
        let tx_mapping = match self.tx_mapping.as_mut() {
            Some(m) => m,
            None => return,
        };

        tx_mapping.enqueue_bulk_bytes(&[(topic.as_ref(), buf)]);
    }

    /// Enqueue a message to be sent to the broker under the given topic.
    /// 
    /// The message is serialized using bincode
    pub fn enqueue_type<S, T>(&mut self, topic: S, item: &T)
    where
        S: AsRef<str>,
        T: serde::Serialize,
    {
        let tx_mapping = match self.tx_mapping.as_mut() {
            Some(m) => m,
            None => return,
        };

        self.serialize_buf.clear();
        match bincode::serialize_into(&mut self.serialize_buf, item) {
            Ok(()) => tx_mapping.enqueue_bulk_bytes(&[(topic.as_ref(), &self.serialize_buf)]),
            Err(_) => return,
        };
    }

    /// Dequeue a message from the broker, returning the topic and the message
    /// in a newly-allocated Vec<u8>
    pub fn dequeue_bytes(&mut self) -> Option<(&str, &[u8])> {
        let rx_mapping = self.rx_mapping.as_mut()?;
        
        self.dequeue_topic.clear();
        self.dequeue_buf.clear();
        
        let mut data = [(Some(&mut self.dequeue_topic), &mut self.dequeue_buf)];
        let dequeued = rx_mapping.dequeue_bulk_bytes(&mut data, true);
        
        if dequeued != 1 {
            return None;
        }
        
        Some((&self.dequeue_topic, &self.dequeue_buf))
    }

    /// Dequeue a message from the broker, returning just the message payload
    /// without allocating memory for the topic string
    pub fn dequeue_bytes_no_topic(&mut self) -> Option<&[u8]> {
        let rx_mapping = self.rx_mapping.as_mut()?;
        
        self.dequeue_buf.clear();
        let mut data = [(None, &mut self.dequeue_buf)];
        let dequeued = rx_mapping.dequeue_bulk_bytes(&mut data, false);
        
        if dequeued != 1 {
            return None;
        }
        
        Some(&self.dequeue_buf)
    }

    /// Dequeue a message from the broker, returning the topic and message
    /// buffer copied into the provided buffer
    pub fn dequeue_bytes_into(&mut self, dst: &mut [u8]) -> Option<(&str, usize)> {
        let (topic, msg) = self.dequeue_bytes()?;
        
        if dst.len() < msg.len() {
            return None;
        }
        
        dst[..msg.len()].copy_from_slice(&msg);
        Some((topic, msg.len()))
    }

    /// Dequeue a message from the broker, returning the topic and the message
    /// deserialized using bincode
    pub fn dequeue_type<T>(&mut self) -> Option<(&str, T)>
    where
        T: serde::de::DeserializeOwned,
    {
        let (topic, msg) = self.dequeue_bytes()?;
        
        let deserialized = bincode::deserialize(&msg).ok()?;
        Some((topic, deserialized))
    }

    /// Sends a request over the control socket and waits for a response. In
    /// the event of an error, the connection is cleared and we return the
    /// specific error
    fn send_control_message(&mut self, req: Request) -> Result<Response, Error> {
        if self.connection.is_none() {
            return Err(Error::Disconnected);
        }

        // create an encoder and encode the request
        let mut encoder = TypeToBytes::<Request>::default();
        let mut req_dst = BytesMut::new();
        encoder.encode(req, &mut req_dst)?;

        // fire off the request, 1st unwrap is for Option which is guaranteed
        // to be Some as we check at the start of the fn
        self.connection
            .as_mut()
            .unwrap()
            .write_all(&req_dst)?;

        // read the first bytes, which should contain the length of the data
        let mut dst = BytesMut::with_capacity(HEADER_SIZE);
        dst.resize(HEADER_SIZE, 0);
        self.connection
            .as_mut()
            .unwrap()
            .read_exact(&mut dst)?;
        let len: usize = Header::from_be_bytes(
            (&dst as &[u8]).try_into().unwrap()
        ) as usize;

        // read the actual data into the buffer
        dst.resize(HEADER_SIZE + len, 0);
        self.connection
            .as_mut()
            .unwrap()
            .read_exact(&mut dst[HEADER_SIZE..])?;

        // create a decoder and decode the response
        let mut decoder = BytesToType::<Response>::default();
        let resp = decoder.decode(&mut dst)?.unwrap();

        Ok(resp)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Codec(e)
    }
}

impl From<MappingError> for Error {
    fn from(e: MappingError) -> Self {
        Error::Mapping(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use serde::{Serialize, Deserialize};
    use crate::broker::Broker;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        value: String,
        count: i32,
    }

    /// Helper struct to manage broker and client setup/teardown
    struct TestContext {
        _broker: Arc<Broker>,
        _broker_threads: Vec<thread::JoinHandle<()>>,
    }

    impl TestContext {
        fn new() -> Self {
            let broker = Arc::new(Broker::new(
                "/tmp/llmq.sock", 
                "/dev/shm"
            ));
            
            let mut broker_threads = Vec::new();
            
            // spawn the control and data planes
            broker_threads.push(thread::spawn({
                let broker = Arc::clone(&broker);
                move || {
                    broker.run_control_plane_blocking();
                }
            }));

            broker_threads.push(thread::spawn({
                let broker = Arc::clone(&broker);
                move || {
                    broker.run_data_plane_blocking();
                }
            }));

            // give broker time to spin up
            thread::sleep(Duration::from_millis(100));

            Self {
                _broker: broker,
                _broker_threads: broker_threads,
            }
        }

        fn connect_clients() -> (PubSub, PubSub) {
            let mut publisher = PubSub::default();
            let mut subscriber = PubSub::default();
            
            publisher.connect().expect("Publisher failed to connect");
            subscriber.connect().expect("Subscriber failed to connect");

            (publisher, subscriber)
        }
    }

    #[test]
    fn test_pubsub_and_subscriptions() {
        let _ctx = TestContext::new();
        let (mut publisher, mut subscriber) = TestContext::connect_clients();

        // messages shouldn't be received when not subscribed
        publisher.enqueue_bytes("unsubscribed-topic", b"Should not receive this");
        thread::sleep(Duration::from_millis(10));
        assert!(subscriber.dequeue_bytes().is_none(), 
            "Received message for unsubscribed topic");

        // basic subscription and message reception
        subscriber.add_subscription("test-topic");
        publisher.enqueue_bytes("test-topic", b"Hello from test!");
        // give broker time to poll
        thread::sleep(Duration::from_millis(10));

        if let Some((topic, msg)) = subscriber.dequeue_bytes() {
            assert_eq!(topic, "test-topic");
            assert_eq!(&msg, b"Hello from test!");
        } else {
            panic!("Failed to receive bytes message");
        }

        // test serialized type
        let test_msg = TestMessage {
            value: "test value".to_string(),
            count: 42,
        };
        publisher.enqueue_type("test-topic", &test_msg);
        // give broker time to poll
        thread::sleep(Duration::from_millis(10));

        if let Some((topic, received_msg)) = subscriber.dequeue_type::<TestMessage>() {
            assert_eq!(topic, "test-topic");
            assert_eq!(received_msg, test_msg);
        } else {
            panic!("Failed to receive typed message");
        }

        // test bytes_into
        let mut buf = vec![0u8; 128];
        publisher.enqueue_bytes("test-topic", b"Testing bytes_into");
        // give broker time to poll
        thread::sleep(Duration::from_millis(10));

        if let Some((topic, len)) = subscriber.dequeue_bytes_into(&mut buf) {
            assert_eq!(topic, "test-topic");
            assert_eq!(&buf[..len], b"Testing bytes_into");
        } else {
            panic!("Failed to receive message into buffer");
        }

        // unsubscribe behavior
        subscriber.del_subscription("test-topic");
        publisher.enqueue_bytes("test-topic", b"Should not receive this after unsubscribe");
        // give broker time to poll
        thread::sleep(Duration::from_millis(10));
        assert!(subscriber.dequeue_bytes().is_none(), 
            "Received message after unsubscribing");

        // multiple topic handling
        subscriber.add_subscription("topic1");
        subscriber.add_subscription("topic2");
        publisher.enqueue_bytes("topic1", b"Message 1");
        publisher.enqueue_bytes("topic2", b"Message 2");
        publisher.enqueue_bytes("topic3", b"Should not receive");
        // give broker time to poll
        thread::sleep(Duration::from_millis(10));

        let mut received_topics = HashSet::new();
        while let Some((topic, _)) = subscriber.dequeue_bytes() {
            received_topics.insert(topic.to_string());
        }

        assert!(received_topics.contains("topic1"), "Missing message from topic1");
        assert!(received_topics.contains("topic2"), "Missing message from topic2");
        assert!(!received_topics.contains("topic3"), "Incorrectly received message from topic3");
    }
}
