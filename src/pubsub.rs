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

/// Error type for the PubSub struct
pub enum Error {
    /// We are not connected to the broker
    Disconnected,

    /// An IO error occurred
    Io(std::io::Error),

    /// A codec error occurred (serialization or deserialization)
    Codec(bincode::Error),

    /// Unexpected type of response from broker
    Unexpected(Response),
}

/// LLMQ messaging client. Can be used to both publish and receive messages
/// from the broker.
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
        self.subscriptions.insert(topic.into());
    }

    /// Removes a subscription that we no longer want to receive messages for.
    /// 
    /// If topic is not subscribed to, this does nothing
    pub fn del_subscription<S>(&mut self, topic: S)
    where
        S: Into<String>,
    {
        self.subscriptions.remove(&topic.into());
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
            Response::Setup(_) => {},
            _ => {
                // we got an unexpected response, disconnect
                self.connection = None;
                return Err(Error::Unexpected(setup_resp));
            },
        }

        // we iterate over a cloned list of subscriptions so that we don't
        // run into issues with the borrow checker and invoking the mutable
        // send_control_message (and we need to clone the strings for that
        // invokation anyway...)
        for topic in self.subscriptions.clone().into_iter() {
            self.send_control_message(Request::AddSubscription(topic))?;
        }

        Ok(())
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
        let mut len_buf = [0u8; HEADER_SIZE];
        self.connection
            .as_mut()
            .unwrap()
            .read_exact(&mut len_buf)?;
        let len: usize = Header::from_be_bytes(
            len_buf.try_into().unwrap()
        ) as usize;

        // read the actual data into a BytesMut
        let mut resp_dst = BytesMut::with_capacity(len);
        resp_dst.resize(len, 0);
        self.connection
            .as_mut()
            .unwrap()
            .read_exact(&mut resp_dst)?;

        // create a decoder and decode the response
        let mut decoder = BytesToType::<Response>::default();
        let resp = decoder.decode(&mut resp_dst)?.unwrap();

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
