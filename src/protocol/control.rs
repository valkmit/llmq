use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Publisher/Subscriber sends Request to Broker
#[derive(Deserialize, Serialize)]
pub enum Request {
    /// Client wishes to check that the connection is alive
    Ping,

    /// Publisher/Subscriber client wants broker to set up a new shared memory
    /// ring for data plane communication. Number of rx and tx slots
    /// respectively
    Setup(usize, usize),

    /// Publisher/Subscriber client wants to add a subscription
    AddSubscription(String),

    /// Publisher/Subscriber client wants to remove a subscription
    RemoveSubscription(String),
}

/// Broker sends Response to Publisher/Subscriber
#[derive(Deserialize, Serialize)]
pub enum Response {
    /// Broker has successfully received a ping request
    Pong,

    /// Broker has successfully set up a new shared memory ring for data plane
    /// communication, and it is now available at the given path
    Setup(String),

    /// Broker has successfully added a subscription. Current subscriptions are
    /// returned
    AddSubscription(HashSet<String>),

    /// Broker has successfully removed a subscription. Current subscriptions
    /// are returned
    RemoveSubscription(HashSet<String>),
}
