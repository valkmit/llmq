use std::collections::HashSet;

use tokio::sync::Mutex;

/// Represents a client connected to the Broker. Note that this is intended
/// to be used by the [`Broker`] internally, and not by external pubsub clients.
pub struct Client {
    /// Base path where rx and tx rings are located
    pub path: String,

    /// Set of subscriptions this client has
    subscriptions: Mutex<HashSet<String>>,
}

impl Client {
    pub fn new<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            path: path.into(),
            subscriptions: Default::default(),
        }
    }

    /// Adds a subscription to this client
    pub async fn add_subscription<S>(&self, topic: S) -> HashSet<String>
    where
        S: Into<String>,
    {
        let mut subs = self.subscriptions.lock().await;
        subs.insert(topic.into());
        subs.clone()
    }

    /// Removes a subscription from this client
    pub async fn remove_subscription<S>(&self, topic: S) -> HashSet<String>
    where
        S: Into<String>,
    {
        let mut subs = self.subscriptions.lock().await;
        subs.remove(&topic.into());
        subs.clone()
    }

    /// Clears all subscriptions for this client
    pub async fn clear_subscriptions(&self) {
        let mut subs = self.subscriptions.lock().await;
        subs.clear();
    }

    /// Returns a copy of the set of subscriptions this client has
    pub async fn subscriptions(&self) -> HashSet<String> {
        self.subscriptions.lock().await.clone()
    }

    /// Returns the paths for the rx and tx rings from the perspective of the
    /// client!
    /// 
    /// This means that the rx ring is the ring the client reads from, and the
    /// broker writes to. Similarly, the tx ring is the ring the client writes
    /// to, and the broker reads from.
    pub fn ring_paths(&self) -> (String, String) {
        let rx = format!("{}_rx", self.path);
        let tx = format!("{}_tx", self.path);
        (rx, tx)
    }
}
