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

    pub async fn add_subscription<S>(&self, topic: S) -> HashSet<String>
    where
        S: Into<String>,
    {
        let mut subs = self.subscriptions.lock().await;
        subs.insert(topic.into());
        subs.clone()
    }

    pub async fn remove_subscription<S>(&self, topic: S) -> HashSet<String>
    where
        S: Into<String>,
    {
        let mut subs = self.subscriptions.lock().await;
        subs.remove(&topic.into());
        subs.clone()
    }

    pub async fn clear_subscriptions(&self) {
        let mut subs = self.subscriptions.lock().await;
        subs.clear();
    }

    pub async fn subscriptions(&self) -> HashSet<String> {
        self.subscriptions.lock().await.clone()
    }

    pub fn ring_paths(&self) -> (String, String) {
        let rx = format!("{}_rx", self.path);
        let tx = format!("{}_tx", self.path);
        (rx, tx)
    }
}
