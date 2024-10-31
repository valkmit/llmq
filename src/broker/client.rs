use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use crate::queue::Mapping;
use crate::queue::Error as MappingError;

// to safely wrap UnsafeCell<Mapping>
pub struct SyncMapping(UnsafeCell<Mapping>);

unsafe impl Send for SyncMapping {}
unsafe impl Sync for SyncMapping {}

impl SyncMapping {
    pub fn new(mapping: Mapping) -> Self {
        SyncMapping(UnsafeCell::new(mapping))
    }

    pub fn get(&self) -> &UnsafeCell<Mapping> {
        &self.0
    }
}

/// Represents a client connected to the Broker. Note that this is intended
/// to be used by the [`Broker`] internally, and not by external pubsub clients.
pub struct Client {
    /// Base path where rx and tx rings are located
    pub path: String,

    /// Set of subscriptions this client has
    subscriptions: Mutex<HashSet<String>>,

    /// From the perspective of the broker, the tx ring.
    /// 
    /// Note that this will be the opposite of what [`ring_paths`] returns.
    /// 
    /// [`ring_paths`]: Client::ring_paths
    pub(super) tx_mapping: ArcSwap<Option<SyncMapping>>,

    /// From the perspective of the broker, the rx ring.
    /// 
    /// Note that this will be the opposite of what [`ring_paths`] returns.
    /// 
    /// [`ring_paths`]: Client::ring_paths
    pub(super) rx_mapping: ArcSwap<Option<SyncMapping>>,
}

impl Client {
    pub fn new<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            path: path.into(),
            subscriptions: Default::default(),  
            tx_mapping: Arc::new(None).into(),
            rx_mapping: Arc::new(None).into(),
        }
    }

    /// Establishes the rx and tx rings based on the sizing info the client
    /// has provided us.
    pub fn setup_rings(
        &self,
        client_rx_slots: usize,
        client_tx_slots: usize,
    ) -> Result<(), MappingError> {
        // note that we are inverting the ordering of the ring paths, since we
        // store the rings from the perspective of the broker, whereas
        // ring_paths returns them from the perspective of the client
        let (tx_path, rx_path) = self.ring_paths();

        // set up the rx and tx rings - note that we flip client_*x_slots and
        // rx/tx, because this is the client itself informing the broker of the
        // sizes of the rings
        let tx = Mapping::new_create(tx_path, 4096, client_rx_slots, client_rx_slots)?;
        let rx = Mapping::new_create(rx_path, 4096, client_tx_slots, client_rx_slots)?;

        self.tx_mapping.store(Some(SyncMapping::new(tx)).into());
        self.rx_mapping.store(Some(SyncMapping::new(rx)).into());
        Ok(())
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

    /// Returns a copy of the set of subscriptions this client has (blocking variant)
    pub fn blocking_subscriptions(&self) -> HashSet<String> {
        self.subscriptions.blocking_lock().clone()
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

impl Drop for Client {
    fn drop(&mut self) {
        // note that we are inverting the ordering of the ring paths, since we
        // store the rings from the perspective of the broker, whereas
        // ring_paths returns them from the perspective of the client
        let (tx_path, rx_path) = self.ring_paths();

        if let Some(rx) = self.rx_mapping.load().as_ref() {
            // TODO: clean up the ring from the filesystem
        }

        if let Some(tx) = self.tx_mapping.load().as_ref() {
            // TODO: clean up the ring from the filesystem
        }
    }
}
