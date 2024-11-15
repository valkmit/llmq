use std::sync::Arc;

use super::client::Client;
use super::radix_tree::RadixTree;

/// Efficient representation of where incoming messages should be forwarded to
/// based on the message topic
pub struct ForwardingTable {
    /// List of clients that are capable of publishing messages
    publishers: Vec<Arc<Client>>,

    /// Radix tree containing topic prefixes and their corresponding
    /// subscribed clients.
    topic_radix: RadixTree<Arc<Client>>,
}

impl Default for ForwardingTable {
    fn default() -> Self {
        Self {
            publishers: Default::default(),
            topic_radix: Default::default(),
        }
    }
}

impl ForwardingTable {
    /// Given a list of clients, builds the forwarding table (blocking variant)
    pub fn blocking_new(clients: Vec<Arc<Client>>) -> Self {
        // build the tree per topic
        let mut topic_radix = RadixTree::new();
        for client in clients.iter() {
            for topic in client.blocking_subscriptions().iter() {
                topic_radix.insert(topic, Arc::clone(client));
            }
        }
        Self {
            publishers: clients,
            topic_radix,
        }
    }

    /// Receives messages from publishers and forwards them to subscribers
    pub fn poll(&self) {
        // iterate over every client that is capable of publishing to the
        // broker...
        for p in self.publishers.iter() {
            // get the next message from the client and break it down into
            // the topic and the body
            let mut buf = vec![(String::new(), Vec::new()); 16];
            let rx_count = if let Some(rx) = p.rx_mapping.load().as_ref() {
                unsafe { &mut *rx.get().get() }.dequeue_bulk_bytes(&mut buf, true)
            } else {
                0
            };

            for idx in 0..rx_count {
                let (topic, body) = &buf[idx];
                
                // walk the list of clients that are subscribed to the topic
                // prefix and forward the message to them
                for d in self.topic_radix.find(topic) {
                    let tx_opt = d.tx_mapping.load();
                    let Some(tx) = tx_opt.as_ref() else {
                        continue;
                    };
                    
                    unsafe { &mut *tx.get().get() }
                        .enqueue_bulk_bytes(&[(topic.as_str(), body)]);
                }
            }
        }
    }
}
