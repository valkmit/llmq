use std::collections::HashMap;
use std::sync::Arc;

use super::client::Client;

/// Efficient representation of where incoming messages should be forwarded to
/// based on the message topic
pub struct ForwardingTable {
    /// List of clients that are capable of publishing messages
    publishers: Vec<Arc<Client>>,

    /// Mapping of topics to clients that elected to receive them at the time
    /// the forwarding table was created
    table: HashMap<String, Vec<Arc<Client>>>,
}

impl Default for ForwardingTable {
    fn default() -> Self {
        Self {
            publishers: Default::default(),
            table: Default::default(),
        }
    }
}

impl ForwardingTable {
    /// Given a list of clients, builds the forwarding table (blocking variant)
    pub fn blocking_new(clients: Vec<Arc<Client>>) -> Self {
        // build the forwarding table per topic
        let mut table = HashMap::<String, Vec<Arc<Client>>>::new();
        for client in clients.iter() {
            for topic in client.blocking_subscriptions().iter() {
                let clients = table
                    .entry(topic.clone())
                    .or_insert_with(Default::default);
                clients.push(client.clone());
            }
        }
        Self {
            publishers: clients,
            table,
        }
    }

    /// Receives messages from publishers and forwards them to subscribers
    pub fn poll(&self) {
        let mut buf = vec![(String::new(), Vec::new()); 16];

        // iterate over every client that is capable of publishing to the
        // broker...
        for p in self.publishers.iter() {
            // get the next message from the client and break it down into
            // the topic and the body
            let rx_count = if let Some(rx) = p.rx_mapping.load().as_ref() {
                unsafe { &mut *rx.get().get() }.dequeue_bulk_bytes(&mut buf)
            } else {
                0
            };

            for idx in 0..rx_count {
                let (ref topic, ref body) = buf[idx];
                
                // retrieve the list of subscribers for this topic
                let subscribers = match self.table.get(topic) {
                    Some(subs) => subs,
                    None => continue,
                };
            
                // walk the list of clients that are subscribed to the topic
                // and forward the message to them
                for d in subscribers.iter() {
                    // skip if the publisher is trying to send to itself
                    if Arc::<Client>::ptr_eq(p, d) {
                        continue;
                    }
                    let tx_opt = d.tx_mapping.load();
                    let Some(tx) = tx_opt.as_ref() else {
                        continue;
                    };
                    
                    unsafe { &mut *tx.get().get() }
                        .enqueue_bulk_bytes(&[(topic.clone(), body)]);
                }
            }
        }
    }
}
