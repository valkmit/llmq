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
    /// Given a list of clients, builds the forwarding table
    pub async fn new(clients: Vec<Arc<Client>>) -> Self {
        // build the forwarding table per topic
        let mut table = HashMap::<String, Vec<Arc<Client>>>::new();
        for client in clients.iter() {
            for topic in client.subscriptions().await.iter() {
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

    pub fn poll(&self) {
        // iterate over every client that is capable of publishing to the
        // broker...
        for p in self.publishers.iter() {
            // get the next message from the client and break it down into
            // the topic and the body

            // walk the list of clients that are interested in the topic
            // and forward the message to them
        }
    }
}
