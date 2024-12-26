mod asynchronous;
pub(crate) mod pubsub;

pub use self::pubsub::PubSub;

#[cfg(test)]
pub(crate) mod test_helpers {
    use rkyv::{Archive, Deserialize, Serialize};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use crate::broker::Broker;
    use super::PubSub;

    #[derive(Archive, Deserialize, Serialize, Clone, Debug, PartialEq)]
    #[rkyv(
        compare(PartialEq),
        derive(Debug),
    )]
    pub struct TestMessage {
        pub value: String,
        pub count: i32,
    }

    pub struct TestContext {
        _broker: Arc<Broker>,
        _broker_threads: Vec<thread::JoinHandle<()>>,
        pub unix_path: String,
    }

    impl TestContext {
        pub fn new<S: Into<String>>(unix_path: S) -> Self {
            let unix_path = unix_path.into();
            let broker = Arc::new(Broker::new(
                &unix_path, 
                "/dev/shm"
            ));
            
            let mut broker_threads = Vec::new();
            
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

            thread::sleep(Duration::from_millis(100));

            Self {
                _broker: broker,
                _broker_threads: broker_threads,
                unix_path,
            }
        }

        pub fn connect_clients(&self) -> (PubSub, PubSub) {
            let mut publisher = PubSub::new(&self.unix_path, 16 * 1024, 16 * 1024);
            let mut subscriber = PubSub::new(&self.unix_path, 16 * 1024, 16 * 1024);
            
            publisher.connect().expect("Publisher failed to connect");
            subscriber.connect().expect("Subscriber failed to connect");

            (publisher, subscriber)
        }
    }
}