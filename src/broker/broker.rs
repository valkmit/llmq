use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use log::info;
use md5::{Md5, Digest};
use tokio::spawn;
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::codec::{FramedRead, FramedWrite};

// use crate::adapter::serde::{DeserializeStream, SerializeSink};
use crate::adapter::serde::{BytesToType, TypeToBytes};
use crate::protocol::control::{Request, Response};

use super::client::Client;

/// Name of the tokio runtime thread that handles control-plane requests
const CTRL_PLANE_THRD_NAME: &str = "llmq-control-plane";

struct Inner {
    /// Where the broker is listening for control-plane requests
    unix_path: String,

    /// Where the broker should coordinate shared memory rings for data plane
    /// communication
    shmem_directory: String,

    /// Clients connected to the server - doesn't guarantee that the clients
    /// have actually established an rx/tx ring yet
    clients: Mutex<HashMap<String, Arc<Client>>>,
}

/// Orchestrator that connects publishers and subscribers to each other. Safe
/// to clone and share across threads, as we mantain an Arc to the inner state.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

impl Broker {
    /// Creates a new broker, where publishers and subscribers may reach it at
    /// the given unix socket path, and where the broker will attempt to
    /// create shared memory rings in the given directory.
    pub fn new<S1, S2>(unix_path: S1, shmem_directory: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Broker {
            inner: Arc::new(Inner {
                unix_path: unix_path.into(),
                shmem_directory: shmem_directory.into(),
                clients: Default::default(),
            }),
        }
    }

    /// Runs the control plane, which allows for publishers and subscribers to
    /// connect to us to synchronize shared memory rings.
    /// 
    /// This function blocks forever. It creates a new tokio runtime with a
    /// single worker thread, used to handle incoming connections and service
    /// them.
    pub fn run_control_plane_blocking(&self) {
        // assuming we are able to bind, this means that we are the only
        // instance running
        let mut listener = UnixListenerStream::new(
            UnixListener::bind(&self.inner.unix_path).unwrap()
        );

        // TODO: clean up old rings that were not properly closed
        
        let rt = RuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .thread_name(CTRL_PLANE_THRD_NAME)
            .enable_all()
            .build()
            .unwrap();

        let inner = self.inner.clone();
        rt.block_on(async move {
            loop {
                let conn = listener.next().await;
                if let Some(Ok(stream)) = conn {
                    let inner = inner.clone();
                    spawn(async move {
                        inner.handle_control_plane_connection(stream).await;
                    });
                }
            }
        });
    }

    pub fn run_data_plane_blocking(&self) {
        loop {

        }
    }
}

impl Inner {
    /// Handles an individual connection to the control plane. Main loop that
    /// waits for requests and responds
    async fn handle_control_plane_connection(
        &self,
        conn: UnixStream
    ) {
        // split up the connection and create framed encoders and decoders for
        // each half
        let (stream, sink) = conn.into_split();
        let mut req_rx = FramedRead::new(
            stream,
            BytesToType::<Request>::default()
        );
        let mut resp_tx = FramedWrite::new(
            sink,
            TypeToBytes::<Response>::default()
        );

        // generate a new client reference for this connection
        let client = self.generate_new_client_reference().await;
        
        // main client loop
        loop {
            // read request from client
            let req = match req_rx.next().await {
                Some(Ok(req)) => req,
                Some(Err(e)) => {
                    info!("Error reading request: {}", e);
                    break;
                },
                None => {
                    info!("Connection closed");
                    break;
                },
            };

            // handle and generate response
            let resp = self.handle_control_plane_connection_request(
                client.clone(),
                req
            ).await;

            // send generated response to client
            if let Err(err) = resp_tx.send(resp).await {
                info!("Error sending response: {}", err);
                break;
            }
        }

        // remove client from clients map
        self.remove_client(client).await;
    }

    /// Handles a single request from a client and generates a response
    async fn handle_control_plane_connection_request(
        &self,
        client: Arc<Client>,
        req: Request
    ) -> Response {
        match req {
            Request::Ping => Response::Pong,

            Request::Setup(rx_slots, tx_slots) => {
                Response::Setup(client.path.clone())
            },

            Request::AddSubscription(topic) => {
                Response::AddSubscription(
                    client.add_subscription(topic).await
                )
            },

            Request::RemoveSubscription(topic) => {
                Response::RemoveSubscription(
                    client.remove_subscription(topic).await
                )
            },
        }
    }

    /// Randomly generates a new base path for a client that is guaranteed to
    /// be unique, inserts it into the clients map, and returns a reference to
    /// it.
    /// 
    /// By the time the client is returned, it could be that the client no
    /// longer exists in the map, as it could have been removed by another
    /// thread due to disconnection.
    async fn generate_new_client_reference(&self) -> Arc<Client> {
        // acquire lock
        let mut clients = self.clients.lock().await;

        // keep generating new paths until we find one that doesn't exist
        let path = loop {
            // generate a random name
            let rng_num = rand::random::<u64>();
            let mut hasher = Md5::new();
            hasher.update(rng_num.to_be_bytes());
            let rng_name = hex::encode(hasher.finalize());

            // generate the name and try again if we already have it (rare)
            let path = format!("{}/{}", self.shmem_directory, rng_name);
            if !clients.contains_key(&path) {
                break path;
            }
        };

        let client = Arc::new(Client::new(&path));
        clients.insert(path, client.clone());
        client
    }

    /// Removes a client from the clients map, if it exists
    async fn remove_client(&self, client: Arc<Client>) {
        let mut clients = self.clients.lock().await;
        clients.remove(client.path.as_str());
    }
}
