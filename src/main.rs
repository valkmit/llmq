pub mod adapter;
mod broker;
pub mod protocol;
pub mod pubsub;

use std::{env, thread};
use std::sync::Arc;

use flexi_logger::{colored_with_thread, Logger, WriteMode};
use log::info;

use broker::Broker;

fn main() {
    // initialize logger and hold reference to it, otherwise it will be dropped
    let _logger = Logger::try_with_str("info")
        .unwrap()
        .log_to_stdout()
        .write_mode(WriteMode::Async)
        .format(colored_with_thread)
        .use_utc()
        .start()
        .unwrap();

    info!("Hello, world!");

    // create broker based on environment variables
    let unix_path = env::var("UNIX_SOCKET_PATH")
        .unwrap_or_else(|_| "/tmp/llmq.sock".to_string());
    let shmem_directory = env::var("SHMEM_DIRECTORY")
        .unwrap_or_else(|_| "/dev/shm".to_string());
    let broker = Arc::new(Broker::new(unix_path, shmem_directory));

    // spawn the control and data planes
    thread::spawn({
        let broker = Arc::clone(&broker);
        move || {
            broker.run_control_plane_blocking();
        }
    });
    info!("Control plane started");
    thread::spawn({
        let broker = Arc::clone(&broker);
        move || {
            broker.run_data_plane_blocking();
        }
    });
    info!("Data plane started");

    // wait forever
    loop {
        thread::park();
    }
}
