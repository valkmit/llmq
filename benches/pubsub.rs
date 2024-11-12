use criterion::{
    black_box, 
    criterion_group, 
    criterion_main, 
    BenchmarkId, 
    Criterion
};
use llmq::{Broker, PubSub};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};


#[derive(Debug, Serialize, Deserialize, Clone)]
struct BenchMessage {
    payload: Vec<u8>,
    sequence: u64,
}

fn setup_broker() -> (Arc<Broker>, Vec<thread::JoinHandle<()>>) {
    let broker = Arc::new(Broker::new(
        "/tmp/llmq.sock",
        "/dev/shm"
    ));
    let mut broker_threads= Vec::new();

    // spawn the control and data planes
    broker_threads.push(thread::spawn({
        let broker = Arc::clone(&broker);
        move || broker.run_control_plane_blocking()
    }));

    broker_threads.push(thread::spawn({
        let broker = Arc::clone(&broker);
        move || broker.run_data_plane_blocking()
    }));

    thread::sleep(Duration::from_millis(100));
    
    (broker, broker_threads)
}


fn enqueue_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let running = Arc::new(AtomicBool::new(true));
    // we can start the dequeue thread outside of bench_with_input
    // as it doesn't depend on the size param
    let dequeue_thread = {
        let running = Arc::clone(&running);
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                let _ = subscriber.dequeue_type::<BenchMessage>();
            }
        })
    };
    
    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("enqueue_throughput_benchmark", size),
            &size,
            |b, &size| {
                let msg = BenchMessage {
                    payload: vec![0u8; size],
                    sequence: 0,
                };

                b.iter(|| {
                    publisher.enqueue_type("bench-topic", &msg)
                });
            },
        );
    }

    // spin down dequeue thread
    running.store(false, Ordering::Relaxed);
    dequeue_thread.join().unwrap();
    
    group.finish();
}

fn dequeue_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();

    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    
    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("dequeue_throughput_benchmark", size),
            &size,
            |b, &size| {
                let msg = BenchMessage {
                    payload: vec![0u8; size],
                    sequence: 0,
                };

                let publisher = Arc::clone(&publisher);
                // publisher thread w/corresponding size to firehose messages
                let publish_thread = thread::spawn(move || {
                    loop {
                        publisher.lock().unwrap().enqueue_type("bench-topic", &msg);
                    }
                });

                b.iter(|| {
                    let _ = black_box(subscriber.dequeue_type::<BenchMessage>());
                });

                drop(publish_thread);
            },
        );
    }
    group.finish();
}

fn dequeue_no_topic_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    
    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("dequeue_no_topic_throughput_benchmark", size),
            &size,
            |b, &size| {
                let msg = BenchMessage {
                    payload: vec![0u8; size],
                    sequence: 0,
                };
                let publisher = Arc::clone(&publisher);
                // publisher thread w/corresponding size to firehose messages
                let publish_thread = thread::spawn(move || {
                    loop {
                        publisher.lock().unwrap().enqueue_type("bench-topic", &msg);
                    }
                });

                b.iter(|| {
                    black_box(subscriber.dequeue_bytes_no_topic());
                });

                drop(publish_thread);
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    enqueue_throughput_benchmark,
    dequeue_throughput_benchmark,
    dequeue_no_topic_throughput_benchmark,
);
criterion_main!(benches);