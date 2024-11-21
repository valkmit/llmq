use criterion::{
    black_box, 
    criterion_group, 
    criterion_main, 
    BenchmarkId, 
    Criterion
};
use llmq::{Broker, PubSub};
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::rancor::Error as RkyvError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;


#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(
    compare(PartialEq),
    derive(Debug),
)]
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


fn enqueue_type_throughput_benchmark(c: &mut Criterion) {
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
            BenchmarkId::new("enqueue_type_throughput_benchmark", size),
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

fn enqueue_bulk_type_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    let batch_sizes = [10, 100, 1000];
    
    let (_broker, _handles) = setup_broker();
    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let running = Arc::new(AtomicBool::new(true));
    let dequeue_thread = {
        let running = Arc::clone(&running);
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                let _ = subscriber.dequeue_type::<BenchMessage>();
            }
        })
    };
    
    for size in message_sizes {
        for batch_size in batch_sizes {
            group.bench_with_input(
                BenchmarkId::new("enqueue_bulk_type_throughput_benchmark", format!("{}bytes_{}batch", size, batch_size)),
                &(size, batch_size),
                |b, &(size, batch_size)| {
                    let messages = vec![BenchMessage {
                        payload: vec![0u8; size],
                        sequence: 0,
                    }; batch_size];
                    
                    let msgs: Vec<_> = messages.iter()
                        .map(|msg| ("bench-topic", msg.clone()))
                        .collect();
                    
                    b.iter(|| {
                        publisher.enqueue_bulk_type(&msgs)
                    });
                },
            );
        }
    }
    
    running.store(false, Ordering::Relaxed);
    dequeue_thread.join().unwrap();
    
    group.finish();
}

fn enqueue_bytes_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();
    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let running = Arc::new(AtomicBool::new(true));
    let dequeue_thread = {
        let running = Arc::clone(&running);
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                let _ = subscriber.dequeue_bytes();
            }
        })
    };
    
    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("enqueue_bytes_throughput_benchmark", size),
            &size,
            |b, &size| {
                let payload = vec![0u8; size];
                b.iter(|| {
                    publisher.enqueue_bytes("bench-topic", &payload)
                });
            },
        );
    }
    
    running.store(false, Ordering::Relaxed);
    dequeue_thread.join().unwrap();
    
    group.finish();
}

fn enqueue_bulk_bytes_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    let batch_sizes = [10, 100, 1000];
    
    let (_broker, _handles) = setup_broker();
    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let running = Arc::new(AtomicBool::new(true));
    let dequeue_thread = {
        let running = Arc::clone(&running);
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                let _ = subscriber.dequeue_bytes();
            }
        })
    };
    
    for size in message_sizes {
        for batch_size in batch_sizes {
            group.bench_with_input(
                BenchmarkId::new("enqueue_bulk_bytes_throughput_benchmark", format!("{}bytes_{}batch", size, batch_size)),
                &(size, batch_size),
                |b, &(size, batch_size)| {
                    let msgs: Vec<_> = (0..batch_size)
                        .map(|_| ("bench-topic", vec![0u8; size]))
                        .collect();
                    
                    b.iter(|| {
                        publisher.enqueue_bulk_bytes(&msgs)
                    });
                },
            );
        }
    }
    
    running.store(false, Ordering::Relaxed);
    dequeue_thread.join().unwrap();
    
    group.finish();
}



fn dequeue_type_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();

    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    let running = Arc::new(AtomicBool::new(true));

    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("dequeue_type_throughput_benchmark", size),
            &size,
            |b, &size| {
                let msg = BenchMessage {
                    payload: vec![0u8; size],
                    sequence: 0,
                };

                let publisher = Arc::clone(&publisher);
                running.store(true, Ordering::Relaxed);
                let thread_running = Arc::clone(&running);
                // publisher thread w/corresponding size to firehose messages
                let publish_thread = thread::spawn(move || {
                    // serialize one time and call enqueue_bytes directly 
                    // so enqueue is speedy
                    let bytes = rkyv::to_bytes::<RkyvError>(&msg).unwrap();
                    while thread_running.load(Ordering::Relaxed) {
                        publisher.lock().unwrap().enqueue_bytes("bench-topic", &bytes);
                    }
                });

                b.iter(|| {
                    let _ = black_box(subscriber.dequeue_type::<BenchMessage>());
                });

                running.store(false, Ordering::Relaxed);
                publish_thread.join().unwrap();
            },
        );
    }

    group.finish();
}

fn dequeue_bulk_type_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    let batch_sizes = [10, 100, 1000];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    let running = Arc::new(AtomicBool::new(true));
    
    for size in message_sizes {
        for batch_size in batch_sizes {
            group.bench_with_input(
                BenchmarkId::new("dequeue_bulk_type_throughput_benchmark", format!("{}bytes_{}batch", size, batch_size)),
                &(size, batch_size),
                |b, &(size, batch_size)| {
                    let msg = BenchMessage {
                        payload: vec![0u8; size],
                        sequence: 0,
                    };

                    let publisher = Arc::clone(&publisher);
                    running.store(true, Ordering::Relaxed);
                    let thread_running = Arc::clone(&running);
                    // publisher thread w/corresponding size to firehose messages
                    let publish_thread = thread::spawn(move || {
                        // serialize one time and call enqueue_bytes directly 
                        // so enqueue is speedy
                        let bytes = rkyv::to_bytes::<RkyvError>(&msg).unwrap();
                        while thread_running.load(Ordering::Relaxed) {
                            let msgs: Vec<_> = (0..batch_size)
                                .map(|_| ("bench-topic", &bytes[..]))
                                .collect();
                            publisher.lock().unwrap().enqueue_bulk_bytes(&msgs);
                        }
                    });

                    b.iter(|| {
                        black_box(subscriber.dequeue_bulk_type::<BenchMessage>(batch_size));
                    });

                    running.store(false, Ordering::Relaxed);
                    publish_thread.join().unwrap();
                },
            );
        }
    }
    
    group.finish();
}

fn dequeue_bytes_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    let running = Arc::new(AtomicBool::new(true));

    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("dequeue_bytes_throughput_benchmark", size),
            &size,
            |b, &size| {
                let payload = vec![0u8; size];
                
                let publisher = Arc::clone(&publisher);
                running.store(true, Ordering::Relaxed);
                let thread_running = Arc::clone(&running);
                // publisher thread w/corresponding size to firehose messages
                let publish_thread = thread::spawn(move || {
                    while thread_running.load(Ordering::Relaxed) {
                        publisher.lock().unwrap().enqueue_bytes("bench-topic", &payload);
                    }
                });

                b.iter(|| {
                    let _ = black_box(subscriber.dequeue_bytes());
                });

                running.store(false, Ordering::Relaxed);
                publish_thread.join().unwrap();
            },
        );
    }

    group.finish();
}

fn dequeue_bulk_bytes_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_throughput");
    let message_sizes = [64, 128, 256, 512];
    let batch_sizes = [10, 100, 1000];
    
    let (_broker, _handles) = setup_broker();

    let mut publisher = PubSub::default();
    let mut subscriber = PubSub::default();
    
    publisher.connect().expect("Failed to connect publisher");
    subscriber.connect().expect("Failed to connect subscriber");
    subscriber.add_subscription("bench-topic");
    
    let publisher = Arc::new(Mutex::new(publisher));
    let running = Arc::new(AtomicBool::new(true));
    
    for size in message_sizes {
        for batch_size in batch_sizes {
            group.bench_with_input(
                BenchmarkId::new("dequeue_bulk_bytes_throughput_benchmark", format!("{}bytes_{}batch", size, batch_size)),
                &(size, batch_size),
                |b, &(size, batch_size)| {
                    let payload = vec![0u8; size];

                    let publisher = Arc::clone(&publisher);
                    running.store(true, Ordering::Relaxed);
                    let thread_running = Arc::clone(&running);
                    // publisher thread w/corresponding size to firehose messages
                    let publish_thread = thread::spawn(move || {
                        while thread_running.load(Ordering::Relaxed) {
                            let msgs: Vec<_> = (0..batch_size)
                                .map(|_| ("bench-topic", &payload[..]))
                                .collect();
                            publisher.lock().unwrap().enqueue_bulk_bytes(&msgs);
                        }
                    });

                    b.iter(|| {
                        black_box(subscriber.dequeue_bulk_bytes(batch_size));
                    });

                    running.store(false, Ordering::Relaxed);
                    publish_thread.join().unwrap();
                },
            );
        }
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
    typed_single_benchmarks,
    enqueue_type_throughput_benchmark,
    dequeue_type_throughput_benchmark,
);

criterion_group!(
    typed_bulk_benchmarks,
    enqueue_bulk_type_throughput_benchmark,
    dequeue_bulk_type_throughput_benchmark,
);

criterion_group!(
    bytes_single_benchmarks,
    enqueue_bytes_throughput_benchmark,
    dequeue_bytes_throughput_benchmark,
);

criterion_group!(
    bytes_bulk_benchmarks,
    enqueue_bulk_bytes_throughput_benchmark,
    dequeue_bulk_bytes_throughput_benchmark,
);

criterion_group!(
    isolated_benchmarks,
    dequeue_no_topic_throughput_benchmark,
);

criterion_main!(
    typed_single_benchmarks,
    typed_bulk_benchmarks,
    bytes_single_benchmarks,
    bytes_bulk_benchmarks,
    isolated_benchmarks,
);