use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[derive(Archive, Deserialize, Serialize, SerdeDeserialize, SerdeSerialize, Clone)]
#[rkyv(
    compare(PartialEq),
    derive(Debug),
)]
struct BenchMessage {
    payload: Vec<u8>,
    sequence: u64,
}

fn serialization_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    for size in message_sizes {
        let msg = BenchMessage {
            payload: vec![0u8; size],
            sequence: 0,
        };
        
        group.bench_with_input(
            BenchmarkId::new("bincode_serialize", size),
            &size,
            |b, _size| {
                b.iter(|| {
                    black_box(bincode::serialize(&msg).unwrap())
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("rkyv_serialize", size),
            &size,
            |b, _size| {
                b.iter(|| {
                    black_box(rkyv::to_bytes::<Error>(&msg).unwrap())
                });
            },
        );
    }
    
    group.finish();
}

fn deserialization_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialization_throughput");
    let message_sizes = [64, 128, 256, 512];
    
    for size in message_sizes {
        let msg = BenchMessage {
            payload: vec![0u8; size],
            sequence: 0,
        };
        
        let bincode_bytes = bincode::serialize(&msg).unwrap();
        let rkyv_bytes = rkyv::to_bytes::<Error>(&msg).unwrap();
        
        group.bench_with_input(
            BenchmarkId::new("bincode_deserialize", size),
            &size,
            |b, _size| {
                b.iter(|| {
                    black_box(bincode::deserialize::<BenchMessage>(&bincode_bytes).unwrap())
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("rkyv_access", size),
            &size,
            |b, _size| {
                b.iter(|| {
                    black_box(rkyv::access::<ArchivedBenchMessage, Error>(&rkyv_bytes).unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rkyv_deserialize", size),
            &size,
            |b, _size| {
                b.iter(|| {
                    black_box(rkyv::from_bytes::<BenchMessage, Error>(&rkyv_bytes).unwrap())
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    serialization_throughput_benchmark,
    deserialization_throughput_benchmark
);
criterion_main!(benches);