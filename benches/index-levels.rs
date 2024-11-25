use std::io::Cursor;
use std::iter;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use grenad::{CompressionType, Reader, Writer};

const NUMBER_OF_ENTRIES: u64 = 1_000_000;

fn index_levels(bytes: &[u8]) {
    let reader = Reader::new(Cursor::new(bytes)).unwrap();
    let mut cursor = reader.into_cursor().unwrap();

    cursor.move_on_last().unwrap().unwrap();
    cursor.move_on_first().unwrap().unwrap();
    cursor.move_on_last().unwrap().unwrap();

    for x in (0..NUMBER_OF_ENTRIES).step_by(1_567) {
        let num = x.to_be_bytes();
        cursor.move_on_key_greater_than_or_equal_to(num).unwrap().unwrap();
    }
}

fn generate(levels: u8, count: u64) -> Vec<u8> {
    let mut writer =
        Writer::builder().compression_type(CompressionType::Snappy).index_levels(levels).memory();
    let value: Vec<u8> = iter::repeat(546738_u32.to_be_bytes()).take(30).flatten().collect();

    for x in 0..count {
        writer.insert(x.to_be_bytes(), &value).unwrap();
    }

    writer.into_inner().unwrap()
}

fn bench_index_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index Levels");
    for level in [0, 1, 2, 3, 4, 5].iter() {
        let vec = generate(*level, NUMBER_OF_ENTRIES);
        let bytes = vec.as_slice();

        group.bench_with_input(BenchmarkId::new("level", level), bytes, |b, bytes| {
            b.iter(|| index_levels(bytes))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_index_levels);
criterion_main!(benches);
