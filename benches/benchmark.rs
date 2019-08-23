extern crate criterion;
extern crate slog;

//use slog::*;

extern crate nacs;

use criterion::*;

use std::sync::Arc;

use tempdir::TempDir;

#[allow(unused)]
use rocksdb::rocksdb::{DBIterator, Snapshot, Writable};
#[allow(unused)]
use rocksdb::{ReadOptions, SeekKey, WriteBatch, WriteOptions, DB};

use nacs::{
    default_test_db_with_path, drain_data, Scanner, ScannerConfig, ValueType, DIST_QT1_KEY,
    DIST_QT3_KEY,
};

macro_rules! black_box_kv {
    ($name:expr) => {
        {
            black_box(($name.key(), $name.value()))
        }
    };
}

macro_rules! cursor_next_ok {
    ($name:expr) => {
        {
            !$name.next()
        }
    };
}

pub fn forward_scan(mut scanner: Scanner, loop_cnt: u64) {
    for _ in 0..loop_cnt {
        // fetch next for "write" field
        cursor_next_ok!(scanner.iter_write);
        black_box_kv!(scanner.iter_write);

        // fetch next for "default" field
        cursor_next_ok!(scanner.iter_default);
        black_box_kv!(scanner.iter_default);
    }
}

pub fn forward_batch_scan(mut scanner: Scanner, batch_size: u64, loop_cnt: u64) {
    let mut write_cache = Vec::with_capacity(1024 * 100);

    for _ in 0..loop_cnt / batch_size {
        for _ in 0..batch_size {
            cursor_next_ok!(scanner.iter_write);
            write_cache.extend_from_slice(scanner.iter_write.key());
            write_cache.extend_from_slice(scanner.iter_write.value());
        }

        for _ in 0..batch_size {
            cursor_next_ok!(scanner.iter_default);
            black_box(scanner.iter_default.key());
            write_cache.extend_from_slice(scanner.iter_default.value());
        }
        write_cache.clear();
    }

    let sz = loop_cnt % batch_size;
    for _ in 0..sz {
        cursor_next_ok!(scanner.iter_write);
//        black_box_kv!(scanner.iter_write);
        write_cache.extend_from_slice(scanner.iter_write.key());
        write_cache.extend_from_slice(scanner.iter_write.value());
    }

    for _ in 0..sz {
        cursor_next_ok!(scanner.iter_default);
        black_box(scanner.iter_default.key());
        write_cache.extend_from_slice(scanner.iter_default.value());
    }
    write_cache.clear();
}

fn bench_scan(c: &mut Criterion) {
    // handle config here
    let common_cfg = ScannerConfig::new(
        Some(DIST_QT1_KEY.clone().to_vec()),
        Some(DIST_QT3_KEY.clone().to_vec()),
    );

    //    let common_cfg = ScannerConfig::default();
    let test_rocks_size: Vec<u64> = vec![20000, 100000];
    let allow_values = vec![
        ValueType::MiddleValue,
        ValueType::LongValue,
        ValueType::LongLongValue,
    ];

    for rocks_size in test_rocks_size {
        for defaultcf_value_length in &allow_values {

            let temp_dir = TempDir::new_in("data", "data").unwrap();
            println!("{:?}", temp_dir.path());
            let mut db = default_test_db_with_path(temp_dir.path());
            drain_data(&mut db, rocks_size, defaultcf_value_length.clone());
            let db = Arc::new(db);
            let cur_db = db.clone();
            let cfg = common_cfg.clone();
            let batch_size = BatchSize::SmallInput;

            // 预热
            println!("预热开始");
            for _ in 0..50 {
                let scanner = Scanner::new(cur_db.clone(), common_cfg.clone());
                forward_scan(scanner, rocks_size / 2);
                let scanner = Scanner::new(cur_db.clone(), common_cfg.clone());
                forward_batch_scan(scanner, 128, rocks_size / 2);
            }
            println!("预热完毕");

            let vl = defaultcf_value_length.value();

            let c = c.bench_function(
                &format!(
                    "forward_scan(rocks db data size {}, value length {})",
                    rocks_size, vl
                ),
                move |b| {
                    b.iter_batched(
                        || (Scanner::new(cur_db.clone(), cfg.clone())),
                        |scanner| forward_scan(scanner, black_box(rocks_size / 2)),
                        batch_size,
                    )
                },
            );

            let scan_batch_size = vec![64, 128, 256];

            let cur_db = db.clone();
            let cfg = common_cfg.clone();

            c.bench_function_over_inputs(
                &format!(
                    "forward_batch_scan(rocks db data size {}, value length {})",
                    rocks_size, vl
                ),
                move |b, &cnt| {
                    b.iter_batched(
                        || Scanner::new(cur_db.clone(), cfg.clone()),
                        |scanner| {
                            forward_batch_scan(scanner, black_box(cnt), black_box(rocks_size / 2))
                        },
                        batch_size,
                    )
                },
                scan_batch_size,
            );
        }
    }
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
