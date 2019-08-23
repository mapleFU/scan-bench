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

use nacs::{forward_batch_scan, forward_scan};
use std::rc::Rc;
use std::cell::RefCell;

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

    let mut common_write_vec = Vec::with_capacity(100 * 1024 * 1024);
    let mut common_write_vec = Rc::new(RefCell::new(common_write_vec));

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
                forward_batch_scan(scanner, 128, rocks_size / 2, &mut (*common_write_vec).borrow_mut());
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

            let cur_write_vec = common_write_vec.clone();
            c.bench_function_over_inputs(
                &format!(
                    "forward_batch_scan(rocks db data size {}, value length {})",
                    rocks_size, vl
                ),
                move |b, &cnt| {
                    b.iter_batched(
                        || (Scanner::new(cur_db.clone(), cfg.clone()), cur_write_vec.clone()),
                        |(scanner, mut v)| {
                            forward_batch_scan(
                                scanner,
                                black_box(cnt),
                                black_box(rocks_size / 2),
                                &mut (*v).borrow_mut(),
                            )
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
