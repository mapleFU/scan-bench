#![feature(test)]

// dev packages
extern crate callgrind;
extern crate cpuprofiler;
extern crate lazy_static;
extern crate valgrind_request;

extern crate test;

extern crate nacs;
extern crate profiler;

use std::sync::Arc;
use test::black_box;

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

fn bench_scan() {
    // handle config here
    let common_cfg = ScannerConfig::new(
        Some(DIST_QT1_KEY.clone().to_vec()),
        Some(DIST_QT3_KEY.clone().to_vec()),
    );

    //    let common_cfg = ScannerConfig::default();
    let test_rocks_size: Vec<u64> = vec![20000, 50000, 100000, 200000, 500000, 1000000];
    let allow_values = vec![
        ValueType::MiddleValue,
        ValueType::LongValue,
        ValueType::LongLongValue,
    ];

    let profile_end = format!(".profile");
    for rocks_size in test_rocks_size {
        for defaultcf_value_length in &allow_values {
            let temp_dir = TempDir::new_in("data", "data").unwrap();
            println!("{:?}", temp_dir.path());
            let mut db = default_test_db_with_path(temp_dir.path());
            drain_data(&mut db, rocks_size, defaultcf_value_length.clone());
            let db = Arc::new(db);
            let cur_db = db.clone();
            let cfg = common_cfg.clone();

            let mut current_vec = Vec::with_capacity(100 * 1024 * 1024);
            // 预热
            println!("预热开始");

            for _ in 0..10 {
                let scanner = Scanner::new(cur_db.clone(), common_cfg.clone());
                forward_scan(scanner, rocks_size / 2);
                let scanner = Scanner::new(cur_db.clone(), common_cfg.clone());
                forward_batch_scan(scanner, 128, rocks_size / 2, &mut current_vec);
            }
            println!("预热完毕");
            // value length of default value field.
            let vl = defaultcf_value_length.value();

            let scan_batch_size = vec![64, 256, 1024];

            let scale = format!("_{}_{}", rocks_size, vl);

            let scanner_forward = Scanner::new(cur_db.clone(), cfg.clone());

            let scanner_forward_name = format!("forward_scan") + &scale + &profile_end;
            println!("start_task: {}", scanner_forward_name);

            profiler::start(&scanner_forward_name);
            forward_scan(scanner_forward, black_box(rocks_size / 2));
            assert!(profiler::stop());

            let scanner_forward_batch_name = format!("forward_scan_batch") + &scale;
            for sbc in scan_batch_size {
                let scanner_forward = Scanner::new(cur_db.clone(), cfg.clone());
                // TODO: should bench on this
                let name = scanner_forward_batch_name.clone() + &format!("_{}", sbc) + &profile_end;
                println!("start_task name {}", name);
                profiler::start(&name);
                forward_batch_scan(
                    scanner_forward,
                    black_box(sbc),
                    black_box(rocks_size / 2),
                    &mut current_vec,
                );
                assert!(profiler::stop());
            }
        }
    }
}

fn main() {
    bench_scan()
}
