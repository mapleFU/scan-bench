#[macro_use]
extern crate criterion;
#[macro_use]
extern crate slog;

use slog::*;

extern crate nacs;

use criterion::*;

use std::sync::Arc;

use tempdir::TempDir;

#[allow(unused)]
use rocksdb::rocksdb::{DBIterator, Snapshot, Writable};
#[allow(unused)]
use rocksdb::{ReadOptions, SeekKey, WriteBatch, WriteOptions, DB};

use nacs::{
    build_read_opts, default_test_db_with_path, drain_data, ValueType, CF_DEFAULT, CF_WRITE,
};

#[allow(unused)]
const LOWEST_KEY_STR: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114, 128, 0, 0, 0, 0, 0, 0, 0,
];
#[allow(unused)]
const LARGEST_KEY_STR: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 30, 95, 114, 128, 0, 0, 0, 0, 0, 7, 208,
];
#[allow(unused)]
const DIST_QT1_KEY: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 7, 95, 114, 128, 0, 0, 0, 0, 0, 0, 0,
];
#[allow(unused)]
const DIST_QT3_KEY: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 22, 95, 114, 128, 0, 0, 0, 0, 0, 7, 208,
];

#[derive(Clone, Debug)]
struct ScannerConfig {
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
}

impl ScannerConfig {
    fn new(lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> ScannerConfig {
        let lower_bound = match lower_bound {
            None => LOWEST_KEY_STR.clone().to_vec(),
            Some(v) => v,
        };

        let upper_bound = match upper_bound {
            None => LARGEST_KEY_STR.clone().to_vec(),
            Some(v) => v,
        };

        ScannerConfig {
            lower_bound,
            upper_bound,
        }
    }
}

impl Default for ScannerConfig {
    /// 全表扫
    fn default() -> Self {
        ScannerConfig::new(None, None)
    }
}

struct Scanner {
    snap: Snapshot<Arc<DB>>,
    iter_write: DBIterator<Arc<DB>>,
    iter_default: DBIterator<Arc<DB>>,

    /// ScannerConfig here holds the lower and upper.
    /// In this poc, it doesn't need to parse the key and fetch the data
    /// from it. It just compare whether the values are equal.
    #[allow(unused)]
    cfg: ScannerConfig,
}

impl Scanner {
    fn new(db_ref: Arc<DB>, cfg: ScannerConfig) -> Scanner {
        let cloned_ref = db_ref.clone();

        let snap = Snapshot::new(cloned_ref.clone());
        let read_write_opts = build_read_opts(cfg.lower_bound.clone(), cfg.upper_bound.clone());

        let mut iter_write = DBIterator::new_cf(
            cloned_ref.clone(),
            db_ref.cf_handle(CF_WRITE).unwrap(),
            read_write_opts,
        );
        iter_write.seek(SeekKey::Key(&cfg.lower_bound));

        let read_default_opts = build_read_opts(cfg.lower_bound.clone(), cfg.upper_bound.clone());
        let mut iter_default = DBIterator::new_cf(
            cloned_ref.clone(),
            db_ref.cf_handle(CF_DEFAULT).unwrap(),
            read_default_opts,
        );
        iter_default.seek(SeekKey::Key(&cfg.lower_bound));

        Scanner {
            snap,
            iter_write,
            iter_default,

            cfg,
        }
    }
}

fn forward_scan(mut scanner: Scanner, loop_cnt: u64) {
    let mut record = 0;
    for _ in 0..loop_cnt {
        if !scanner.iter_write.next() {
            break;
        }
        black_box((scanner.iter_write.key(), scanner.iter_write.value()));

        // read
        if !scanner.iter_default.next() {
            break;
        }
        record = record + 1;
        black_box((scanner.iter_default.key(), scanner.iter_default.value()));
    }
    println!("forward-scan {}", record);
}

fn forward_batch_scan(mut scanner: Scanner, batch_size: u64, loop_cnt: u64) {
    let mut record = 0;
    let outer_loop = loop_cnt / batch_size;
    let other_loop = loop_cnt % batch_size;

    for _ in 0..outer_loop {
        for _ in 0..batch_size {
            if !scanner.iter_write.next() {
                break;
            }
            black_box((scanner.iter_write.key(), scanner.iter_write.value()));
        }

        for _ in 0..batch_size {
            if !scanner.iter_default.next() {
                println!("forward-batch-scan {}", record);
                return;
            }
            record += 1;
            black_box((scanner.iter_default.key(), scanner.iter_default.value()));
        }
    }
    for _ in 0..other_loop {
        if !scanner.iter_write.next() {
            break;
        }
        black_box((scanner.iter_write.key(), scanner.iter_write.value()));
    }

    for _ in 0..other_loop {
        if !scanner.iter_default.next() {
            return;
        }
        record += 1;
        black_box((scanner.iter_default.key(), scanner.iter_default.value()));
    }
    println!("forward-batch-scan {}", record);
}

fn bench_scan(c: &mut Criterion) {
    // handle config here
    let common_cfg = ScannerConfig::new(
        Some(DIST_QT1_KEY.clone().to_vec()),
        Some(DIST_QT3_KEY.clone().to_vec()),
    );
    //    let common_cfg = ScannerConfig::default();

    let test_rocks_size: Vec<u64> = vec![20000];
    let allow_values = vec![
        ValueType::MiddleValue,
        ValueType::LongValue,
        ValueType::LongLongValue,
    ];

    for rocks_size in test_rocks_size {
        for defaultcf_value_length in &allow_values {
            let temp_dir = tempdir::TempDir::new_in("data", "data").unwrap();
            println!("{:?}", temp_dir.path());
            let mut db = default_test_db_with_path(temp_dir.path());
            drain_data(&mut db, rocks_size, defaultcf_value_length.clone());
            let db = Arc::new(db);

            let cur_db = db.clone();
            let cfg = common_cfg.clone();
            let batch_size = BatchSize::SmallInput;

            let vl = defaultcf_value_length.value();

            let scan_batch_size = vec![128];

            //            let base_line = Fun::new("non-batch", move |b: &mut Bencher, cfg: &ScannerConfig| {
            //                b.iter_batched(
            //                    || (Scanner::new(cur_db.clone(), cfg.clone())),
            //                    |scanner| forward_scan(scanner, rocks_size / 2),
            //                    batch_size.clone(),
            //                )
            //            });
            //
            //            let mut func_vec = vec![base_line];
            //            for sbc in scan_batch_size {
            //                let cur_func = Fun::new(&format!("batch/{}", sbc), move |b: &mut Bencher, cfg: &ScannerConfig| {
            //                    b.iter_batched(
            //                        || Scanner::new(cur_db.clone(), cfg.clone()),
            //                        |scanner| forward_batch_scan(scanner, black_box(sbc), rocks_size / 2),
            //                        batch_size.clone(),
            //                    )
            //                });
            //
            //                func_vec.push(cur_func);
            //            }
            //            c.bench_functions(
            //                &format!(
            //                    "forward-scan(rocks db data size {}, value length {})",
            //                    rocks_size, vl
            //                ),
            //                func_vec,
            //                common_cfg,
            //            );

//            c.bench_function(
//                &format!(
//                    "forward_scan(rocks db data size {}, value length {})",
//                    rocks_size, vl
//                ),
//                move |b| {
//                    b.iter_batched(
//                        || (Scanner::new(cur_db.clone(), cfg.clone())),
//                        |scanner| forward_scan(scanner, rocks_size / 2),
//                        batch_size,
//                    )
//                },
//            );

            let scan_batch_size = vec![128];

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
                        |scanner| forward_batch_scan(scanner, black_box(cnt), rocks_size / 2),
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
