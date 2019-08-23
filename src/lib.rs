#![feature(repeat_generic_slice)]
#![feature(test)]

extern crate rocksdb;

#[macro_use]
extern crate quick_error;

extern crate num_traits;

#[allow(unused)]
#[macro_use]
extern crate serde;

extern crate sys_info;

extern crate hex;

extern crate rand;

#[allow(unused)]
#[macro_use]
extern crate slog;

extern crate test;

mod db_opts;
mod tikv_code;

pub mod drain;
pub mod gen_db;
#[allow(unused)]
mod schema;

use tikv_code::key::Key;

pub use db_opts::build_read_opts;
pub use drain::*;
pub use gen_db::default_test_db_with_path;
pub use tikv_code::constexpr::*;

#[allow(unused)]
use rocksdb::rocksdb::{DBIterator, Snapshot, Writable};
#[allow(unused)]
use rocksdb::{ReadOptions, SeekKey, WriteBatch, WriteOptions, DB};

use std::sync::Arc;

#[allow(unused)]
pub const LOWEST_KEY_STR: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114, 128, 0, 0, 0, 0, 0, 0, 0,
];
#[allow(unused)]
pub const LARGEST_KEY_STR: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 30, 95, 114, 128, 0, 0, 0, 0, 0, 7, 208,
];
#[allow(unused)]
pub const DIST_QT1_KEY: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 7, 95, 114, 128, 0, 0, 0, 0, 0, 0, 0,
];
#[allow(unused)]
pub const DIST_QT3_KEY: [u8; 19] = [
    116, 128, 0, 0, 0, 0, 0, 0, 22, 95, 114, 128, 0, 0, 0, 0, 0, 7, 208,
];

#[derive(Clone, Debug)]
pub struct ScannerConfig {
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

impl ScannerConfig {
    pub fn new(lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> ScannerConfig {
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

pub struct Scanner {
    pub snap: Snapshot<Arc<DB>>,
    pub iter_write: DBIterator<Arc<DB>>,
    pub iter_default: DBIterator<Arc<DB>>,

    /// ScannerConfig here holds the lower and upper.
    /// In this poc, it doesn't need to parse the key and fetch the data
    /// from it. It just compare whether the values are equal.
    #[allow(unused)]
    pub cfg: ScannerConfig,
}

impl Scanner {
    pub fn new(db_ref: Arc<DB>, cfg: ScannerConfig) -> Scanner {
        let cloned_ref = db_ref.clone();

        let snap = Snapshot::new(cloned_ref.clone());
        let mut read_write_opts = build_read_opts(cfg.lower_bound.clone(), cfg.upper_bound.clone());
        read_write_opts.fill_cache(true);

        let mut iter_write = DBIterator::new_cf(
            cloned_ref.clone(),
            db_ref.cf_handle(CF_WRITE).unwrap(),
            read_write_opts,
        );
        iter_write.seek(SeekKey::Key(&cfg.lower_bound));

        let mut read_default_opts = ReadOptions::new();
        read_default_opts.fill_cache(true);

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

use test::black_box;

pub fn forward_scan(mut scanner: Scanner, loop_cnt: u64) {
    for _ in 0..loop_cnt {
        // fetch next for "write" field
        scanner.iter_write.next();
        black_box((scanner.iter_write.key(), scanner.iter_write.value()));

        // fetch next for "default" field
        scanner.iter_default.next();
        black_box((scanner.iter_default.key(), scanner.iter_default.value()));
    }
}

pub fn forward_batch_scan(mut scanner: Scanner, batch_size: u64, loop_cnt: u64) {
    let mut write_cache = Vec::with_capacity(1024 * 1024 * 100);

    for _ in 0..loop_cnt / batch_size {
        for _ in 0..batch_size {
            scanner.iter_write.next();
            write_cache.extend_from_slice(scanner.iter_write.key());
            write_cache.extend_from_slice(scanner.iter_write.value());
        }

        for _ in 0..batch_size {
            scanner.iter_default.next();
            black_box(scanner.iter_default.key());
            write_cache.extend_from_slice(scanner.iter_default.value());
        }
        write_cache.clear();
    }

    let sz = loop_cnt % batch_size;
    for _ in 0..sz {
        scanner.iter_write.next();
        write_cache.extend_from_slice(scanner.iter_write.key());
        write_cache.extend_from_slice(scanner.iter_write.value());
    }

    for _ in 0..sz {
        scanner.iter_default.next();
        black_box(scanner.iter_default.key());
        write_cache.extend_from_slice(scanner.iter_default.value());
    }
    write_cache.clear();
}