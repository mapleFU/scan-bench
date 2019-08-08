use rocksdb::{self, DB};

use std::path::Path;

use crate::tikv_code::constexpr::{CF_DEFAULT, CF_WRITE};

use crate::db_opts::*;

pub fn default_test_db_with_path<P: AsRef<Path>>(path: P) -> DB {
    let opt = default_db_options();

    let mut db_res = DB::open(opt.clone(), path.as_ref().to_str().unwrap()).unwrap();
    if db_res.cf_handle(CF_DEFAULT).is_none() {
        let default_cf_cfg = default_dcf_config();
        let cf_opts = default_cf_cfg;
        let ccd = rocksdb::rocksdb_options::ColumnFamilyDescriptor::new(CF_DEFAULT, cf_opts);
        db_res.create_cf(ccd).unwrap();
    }
    if db_res.cf_handle(CF_WRITE).is_none() {
        let write_cf_cfg = default_wcf_config();
        let cf_opts = write_cf_cfg;
        let ccd = rocksdb::rocksdb_options::ColumnFamilyDescriptor::new(CF_WRITE, cf_opts);
        db_res.create_cf(ccd).unwrap();
    }

    db_res
}

pub fn default_test_db() -> DB {
    default_test_db_with_path("data")
}
