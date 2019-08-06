use rocksdb::{self, DB};

use std::path::Path;
use std::sync::Arc;

use crate::tikv_code::constexpr::{CF_DEFAULT, CF_WRITE};

use crate::db_opts::*;

pub fn default_test_db_with_path<P: AsRef<Path>>(path: P) -> Arc<DB> {
    let opt = default_db_options();

    let mut db;
    let db_res = DB::open(opt.clone(), path.as_ref().to_str().unwrap());
    if db_res.is_err() {
        db = DB::open_cf(
            opt,
            path.as_ref().to_str().unwrap(),
            [CF_WRITE, CF_DEFAULT].to_vec(),
        )
        .unwrap();
    } else {
        db = db_res.unwrap();
        let write_cf_cfg = default_wcf_config();
        let default_cf_cfg = default_dcf_config();

        {
            let cf_opts = write_cf_cfg;
            let ccd = rocksdb::rocksdb_options::ColumnFamilyDescriptor::new(CF_WRITE, cf_opts);
            db.create_cf(ccd).unwrap();
        }
        {
            let cf_opts = default_cf_cfg;
            let ccd = rocksdb::rocksdb_options::ColumnFamilyDescriptor::new(CF_DEFAULT, cf_opts);
            db.create_cf(ccd).unwrap();
        }
    }

    Arc::new(db)
}

pub fn default_test_db() -> Arc<DB> {
    default_test_db_with_path("data")
}
