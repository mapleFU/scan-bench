use rand::prelude::*;

use crate::schema;
use crate::tikv_code::{CF_DEFAULT, CF_WRITE};
use crate::Key;

use rocksdb::{rocksdb::Writable, WriteBatch, DB};

#[allow(unused)]
#[derive(Copy, Clone, Debug)]
pub enum ValueType {
    /// 64 bytes
    MiddleValue,
    /// 128 bytes
    LongValue,
    /// 256 bytes
    LongLongValue,
    /// 1:1:1
    MixValue,
}

const CHOICES: [usize; 3] = [64, 128, 256];

impl ValueType {
    pub fn value(&self) -> u64 {
        (match *self {
            ValueType::MiddleValue => 64,
            ValueType::LongValue => 128,
            ValueType::LongLongValue => 256,
            ValueType::MixValue => CHOICES[thread_rng().gen_range(0, 3)],
        }) as u64
    }
}

pub fn drain_data(db: &mut DB, data_scale: u64, vt: ValueType) {
    let mut rng = thread_rng();

    let batch = WriteBatch::new();

    for _ in 0..data_scale {
        let table_id = rng.gen_range(0, 30);
        let column_id = rng.gen_range(0, 2000);
        let start_ts = rng.gen_range(0, 49998);
        let commit_ts = rng.gen_range(start_ts, 50000);

        let write_key = schema::encode_row_key(table_id, column_id);
        let commit_key = Key::from_encoded_slice(&write_key);
        let commit_key = commit_key.append_ts(commit_ts).into_encoded();
        let commit_value = schema::generate_write_value();

        let start_key = Key::from_encoded_slice(&write_key);
        let start_key = start_key.append_ts(start_ts).into_encoded();
        let start_value = schema::generate_default_value(vt.value());

        batch
            .put_cf(
                db.cf_handle(CF_WRITE.as_ref()).unwrap(),
                commit_key.as_slice(),
                commit_value.as_slice(),
            )
            .unwrap();

        // key: write_key + start_ts
        // value: data_vec 应该是纯粹数据了
        batch
            .put_cf(
                db.cf_handle(CF_DEFAULT).unwrap(),
                start_key.as_slice(),
                start_value.as_slice(),
            )
            .unwrap();

        if batch.data_size() > 100 {
            db.write(&batch).unwrap();
            batch.clear();
        }
    }
    db.write(&batch).unwrap();
    println!("done");
}

#[test]
fn test_drain_data() {
    use crate::gen_db::default_test_db_with_path;
    use rocksdb::SeekKey;

    let temp_dir = tempdir::TempDir::new("data").unwrap();
    let mut db = default_test_db_with_path(temp_dir.path());
    drain_data(&mut db, 1000, ValueType::LongValue);

    let mut iter = db.iter_cf(db.cf_handle(CF_DEFAULT).unwrap());
    iter.seek(SeekKey::Start);
    let mut cnt = 0;
    while iter.valid() {
        iter.next();
        cnt += 1;
    }

    assert_eq!(cnt, 1000, "cnt should be {:?}", cnt);
}
