use std::io::Write;
#[allow(unused)]
use std::{cmp, u8};

use crate::tikv_code::number::*;

// handle or index id
pub const ID_LEN: usize = 8;
pub const PREFIX_LEN: usize = TABLE_PREFIX_LEN + ID_LEN /*table_id*/ + SEP_LEN;
pub const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
pub const TABLE_PREFIX: &[u8] = b"t";
pub const RECORD_PREFIX_SEP: &[u8] = b"_r";
pub const INDEX_PREFIX_SEP: &[u8] = b"_i";
pub const SEP_LEN: usize = 2;
pub const TABLE_PREFIX_LEN: usize = 1;
pub const TABLE_PREFIX_KEY_LEN: usize = TABLE_PREFIX_LEN + ID_LEN;


/// `TableEncoder` encodes the table record/index prefix.
trait TableEncoder: NumberEncoder {
    fn append_table_record_prefix(&mut self, table_id: i64) {
        self.write_all(TABLE_PREFIX).unwrap();
        self.encode_i64(table_id).unwrap();
        self.write_all(RECORD_PREFIX_SEP).unwrap();
    }

    fn append_table_index_prefix(&mut self, table_id: i64) {
        self.write_all(TABLE_PREFIX).unwrap();
        self.encode_i64(table_id).unwrap();
        self.write_all(INDEX_PREFIX_SEP).unwrap();
    }
}

impl<T: Write> TableEncoder for T {}

/// Extracts table prefix from table record or index.
#[inline]
pub fn extract_table_prefix(key: &[u8]) -> &[u8] {
    if !key.starts_with(TABLE_PREFIX) || key.len() < TABLE_PREFIX_KEY_LEN {
        panic!(
            "record key or index key expected, but got {:?}",
            key
        )
    } else {
        &key[..TABLE_PREFIX_KEY_LEN]
    }
}

/// `encode_row_key` encodes the table id and record handle into a byte array.
pub fn encode_row_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN);
    // can't panic
    key.append_table_record_prefix(table_id);
    key.encode_i64(handle).unwrap();
    key
}

/// `encode_column_key` encodes the table id, row handle and column id into a byte array.
pub fn encode_column_key(table_id: i64, handle: i64, column_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN + ID_LEN);
    key.append_table_record_prefix(table_id);
    key.encode_i64(handle).unwrap();
    key.encode_i64(column_id).unwrap();
    key
}