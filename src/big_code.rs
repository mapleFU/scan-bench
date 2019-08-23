
fn process_kv_pair(
    key: &[u8],
    value: &[u8],
    columns: &mut LazyBatchColumnVec,
) -> Result<()> {
    use crate::codec::{datum, table};
    use tikv_util::codec::number;

    let columns_len = self.schema.len();
    let mut decoded_columns = 0;

    if !self.handle_indices.is_empty() {
        let handle_id = table::decode_handle(key)?;
        for handle_index in &self.handle_indices {
            // TODO: We should avoid calling `push_int` repeatedly. Instead we should specialize
            // a `&mut Vec` first. However it is hard to program due to lifetime restriction.
            columns[*handle_index]
                .mut_decoded()
                .push_int(Some(handle_id));
            decoded_columns += 1;
            self.is_column_filled[*handle_index] = true;
        }
    }

    if value.is_empty() || (value.len() == 1 && value[0] == datum::NIL_FLAG) {
        // Do nothing
    } else {
        // The layout of value is: [col_id_1, value_1, col_id_2, value_2, ...]
        // where each element is datum encoded.
        // The column id datum must be in var i64 type.
        let mut remaining = value;
        while !remaining.is_empty() && decoded_columns < columns_len {
            if remaining[0] != datum::VAR_INT_FLAG {
                return Err(other_err!(
                        "Unable to decode row: column id must be VAR_INT"
                    ));
            }
            remaining = &remaining[1..];
            let column_id = box_try!(number::decode_var_i64(&mut remaining));
            let (val, new_remaining) = datum::split_datum(remaining, false)?;
            // Note: The produced columns may be not in the same length if there is error due
            // to corrupted data. It will be handled in `ScanExecutor`.
            let some_index = self.column_id_index.get(&column_id);
            if let Some(index) = some_index {
                let index = *index;
                if !self.is_column_filled[index] {
                    columns[index].mut_raw().push(val);
                    decoded_columns += 1;
                    self.is_column_filled[index] = true;
                } else {
                    // This indicates that there are duplicated elements in the row, which is
                    // unexpected. We won't abort the request or overwrite the previous element,
                    // but will output a log anyway.
                    warn!(
                            "Ignored duplicated row datum in table scan";
                            "key" => hex::encode_upper(&key),
                            "value" => hex::encode_upper(&value),
                            "dup_column_id" => column_id,
                        );
                }
            }
            remaining = new_remaining;
        }
    }

    // Some fields may be missing in the row, we push corresponding default value to make all
    // columns in same length.
    for i in 0..columns_len {
        if !self.is_column_filled[i] {
            // Missing fields must not be a primary key, so it must be
            // `LazyBatchColumn::raw`.

            let default_value = if !self.columns_default_value[i].is_empty() {
                // default value is provided, use the default value
                self.columns_default_value[i].as_slice()
            } else if !self.schema[i]
                .as_accessor()
                .flag()
                .contains(tidb_query_datatype::FieldTypeFlag::NOT_NULL)
            {
                // NULL is allowed, use NULL
                datum::DATUM_DATA_NULL
            } else {
                return Err(other_err!(
                        "Data is corrupted, missing data for NOT NULL column (offset = {})",
                        i
                    ));
            };

            columns[i].mut_raw().push(default_value);
        } else {
            // Reset to not-filled, prepare for next function call.
            self.is_column_filled[i] = false;
        }
    }

    Ok(())
}