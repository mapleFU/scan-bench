// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::ReadBytesExt;
use quick_error;

use std::io::{BufRead, Write};

use crate::tikv_code::number::{self, NumberEncoder};
use std::io;
use std::io::ErrorKind;
use std::ptr;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];
const ENC_DESC_PADDING: [u8; ENC_GROUP_SIZE] = [!0; ENC_GROUP_SIZE];

pub type BytesSlice<'a> = &'a [u8];

#[inline]
pub fn read_slice<'a>(data: &mut BytesSlice<'a>, size: usize) -> Result<BytesSlice<'a>> {
    if data.len() >= size {
        let buf = &data[0..size];
        *data = &data[size..];
        Ok(buf)
    } else {
        Err(Error::unexpected_eof())
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyLength {description("bad format key(length)")}
        KeyPadding {description("bad format key(padding)")}
        KeyNotFound {description("key not found")}
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::KeyLength => Some(Error::KeyLength),
            Error::KeyPadding => Some(Error::KeyPadding),
            Error::KeyNotFound => Some(Error::KeyNotFound),
            Error::Io(_) => None,
        }
    }
    pub fn unexpected_eof() -> Error {
        Error::Io(io::Error::new(ErrorKind::UnexpectedEof, "eof"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Returns the maximum encoded bytes size.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

pub trait BytesEncoder: NumberEncoder {
    /// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    fn encode_bytes(&mut self, key: &[u8], desc: bool) -> Result<()> {
        let len = key.len();
        let mut index = 0;
        let mut buf = [0; ENC_GROUP_SIZE];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                self.write_all(adjust_bytes_order(
                    &key[index..index + ENC_GROUP_SIZE],
                    desc,
                    &mut buf,
                ))?;
            } else {
                pad = ENC_GROUP_SIZE - remain;
                self.write_all(adjust_bytes_order(&key[index..], desc, &mut buf))?;
                if desc {
                    self.write_all(&ENC_DESC_PADDING[..pad])?;
                } else {
                    self.write_all(&ENC_ASC_PADDING[..pad])?;
                }
            }
            self.write_all(adjust_bytes_order(
                &[ENC_MARKER - (pad as u8)],
                desc,
                &mut buf,
            ))?;
            index += ENC_GROUP_SIZE;
        }
        Ok(())
    }

    /// Joins bytes with its length into a byte slice. It is more
    /// efficient in both space and time compared to `encode_bytes`. Note that the encoded
    /// result is not memcomparable.
    fn encode_compact_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.encode_var_i64(data.len() as i64).unwrap();
        self.write_all(data).map_err(From::from)
    }
}

fn adjust_bytes_order<'a>(bs: &'a [u8], desc: bool, buf: &'a mut [u8]) -> &'a [u8] {
    if desc {
        let mut buf_idx = 0;
        for &b in bs {
            buf[buf_idx] = !b;
            buf_idx += 1;
        }
        &buf[..buf_idx]
    } else {
        bs
    }
}

impl<T: Write> BytesEncoder for T {}

pub fn encode_bytes(bs: &[u8]) -> Vec<u8> {
    encode_order_bytes(bs, false)
}

pub fn encode_bytes_desc(bs: &[u8]) -> Vec<u8> {
    encode_order_bytes(bs, true)
}

fn encode_order_bytes(bs: &[u8], desc: bool) -> Vec<u8> {
    let cap = max_encoded_bytes_size(bs.len());
    let mut encoded = Vec::with_capacity(cap);
    encoded.encode_bytes(bs, desc).unwrap();
    encoded
}

/// Gets the first encoded bytes' length in compactly encoded data.
///
/// Compact-encoding includes a VarInt encoded length prefix (1 ~ 9 bytes) and N bytes payload.
/// This function gets the total bytes length of compact-encoded data, including the length prefix.
///
/// Note:
///     - This function won't check whether the bytes are encoded correctly.
///     - There can be multiple compact-encoded data, placed one by one. This function only returns
///       the length of the first one.
pub fn encoded_compact_len(mut encoded: &[u8]) -> usize {
    let last_encoded = encoded.as_ptr() as usize;
    let total_len = encoded.len();
    let vn = match number::decode_var_i64(&mut encoded) {
        Ok(vn) => vn as usize,
        Err(e) => {
            panic!("failed to decode bytes' length: {:?}", e);
            return total_len;
        }
    };
    vn + (encoded.as_ptr() as usize - last_encoded)
}

pub trait CompactBytesFromFileDecoder: BufRead {
    /// Decodes bytes which are encoded by `encode_compact_bytes` before.
    fn decode_compact_bytes(&mut self) -> Result<Vec<u8>> {
        let mut var_data = Vec::with_capacity(number::MAX_VAR_I64_LEN);
        while var_data.len() < number::MAX_VAR_U64_LEN {
            let b = self.read_u8()?;
            var_data.push(b);
            if b < 0x80 {
                break;
            }
        }
        let vn = number::decode_var_i64(&mut var_data.as_slice()).unwrap() as usize;
        let mut data = vec![0; vn];
        self.read_exact(&mut data)?;
        Ok(data)
    }
}

impl<T: BufRead> CompactBytesFromFileDecoder for T {}

/// Gets the first encoded bytes' length in memcomparable-encoded data.
///
/// Memcomparable-encoding includes a VarInt encoded length prefix (1 ~ 9 bytes) and N bytes payload.
/// This function gets the total bytes length of memcomparable-encoded data, including the length prefix.
///
/// Note:
///     - This function won't check whether the bytes are encoded correctly.
///     - There can be multiple memcomparable-encoded data, placed one by one. This function only returns
///       the length of the first one.
pub fn encoded_bytes_len(encoded: &[u8], desc: bool) -> usize {
    let mut idx = ENC_GROUP_SIZE;
    loop {
        if encoded.len() < idx + 1 {
            return encoded.len();
        }
        let marker = encoded[idx];
        if desc && marker != 0 || !desc && marker != ENC_MARKER {
            return idx + 1;
        }
        idx += ENC_GROUP_SIZE + 1;
    }
}

/// Decodes bytes which are encoded by `encode_compact_bytes` before.
pub fn decode_compact_bytes(data: &mut BytesSlice<'_>) -> Result<Vec<u8>> {
    let vn = number::decode_var_i64(data).unwrap() as usize;
    if data.len() >= vn {
        let bs = data[0..vn].to_vec();
        *data = &data[vn..];
        return Ok(bs);
    }
    Err(Error::unexpected_eof())
}

/// Decodes bytes which are encoded by `encode_bytes` before.
///
/// Please note that, data is a mut reference to slice. After calling this the
/// slice that data point to would change.
pub fn decode_bytes(data: &mut BytesSlice<'_>, desc: bool) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(data.len() / (ENC_GROUP_SIZE + 1) * ENC_GROUP_SIZE);
    let mut offset = 0;
    let chunk_len = ENC_GROUP_SIZE + 1;
    loop {
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        let next_offset = offset + chunk_len;
        let chunk = if next_offset <= data.len() {
            &data[offset..next_offset]
        } else {
            return Err(Error::unexpected_eof());
        };
        offset = next_offset;
        // the last byte in decode unit is for marker which indicates pad size
        let (&marker, bytes) = chunk.split_last().unwrap();
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };
        // no padding, just push 8 bytes
        if pad_size == 0 {
            key.write_all(bytes).unwrap();
            continue;
        }
        if pad_size > ENC_GROUP_SIZE {
            return Err(Error::KeyPadding);
        }
        // if has padding, split the padding pattern and push rest bytes
        let (bytes, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
        key.write_all(bytes).unwrap();
        let pad_byte = if desc { !0 } else { 0 };
        // check the padding pattern whether validate or not
        if padding.iter().any(|x| *x != pad_byte) {
            return Err(Error::KeyPadding);
        }

        if desc {
            for k in &mut key {
                *k = !*k;
            }
        }
        // data will point to following unencoded bytes, maybe timestamp
        *data = &data[offset..];
        return Ok(key);
    }
}

/// Decodes bytes which are encoded by `encode_bytes` before just in place without malloc.
/// Please use this instead of `decode_bytes` if possible.
pub fn decode_bytes_in_place(data: &mut Vec<u8>, desc: bool) -> Result<()> {
    let mut write_offset = 0;
    let mut read_offset = 0;
    loop {
        let marker_offset = read_offset + ENC_GROUP_SIZE;
        if marker_offset >= data.len() {
            return Err(Error::unexpected_eof());
        };

        unsafe {
            // it is semantically equivalent to C's memmove()
            // and the src and dest may overlap
            // if src == dest do nothing
            ptr::copy(
                data.as_ptr().add(read_offset),
                data.as_mut_ptr().add(write_offset),
                ENC_GROUP_SIZE,
            );
        }
        write_offset += ENC_GROUP_SIZE;
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        read_offset += ENC_GROUP_SIZE + 1;

        // the last byte in decode unit is for marker which indicates pad size
        let marker = data[marker_offset];
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };

        if pad_size > 0 {
            if pad_size > ENC_GROUP_SIZE {
                return Err(Error::KeyPadding);
            }

            // check the padding pattern whether validate or not
            let padding_slice = if desc {
                &ENC_DESC_PADDING[..pad_size]
            } else {
                &ENC_ASC_PADDING[..pad_size]
            };
            if &data[write_offset - pad_size..write_offset] != padding_slice {
                return Err(Error::KeyPadding);
            }
            unsafe {
                data.set_len(write_offset - pad_size);
            }
            if desc {
                for k in data {
                    *k = !*k;
                }
            }
            return Ok(());
        }
    }
}
