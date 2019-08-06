// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{BigEndian, ByteOrder, LittleEndian, WriteBytesExt};
use std::io::{self, ErrorKind, Write};
use std::mem;

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


const SIGN_MARK: u64 = 0x8000000000000000;
pub const MAX_VAR_I64_LEN: usize = 10;
pub const MAX_VAR_U64_LEN: usize = 10;
pub const U64_SIZE: usize = 8;
pub const I64_SIZE: usize = 8;
pub const F64_SIZE: usize = 8;

fn order_encode_i64(v: i64) -> u64 {
    v as u64 ^ SIGN_MARK
}

fn order_decode_i64(u: u64) -> i64 {
    (u ^ SIGN_MARK) as i64
}

fn order_encode_f64(v: f64) -> u64 {
    let u = v.to_bits();
    if v.is_sign_positive() {
        u | SIGN_MARK
    } else {
        !u
    }
}

fn order_decode_f64(u: u64) -> f64 {
    let u = if u & SIGN_MARK > 0 {
        u & (!SIGN_MARK)
    } else {
        !u
    };
    f64::from_bits(u)
}

pub trait NumberEncoder: Write {
    /// Writes the encoded value to buf.
    /// It guarantees that the encoded value is in ascending order for comparison.
    fn encode_i64(&mut self, v: i64) -> Result<()> {
        let u = order_encode_i64(v);
        self.encode_u64(u)
    }

    /// Writes the encoded value to buf.
    /// It guarantees that the encoded value is in descending order for comparison.
    fn encode_i64_desc(&mut self, v: i64) -> Result<()> {
        let u = order_encode_i64(v);
        self.encode_u64_desc(u)
    }

    /// Writes the encoded value to slice buf.
    /// It guarantees that the encoded value is in ascending order for comparison.
    fn encode_u64(&mut self, v: u64) -> Result<()> {
        self.write_u64::<BigEndian>(v).map_err(From::from)
    }

    /// Writes the encoded value to slice buf.
    /// It guarantees that the encoded value is in descending order for comparison.
    fn encode_u64_desc(&mut self, v: u64) -> Result<()> {
        self.write_u64::<BigEndian>(!v).map_err(From::from)
    }

    /// Writes the encoded value to slice buf in big endian order.
    fn encode_u32(&mut self, v: u32) -> Result<()> {
        self.write_u32::<BigEndian>(v).map_err(From::from)
    }

    /// Writes the encoded value to slice buf in big endian order.
    fn encode_u16(&mut self, v: u16) -> Result<()> {
        self.write_u16::<BigEndian>(v).map_err(From::from)
    }

    /// Writes the encoded value to slice buf.
    /// Note that the encoded result is not memcomparable.
    fn encode_var_i64(&mut self, v: i64) -> Result<()> {
        let mut vx = (v as u64) << 1;
        if v < 0 {
            vx = !vx;
        }
        self.encode_var_u64(vx)
    }

    /// Writes the encoded value to slice buf.
    /// Note that the encoded result is not memcomparable.
    fn encode_var_u64(&mut self, mut v: u64) -> Result<()> {
        while v >= 0x80 {
            self.write_u8(v as u8 | 0x80)?;
            v >>= 7;
        }
        self.write_u8(v as u8).map_err(From::from)
    }

    /// Writes the encoded value to slice buf.
    /// It guarantees that the encoded value is in ascending order for comparison.
    fn encode_f64(&mut self, f: f64) -> Result<()> {
        let u = order_encode_f64(f);
        self.encode_u64(u)
    }

    /// Writes the encoded value to slice buf.
    /// It guarantees that the encoded value is in descending order for comparison.
    fn encode_f64_desc(&mut self, f: f64) -> Result<()> {
        let u = order_encode_f64(f);
        self.encode_u64_desc(u)
    }

    /// Writes `u16` numbers in little endian order.
    fn encode_u16_le(&mut self, v: u16) -> Result<()> {
        self.write_u16::<LittleEndian>(v).map_err(From::from)
    }

    /// Writes `u32` numbers in little endian order.
    fn encode_u32_le(&mut self, v: u32) -> Result<()> {
        self.write_u32::<LittleEndian>(v).map_err(From::from)
    }

    /// Writes `i32` numbers in little endian order.
    fn encode_i32_le(&mut self, v: i32) -> Result<()> {
        self.write_i32::<LittleEndian>(v).map_err(From::from)
    }

    /// Writes `f64` numbers in little endian order.
    fn encode_f64_le(&mut self, v: f64) -> Result<()> {
        self.write_f64::<LittleEndian>(v).map_err(From::from)
    }

    /// Writes `i64` numbers in little endian order.
    fn encode_i64_le(&mut self, v: i64) -> Result<()> {
        self.write_i64::<LittleEndian>(v).map_err(From::from)
    }

    /// Writes `u64` numbers in little endian order.
    fn encode_u64_le(&mut self, v: u64) -> Result<()> {
        self.write_u64::<LittleEndian>(v).map_err(From::from)
    }
}

impl<T: Write> NumberEncoder for T {}

#[inline]
fn read_num_bytes<T, F>(size: usize, data: &mut &[u8], f: F) -> Result<T>
    where
        F: Fn(&[u8]) -> T,
{
    if data.len() >= size {
        let buf = &data[..size];
        *data = &data[size..];
        return Ok(f(buf));
    }
    Err(Error::unexpected_eof())
}

/// Decodes value encoded by `encode_i64` before.
#[inline]
pub fn decode_i64(data: &mut BytesSlice<'_>) -> Result<i64> {
    decode_u64(data).map(order_decode_i64)
}

/// Decodes value encoded by `encode_i64_desc` before.
#[inline]
pub fn decode_i64_desc(data: &mut BytesSlice<'_>) -> Result<i64> {
    decode_u64_desc(data).map(order_decode_i64)
}

/// Decodes value encoded by `encode_u64` before.
#[inline]
pub fn decode_u64(data: &mut BytesSlice<'_>) -> Result<u64> {
    read_num_bytes(mem::size_of::<u64>(), data, BigEndian::read_u64)
}

/// Decodes value encoded by `encode_u32` before.
#[inline]
pub fn decode_u32(data: &mut BytesSlice<'_>) -> Result<u32> {
    read_num_bytes(mem::size_of::<u32>(), data, BigEndian::read_u32)
}

/// Decodes value encoded by `encode_u16` before.
#[inline]
pub fn decode_u16(data: &mut BytesSlice<'_>) -> Result<u16> {
    read_num_bytes(mem::size_of::<u16>(), data, BigEndian::read_u16)
}

/// Decodes value encoded by `encode_u64_desc` before.
#[inline]
pub fn decode_u64_desc(data: &mut BytesSlice<'_>) -> Result<u64> {
    let v = decode_u64(data)?;
    Ok(!v)
}

/// Decodes value encoded by `encode_var_i64` before.
#[inline]
pub fn decode_var_i64(data: &mut BytesSlice<'_>) -> Result<i64> {
    let v = decode_var_u64(data)?;
    let vx = v >> 1;
    if v & 1 == 0 {
        Ok(vx as i64)
    } else {
        Ok(!vx as i64)
    }
}

/// Decodes value encoded by `encode_var_u64` before.
#[inline]
pub fn decode_var_u64(data: &mut BytesSlice<'_>) -> Result<u64> {
    if !data.is_empty() {
        // process with value < 127 independently at first
        // since it matches most of the cases.
        if data[0] < 0x80 {
            let res = u64::from(data[0]) & 0x7f;
            *data = unsafe { data.get_unchecked(1..) };
            return Ok(res);
        }

        // process with data's len >=10 or data ends with var u64
        if data.len() >= 10 || *data.last().unwrap() < 0x80 {
            let mut res = 0;
            for i in 0..9 {
                let b = unsafe { *data.get_unchecked(i) };
                res |= (u64::from(b) & 0x7f) << (i * 7);
                if b < 0x80 {
                    *data = unsafe { data.get_unchecked(i + 1..) };
                    return Ok(res);
                }
            }
            let b = unsafe { *data.get_unchecked(9) };
            if b <= 1 {
                res |= ((u64::from(b)) & 0x7f) << (9 * 7);
                *data = unsafe { data.get_unchecked(10..) };
                return Ok(res);
            }
            return Err(Error::Io(io::Error::new(
                ErrorKind::InvalidData,
                "overflow",
            )));
        }
    }

    // process data's len < 10 && data not end with var u64.
    let mut res = 0;
    for i in 0..data.len() {
        let b = data[i];
        res |= (u64::from(b) & 0x7f) << (i * 7);
        if b < 0x80 {
            *data = unsafe { data.get_unchecked(i + 1..) };
            return Ok(res);
        }
    }
    Err(Error::unexpected_eof())
}

/// Decodes value encoded by `encode_f64` before.
#[inline]
pub fn decode_f64(data: &mut BytesSlice<'_>) -> Result<f64> {
    decode_u64(data).map(order_decode_f64)
}

/// Decodes value encoded by `encode_f64_desc` before.
#[inline]
pub fn decode_f64_desc(data: &mut BytesSlice<'_>) -> Result<f64> {
    decode_u64_desc(data).map(order_decode_f64)
}

/// Decodes value encoded by `encode_u16_le` before.
#[inline]
pub fn decode_u16_le(data: &mut BytesSlice<'_>) -> Result<u16> {
    read_num_bytes(mem::size_of::<u16>(), data, LittleEndian::read_u16)
}

/// Decodes value encoded by `encode_u32_le` before.
#[inline]
pub fn decode_u32_le(data: &mut BytesSlice<'_>) -> Result<u32> {
    read_num_bytes(mem::size_of::<u32>(), data, LittleEndian::read_u32)
}

/// Decodes value encoded by `encode_i32_le` before.
#[inline]
pub fn decode_i32_le(data: &mut BytesSlice<'_>) -> Result<i32> {
    read_num_bytes(mem::size_of::<i32>(), data, LittleEndian::read_i32)
}

/// Decodes value encoded by `encode_f64_le` before.
#[inline]
pub fn decode_f64_le(data: &mut BytesSlice<'_>) -> Result<f64> {
    read_num_bytes(mem::size_of::<f64>(), data, LittleEndian::read_f64)
}

/// Decodes value encoded by `encode_i64_le` before.
#[inline]
pub fn decode_i64_le(data: &mut BytesSlice<'_>) -> Result<i64> {
    let v = decode_u64_le(data)?;
    Ok(v as i64)
}

/// Decodes value encoded by `encode_u64_le` before.
#[inline]
pub fn decode_u64_le(data: &mut BytesSlice<'_>) -> Result<u64> {
    read_num_bytes(mem::size_of::<u64>(), data, LittleEndian::read_u64)
}

#[inline]
pub fn read_u8(data: &mut BytesSlice<'_>) -> Result<u8> {
    if !data.is_empty() {
        let v = data[0];
        *data = &data[1..];
        Ok(v)
    } else {
        Err(Error::unexpected_eof())
    }
}