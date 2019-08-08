#![feature(repeat_generic_slice)]

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
