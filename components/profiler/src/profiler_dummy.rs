// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// Start profiling. Always returns false if `profiling` feature is not enabled.
#[inline]
pub fn start(_name: impl AsRef<str>) -> bool {
    // Do nothing
    println!("profiling feature not enabled");
    false
}

/// Stop profiling. Always returns false if `profiling` feature is not enabled.
#[inline]
pub fn stop() -> bool {
    println!("profiling feature not enabled");
    // Do nothing
    false
}
