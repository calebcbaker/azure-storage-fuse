extern crate libc;

use std::os::raw::c_char;
use std::ffi::CStr;

pub struct fuse_file_info {
    flags: i32,
    fh_old: usize,
    writepage: i32,
    direct_io: u32,
    keep_cache: u32,
    flush: u32,
    nonseekable: u32,
    flock_release: u32,
    padding: u32,
    fh: u64,
    lock_owner: u64,
}

#[no_mangle]
pub extern "C" fn azs_rust_readlink(path: *const c_char, buf: *const c_char, size: libc::size_t) -> i32{
    return 0;
}

#[no_mangle]
pub extern "C" fn azs_rust_symlink(from: *const c_char, to: *const c_char) ->i32 {
    return 0;
}

#[no_mangle]
pub extern "C" fn azs_rust_open(path: *const c_char, fi: *mut fuse_file_info) -> i32{
    return 0;
}

#[no_mangle]
pub extern "C" fn azs_rust_read(path: *const c_char, buf: *const c_char, size: libc::size_t, offset: libc::off_t, fi: *mut fuse_file_info) -> i32{
    return 0;
}

#[no_mangle]
pub extern "C" fn print_custom_string(string: *mut c_char) {
    let c_str: &CStr = unsafe { CStr::from_ptr(string)};
    let str_slice: &str = c_str.to_str().unwrap();
    println!("Hello, {} ", str_slice);
}



