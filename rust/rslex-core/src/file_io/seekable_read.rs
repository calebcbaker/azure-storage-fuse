use std::{
    fs::File,
    io::{Cursor, Read, Seek},
};

pub trait SeekableRead: Read + Seek + Send {
    fn length(&self) -> u64;
}

impl<T: AsRef<[u8]> + Send> SeekableRead for Cursor<T> {
    fn length(&self) -> u64 {
        self.get_ref().as_ref().len() as u64
    }
}

impl SeekableRead for File {
    fn length(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0_u64)
    }
}
