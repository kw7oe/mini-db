use crate::pager::PAGE_SIZE;
use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::PathBuf,
};

pub struct DiskManager {
    write_file: File,
    read_file: File,
    file_len: usize,
}

impl DiskManager {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();

        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let read_file = File::open(&path).unwrap();
        let file_len = read_file.metadata().unwrap().len() as usize;

        Self {
            write_file,
            read_file,
            file_len,
        }
    }

    pub fn write_page(&mut self, page_id: usize, page_bytes: &[u8]) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE;
        self.read_file.seek(SeekFrom::Start(offset as u64))?;
        self.write_file.write_all(page_bytes)?;
        self.write_file.flush()
    }

    pub fn read_page(&mut self, page_id: usize) -> Result<[u8; PAGE_SIZE], std::io::Error> {
        let offset = page_id * PAGE_SIZE;

        // TODO: probably need to handle when offset < file_len
        self.read_file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer = [0; PAGE_SIZE];
        self.read_file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}
