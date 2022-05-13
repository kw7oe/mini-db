use super::pager::PAGE_SIZE;
use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::Path,
    sync::Mutex,
};

#[derive(Debug)]
pub struct DiskManager {
    write_file: Mutex<File>,
    read_file: Mutex<File>,
    pub file_len: usize,
}

impl DiskManager {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let read_file = File::open(&path).unwrap();
        let file_len = read_file.metadata().unwrap().len() as usize;

        Self {
            write_file: Mutex::new(write_file),
            read_file: Mutex::new(read_file),
            file_len,
        }
    }

    pub fn write_page(&self, page_id: usize, page_bytes: &[u8]) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE;
        let mut write_file = self.write_file.lock().unwrap();
        write_file.seek(SeekFrom::Start(offset as u64))?;
        write_file.write_all(page_bytes)?;
        write_file.flush()
    }

    pub fn read_page(&self, page_id: usize) -> Result<[u8; PAGE_SIZE], std::io::Error> {
        let offset = page_id * PAGE_SIZE;

        // TODO: probably need to handle when offset < file_len
        let mut read_file = self.read_file.lock().unwrap();
        read_file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer = [0; PAGE_SIZE];
        read_file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn read_file_concurrently() {
        let disk_manager = Arc::new(DiskManager::new("test_file"));

        // Setup file
        for i in 0..8 {
            disk_manager.write_page(i, &[i as u8; 4096]).unwrap();
        }

        // Try 1000 times since concurrency bugs sometime occurs sometimes don't
        for _ in 0..1000 {
            let mut handles = vec![];
            for i in 0..8 {
                let disk_manager = disk_manager.clone();
                let handle = thread::spawn(move || disk_manager.read_page(i).unwrap());
                handles.push((i, handle));
            }

            for (i, handle) in handles {
                let result = handle.join().unwrap();
                assert_eq!(result, [i as u8; 4096]);
            }
        }

        let _ = std::fs::remove_file("test_file");
    }

    #[test]
    fn write_file_concurrently() {
        let disk_manager = Arc::new(DiskManager::new("test_file"));

        let mut handles = vec![];
        for i in 0..8 {
            let disk_manager = disk_manager.clone();
            let handle =
                thread::spawn(move || disk_manager.write_page(i, &[i as u8; 4096]).unwrap());
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..8 {
            let result = disk_manager.read_page(i).unwrap();
            assert_eq!(result, [i as u8; 4096]);
        }

        let _ = std::fs::remove_file("test_file");
    }

    #[test]
    fn write_and_read_file_concurrently() {
        let disk_manager = Arc::new(DiskManager::new("test_file"));

        // Setup file
        for i in 0..8 {
            disk_manager.write_page(i, &[i as u8; 4096]).unwrap();
        }

        // Read concurrently
        let mut read_handles = vec![];
        for i in 0..8 {
            let disk_manager = disk_manager.clone();
            let handle = thread::spawn(move || disk_manager.read_page(i).unwrap());
            read_handles.push((i, handle));
        }

        // Write concurrently
        let mut write_handles = vec![];
        for i in 8..16 {
            let disk_manager = disk_manager.clone();
            let handle =
                thread::spawn(move || disk_manager.write_page(i, &[i as u8; 4096]).unwrap());
            write_handles.push(handle);
        }

        for handle in write_handles {
            handle.join().unwrap();
        }

        for (i, handle) in read_handles {
            let result = handle.join().unwrap();
            assert_eq!(result, [i as u8; 4096]);
        }

        // Verify write
        for i in 8..16 {
            let result = disk_manager.read_page(i).unwrap();
            assert_eq!(result, [i as u8; 4096]);
        }

        let _ = std::fs::remove_file("test_file");
    }
}
