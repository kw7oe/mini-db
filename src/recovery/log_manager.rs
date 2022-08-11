use tracing::trace;

use super::log_record::LogRecord;
use crate::storage::DiskManager;
use std::{path::Path, sync::atomic::AtomicU32, sync::Mutex, thread::JoinHandle};

const LOG_BUFFER_SIZE: usize = 4096;

struct LogManager {
    disk_manager: DiskManager,
    next_lsn: AtomicU32,
    persistent_lsn: Option<AtomicU32>,

    // Alternatively, we should wrap the following 3 fields
    // in its own data structure and so we can just use a single Mutex to
    // wrap around it.
    log_buffer: Mutex<[u8; LOG_BUFFER_SIZE]>,
    flush_buffer: Mutex<[u8; LOG_BUFFER_SIZE]>,
    pub offset: Mutex<usize>,

    join_handle: Option<JoinHandle<()>>,
}

impl LogManager {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            disk_manager: DiskManager::new(path),
            next_lsn: AtomicU32::new(1),
            persistent_lsn: None,
            log_buffer: Mutex::new([0; LOG_BUFFER_SIZE]),
            flush_buffer: Mutex::new([0; LOG_BUFFER_SIZE]),
            offset: Mutex::new(0),
            join_handle: None,
        }
    }

    pub fn flush(&self) {
        trace!("flush WAL to disk");
        let flush_buffer = self.flush_buffer.lock().unwrap();
        self.disk_manager.write_page(1, &*flush_buffer).unwrap();
    }

    pub fn next_lsn(&self) -> u32 {
        self.next_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn persistent_lsn(&self) -> Option<u32> {
        self.persistent_lsn
            .as_ref()
            .map(|p_lsn| p_lsn.load(std::sync::atomic::Ordering::SeqCst))
    }

    pub fn offset(&self) -> usize {
        *self.offset.lock().unwrap()
    }

    pub fn log_buffer(&self) -> [u8; 4096] {
        *self.log_buffer.lock().unwrap()
    }

    pub fn append_log(&self, log_record: &mut LogRecord) -> u32 {
        // One Lock
        let lsn = self
            .next_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        log_record.lsn = Some(lsn);

        // TODO: Append to buffer or flush to disk if buffer full?
        let bytes = bincode::serialize(&log_record).unwrap();

        // Another lock
        let mut offset = self.offset.lock().unwrap();
        let mut log_buffer = self.log_buffer.lock().unwrap();
        let mut end = *offset + bytes.len();

        // If our log buffer is full, we swap it with the flush_buffer
        // so we can just flush to disk from the flush buffer while
        // continue using the log buffer.
        if end > log_buffer.len() {
            println!("log buffer full, swapping with flush_buffer");

            let mut flush_buffer = self.flush_buffer.lock().unwrap();

            // Since we are wrapping both buffer in a Mutex, we need to
            // dereference it before swapping. Else, we are essentially
            // swapping the MutexGuard.
            //
            // This will cause a deadlock as we aren't dropping the correct
            // MutexGuard of flush_buffer. It caused self.flush() to attempt
            // to acquire the same lock and lead to deadlock.
            std::mem::swap(&mut *log_buffer, &mut *flush_buffer);
            drop(flush_buffer);

            // Reset the range as well.
            *offset = 0;
            end = *offset + bytes.len();

            // Flush manually once we full.
            self.flush();
        }

        log_buffer[*offset..end].copy_from_slice(&bytes[..]);
        *offset += bytes.len();

        lsn
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::recovery::log_record::LogRecordType;
    use std::sync::Arc;

    #[test]
    fn append_log() {
        let lm = LogManager::new("test.wal");
        let mut lr = LogRecord::new(1, None, LogRecordType::Insert);
        assert_eq!(lm.next_lsn(), 1);

        lm.append_log(&mut lr);
        assert_eq!(lr.lsn, Some(1));

        assert_eq!(lm.next_lsn(), 2);
        lm.append_log(&mut lr);
        assert_eq!(lr.lsn, Some(2));

        _ = std::fs::remove_file("test.wal");
    }

    #[test]
    fn swap_and_flush_when_log_buffer_full() {
        let lm = LogManager::new("test.wal");
        let mut lr = LogRecord::new(1, None, LogRecordType::Insert);

        // Sample LSN to calculate number of lr accurately
        lr.lsn = Some(1);
        let bytes = bincode::serialize(&lr).unwrap();
        let number_of_lr = LOG_BUFFER_SIZE / bytes.len();

        // Reset lsn to None, since it will be added when append_log
        // is called.
        lr.lsn = None;
        for _ in 0..number_of_lr + 1 {
            lm.append_log(&mut lr);
        }

        // Offset should be bytes.len(), since we fill the log_buffer
        // by extra one record. So the new offset should be the same
        // as the len of a single log record.
        assert_eq!(lm.offset(), bytes.len());

        let _ = std::fs::remove_file("test.wal");
    }

    #[test]
    fn append_log_concurrently() {
        let log_manager = Arc::new(LogManager::new("test.wal"));

        let lm = log_manager.clone();
        let handle = std::thread::spawn(move || {
            let mut lr = LogRecord::new(1, None, LogRecordType::Insert);
            lm.append_log(&mut lr);
        });

        let lm = log_manager;
        let handle2 = std::thread::spawn(move || {
            let mut lr = LogRecord::new(2, None, LogRecordType::Insert);
            lm.append_log(&mut lr);
        });

        for h in [handle, handle2] {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_race_condition_of_swapping_buffer() {
        let log_manager = Arc::new(LogManager::new("test.wal"));

        let mut handles = Vec::new();
        for i in 1..500 {
            let lm = log_manager.clone();
            let handle = std::thread::spawn(move || {
                let mut lr = LogRecord::new(i, None, LogRecordType::Insert);
                lm.append_log(&mut lr);
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        // TODO: Verify that a race condition doesn't happen
        // when we are swapping log_buffer and flush_buffer, and invoke
        // a flush to disk.
        //
        // One way is to implement flush correctly and assert that we
        // have all log records of  1..500.
    }
}
