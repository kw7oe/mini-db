use tracing::trace;

use super::log_record::LogRecord;
use crate::storage::DiskManager;
use std::{path::Path, sync::atomic::AtomicU32, thread::JoinHandle};

const LOG_BUFFER_SIZE: usize = 4096;

struct LogManager {
    disk_manager: DiskManager,
    next_lsn: AtomicU32,
    persistent_lsn: Option<AtomicU32>,
    log_buffer: [u8; LOG_BUFFER_SIZE],
    flush_buffer: [u8; LOG_BUFFER_SIZE],
    pub offset: usize,
    join_handle: Option<JoinHandle<()>>,
}

impl LogManager {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            disk_manager: DiskManager::new(path),
            next_lsn: AtomicU32::new(1),
            persistent_lsn: None,
            log_buffer: [0; LOG_BUFFER_SIZE],
            flush_buffer: [0; LOG_BUFFER_SIZE],
            offset: 0,
            join_handle: None,
        }
    }

    pub fn flush(&self) {
        trace!("flush WAL to disk");
        self.disk_manager.write_page(1, &self.flush_buffer).unwrap();
    }

    pub fn next_lsn(&self) -> u32 {
        self.next_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn persistent_lsn(&self) -> Option<u32> {
        self.persistent_lsn
            .as_ref()
            .map(|p_lsn| p_lsn.load(std::sync::atomic::Ordering::SeqCst))
    }

    pub fn log_buffer(&self) -> &[u8] {
        &self.log_buffer
    }

    pub fn append_log(&mut self, log_record: &mut LogRecord) -> u32 {
        let lsn = self
            .next_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        log_record.lsn = Some(lsn);

        // TODO: Append to buffer or flush to disk if buffer full?
        let bytes = bincode::serialize(&log_record).unwrap();
        println!("bytes: {:?}", bytes);
        let mut end = self.offset + bytes.len();

        // If our log buffer is full, we swap it with the flush_buffer
        // so we can just flush to disk from the flush buffer while
        // continue using the log buffer.
        if end > self.log_buffer.len() {
            println!("log buffer full, swapping with flush_buffer");
            std::mem::swap(&mut self.log_buffer, &mut self.flush_buffer);

            // Reset the range as well.
            self.offset = 0;
            end = self.offset + bytes.len();

            // Flush manually once we full.
            self.flush();
        }

        self.log_buffer[self.offset..end].copy_from_slice(&bytes[..]);
        self.offset += bytes.len();

        lsn
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::recovery::log_record::LogRecordType;

    #[test]
    fn append_log() {
        let mut lm = LogManager::new("test.wal");
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
        let mut lm = LogManager::new("test.wal");
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
        assert_eq!(lm.offset, bytes.len());

        let _ = std::fs::remove_file("test.wal");
    }
}
