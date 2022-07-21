use super::log_record::LogRecord;
use crate::storage::DiskManager;
use std::{path::Path, sync::atomic::AtomicU32};

const LOG_BUFFER_SIZE: usize = 4096;

struct LogManager {
    disk_manager: DiskManager,
    next_lsn: AtomicU32,
    persistent_lsn: Option<AtomicU32>,
    log_buffer: [u8; LOG_BUFFER_SIZE],
    flush_buffer: [u8; LOG_BUFFER_SIZE],
}

impl LogManager {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            disk_manager: DiskManager::new(path),
            next_lsn: AtomicU32::new(1),
            persistent_lsn: None,
            log_buffer: [0; LOG_BUFFER_SIZE],
            flush_buffer: [0; LOG_BUFFER_SIZE],
        }
    }

    pub fn append_log(&self, log_record: &mut LogRecord) -> u32 {
        let lsn = self
            .next_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        log_record.lsn = Some(lsn);

        // TODO: Append to buffer or flush to disk if buffer full?

        lsn
    }
}
