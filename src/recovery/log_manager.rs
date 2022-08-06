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

    pub fn next_lsn(&self) -> u32 {
        self.next_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn persistent_lsn(&self) -> Option<u32> {
        self.persistent_lsn
            .as_ref()
            .map(|p_lsn| p_lsn.load(std::sync::atomic::Ordering::SeqCst))
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::recovery::log_record::LogRecordType;

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
}
