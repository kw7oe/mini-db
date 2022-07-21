// use crate::{concurrency::RowID, row::Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
enum LogRecordType {
    Invalid,
    Insert,
    MarkDelete,
    ApplyDelete,
    RollbackDelete,
    Update,
    Begin,
    Commit,
    Abort,
    NewPage,
}

pub const LOG_RECORD_HEADER_SIZE: usize =
    std::mem::size_of::<LogRecordType>() + std::mem::size_of::<u32>() * 5;

#[derive(Debug, Serialize, Deserialize)]
struct LogRecord {
    // Common Header
    size: u32,
    lsn: Option<u32>,
    txn_id: u32,
    prev_lsn: Option<u32>,
    log_type: LogRecordType,
    // Insert
    // insert_rid: Option<RowID>,
    // insert_row: Option<Row>,

    // Delete
    // delete_rid: Option<RowID>,
    // delete_row: Option<Row>,

    // Update
    // update_rid: Option<RowID>,
    // old_row: Option<Row>,
    // new_row: Option<Row>,

    // New Page
    // prev_page_id: Option<usize>,
    // page_id: Option<usize>,
}

impl LogRecord {
    pub fn new(txn_id: u32, prev_lsn: Option<u32>, log_type: LogRecordType) -> Self {
        Self {
            size: LOG_RECORD_HEADER_SIZE as u32,
            lsn: None,
            txn_id,
            prev_lsn,
            log_type,
            // insert_rid: None,
            // insert_row: None,

            // delete_rid: None,
            // delete_row: None,

            // update_rid: None,
            // old_row: None,
            // new_row: None,

            // prev_page_id: None,
            // page_id: None,
        }
    }

    fn header_size() -> usize {
        LOG_RECORD_HEADER_SIZE
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn header_size() {
        let size = LogRecord::header_size();
        println!("{size}");
    }
}
