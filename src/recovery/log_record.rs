// use crate::{concurrency::RowID, row::Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum LogRecordType {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    // Common Header
    log_type: LogRecordType,
    size: u32,
    pub lsn: Option<u32>,
    pub txn_id: u32,

    // This is not required but it makes recovery implementation easier,
    // as we could just tranverse the log records of a transaction through
    // following the prev_lsn link.
    prev_lsn: Option<u32>,
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
            size: 0,
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

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        bincode::deserialize(&bytes).unwrap()
    }

    fn as_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let lr = LogRecord::new(1, None, LogRecordType::Begin);
        let lr2 = LogRecord::new(2, Some(1), LogRecordType::Insert);
        println!("{:?}", lr);
        println!("{:?}", lr2);

        let bytes = lr.as_bytes();
        println!("{}", bytes.len());

        let bytes = lr2.as_bytes();
        println!("{}", bytes.len());
    }
}
