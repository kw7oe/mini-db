use super::table::RowID;
use crate::row::Row;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq)]
pub enum WriteRecordType {
    Insert,
    Delete,
    Update,
}

#[derive(Debug)]
pub struct WriteRecord {
    pub rid: RowID,
    pub key: u32,
    pub wr_type: WriteRecordType,
    pub old_row: Option<Row>,
    pub columns: Vec<String>,
}

impl WriteRecord {
    pub fn new(wr_type: WriteRecordType, rid: RowID, key: u32) -> Self {
        Self {
            wr_type,
            rid,
            key,
            old_row: None,
            columns: vec![],
        }
    }
}

#[derive(Debug)]
pub enum IsolationLevel {
    ReadUncommited,
    ReadCommited,
    RepeatableRead,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TransactionState {
    Growing,
    Shrinking,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct Transaction {
    pub txn_id: u32,
    pub iso_level: IsolationLevel,
    pub state: TransactionState,
    write_sets: Vec<WriteRecord>,
    pub shared_lock_sets: HashSet<RowID>,
    pub exclusive_lock_sets: HashSet<RowID>,

    // The LSN of the last record written by the transaciton
    prev_lsn: Option<u32>,
}

impl Transaction {
    pub fn new(txn_id: u32, iso_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            iso_level,
            state: TransactionState::Growing,
            write_sets: Vec::new(),
            shared_lock_sets: HashSet::new(),
            exclusive_lock_sets: HashSet::new(),
            prev_lsn: None,
        }
    }

    pub fn update_prev_lsn(&mut self, lsn: u32) {
        self.prev_lsn = Some(lsn);
    }

    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    pub fn push_write_set(&mut self, write_set: WriteRecord) {
        self.write_sets.push(write_set);
    }

    pub fn pop_write_set(&mut self) -> Option<WriteRecord> {
        self.write_sets.pop()
    }

    pub fn is_shared_lock(&self, rid: &RowID) -> bool {
        self.shared_lock_sets.contains(rid)
    }

    pub fn is_exclusive_lock(&self, rid: &RowID) -> bool {
        self.exclusive_lock_sets.contains(rid)
    }
}
