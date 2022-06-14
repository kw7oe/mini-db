use super::table::RowID;
use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum TransactionState {
    Growing,
    Sinking,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub enum IsolationLevel {
    ReadUncommited,
    ReadCommited,
    RepeatableRead,
}

#[derive(Debug, PartialEq)]
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
}

impl WriteRecord {
    pub fn new(wr_type: WriteRecordType, rid: RowID, key: u32) -> Self {
        Self { wr_type, rid, key }
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub txn_id: u32,
    pub iso_level: IsolationLevel,
    pub state: TransactionState,
    write_sets: Vec<WriteRecord>,
    pub shared_lock_sets: HashSet<RowID>,
    pub exclusive_lock_sets: HashSet<RowID>,
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
        }
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
}
