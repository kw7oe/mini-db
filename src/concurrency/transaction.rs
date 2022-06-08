use super::table::RowID;

#[derive(Debug, PartialEq)]
pub enum TransactionState {
    Growing,
    Sinking,
    Committed,
    Aborted,
}

pub enum IsolationLevel {
    ReadUncommited,
    ReadCommited,
    RepeatableRead,
}

pub enum WriteRecordType {
    Insert,
    Delete,
    Update,
}

pub struct WriteRecord {
    rid: RowID,
    key: u32,
    wr_type: WriteRecordType,
}

impl WriteRecord {
    pub fn new(wr_type: WriteRecordType, rid: RowID, key: u32) -> Self {
        Self { wr_type, rid, key }
    }
}

pub struct Transaction {
    pub txn_id: u32,
    pub iso_level: IsolationLevel,
    pub state: TransactionState,
    write_sets: Vec<WriteRecord>,
}

impl Transaction {
    pub fn new(txn_id: u32, iso_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            iso_level,
            state: TransactionState::Growing,
            write_sets: Vec::new(),
        }
    }

    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    pub fn push_write_set(&mut self, write_set: WriteRecord) {
        self.write_sets.push(write_set);
    }
}
