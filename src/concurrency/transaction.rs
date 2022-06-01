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

pub struct Transaction {
    pub txn_id: u32,
    pub iso_level: IsolationLevel,
    pub state: TransactionState,
}

impl Transaction {
    pub fn new(txn_id: u32, iso_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            iso_level,
            state: TransactionState::Growing,
        }
    }

    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }
}
