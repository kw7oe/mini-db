use std::sync::atomic::AtomicU32;
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
    txn_id: u32,
    iso_level: IsolationLevel,
}

impl Transaction {
    fn new(txn_id: u32, iso_level: IsolationLevel) -> Self {
        Self { txn_id, iso_level }
    }
}

pub struct TransactionManager {
    next_txn_id: AtomicU32,
}

impl TransactionManager {
    fn begin() -> Transaction {
        todo!("implement begin");
    }

    fn commit() {}
    fn abort() {}

    fn get_transaction(txn_id: u32) -> Transaction {
        todo!("implement get transaction");
    }
}
