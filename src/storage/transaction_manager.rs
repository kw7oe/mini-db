use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{self, atomic::AtomicU32, Arc};

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
    txn_id: u32,
    iso_level: IsolationLevel,
    state: TransactionState,
}

impl Transaction {
    fn new(txn_id: u32, iso_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            iso_level,
            state: TransactionState::Growing,
        }
    }

    fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }
}

pub struct TransactionManager {
    next_txn_id: AtomicU32,
    transaction_map: Arc<RwLock<HashMap<u32, Arc<Transaction>>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU32::new(1),
            transaction_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn begin(&self, iso_level: IsolationLevel) -> Arc<Transaction> {
        let txn_id = self
            .next_txn_id
            .fetch_add(1, sync::atomic::Ordering::SeqCst);

        let transaction = Arc::new(Transaction::new(txn_id, iso_level));

        let mut map = self.transaction_map.write();
        map.insert(transaction.txn_id, transaction.clone());
        drop(map);

        transaction
    }

    fn commit(&self, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Committed);

        // Apply changes

        // Release locks
    }

    fn abort(&self, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Aborted);

        // Rollback changes

        // Rollback index changes

        // Release locks
    }

    fn get_transaction(&self, txn_id: &u32) -> Arc<Transaction> {
        let map = self.transaction_map.read();
        map.get(txn_id).expect("transaction not found").clone()
    }
}

#[cfg(test)]
mod test {
    use super::{IsolationLevel, TransactionManager, TransactionState};

    #[test]
    fn transaction_begin_and_get() {
        let tm = TransactionManager::new();
        let transaction = tm.begin(IsolationLevel::ReadUncommited);

        assert_eq!(transaction.txn_id, 1);
        assert_eq!(transaction.state, TransactionState::Growing);

        let map = tm.transaction_map.read();
        assert_eq!(map.len(), 1);

        let tx = tm.get_transaction(&1);
        assert_eq!(tx.txn_id, 1);
        assert_eq!(tx.state, TransactionState::Growing);
    }
}
