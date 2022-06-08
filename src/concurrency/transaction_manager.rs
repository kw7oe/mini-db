use super::transaction::{IsolationLevel, Transaction, TransactionState};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{self, atomic::AtomicU32, Arc};

pub struct TransactionManager {
    next_txn_id: AtomicU32,
    transaction_map: Arc<RwLock<HashMap<u32, Arc<RwLock<Transaction>>>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU32::new(1),
            transaction_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn begin(&self, iso_level: IsolationLevel) -> Arc<RwLock<Transaction>> {
        let txn_id = self
            .next_txn_id
            .fetch_add(1, sync::atomic::Ordering::SeqCst);

        let transaction = Arc::new(RwLock::new(Transaction::new(txn_id, iso_level)));

        let mut map = self.transaction_map.write();
        map.insert(txn_id, transaction.clone());
        drop(map);

        transaction
    }

    fn commit(&self, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Committed);

        // Apply changes

        // Release locks from lock manager I assumed
    }

    fn abort(&self, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Aborted);

        // Rollback changes

        // Rollback index changes

        // Release locks
    }

    fn get_transaction(&self, txn_id: &u32) -> Arc<RwLock<Transaction>> {
        let map = self.transaction_map.read();
        map.get(txn_id).expect("transaction not found").clone()
    }
}

#[cfg(test)]
mod test {
    use super::{IsolationLevel, TransactionManager, TransactionState};

    #[test]
    fn transaction_operations() {
        let tm = TransactionManager::new();
        let transaction = tm.begin(IsolationLevel::ReadUncommited);
        let transaction = transaction.read();
        assert_eq!(transaction.txn_id, 1);
        assert_eq!(transaction.state, TransactionState::Growing);
        drop(transaction);

        let map = tm.transaction_map.read();
        assert_eq!(map.len(), 1);

        let tx = tm.get_transaction(&1);
        let mut tx = tx.write();
        assert_eq!(tx.txn_id, 1);
        assert_eq!(tx.state, TransactionState::Growing);

        tm.commit(&mut tx);
        assert_eq!(tx.state, TransactionState::Committed);

        // Shouldn't be possible in the first place,
        // but this is just a quick test to verify things
        tm.abort(&mut tx);
        assert_eq!(tx.state, TransactionState::Aborted);
    }
}
