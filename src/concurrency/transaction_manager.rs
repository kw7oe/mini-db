use super::lock_manager::LockManager;
use super::table::Table;
use super::transaction::{IsolationLevel, Transaction, TransactionState, WriteRecordType};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{self, atomic::AtomicU32, Arc};

pub struct TransactionManager {
    next_txn_id: AtomicU32,
    transaction_map: Arc<RwLock<HashMap<u32, Arc<RwLock<Transaction>>>>>,
    lock_manager: Arc<LockManager>,
}

// A couple of things we have potentially not implemented:
//
//   - Cleaning up `transaction_map`. Currently, aborted and committed transactions
//     are not removed from the map yet.
impl TransactionManager {
    pub fn new(lock_manager: Arc<LockManager>) -> Self {
        Self {
            next_txn_id: AtomicU32::new(1),
            transaction_map: Arc::new(RwLock::new(HashMap::new())),
            lock_manager,
        }
    }

    pub fn execute<F, T>(&self, table: &Table, iso_level: IsolationLevel, f: F) -> T
    where
        F: FnOnce(Arc<RwLock<Transaction>>, &TransactionManager) -> T,
    {
        let transaction = self.begin(iso_level);
        let result = f(Arc::clone(&transaction), self);

        // We only automatically commit transactions that
        // are not aborted or committed.
        let mut t = transaction.write();
        if t.state != TransactionState::Aborted && t.state != TransactionState::Committed {
            self.commit(table, &mut t);
        }

        result
    }

    pub fn begin(&self, iso_level: IsolationLevel) -> Arc<RwLock<Transaction>> {
        let txn_id = self
            .next_txn_id
            .fetch_add(1, sync::atomic::Ordering::SeqCst);

        let transaction = Arc::new(RwLock::new(Transaction::new(txn_id, iso_level)));

        let mut map = self.transaction_map.write();
        map.insert(txn_id, Arc::clone(&transaction));
        drop(map);

        transaction
    }

    pub fn commit(&self, table: &Table, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Committed);

        while let Some(wr) = transaction.pop_write_set() {
            if wr.wr_type == WriteRecordType::Delete {
                table.apply_delete(wr.key);
            }
        }

        self.release_locks(transaction);
    }

    pub fn abort(&self, table: &Table, transaction: &mut Transaction) {
        transaction.set_state(TransactionState::Aborted);

        while let Some(wr) = transaction.pop_write_set() {
            match wr.wr_type {
                WriteRecordType::Insert => table.apply_delete(wr.key),
                WriteRecordType::Delete => table.rollback_delete(&wr.rid),
                WriteRecordType::Update => {
                    table.rollback_update(&wr.rid, &wr.old_row.unwrap(), &wr.columns)
                }
            }
        }

        self.release_locks(transaction);
    }

    fn release_locks(&self, transaction: &mut Transaction) {
        let mut lock_sets = HashSet::new();
        for rid in &transaction.shared_lock_sets {
            lock_sets.insert(*rid);
        }

        for rid in &transaction.exclusive_lock_sets {
            lock_sets.insert(*rid);
        }

        for rid in lock_sets {
            self.lock_manager.unlock(transaction, &rid);
        }
    }

    fn get_transaction(&self, txn_id: &u32) -> Arc<RwLock<Transaction>> {
        let map = self.transaction_map.read();
        map.get(txn_id).expect("transaction not found").clone()
    }
}

#[cfg(test)]
mod test {
    use super::{IsolationLevel, LockManager, TransactionManager, TransactionState};
    use crate::{concurrency::table::Table, row::Row};
    use std::str::FromStr;
    use std::sync::Arc;

    fn setup_table(lm: Arc<LockManager>) -> Table {
        Table::new(format!("test-{:?}.db", std::thread::current().id()), 4, lm)
    }

    fn cleanup_table() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }

    #[test]
    fn transaction_operations() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
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

        let table = setup_table(lm);
        tm.commit(&table, &mut tx);
        assert_eq!(tx.state, TransactionState::Committed);

        cleanup_table();
    }

    #[test]
    fn execute_transaction() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(lm.clone());
        let row = Row::from_str("1 apple apple@apple.com").unwrap();
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            let rid = table.insert(&row, &mut t).unwrap();
            drop(t);

            let mut t = transaction.write();
            let inserted_row = table.get(rid, &mut t).unwrap();

            assert_eq!(row, inserted_row);
        });

        cleanup_table();
    }

    #[test]
    fn abort_transaction() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(lm.clone());
        let row = Row::from_str("1 apple apple@apple.com").unwrap();
        let rid = tm.execute(&table, IsolationLevel::ReadCommited, |transaction, tm| {
            let mut t = transaction.write();
            let rid = table.insert(&row, &mut t).unwrap();
            drop(t);

            let mut t = transaction.write();
            tm.abort(&table, &mut t);

            assert_eq!(t.state, TransactionState::Aborted);
            rid
        });

        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            assert_eq!(table.get(rid, &mut t), None);
        });

        // We should have an aborted transaciton.
        let map = tm.transaction_map.read();
        let transaction = map.iter().find(|(_, t)| {
            let t = t.read();
            t.state == TransactionState::Aborted
        });
        assert!(transaction.is_some());

        cleanup_table();
    }

    #[test]
    fn delete_abort_and_commit_transaction() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(lm.clone());
        let row = Row::from_str("1 apple apple@apple.com").unwrap();
        let rid = tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            table.insert(&row, &mut t).unwrap()
        });

        // Delete and abort
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, tm| {
            let mut t = transaction.write();
            assert!(table.delete(&row, &rid, &mut t));
            tm.abort(&table, &mut t);
            assert_eq!(t.state, TransactionState::Aborted);
        });

        // We should have an aborted transaciton.
        let map = tm.transaction_map.read();
        let transaction = map.iter().find(|(_, t)| {
            let t = t.read();
            t.state == TransactionState::Aborted
        });
        assert!(transaction.is_some());
        drop(map);

        // Make sure row is still there
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            let row = table.get(rid, &mut t);
            assert!(row.is_some());

            let row = row.unwrap();
            assert!(!row.is_deleted);
        });

        // Finally delete and commit it
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            assert!(table.delete(&row, &rid, &mut t));
        });

        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            assert_eq!(table.get(rid, &mut t), None);
        });

        cleanup_table();
    }

    #[test]
    fn update_abort_and_commit_transaction() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(lm.clone());
        let row = Row::from_str("1 apple apple@apple.com").unwrap();
        let rid = tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            table.insert(&row, &mut t).unwrap()
        });

        // Update and abort
        let new_row = Row::from_str("1 john tim@apple.com").unwrap();
        let columns = vec!["username".to_string(), "email".to_string()];
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, tm| {
            let mut t = transaction.write();
            assert!(table.update(&row, &new_row, &columns, &rid, &mut t));
            tm.abort(&table, &mut t);
            assert_eq!(t.state, TransactionState::Aborted);
        });

        // We should have an aborted transaciton.
        let map = tm.transaction_map.read();
        let transaction = map.iter().find(|(_, t)| {
            let t = t.read();
            t.state == TransactionState::Aborted
        });
        assert!(transaction.is_some());
        drop(map);

        // Make sure row is still there and the value is unchange
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            let row = table.get(rid, &mut t);
            assert!(row.is_some());

            let row = row.unwrap();
            assert_eq!(row.username(), "apple");
            assert_eq!(row.email(), "apple@apple.com");
        });

        // Finally delete and commit it
        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            assert!(table.update(&row, &new_row, &columns, &rid, &mut t));
        });

        tm.execute(&table, IsolationLevel::ReadCommited, |transaction, _tm| {
            let mut t = transaction.write();
            let row = table.get(rid, &mut t);
            assert!(row.is_some());

            let row = row.unwrap();
            assert_eq!(row.username(), "john");
            assert_eq!(row.email(), "tim@apple.com");
        });

        cleanup_table();
    }
}
