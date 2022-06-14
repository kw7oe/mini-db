use super::table::RowID;
use super::transaction::Transaction;
use parking_lot::RwLockWriteGuard;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LockManager {
    lock_table: Arc<HashMap<u32, u32>>,
}
impl LockManager {
    pub fn new() -> Self {
        LockManager {
            lock_table: Arc::new(HashMap::new()),
        }
    }

    pub fn lock_shared(&self, transaction: &mut RwLockWriteGuard<Transaction>, rid: RowID) -> bool {
        transaction.shared_lock_sets.insert(rid);
        true
    }

    pub fn lock_exclusive(
        &self,
        transaction: &mut RwLockWriteGuard<Transaction>,
        rid: RowID,
    ) -> bool {
        transaction.exclusive_lock_sets.insert(rid);
        true
    }

    pub fn lock_upgrade(
        &self,
        transaction: &mut RwLockWriteGuard<Transaction>,
        rid: RowID,
    ) -> bool {
        transaction.shared_lock_sets.remove(&rid);
        transaction.exclusive_lock_sets.insert(rid);
        true
    }

    pub fn unlock(&self, transaction: &mut Transaction, rid: &RowID) -> bool {
        transaction.shared_lock_sets.remove(rid);
        transaction.exclusive_lock_sets.remove(rid);
        true
    }
}
