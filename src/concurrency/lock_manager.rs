use super::transaction::Transaction;
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

    pub fn lock_shared(&self, transaction: &Transaction, rid: u32) {}

    pub fn lock_exclusive(&self, transaction: &Transaction, rid: u32) {}

    pub fn lock_upgrade(&self, transaction: &Transaction, rid: u32) {}

    pub fn unlock(&self, transaction: &Transaction, rid: u32) {}
}
