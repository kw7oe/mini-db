use super::transaction_manager::Transaction;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LockManager {
    lock_table: Arc<HashMap<u32, u32>>,
}
impl LockManager {
    pub fn lock_shared(transaction: &Transaction, rid: u32) {}

    pub fn lock_exclusive(transaction: &Transaction, rid: u32) {}

    pub fn lock_upgrade(transaction: &Transaction, rid: u32) {}

    pub fn unlock(transaction: &Transaction, rid: u32) {}
}
