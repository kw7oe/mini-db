use super::table::RowID;
use super::transaction::{Transaction, TransactionState};
use parking_lot::RwLockWriteGuard;
use std::collections::{HashMap, VecDeque};

pub enum LockMode {
    Shared,
    Exclusive,
}

pub struct LockRequest {
    txn_id: u32,
    mode: LockMode,
    granted: bool,
}

impl LockRequest {
    pub fn new(txn_id: u32, mode: LockMode) -> Self {
        Self {
            txn_id,
            mode,
            granted: false,
        }
    }
}

pub struct LockManager {
    lock_table: HashMap<RowID, VecDeque<LockRequest>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            lock_table: HashMap::new(),
        }
    }

    pub fn lock_shared(
        &mut self,
        transaction: &mut RwLockWriteGuard<Transaction>,
        rid: RowID,
    ) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        if let Some(request_queue) = self.lock_table.get(&rid) {
            // We need to check if the we granted any exclusive lock.
            // If yes, blocked.
            //
            // Else, grant the lock.
            false
        } else {
            let mut request = LockRequest::new(transaction.txn_id, LockMode::Shared);
            request.granted = true;

            let mut queue = VecDeque::new();
            queue.push_back(request);
            self.lock_table.insert(rid, queue);

            transaction.shared_lock_sets.insert(rid);
            true
        }
    }

    pub fn lock_exclusive(
        &mut self,
        transaction: &mut RwLockWriteGuard<Transaction>,
        rid: RowID,
    ) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        if let Some(request_queue) = self.lock_table.get(&rid) {
            // We need to check if the we granted any shared lock.
            // If yes, blocked.
            //
            // Else, grant the lock.
            false
        } else {
            let mut request = LockRequest::new(transaction.txn_id, LockMode::Exclusive);
            request.granted = true;

            let mut queue = VecDeque::new();
            queue.push_back(request);
            self.lock_table.insert(rid, queue);

            transaction.shared_lock_sets.insert(rid);
            true
        }
    }

    pub fn lock_upgrade(
        &mut self,
        transaction: &mut RwLockWriteGuard<Transaction>,
        rid: RowID,
    ) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        // Upgrade the lock request owned by transaction to Exclusive mode
        self.lock_table
            .get_mut(&rid)
            .map(|request_vec| {
                request_vec
                    .iter_mut()
                    .find(|r| r.txn_id == transaction.txn_id)
                    .map_or(false, |r| {
                        r.mode = LockMode::Exclusive;
                        transaction.shared_lock_sets.remove(&rid);
                        transaction.exclusive_lock_sets.insert(rid);
                        true
                    })
            })
            .map_or(false, |v| v)
    }

    pub fn unlock(&self, transaction: &mut Transaction, rid: &RowID) -> bool {
        transaction.shared_lock_sets.remove(rid);
        transaction.exclusive_lock_sets.remove(rid);

        transaction.set_state(TransactionState::Shrinking);

        true
    }
}
