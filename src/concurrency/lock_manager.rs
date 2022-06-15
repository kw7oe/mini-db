use super::table::RowID;
use super::transaction::{Transaction, TransactionState};
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

    pub fn lock_shared(&mut self, transaction: &mut Transaction, rid: RowID) -> bool {
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

    pub fn lock_exclusive(&mut self, transaction: &mut Transaction, rid: RowID) -> bool {
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

    pub fn lock_upgrade(&mut self, transaction: &mut Transaction, rid: RowID) -> bool {
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
        false
        // We need interior mutability.

        // self.lock_table
        //     .get_mut(&rid)
        //     .map(|request_vec| {
        //         // Find the index of the transaction
        //         let index = request_vec
        //             .iter()
        //             .position(|r| r.txn_id == transaction.txn_id)
        //             .unwrap();
        //         request_vec.remove(index);

        //         // Update transaction state
        //         transaction.shared_lock_sets.remove(rid);
        //         transaction.exclusive_lock_sets.remove(rid);
        //         transaction.set_state(TransactionState::Shrinking);

        //         true
        //     })
        //     .map_or(false, |v| v)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::concurrency::transaction;

    #[test]
    fn lock_shared() {
        let mut lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_shared(&mut transaction, row_id));
    }

    #[test]
    fn lock_exclusive() {
        let mut lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_exclusive(&mut transaction, row_id));
    }

    #[test]
    fn lock_upgrade() {
        let mut lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_shared(&mut transaction, row_id));
        assert!(lm.lock_upgrade(&mut transaction, row_id));
    }
}
