use super::table::RowID;
use super::transaction::{Transaction, TransactionState};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(PartialEq)]
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
    lock_table: Arc<Mutex<HashMap<RowID, VecDeque<LockRequest>>>>,
}

// The behaviour depends on the isolation level of the transaciton:
//
// - ReadUncommited: No shared lock is needed.
// - ReadCommitted: Shared lock is release immediately.
// - RepeatableRead: Strict 2PL, without index lock.
// - Serializable: Strict 2PL and all locks.
//
// Since, our current implmentation is a clustered table, where index and the row is stored
// together, obtaining a lock on rid also obtained the lock on it's index. Hence,
// we can't really have RepeatableRead level as we always hold the index lock together
// with the row lock.
//
// Furthermore, ReadCommitted is also not possible since, we will be implmenting strict 2PL
// in our system. Hence, we will be holding all the locks and only release it at the end.
impl LockManager {
    pub fn new() -> Self {
        LockManager {
            lock_table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn lock_shared(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let mut lock_table = self.lock_table.lock().unwrap();
        let mut request = LockRequest::new(transaction.txn_id, LockMode::Shared);

        // To prevent starvation, we can only grant the lock if and only if:
        //
        // - There is no other transaction holding a lock that conflict with us.
        // - There is not other transaction that is waiting for lock before us.
        if let Some(request_queue) = lock_table.get_mut(&rid) {
            // If we have a request, check if it's shared or exclusive.
            //
            // If it's shared, we need to make sure that there's no other transaction in
            // front of us that is waiting to be granted. This mean we have to loop through
            // each of the transaciton infront of us to check if there's any exclusive or lock
            // that is not granted. If yes, we have to block, else, we can obtain the shared lock
            // if the lock obtained in front is share.
            //
            // If it's exclusive, we block.
            let prev_request = request_queue.front().unwrap();

            if prev_request.mode == LockMode::Shared && prev_request.granted {
                request.granted = true;
            }

            if !request.granted {
                println!("block please...");
                // We should block if is not granted.
                //
                // Someone will need to notify us when it's unlock.
            }

            request_queue.push_back(request);
            transaction.shared_lock_sets.insert(rid);
        } else {
            request.granted = true;

            let mut queue = VecDeque::new();
            queue.push_back(request);
            lock_table.insert(rid, queue);

            transaction.shared_lock_sets.insert(rid);
        };

        true
    }

    pub fn lock_exclusive(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let mut lock_table = self.lock_table.lock().unwrap();
        if let Some(request_queue) = lock_table.get(&rid) {
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
            lock_table.insert(rid, queue);

            transaction.shared_lock_sets.insert(rid);
            true
        }
    }

    pub fn lock_upgrade(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let mut lock_table = self.lock_table.lock().unwrap();
        // Upgrade the lock request owned by transaction to Exclusive mode
        lock_table
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
        let mut lock_table = self.lock_table.lock().unwrap();

        lock_table
            .get_mut(rid)
            .map(|request_vec| {
                // Find the index of the transaction
                let index = request_vec
                    .iter()
                    .position(|r| r.txn_id == transaction.txn_id)
                    .unwrap();
                request_vec.remove(index);

                // Update transaction state
                transaction.shared_lock_sets.remove(rid);
                transaction.exclusive_lock_sets.remove(rid);
                transaction.set_state(TransactionState::Shrinking);

                true
            })
            .map_or(false, |v| v)
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

        // False, if we have no shared lock yet.
        assert!(!lm.lock_upgrade(&mut transaction, row_id));

        assert!(lm.lock_shared(&mut transaction, row_id));
        assert!(lm.lock_upgrade(&mut transaction, row_id));
    }
}
