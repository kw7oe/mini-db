use super::table::RowID;
use super::transaction::{Transaction, TransactionState};
use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug)]
pub struct LockRequest {
    txn_id: u32,
    mode: LockMode,
    granted: bool,
}

#[derive(Debug)]
pub struct LockRequestQueue {
    queue: VecDeque<LockRequest>,
}

impl LockRequestQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
}

impl Deref for LockRequestQueue {
    type Target = VecDeque<LockRequest>;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

impl DerefMut for LockRequestQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.queue
    }
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
    lock_table: Arc<RwLock<HashMap<RowID, Arc<(Mutex<LockRequestQueue>, Condvar)>>>>,
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
            lock_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn lock_shared(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let lock_table = self.lock_table.upgradable_read();
        let mut request = LockRequest::new(transaction.txn_id, LockMode::Shared);

        // To prevent starvation, we can only grant the lock if and only if:
        //
        // - There is no other transaction holding a lock that conflict with us.
        // - There is not other transaction that is waiting for lock before us.
        if let Some(inner) = lock_table.get(&rid) {
            // If we have a request, check if it's shared or exclusive.
            //
            // If it's shared, we need to make sure that there's no other transaction in
            // front of us that is waiting to be granted. This mean we have to loop through
            // each of the transaciton infront of us to check if there's any exclusive or lock
            // that is not granted. If yes, we have to block, else, we can obtain the shared lock
            // if the lock obtained in front is share.
            //
            // If it's exclusive, we block.

            let (request_queue, condvar) = &*inner.clone();
            let mut request_queue = request_queue.lock();
            let mut should_block = false;
            for req in request_queue.iter() {
                if req.mode == LockMode::Shared && req.granted {
                    continue;
                } else {
                    println!("block please...");

                    println!("unblock!");
                    // We should block if is not granted.
                    //
                    // Someone will need to notify us when it's unlock.
                    should_block = true;
                    break;
                }
            }

            if should_block {
                condvar.wait(&mut request_queue);
            } else {
                println!("{:?}", request_queue);
                request_queue.push_back(request);
                transaction.shared_lock_sets.insert(rid);
            }
        } else {
            request.granted = true;

            let mut queue = LockRequestQueue::new();
            queue.push_back(request);
            let mut lock_table = RwLockUpgradableReadGuard::upgrade(lock_table);
            lock_table.insert(rid, Arc::new((Mutex::new(queue), Condvar::new())));

            transaction.shared_lock_sets.insert(rid);
        };

        true
    }

    pub fn lock_exclusive(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let lock_table = self.lock_table.upgradable_read();
        let mut request = LockRequest::new(transaction.txn_id, LockMode::Exclusive);

        if let Some(inner) = lock_table.get(&rid) {
            // We need to check if the we granted any shared lock.
            // If yes, blocked.
            let (request_queue, condvar) = &*inner.clone();
            let mut request_queue = request_queue.lock();
            if let Some(req) = request_queue.front() {
                println!("block please...");
                let index = request_queue.len();
                request_queue.push_back(request);
                condvar.wait(&mut request_queue);

                // drop(guard);
                println!("unblock");
                println!("{:?}", request_queue);
                let request = request_queue.iter_mut().last().unwrap();

                request.granted = true;
                transaction.shared_lock_sets.insert(rid);
                true
            } else {
                request.granted = true;
                request_queue.push_back(request);
                transaction.shared_lock_sets.insert(rid);
                true
            }
        } else {
            request.granted = true;

            let mut queue = LockRequestQueue::new();
            queue.push_back(request);

            let mut lock_table = RwLockUpgradableReadGuard::upgrade(lock_table);
            lock_table.insert(rid, Arc::new((Mutex::new(queue), Condvar::new())));

            transaction.shared_lock_sets.insert(rid);
            true
        }
    }

    pub fn lock_upgrade(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let lock_table = self.lock_table.read();
        // Upgrade the lock request owned by transaction to Exclusive mode
        lock_table
            .get(&rid)
            .map(|inner| {
                let (request_queue, _cond_var) = &*inner.clone();
                let mut request_queue = request_queue.lock();
                request_queue
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
        let lock_table = self.lock_table.read();

        lock_table
            .get(rid)
            .map(|inner| {
                let (request_queue, condvar) = &*inner.clone();
                let mut request_queue = request_queue.lock();

                // Find the index of the transaction
                let index = request_queue
                    .iter()
                    .position(|r| r.txn_id == transaction.txn_id)
                    .unwrap();
                request_queue.remove(index);
                condvar.notify_one();

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
    use std::{thread, time::Duration};

    #[test]
    fn lock_shared() {
        let lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_shared(&mut transaction, row_id));
    }

    #[test]
    fn lock_exclusive() {
        let lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_exclusive(&mut transaction, row_id));
    }

    #[test]
    fn lock_upgrade() {
        let lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);

        // False, if we have no shared lock yet.
        assert!(!lm.lock_upgrade(&mut transaction, row_id));

        assert!(lm.lock_shared(&mut transaction, row_id));
        assert!(lm.lock_upgrade(&mut transaction, row_id));
    }

    #[test]
    #[ignore]
    // Test case if we return false when is blocked
    fn multiple_locks() {
        let lm = LockManager::new();

        // RID 1: Grant two consecutive shared lock
        let row_id = RowID::new(0, 0);
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        assert!(lm.lock_shared(&mut transaction, row_id));

        let mut transaction = Transaction::new(1, transaction::IsolationLevel::ReadCommited);
        assert!(lm.lock_shared(&mut transaction, row_id));

        // RID 2: Grant a shared lock but block at execlusive lock
        let row_id = RowID::new(0, 1);
        let mut transaction = Transaction::new(2, transaction::IsolationLevel::ReadCommited);
        assert!(lm.lock_shared(&mut transaction, row_id));

        let mut transaction = Transaction::new(3, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_exclusive(&mut transaction, row_id));

        // RID 2: Block on shared locks as an exclusive lock is waiting.
        let mut transaction = Transaction::new(4, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_shared(&mut transaction, row_id));

        let mut transaction = Transaction::new(5, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_shared(&mut transaction, row_id));

        // RID 3: Grant a exclusive lock but block at multiple share locks
        let row_id = RowID::new(0, 2);
        let mut transaction = Transaction::new(6, transaction::IsolationLevel::ReadCommited);
        assert!(lm.lock_exclusive(&mut transaction, row_id));

        let mut transaction = Transaction::new(7, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_shared(&mut transaction, row_id));

        let mut transaction = Transaction::new(8, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_shared(&mut transaction, row_id));

        let mut transaction = Transaction::new(9, transaction::IsolationLevel::ReadCommited);
        assert!(!lm.lock_exclusive(&mut transaction, row_id));
    }

    #[test]
    fn concurrent_lock_and_unlock_shared_shared() {
        let lock_manager = Arc::new(LockManager::new());

        let row_id = RowID::new(0, 0);
        let lm = Arc::clone(&lock_manager);
        let handle = thread::spawn(move || {
            let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
            assert!(lm.lock_shared(&mut transaction, row_id));

            // Simulate some operation
            thread::sleep(Duration::from_millis(250));
            assert!(lm.unlock(&mut transaction, &row_id));
        });

        let lm = Arc::clone(&lock_manager);
        let handle2 = thread::spawn(move || {
            let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
            assert!(lm.lock_shared(&mut transaction, row_id));

            // Simulate some operation
            thread::sleep(Duration::from_millis(200));
            assert!(lm.unlock(&mut transaction, &row_id));
        });

        handle.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn concurrent_lock_and_unlock_shared_exclusive() {
        let lock_manager = Arc::new(LockManager::new());

        let row_id = RowID::new(0, 0);
        let lm = Arc::clone(&lock_manager);
        let handle = thread::spawn(move || {
            let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
            assert!(lm.lock_shared(&mut transaction, row_id));

            // Simulate some operation
            thread::sleep(Duration::from_millis(250));

            assert!(lm.unlock(&mut transaction, &row_id));
            println!("unlock");
        });

        let lm = Arc::clone(&lock_manager);
        let handle2 = thread::spawn(move || {
            let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);

            // It should block until successful once shared lock is released.
            assert!(lm.lock_exclusive(&mut transaction, row_id));
            assert!(lm.unlock(&mut transaction, &row_id));
        });

        handle.join().unwrap();
        handle2.join().unwrap();
    }
}