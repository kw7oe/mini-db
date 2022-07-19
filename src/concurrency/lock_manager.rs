use super::table::RowID;
use super::transaction::{Transaction, TransactionState};
use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tracing::trace;

#[derive(Clone, Copy, Debug, PartialEq)]
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

// Actually this is a bit unncessary but
// let it be this way first...
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

type RequestQueue = Arc<(Mutex<LockRequestQueue>, Condvar)>;
pub struct LockManager {
    lock_table: Arc<RwLock<HashMap<RowID, RequestQueue>>>,
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
        trace!("lock_shared");
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
            let (request_queue, condvar) = &*inner.clone();
            drop(lock_table);

            let mut request_queue = request_queue.lock();
            let mut should_block = false;

            // If we have request, check if it's shared or exclusive.
            //
            // If it's shared, we need to make sure that there's no other transaction in
            // front of us that is waiting to be granted. This mean we have to loop through
            // each of the transaciton infront of us to check if there's any exclusive or lock
            // that is not granted. If yes, we have to block, else, we can obtain the shared lock
            // if the lock obtained in front is share. If it's exclusive, we block.
            for req in request_queue.iter() {
                if req.mode == LockMode::Shared && req.granted {
                    continue;
                } else {
                    should_block = true;
                    break;
                }
            }

            // Not really sure if it is correct... Let say we have a queue of:
            //
            // [T1(e, t), T2(e, f), Tcurrent(s, f)]
            //
            // where, T1 is holding exclusive lock, T2 is waiting for exlcusive lock
            // and current T is waiting for shared lock.
            //
            // This implementation possibly allow Tcurrent to acquire the shared lock once T1
            // called condvar.notify_one(), which ideally we want T2 to be the one who acquire
            // the shared lock first, to prevent starvation.
            //
            // Some thoughts and reasoning going through my mind:
            //
            // There are two scenario, a shared lock will block:
            //   - When an exclusive lock is hold.
            //   - When there is an exclusive lock waiting to be hold.
            //
            // In case 1, when an exclusive lock is hold, and we are being blocked, this
            // implementation is technically correct, since there is only one exclusive lock.
            //
            // If condvar awake, it will be always from the exlusive lock, as no others lock can
            // be hold, and thus release and notify at the same time.
            //
            // In case 2, if both T2 and Tcurrent might be awake from the notify. If we got
            // notified, T2 will continue blocking, Tcurrent will unlock soon, and hoping that
            // T2 will acquire the lock. So, to prevent starvation on the lower level, it depends
            // on the behaviour of condvar.notfiy_one().
            if should_block {
                trace!("lock_shared: waiting for lock");
                condvar.wait(&mut request_queue);
            }

            request.granted = true;
            request_queue.push_back(request);
            transaction.shared_lock_sets.insert(rid);
        } else {
            request.granted = true;

            let mut queue = LockRequestQueue::new();
            queue.push_back(request);

            let mut lock_table = RwLockUpgradableReadGuard::upgrade(lock_table);
            lock_table.insert(rid, Arc::new((Mutex::new(queue), Condvar::new())));
            drop(lock_table);

            transaction.shared_lock_sets.insert(rid);
        };

        true
    }

    pub fn lock_exclusive(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        trace!("lock_exclusive");
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let lock_table = self.lock_table.upgradable_read();
        let mut request = LockRequest::new(transaction.txn_id, LockMode::Exclusive);

        if let Some(inner) = lock_table.get(&rid) {
            let (request_queue, condvar) = &*inner.clone();
            drop(lock_table);

            let mut request_queue = request_queue.lock();
            request_queue.push_back(request);

            // The following implementation is not entirely correct:
            //
            // if let Some(r) = request_queue.front() {
            //     // Wait if there is other request other than our newly added
            //     // request:
            //     if r.txn_id != transaction.txn_id {
            //         condvar.wait(&mut request_queue);
            //     }
            // }
            //
            // Let say we have a queue of:
            //
            // [T1(s, t), T2(s, t), Tcurrent(e, f)]
            //
            // where, T1 and T2 is holding shared lock and current T is waiting for
            // exclusive lock.
            //
            // This implementation possibly allow current T to acquire the exclusive lock once T1
            // called condvar.notify_one(). However, this isn't correct since T2 is still holding
            // the shared lock, and current T shouldn't be able to acquire the lock until
            // any T is not holding a lock.

            // Hence, we have to continue to wait until the front element is not granted:
            while let Some(r) = request_queue.front() {
                if r.granted {
                    condvar.wait(&mut request_queue);
                } else {
                    break;
                }
            }

            // We are looping manually to ensure that
            // we don't have any request infront that still have
            // granted = true.
            let mut request = None;
            for r in request_queue.iter_mut() {
                assert!(!r.granted);
                if r.txn_id == transaction.txn_id {
                    request = Some(r);
                    break;
                }
            }

            let request = request.unwrap();
            request.granted = true;
            transaction.exclusive_lock_sets.insert(rid);
            trace!("lock_exclusive end");
            true
        } else {
            request.granted = true;

            let mut queue = LockRequestQueue::new();
            queue.push_back(request);
            let mut lock_table = RwLockUpgradableReadGuard::upgrade(lock_table);
            lock_table.insert(rid, Arc::new((Mutex::new(queue), Condvar::new())));
            drop(lock_table);

            transaction.exclusive_lock_sets.insert(rid);
            trace!("lock_exclusive end");
            true
        }
    }

    pub fn lock_upgrade(&self, transaction: &mut Transaction, rid: RowID) -> bool {
        trace!("lock_upgrade");
        if transaction.state == TransactionState::Aborted {
            return false;
        }

        let lock_table = self.lock_table.read();

        // Upgrade the lock request owned by transaction to Exclusive mode
        if let Some(inner) = lock_table.get(&rid) {
            let (request_queue, condvar) = &*inner.clone();
            let mut request_queue = request_queue.lock();

            while request_queue
                .iter()
                .any(|r| r.txn_id != transaction.txn_id && r.granted)
            {
                condvar.wait(&mut request_queue)
            }

            // Adding assert to make sure it behaves correctly as I'm
            // unsure how to really simulate the scenario that might break
            // this.
            assert!(!request_queue
                .iter()
                .any(|r| r.txn_id != transaction.txn_id && r.granted));

            let result = request_queue
                .iter_mut()
                .find(|r| r.txn_id == transaction.txn_id)
                .map_or(false, |r| {
                    assert!(r.granted);
                    r.mode = LockMode::Exclusive;
                    transaction.shared_lock_sets.remove(&rid);
                    transaction.exclusive_lock_sets.insert(rid);
                    true
                });

            result
        } else {
            false
        }
    }

    pub fn unlock(&self, transaction: &mut Transaction, rid: &RowID) -> bool {
        trace!("unlock");
        let lock_table = self.lock_table.read();

        if let Some(inner) = lock_table.get(rid) {
            let (request_queue, condvar) = &*inner.clone();
            drop(lock_table);
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
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::concurrency::transaction;
    use std::{thread, thread::JoinHandle, time::Duration};

    #[test]
    fn lock_shared() {
        let lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_shared(&mut transaction, row_id));
        assert!(transaction.shared_lock_sets.contains(&row_id));
    }

    #[test]
    fn lock_exclusive() {
        let lm = LockManager::new();
        let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
        let row_id = RowID::new(0, 0);
        assert!(lm.lock_exclusive(&mut transaction, row_id));
        assert!(transaction.exclusive_lock_sets.contains(&row_id));
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
        assert!(transaction.exclusive_lock_sets.contains(&row_id));
    }

    #[test]
    fn concurrent_lock_sha_ex() {
        let lock_manager = Arc::new(LockManager::new());
        let sequences = vec![LockMode::Shared, LockMode::Exclusive];
        test_lock_with_sequences(&lock_manager, sequences);
    }

    #[test]
    fn concurrent_lock_ex_sha() {
        let lock_manager = Arc::new(LockManager::new());
        let sequences = vec![LockMode::Exclusive, LockMode::Shared];
        test_lock_with_sequences(&lock_manager, sequences);
    }

    #[test]
    fn concurrent_lock_sha_sha_sha() {
        // let _ = tracing_subscriber::fmt().try_init();

        let lock_manager = Arc::new(LockManager::new());
        let sequences = vec![LockMode::Shared, LockMode::Shared, LockMode::Shared];

        for i in 0..10 {
            tracing::info!("test lock with sequences {i}...");
            test_lock_with_sequences(&lock_manager, sequences.clone());
        }
    }

    #[test]
    fn concurrent_lock_ex_ex_ex() {
        // let _ = tracing_subscriber::fmt().try_init();

        let lock_manager = Arc::new(LockManager::new());
        let sequences = vec![
            LockMode::Exclusive,
            LockMode::Exclusive,
            LockMode::Exclusive,
        ];

        for i in 0..10 {
            tracing::info!("test lock with sequences {i}...");
            test_lock_with_sequences(&lock_manager, sequences.clone());
        }
    }

    #[test]
    fn concurrent_lock_sha_sha_ex_sha() {
        // let _ = tracing_subscriber::fmt().try_init();

        let lock_manager = Arc::new(LockManager::new());
        let sequences = vec![
            LockMode::Exclusive,
            LockMode::Shared,
            LockMode::Shared,
            LockMode::Exclusive,
            LockMode::Exclusive,
            LockMode::Shared,
            LockMode::Shared,
            LockMode::Exclusive,
        ];

        for i in 0..10 {
            tracing::info!("test lock with sequences {i}...");
            test_lock_with_sequences(&lock_manager, sequences.clone());
        }
    }

    fn test_lock_with_sequences(lock_manager: &Arc<LockManager>, sequences: Vec<LockMode>) {
        let row_id = RowID::new(0, 0);
        let handles: Vec<JoinHandle<_>> = sequences
            .into_iter()
            .enumerate()
            .map(|(i, mode)| {
                let lm = Arc::clone(lock_manager);
                thread::spawn(move || {
                    trace!("spawn {:?}", mode);
                    let mut transaction =
                        Transaction::new(i as u32, transaction::IsolationLevel::ReadCommited);

                    // It should block until successful once shared lock is released.
                    match mode {
                        LockMode::Shared => {
                            assert!(lm.lock_shared(&mut transaction, row_id));
                            assert!(transaction.shared_lock_sets.contains(&row_id));
                        }
                        LockMode::Exclusive => {
                            assert!(lm.lock_exclusive(&mut transaction, row_id));
                            assert!(transaction.exclusive_lock_sets.contains(&row_id));
                        }
                    }

                    // Simulate some operation
                    thread::sleep(Duration::from_millis(20));

                    assert!(lm.unlock(&mut transaction, &row_id));

                    match mode {
                        LockMode::Shared => {
                            assert!(transaction.shared_lock_sets.is_empty());
                        }
                        LockMode::Exclusive => {
                            assert!(transaction.exclusive_lock_sets.is_empty());
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn concurrent_lock_upgrade() {
        // tracing_subscriber::fmt()
        //     .with_thread_ids(true)
        //     .with_max_level(tracing::Level::TRACE)
        //     .init();

        let lock_manager = Arc::new(LockManager::new());

        let row_id = RowID::new(0, 0);
        let mut handles = Vec::new();

        for i in 1..5 {
            let lm = Arc::clone(&lock_manager);
            let handle = thread::spawn(move || {
                let mut transaction =
                    Transaction::new(i, transaction::IsolationLevel::ReadCommited);
                assert!(lm.lock_shared(&mut transaction, row_id));

                thread::sleep(Duration::from_millis(80));

                assert!(lm.unlock(&mut transaction, &row_id));
            });
            handles.push(handle);
        }

        let lm = Arc::clone(&lock_manager);
        let handle = thread::spawn(move || {
            // Sleep so that the thread above get executed first...
            thread::sleep(Duration::from_millis(50));

            let mut transaction = Transaction::new(0, transaction::IsolationLevel::ReadCommited);
            assert!(lm.lock_shared(&mut transaction, row_id));

            assert!(lm.lock_upgrade(&mut transaction, row_id));
            assert!(transaction.shared_lock_sets.is_empty());
            assert!(transaction.exclusive_lock_sets.contains(&row_id));

            assert!(lm.unlock(&mut transaction, &row_id));
        });
        handles.push(handle);

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
