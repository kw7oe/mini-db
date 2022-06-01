use super::{
    lock_manager::LockManager, transaction::Transaction, transaction_manager::TransactionManager,
};
use crate::row::Row;
use crate::storage::Pager;
use std::path::Path;

pub struct RowID {
    page_id: usize,
    slot_num: usize,
}

pub struct Table {
    root_page_num: usize,
    pager: Pager,
    transaction_manager: TransactionManager,
    lock_manager: LockManager,
}

impl Table {
    pub fn new(path: impl AsRef<Path>, pool_size: usize) -> Table {
        let pager = Pager::new(path, pool_size);
        Table {
            root_page_num: 0,
            pager,
            lock_manager: LockManager::new(),
            transaction_manager: TransactionManager::new(),
        }
    }

    pub fn insert(row: &Row, rid: &mut RowID, transaction: Transaction) {
        // Insert into Page
        //   - We will need to set Row ID based on the page id and slot num
        //     after we find the right page and slot to insert.

        // After insertion place it into Transaction write set,
        // so that if a transaction is aborted we can revert it back.
        //   - What are the information we need to revert a insert?
        //   - We need to delete the inserted record.
        //   - To delete an inserted record, we probably need the key or row id.
    }

    pub fn delete(row: &Row, rid: &mut RowID, transaction: Transaction) {
        // Delete from Page

        // Mark row as delete. We only apply delete after transaction is committed.
        // But what happen to our B+ Tree if we mark something as delete?
        // We don't merge until is committed.
        //
        // This mean that our pager need to support two kind of delete: soft delete and
        // hard delete. On top of that, we also need to be able to rollback soft delete,
        // when a transaction is aborted.
    }

    pub fn update(row: &Row, rid: &mut RowID, transaction: Transaction) {
        // Update Page

        // Store old row? So that we can rollback the the old row when it is aborted.
    }
}
