use super::{
    lock_manager::LockManager,
    transaction::{Transaction, WriteRecord, WriteRecordType},
    transaction_manager::TransactionManager,
};
use crate::row::Row;
use crate::storage::Pager;
use std::path::Path;

#[derive(Copy, Clone)]
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

    pub fn insert(&self, row: &Row, rid: &mut RowID, mut transaction: Transaction) {
        if let Ok((page_id, slot_num)) = self.pager.insert_row(0, row) {
            // The RID probably need to be added to the row
            // as well?
            //
            // It's currently unused by row/tuple.
            rid.page_id = page_id;
            rid.slot_num = slot_num;
            transaction.push_write_set(WriteRecord::new(WriteRecordType::Insert, *rid, row.id));
        }
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
