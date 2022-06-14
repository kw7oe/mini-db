use super::{
    lock_manager::LockManager,
    transaction::{Transaction, WriteRecord, WriteRecordType},
    transaction_manager::TransactionManager,
};
use crate::row::Row;
use crate::storage::Pager;
use parking_lot::RwLockWriteGuard;
use std::path::Path;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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

    pub fn get(&self, rid: RowID, transaction: &mut RwLockWriteGuard<Transaction>) -> Option<Row> {
        if let Ok(page) = self.pager.fetch_read_page_guard(rid.page_id) {
            page.get_row(rid.slot_num)
        } else {
            transaction.set_state(super::transaction::TransactionState::Aborted);
            None
        }
    }

    pub fn insert(
        &self,
        row: &Row,
        transaction: &mut RwLockWriteGuard<Transaction>,
    ) -> Option<RowID> {
        if let Ok((page_id, slot_num)) = self.pager.insert_row(0, row) {
            // The RID probably need to be added to the row
            // as well? It's currently unused by row/tuple.
            let rid = RowID { page_id, slot_num };
            transaction.push_write_set(WriteRecord::new(WriteRecordType::Insert, rid, row.id));
            Some(rid)
        } else {
            None
        }
    }

    pub fn apply_delete(&self, key: u32) {
        self.pager.delete_by_key(0, key);
    }

    pub fn rollback_delete(&self, rid: &RowID) {
        let mut page = self.pager.fetch_write_page_guard(rid.page_id).unwrap();
        page.mark_row_as_undeleted(rid.slot_num);
        self.pager.unpin_page_with_write_guard(page, true);
    }

    pub fn delete(
        &self,
        row: &Row,
        rid: &RowID,
        transaction: &mut RwLockWriteGuard<Transaction>,
    ) -> bool {
        if let Ok(mut page) = self.pager.fetch_write_page_guard(rid.page_id) {
            page.mark_row_as_deleted(rid.slot_num);
            self.pager.unpin_page_with_write_guard(page, true);

            transaction.push_write_set(WriteRecord::new(WriteRecordType::Delete, *rid, row.id));
            true
        } else {
            false
        }
    }

    pub fn update(row: &Row, rid: &mut RowID, transaction: Transaction) {
        // Update Page

        // Store old row? So that we can rollback the the old row when it is aborted.
    }
}
