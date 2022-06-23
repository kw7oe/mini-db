use super::{
    lock_manager::LockManager,
    transaction::{Transaction, WriteRecord, WriteRecordType},
};
use crate::storage::{Node, NodeType, Pager};
use crate::{row::Row, storage::Page};
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::path::Path;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RowID {
    page_id: usize,
    slot_num: usize,
}

impl RowID {
    pub fn new(page_id: usize, slot_num: usize) -> Self {
        Self { page_id, slot_num }
    }
}

pub struct Table {
    pager: Pager,
    lock_manager: LockManager,
}

pub struct TableIntoIter<'a> {
    pager: &'a Pager,
    node: Option<Node>,
    slot_num: usize,
}

impl<'a> Iterator for TableIntoIter<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Figure out:
        //
        // - Can we achieve this without cloning?
        // - Do we want to implement Iter and IntoIter trait for table?
        // - Can table still be used after iteration?
        self.node.clone().map(|node| {
            let item = node.get(self.slot_num);
            self.slot_num += 1;

            if self.slot_num == node.num_of_cells as usize && node.next_leaf_offset == 0 {
                self.node = None;
            } else if self.slot_num >= node.num_of_cells as usize {
                let page = self
                    .pager
                    .fetch_read_page_with_retry(node.next_leaf_offset as usize);
                self.node = page.node.clone();
                self.pager.unpin_page_with_read_guard(page, false);
                self.slot_num = 0;
            }

            item
        })
    }
}

impl Table {
    pub fn new(path: impl AsRef<Path>, pool_size: usize) -> Table {
        let pager = Pager::new(path, pool_size);
        Table {
            pager,
            lock_manager: LockManager::new(),
        }
    }

    pub fn index_scan(
        &self,
        key: u32,
        transaction: &mut RwLockWriteGuard<Transaction>,
    ) -> Option<RowID> {
        self.pager.search(0, key).map(|(page_id, slot_num)| {
            let row_id = RowID::new(page_id, slot_num);
            self.lock_manager.lock_shared(transaction, row_id);
            row_id
        })
    }

    pub fn iter(&self) -> TableIntoIter {
        // Search for the first leaf node
        let page = self.search_page(0, 0);
        let node = page.node.clone().unwrap();
        self.pager.unpin_page_with_read_guard(page, false);
        assert_eq!(node.node_type, NodeType::Leaf);

        TableIntoIter {
            pager: &self.pager,
            node: Some(node),
            slot_num: 0,
        }
    }

    fn search_page(&self, page_num: usize, key: u32) -> RwLockUpgradableReadGuard<Page> {
        match self.pager.fetch_read_page_guard(page_num) {
            Err(_) => {
                let duration = std::time::Duration::from_millis(1000);
                std::thread::sleep(duration);

                self.search_page(page_num, key)
            }
            Ok(page) => {
                let node = page.node.as_ref().unwrap();

                if node.node_type == NodeType::Leaf {
                    return page;
                }

                let next_page_num = node.search(key).unwrap();
                self.pager.unpin_page_with_read_guard(page, false);
                self.search_page(next_page_num, key)
            }
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
