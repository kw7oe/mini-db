use super::{
    lock_manager::LockManager,
    transaction::{Transaction, WriteRecord, WriteRecordType},
};
use crate::storage::{Node, NodeType, Pager};
use crate::{row::Row, storage::Page};
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash)]
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
    pager: Arc<Pager>,
    lock_manager: Arc<LockManager>,
}

pub struct TableIntoIter {
    pager: Arc<Pager>,
    node: Option<Node>,
    page_id: usize,
    slot_num: usize,
}

impl Iterator for TableIntoIter {
    type Item = (RowID, Row);

    fn next(&mut self) -> Option<Self::Item> {
        self.node.clone().and_then(|node| {
            let rid = RowID::new(self.page_id, self.slot_num);
            let item = node.get_row(self.slot_num);
            let item = item.as_ref()?.to_owned();

            self.slot_num += 1;

            if self.slot_num == node.num_of_cells as usize && node.next_leaf_offset == 0 {
                self.node = None;
            } else if self.slot_num >= node.num_of_cells as usize {
                let page = self
                    .pager
                    .fetch_read_page_with_retry(node.next_leaf_offset as usize);
                self.page_id = page.page_id.unwrap();
                self.node = page.node.clone();
                self.pager.unpin_page_with_read_guard(page, false);
                self.slot_num = 0;
            }

            Some((rid, item))
        })
    }
}

impl Table {
    pub fn new(path: impl AsRef<Path>, pool_size: usize, lock_manager: Arc<LockManager>) -> Table {
        let pager = Pager::new(path, pool_size);
        Table {
            pager: Arc::new(pager),
            lock_manager,
        }
    }

    pub fn get_row_id(
        &self,
        key: u32,
        transaction: &mut RwLockWriteGuard<Transaction>,
    ) -> Option<RowID> {
        self.pager
            .search(0, key)
            .map(|(page_id, slot_num)| RowID::new(page_id, slot_num))
    }

    pub fn iter(&self) -> TableIntoIter {
        // Search for the first leaf node
        let page = self.search_page(0, 0);
        let page_id = page.page_id.unwrap();
        let node = page.node.clone().unwrap();
        self.pager.unpin_page_with_read_guard(page, false);
        assert_eq!(node.node_type, NodeType::Leaf);

        TableIntoIter {
            pager: self.pager.clone(),
            node: Some(node),
            page_id,
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

    pub fn update(
        &self,
        row: &Row,
        new_row: &Row,
        columns: &Vec<String>,
        rid: &RowID,
        transaction: &mut RwLockWriteGuard<Transaction>,
    ) -> bool {
        // Make sure we have access to a lock first before we acquire the write page
        // from our pager.
        if transaction.is_shared_lock(rid) {
            assert!(self.lock_manager.lock_upgrade(transaction, *rid));
        }

        if let Ok(mut page) = self.pager.fetch_write_page_guard(rid.page_id) {
            assert!(page.update_row(rid.slot_num, new_row, columns));
            self.pager.unpin_page_with_write_guard(page, true);

            let mut write_record = WriteRecord::new(WriteRecordType::Update, *rid, row.id);
            write_record.old_row = Some(row.clone());
            write_record.columns = columns.clone();
            transaction.push_write_set(write_record);

            true
        } else {
            false
        }
    }

    pub fn rollback_update(&self, rid: &RowID, row: &Row, columns: &Vec<String>) {
        if let Ok(mut page) = self.pager.fetch_write_page_guard(rid.page_id) {
            page.update_row(rid.slot_num, row, columns);
            self.pager.unpin_page_with_write_guard(page, true);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::concurrency::{IsolationLevel, TransactionManager};
    use std::str::FromStr;

    #[test]
    fn iter() {
        let lock_manager = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lock_manager.clone());
        let table = setup_table(&tm, lock_manager.clone());

        let mut rid = 1;
        for (_, row) in table.iter() {
            assert_eq!(row.id, rid);
            rid += 1;
        }

        // Verify it can be iterate multiple times
        // without table being consumed.
        rid = 1;
        for (_, row) in table.iter() {
            assert_eq!(row.username(), format!("user{rid}"));
            rid += 1;
        }

        cleanup_table();
    }

    #[test]
    fn update_row() {
        let lock_manager = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lock_manager.clone());
        let table = setup_table(&tm, lock_manager.clone());

        let transaction = tm.begin(IsolationLevel::ReadCommited);
        let mut t = transaction.write();
        let rid = table.get_row_id(1, &mut t).unwrap();
        let row = Row::new("1", "user1", "user1@email.com").unwrap();
        let new_row = Row::new("1", "john", "john@email.com").unwrap();
        let columns = vec!["username".to_string(), "email".to_string()];
        assert!(table.update(&row, &new_row, &columns, &rid, &mut t));

        let row = table.get(rid, &mut t).unwrap();
        assert_eq!(row.id, 1);
        assert_eq!(row.username(), "john");
        assert_eq!(row.email(), "john@email.com");
        tm.commit(&table, &mut t);

        cleanup_table();
    }

    fn setup_table(tm: &TransactionManager, lm: Arc<LockManager>) -> Table {
        let table = Table::new(format!("test-{:?}.db", std::thread::current().id()), 4, lm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);
        let mut t = transaction.write();
        for i in 1..50 {
            let row = Row::from_str(&format!("{i} user{i} user{i}@email.com")).unwrap();
            table.insert(&row, &mut t);
        }
        tm.commit(&table, &mut t);

        table
    }

    fn cleanup_table() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }
}
