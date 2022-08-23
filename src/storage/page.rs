use serde::{Deserialize, Serialize};

use super::node::Node;
use crate::row::Row;

// Since bincode serialize Option<usize> as [0, 0, 0, 0, 0]
//                                           -  ----------
//                                           ^       ^
//                                        Option   usize
//
// Hence, we need to add one more byte.
pub const PAGE_HEADER_BYTES: usize = 1 + std::mem::size_of::<usize>() + std::mem::size_of::<u32>();

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Page {
    // Header
    pub page_id: Option<usize>,
    pub lsn: u32,

    // Body (we will serialize/deserialize manually)
    #[serde(skip)]
    pub node: Option<Node>,

    // Metadata (in mem only)
    #[serde(skip)]
    pub is_dirty: bool,
    #[serde(skip)]
    pub pin_count: usize,
}

impl Page {
    pub fn new(page_id: Option<usize>) -> Self {
        Self {
            page_id,
            lsn: 0,
            is_dirty: false,
            pin_count: 0,
            node: None,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let header_bytes = &bytes[..PAGE_HEADER_BYTES];
        let mut page: Page = bincode::deserialize(header_bytes).unwrap();

        let body_bytes = &bytes[PAGE_HEADER_BYTES..];
        let node = Node::new_from_bytes(body_bytes);
        page.node = Some(node);

        page
    }

    pub fn deallocate(&mut self) {
        self.page_id = None;
        self.node = None;
        self.is_dirty = false;
        self.pin_count = 0;
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        // To ensure that we can only serialize if page_id and node
        // is not None.
        assert!(self.page_id.is_some());
        assert!(self.node.is_some());

        let mut header_bytes = bincode::serialize(&self).unwrap();
        let mut body_bytes = self.node.as_ref().unwrap().to_bytes();

        header_bytes.append(&mut body_bytes);
        header_bytes
    }

    // TRADEOFF: We are always cloning/copying the row values
    // to a new memory location.
    //
    // This might be fine for a prototype, but we should reduce the cost
    // of serialization and deserialization as much as possible.
    //
    // We should consider exploring the using of references to our data. But
    // this mean I have to potentially deal with lifetime and borrowing issue.
    pub fn get_row(&self, slot_num: usize) -> Option<Row> {
        self.node.as_ref().and_then(|node| node.get_row(slot_num))
    }

    pub fn mark_row_as_deleted(&mut self, slot_num: usize) -> bool {
        self.node
            .as_mut()
            .and_then(|node| node.get_mut_cell(slot_num))
            .map_or(false, |cell| {
                cell.mark_as_deleted();
                true
            })
    }

    pub fn mark_row_as_undeleted(&mut self, slot_num: usize) -> bool {
        self.node
            .as_mut()
            .and_then(|node| node.get_mut_cell(slot_num))
            .map_or(false, |cell| {
                cell.mark_as_undeleted();
                true
            })
    }

    pub fn update_row(&mut self, slot_num: usize, new_row: &Row, columns: &Vec<String>) -> bool {
        self.node
            .as_mut()
            .and_then(|node| node.get_mut_cell(slot_num))
            .map_or(false, |cell| {
                cell.update(columns, new_row);
                true
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::{Cursor, NodeType};

    #[test]
    fn deallocate() {
        let mut page = Page::new(Some(1));
        page.pin_count = 2;
        page.is_dirty = true;
        page.node = Some(Node::new(true, NodeType::Internal));
        page.deallocate();

        assert_eq!(page.page_id, None);
        assert_eq!(page.node, None);
        assert_eq!(page.pin_count, 0);
        assert!(!page.is_dirty);
    }

    #[test]
    fn test_as_bytes_from_bytes() {
        let mut page = Page::new(Some(0));
        let mut node = Node::new(true, NodeType::Leaf);
        assert_eq!(page.get_row(0), None);

        let cursor = Cursor {
            page_num: 0,
            cell_num: 0,
            end_of_table: false,
            key_existed: false,
        };
        let row = Row::new("1", "name", "email").unwrap();
        node.insert(&row, &cursor);
        page.node = Some(node);
        page.lsn = 10;

        let bytes = page.as_bytes();
        let from_byte_page = Page::from_bytes(&bytes);

        // struct is equal
        // assert_eq!(from_byte_page, page);

        // bytes is equal
        assert_eq!(bytes, from_byte_page.as_bytes());
    }

    #[test]
    fn get_row() {
        let mut page = Page::new(Some(0));
        let mut node = Node::new(true, NodeType::Leaf);
        assert_eq!(page.get_row(0), None);

        let cursor = Cursor {
            page_num: 0,
            cell_num: 0,
            end_of_table: false,
            key_existed: false,
        };
        let row = Row::new("1", "name", "email").unwrap();
        node.insert(&row, &cursor);
        page.node = Some(node);
        let row = page.get_row(0);
        assert!(row.is_some());

        let row = row.unwrap();
        assert_eq!(row.id, 1);
    }

    #[test]
    fn mark_row_as_deleted_and_undeleted() {
        let mut page = Page::new(Some(0));
        let mut node = Node::new(true, NodeType::Leaf);
        assert!(!page.mark_row_as_deleted(0));

        let cursor = Cursor {
            page_num: 0,
            cell_num: 0,
            end_of_table: false,
            key_existed: false,
        };
        let row = Row::new("1", "name", "email").unwrap();
        node.insert(&row, &cursor);
        page.node = Some(node);
        assert!(page.mark_row_as_deleted(0));

        let row = page.get_row(0).unwrap();
        assert!(row.is_deleted);

        assert!(page.mark_row_as_undeleted(0));
        let row = page.get_row(0).unwrap();
        assert!(!row.is_deleted);
    }
}
