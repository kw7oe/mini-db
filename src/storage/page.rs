use super::node::Node;
use crate::row::Row;

#[derive(Debug)]
pub struct Page {
    pub page_id: Option<usize>,
    pub is_dirty: bool,
    pub pin_count: usize,
    pub node: Option<Node>,
}

impl Page {
    pub fn new(page_id: Option<usize>) -> Self {
        Self {
            page_id,
            is_dirty: false,
            pin_count: 0,
            node: None,
        }
    }

    pub fn deallocate(&mut self) {
        self.page_id = None;
        self.node = None;
        self.is_dirty = false;
        self.pin_count = 0;
    }

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
