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
}
