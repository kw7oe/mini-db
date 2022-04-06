use std::collections::HashMap;
use std::path::PathBuf;

use super::node::{Node, LEAF_NODE_MAX_CELLS};
use super::tree::Tree;
use crate::row::Row;
use crate::storage::DiskManager;
use crate::table::Cursor;
use std::time::Instant;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
struct PageMetadata {
    frame_id: usize,
    last_accessed_at: Instant,
}

impl PageMetadata {
    pub fn new(frame_id: usize) -> Self {
        Self {
            frame_id,
            last_accessed_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct LRUReplacer {
    // We are using Vec instead of HashMap as the size
    // of the Vec is limited. Hence, a linear search
    // would not caused much performance problem as well?
    //
    // And it's a bit easier to deal with Vec than
    // HashMap for the time being.
    page_table: Vec<PageMetadata>,
}

impl LRUReplacer {
    pub fn new(pool_size: usize) -> Self {
        Self {
            page_table: Vec::with_capacity(pool_size),
        }
    }

    /// Number of frames that are currently in the replacer.
    pub fn size(&self) -> usize {
        self.page_table.len()
    }

    /// Return frame metadata that are accessed least recently
    /// as compared to the other frame.
    pub fn victim(&mut self) -> Option<PageMetadata> {
        self.page_table
            .sort_by(|a, b| b.last_accessed_at.cmp(&a.last_accessed_at));

        let metadata = self.page_table.pop();
        println!("metadata: {:?}", metadata);
        metadata
    }

    /// This should be called after our Pager place the page into
    /// our memory. Here, pin a frame means removing it from our
    /// replacer. I guess this prevent it from the page being
    /// evicted
    pub fn pin(&mut self, frame_id: usize) {
        if let Some(index) = self
            .page_table
            .iter()
            .position(|md| md.frame_id == frame_id)
        {
            self.page_table.remove(index);
        }
    }

    /// This should be called by our Pager when the page pin_count
    /// becomes 0. Here, unpin a frame means adding it to our
    /// replacer. This allow the page to be evicted.
    pub fn unpin(&mut self, frame_id: usize) {
        self.page_table.push(PageMetadata::new(frame_id));
    }
}

struct Page {
    page_id: Option<usize>,
    is_dirty: bool,
    pin_count: usize,
    node: Option<Node>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            page_id: None,
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
}

pub struct Pager {
    disk_manager: DiskManager,
    replacer: LRUReplacer,
    pages: Vec<Page>,
    // Indexes in our `pages` that are "free", which mean
    // it is uninitialize.
    free_list: Vec<usize>,
    // Mapping page id to frame id
    page_table: HashMap<usize, usize>,
    tree: Tree,
}

impl Pager {
    pub fn new(path: impl Into<PathBuf>) -> Pager {
        let pool_size = 4;

        // Initialize empty pages and our free list.
        let mut pages = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            pages.push(Page::new());
            free_list.push(i);
        }

        Pager {
            disk_manager: DiskManager::new(path),
            replacer: LRUReplacer::new(pool_size),
            pages,
            free_list,
            page_table: HashMap::new(),
            tree: Tree::new(),
        }
    }

    // Temporarily create a new function for our buffer pool work.
    //
    // Once, it's completed it should replace the get_page function.
    pub fn get_page_from_buffer_pool(&mut self, page_num: usize) -> &Option<Node> {
        if let Some(&frame_id) = self.page_table.get(&page_num) {
            return &self.pages[frame_id].node;
        }

        if let Ok(bytes) = self.disk_manager.read_page(page_num) {
            if let Some(frame_id) = self.free_list.pop() {
                let page = &mut self.pages[frame_id];
                page.page_id = Some(page_num);
                page.node = Some(Node::new_from_bytes(&bytes));

                self.page_table.insert(page_num, frame_id);
                return &self.pages[frame_id].node;
            } else {
                unimplemented!("implement replacing page...")
            }
        }

        panic!("error getting page {page_num}...")
    }

    pub fn flush_page(&mut self, page_num: usize) {
        if let Some(&frame_id) = self.page_table.get(&page_num) {
            if let Some(node) = &self.pages[frame_id].node {
                let bytes = node.to_bytes();
                self.disk_manager.write_page(page_num, &bytes).unwrap();
            }
        }
    }

    pub fn delete_page(&mut self, page_num: usize) -> bool {
        if let Some(&frame_id) = self.page_table.get(&page_num) {
            let page = &self.pages[frame_id];
            if page.pin_count == 0 {
                // Deallocate page
                self.pages[frame_id].deallocate();

                // Add removed page index to free list
                self.free_list.push(frame_id);

                true
            } else {
                // Don't do anyting since someone is using the page
                false
            }
        } else {
            true
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> &Node {
        if self.tree.nodes().get(page_num).is_none() {
            if page_num > self.tree.nodes().len() {
                for i in self.tree.nodes().len()..page_num {
                    self.tree.mut_nodes().insert(i, Node::uninitialize());
                }
            }

            self.tree.mut_nodes().insert(page_num, Node::uninitialize());
        }

        let node = self.tree.mut_nodes().get_mut(page_num).unwrap();
        if !node.has_initialize {
            if let Ok(bytes) = self.disk_manager.read_page(page_num) {
                node.from_bytes(&bytes);
            }
        }

        &self.tree.nodes()[page_num]
    }

    pub fn flush_all(&mut self) {
        // Again, the reason why we can't just deserialize whole node
        // with bincode is because we are tracking our own num_of_cells.
        //
        // So, if we just use deserialize directly, it will also include
        // the node.cells len by Vec<Cell>.
        //
        // Ideally, we should have just need to call bincode deserialize.
        for (i, node) in self.tree.nodes().iter().enumerate() {
            self.disk_manager.write_page(i, &node.to_bytes()).unwrap();
        }
    }

    pub fn serialize_row(&mut self, row: &Row, cursor: &Cursor) {
        let node = &mut self.tree.mut_nodes()[cursor.page_num];
        let num_of_cells = node.num_of_cells as usize;
        if num_of_cells >= LEAF_NODE_MAX_CELLS {
            self.tree.split_and_insert_leaf_node(cursor, row);
        } else {
            node.insert(row, cursor);
        }
    }

    pub fn deserialize_row(&mut self, cursor: &Cursor) -> Row {
        self.get_page(cursor.page_num);
        let node = &mut self.tree.mut_nodes()[cursor.page_num];
        node.get(cursor.cell_num)
    }

    pub fn delete_row(&mut self, cursor: &Cursor) {
        self.get_page(cursor.page_num);
        self.tree.delete(cursor);
    }
}

impl std::string::ToString for Pager {
    fn to_string(&self) -> String {
        self.tree.to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn lru_replacer_evict_least_recently_accessed_page() {
        let mut replacer = LRUReplacer::new(4);

        // We have 3 candidates that can be choose to
        // be evicted by our buffer pool.
        replacer.unpin(2);
        replacer.unpin(0);
        replacer.unpin(1);

        let evicted_page = replacer.victim().unwrap();
        assert_eq!(evicted_page.frame_id, 2);
    }

    #[test]
    fn lru_replacer_do_not_evict_pin_page() {
        let mut replacer = LRUReplacer::new(4);

        // We have 3 candidates that can be choose to
        // be evicted by our buffer pool.
        replacer.unpin(2);
        replacer.unpin(0);
        replacer.unpin(1);
        replacer.pin(2);

        let evicted_page = replacer.victim().unwrap();
        assert_eq!(evicted_page.frame_id, 0);
    }
}
