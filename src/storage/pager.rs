use std::collections::HashMap;
use std::path::PathBuf;

use super::node::{InternalCell, Node, LEAF_NODE_MAX_CELLS, LEAF_NODE_RIGHT_SPLIT_COUNT};
use super::tree::Tree;
use crate::row::Row;
use crate::storage::{DiskManager, NodeType};
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

        self.page_table.pop()
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

#[derive(Debug)]
pub struct Page {
    page_id: Option<usize>,
    is_dirty: bool,
    pin_count: usize,
    node: Option<Node>,
}

impl Page {
    pub fn new(page_id: usize) -> Self {
        Self {
            page_id: Some(page_id),
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

#[derive(Debug)]
pub struct Pager {
    disk_manager: DiskManager,
    replacer: LRUReplacer,
    pages: Vec<Page>,
    next_page_id: usize,
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

        // Initialize free list.
        let mut free_list = Vec::with_capacity(pool_size);
        for i in (0..pool_size).rev() {
            free_list.push(i);
        }

        let disk_manager = DiskManager::new(path);
        let next_page_id = disk_manager.file_len / PAGE_SIZE;

        Pager {
            disk_manager,
            replacer: LRUReplacer::new(pool_size),
            pages: Vec::with_capacity(pool_size),
            // This would probably need to be based on the number of pages we
            // already have in our disk.
            next_page_id,
            free_list,
            page_table: HashMap::new(),
            tree: Tree::new(),
        }
    }

    fn new_page(&mut self) -> Option<usize> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        self.find_or_create_page(page_id)
    }

    fn find_or_create_page(&mut self, page_id: usize) -> Option<usize> {
        let frame_id = if let Some(frame_id) = self.free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.victim().map(|md| md.frame_id)
        };

        if let Some(frame_id) = frame_id {
            if let Some(page) = self.pages.get_mut(frame_id) {
                page.is_dirty = false;
                page.pin_count = 0;
                page.page_id = Some(page_id);
                page.node = None;
                self.page_table.insert(page_id, frame_id);
            } else {
                let page = Page::new(page_id);
                self.pages.insert(frame_id, page);
                self.page_table.insert(page_id, frame_id);
            }

            Some(frame_id)
        } else {
            None
        }
    }

    pub fn get_node(&mut self, page_id: usize) -> Option<&Node> {
        self.fetch_page(page_id).and_then(|page| page.node.as_ref())
    }

    pub fn fetch_node(&mut self, page_id: usize) -> Option<&mut Node> {
        self.fetch_page(page_id).and_then(|page| page.node.as_mut())
    }

    pub fn fetch_page(&mut self, page_id: usize) -> Option<&mut Page> {
        debug!("--- fetch page {page_id}");
        // Check if the page is already in memory. If yes,
        // just pin the page and return the node.
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            debug!("--- found page {page_id} in memory");
            let mut page = &mut self.pages[frame_id];
            page.pin_count += 1;
            self.replacer.pin(frame_id);
            return self.pages.get_mut(frame_id);
        }

        if let Some(frame_id) = self.find_or_create_page(page_id) {
            debug!("--- allocate new page {page_id}");
            let page = &self.pages[frame_id];
            if page.is_dirty {
                self.flush_page(page_id);
            }

            let page = &mut self.pages[frame_id];
            match self.disk_manager.read_page(page_id) {
                Ok(bytes) => {
                    debug!("--- read from disk successfully");
                    page.node = Some(Node::new_from_bytes(&bytes));
                }
                Err(err) => {
                    // This either mean the file is corrupted or is a partial page
                    // or it's just a new file.
                    debug!("--- fail reading from disk: {:?}", err);
                    page.node = Some(Node::root());
                }
            }

            page.pin_count += 1;
            self.replacer.pin(frame_id);
            return self.pages.get_mut(frame_id);
        };

        None
    }

    pub fn flush_page(&mut self, page_id: usize) {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if let Some(node) = &self.pages[frame_id].node {
                let bytes = node.to_bytes();
                self.disk_manager.write_page(page_id, &bytes).unwrap();
            }
        }
    }

    pub fn flush_all_pages(&mut self) {
        for page in &self.pages {
            if page.page_id.is_none() {
                break;
            }

            if let Some(node) = &page.node {
                let bytes = node.to_bytes();
                self.disk_manager
                    .write_page(page.page_id.unwrap(), &bytes)
                    .unwrap();
            }
        }
    }

    pub fn delete_page(&mut self, page_id: usize) -> bool {
        debug!("--- delete page {page_id}");
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page = &self.pages[frame_id];
            if page.pin_count == 0 {
                debug!("--- page {page_id} found, deleting it...");
                self.pages[frame_id].deallocate();
                self.page_table.remove(&page_id);
                self.free_list.push(frame_id);

                true
            } else {
                debug!("--- page is being used, can't be deleted");
                false
            }
        } else {
            debug!("--- page {page_id} not found");
            true
        }
    }

    pub fn unpin_page(&mut self, page_id: usize, is_dirty: bool) {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page = &mut self.pages[frame_id];
            if !page.is_dirty {
                page.is_dirty = is_dirty;
            }
            page.pin_count -= 1;

            if page.pin_count == 0 {
                self.replacer.unpin(frame_id);
            }
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

    pub fn insert_record(&mut self, row: &Row, cursor: &Cursor) {
        if let Some(page) = self.fetch_page(cursor.page_num) {
            let node = page.node.as_mut().unwrap();
            let num_of_cells = node.num_of_cells as usize;
            if num_of_cells >= LEAF_NODE_MAX_CELLS {
                self.insert_and_split_leaf_node(cursor, row);
            } else {
                node.insert(row, cursor);
                self.unpin_page(cursor.page_num, true)
            }
        }
    }

    pub fn create_new_root(&mut self, right_node_page_num: usize, mut left_node: Node) {
        debug!("--- create_new_root");
        let right_node = self.fetch_node(right_node_page_num).unwrap();
        let mut root_node = Node::new(true, NodeType::Internal);
        right_node.is_root = false;

        // The only reason we could hardcode all of the offsets
        // is becuase we always insert root node to 0 and it's children to index
        // 1 and 2, when we create a new root node.
        root_node.num_of_cells += 1;
        root_node.right_child_offset = 2;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        left_node.next_leaf_offset = 2;
        left_node.parent_offset = 0;

        let left_max_key = left_node.get_max_key();
        let cell = InternalCell::new(right_node_page_num as u32, left_max_key);
        root_node.internal_cells.insert(0, cell);
    }

    fn insert_and_split_leaf_node(&mut self, cursor: &Cursor, row: &Row) {
        // We can unwrap here since, this will be called by insert_record
        // which have already check if the page of cursor.page_num existed.
        let right_node = self.fetch_node(cursor.page_num).unwrap();
        // let old_max = right_node.get_max_key();
        right_node.insert(row, cursor);

        let mut left_node = Node::new(false, right_node.node_type);
        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = right_node.cells.remove(0);
            right_node.num_of_cells -= 1;

            left_node.cells.push(cell);
            left_node.num_of_cells += 1;
        }

        if right_node.is_root {
            self.create_new_root(cursor.page_num, left_node);
        } else {
            unimplemented!("need to split internal node and update parent");
        }
    }

    pub fn get_record(&mut self, cursor: &Cursor) -> Row {
        debug!("--- get_record: {:?}", cursor);
        if let Some(page) = self.fetch_page(cursor.page_num) {
            let node = page.node.as_mut().unwrap();
            let row = node.get(cursor.cell_num);
            self.unpin_page(cursor.page_num, false);
            return row;
        }

        panic!("row not found...");
    }

    pub fn delete_row(&mut self, cursor: &Cursor) {
        self.get_page(cursor.page_num);
        self.tree.delete(cursor);
    }

    pub fn tree_len(&self) -> usize {
        self.tree.len()
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
    use crate::table::Table;

    #[test]
    fn lru_replacer_evict_least_recently_accessed_page() {
        let mut replacer = LRUReplacer::new(4);

        // We have 3 candidates that can be choose to
        // be evicted by our buffer pool.
        replacer.unpin(2);
        sleep(5);
        replacer.unpin(0);
        sleep(5);
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
        sleep(5);
        replacer.unpin(0);
        sleep(5);
        replacer.unpin(1);
        replacer.pin(2);

        let evicted_page = replacer.victim().unwrap();
        assert_eq!(evicted_page.frame_id, 0);
    }

    #[test]
    fn pager_new_page_when_page_cache_is_not_full() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        let frame_id = pager.find_or_create_page(0);
        assert!(frame_id.is_some());
        let frame_id = frame_id.unwrap();

        let page = &pager.pages[frame_id];
        assert_eq!(page.page_id, Some(0));
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
        assert!(page.node.is_none());
        cleanup_test_db_file();
    }

    #[test]
    fn pager_find_or_create_page_when_page_cache_is_full_with_victims_in_replacer() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        // Since our pool size is hardcoded to 4,
        // we just need to fetch 4 pages to fill
        // up the page cache.
        pager.fetch_page(0);
        pager.fetch_page(4);
        pager.fetch_page(2);
        pager.fetch_page(5);

        // Unpin some of the pages so there's
        // victim from our replacer.
        pager.unpin_page(2, false);
        pager.unpin_page(5, false);

        let frame_id = pager.find_or_create_page(7);
        assert!(frame_id.is_some());

        let frame_id = frame_id.unwrap();
        let page = &pager.pages[frame_id];
        assert_eq!(page.page_id, Some(7));
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
        assert!(page.node.is_none());
        cleanup_test_db_file();
    }

    #[test]
    fn pager_find_or_create_page_page_when_no_pages_can_be_freed() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        // Since our pool size is hardcoded to 4,
        // we just need to fetch 4 pages to fill
        // up the page cache.
        pager.fetch_page(0);
        pager.fetch_page(4);
        pager.fetch_page(2);
        pager.fetch_page(5);

        let frame_id = pager.find_or_create_page(7);
        assert!(frame_id.is_none());
        cleanup_test_db_file();
    }

    #[test]
    fn pager_fetch_page() {}

    #[test]
    fn pager_delete_page() {}

    #[test]
    fn pager_unpin_page() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        pager.fetch_page(0);
        pager.fetch_page(0);

        pager.unpin_page(0, true);
        assert_eq!(pager.replacer.size(), 0);

        let page = pager.pages.get(0);
        assert!(page.is_some());

        let page = page.unwrap();
        assert_eq!(page.pin_count, 1);
        assert!(page.is_dirty);

        pager.unpin_page(0, false);

        // If a page pin_count reach 0,
        // it should be place into replacer.
        assert_eq!(pager.replacer.size(), 1);

        let page = pager.pages.get(0);
        assert!(page.is_some());

        let page = page.unwrap();
        assert_eq!(page.pin_count, 0);

        // If a page is previously dirty, it should stay
        // dirty.
        assert!(page.is_dirty);

        cleanup_test_db_file();
    }

    #[test]
    fn pager_flush_page() {}

    #[test]
    fn pager_flush_all_pages() {}

    #[test]
    fn pager_get_record() {
        setup_test_db_file();

        let mut pager = Pager::new("test.db");
        let cursor = Cursor {
            page_num: 1,
            cell_num: 0,
            key_existed: false,
            end_of_table: false,
        };

        let row = pager.get_record(&cursor);
        assert_eq!(row.id, 1);
        assert_eq!(row.username(), "user1");
        assert_eq!(row.email(), "user1@email.com");
        let page = pager.fetch_page(cursor.page_num).unwrap();
        assert_eq!(page.pin_count, 1);

        let cursor = Cursor {
            page_num: 2,
            cell_num: 1,
            key_existed: false,
            end_of_table: false,
        };

        let row = pager.get_record(&cursor);
        assert_eq!(row.id, 9);
        assert_eq!(row.username(), "user9");
        assert_eq!(row.email(), "user9@email.com");
        let page = pager.fetch_page(cursor.page_num).unwrap();
        assert_eq!(page.pin_count, 1);

        cleanup_test_db_file();
    }

    fn setup_test_db_file() {
        let mut table = Table::new("test.db");

        for i in 1..50 {
            let row =
                Row::from_statement(&format!("insert {i} user{i} user{i}@email.com")).unwrap();
            table.insert(&row);
        }

        table.flush();
    }

    fn cleanup_test_db_file() {
        let _ = std::fs::remove_file("test.db");
    }

    fn sleep(duration_in_ms: u64) {
        let ten_millis = std::time::Duration::from_millis(duration_in_ms);
        std::thread::sleep(ten_millis);
    }
}
