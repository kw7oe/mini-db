use std::collections::HashMap;
use std::path::PathBuf;

use super::node::{
    InternalCell, Node, INTERNAL_NODE_MAX_CELLS, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_RIGHT_SPLIT_COUNT,
};
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

    fn create_or_replace_page(&mut self, page_id: usize) -> Option<usize> {
        let frame_id = if let Some(frame_id) = self.free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.victim().map(|md| md.frame_id)
        };

        if let Some(frame_id) = frame_id {
            if let Some(page) = self.pages.get(frame_id) {
                if page.is_dirty {
                    let dirty_page_id = page.page_id.unwrap();
                    self.flush_page(dirty_page_id);
                }

                let page = self.pages.get_mut(frame_id).unwrap();
                page.is_dirty = false;
                page.pin_count = 0;
                page.page_id = Some(page_id);
                page.node = None;
                self.page_table.retain(|_, &mut fid| fid != frame_id);
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
        // Check if the page is already in memory. If yes,
        // just pin the page and return the node.

        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let mut page = &mut self.pages[frame_id];
            page.pin_count += 1;
            self.replacer.pin(frame_id);
            return self.pages.get_mut(frame_id);
        }

        if let Some(frame_id) = self.create_or_replace_page(page_id) {
            let page = &mut self.pages[frame_id];
            debug!("reading page {page_id} into frame {frame_id}");
            match self.disk_manager.read_page(page_id) {
                Ok(bytes) => {
                    page.node = Some(Node::new_from_bytes(&bytes));
                }
                Err(err) => {
                    // This either mean the file is corrupted or is a partial page
                    // or it's just a new file.
                    if page_id == 0 {
                        page.node = Some(Node::root());
                    }
                    self.next_page_id += 1;
                }
            }

            page.pin_count += 1;
            self.replacer.pin(frame_id);
            return self.pages.get_mut(frame_id);
        };

        None
    }

    pub fn flush_page(&mut self, page_id: usize) {
        debug!("flush page {page_id}");
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
        debug!(
            "insert record {} at page {}, cell {}",
            row.id, cursor.page_num, cursor.cell_num
        );

        if let Some(page) = self.fetch_page(cursor.page_num) {
            let node = page.node.as_mut().unwrap();
            let num_of_cells = node.num_of_cells as usize;
            if num_of_cells >= LEAF_NODE_MAX_CELLS {
                self.unpin_page(cursor.page_num, true);
                self.insert_and_split_leaf_node(cursor, row);
            } else {
                node.insert(row, cursor);
                self.unpin_page(cursor.page_num, true)
            }
        }
    }

    pub fn create_new_root(
        &mut self,
        left_node_page_num: usize,
        mut right_node: Node,
        max_key: u32,
    ) {
        debug!("--- create_new_root");
        let next_page_id = self.next_page_id as u32;
        let root_page = self.fetch_page(left_node_page_num).unwrap();
        let root_page_id = root_page.page_id.unwrap();

        let mut root_node = Node::new(true, NodeType::Internal);
        root_node.num_of_cells += 1;
        root_node.right_child_offset = next_page_id + 1;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        let mut left_node = root_page.node.take().unwrap();
        left_node.is_root = false;
        left_node.next_leaf_offset = next_page_id + 1;
        left_node.parent_offset = 0;

        let cell = InternalCell::new(next_page_id, max_key);
        root_node.internal_cells.insert(0, cell);

        root_page.node = Some(root_node);
        self.unpin_page(root_page_id, true);

        let left_page = self.fetch_page(self.next_page_id).unwrap();
        let left_page_id = left_page.page_id.unwrap();
        left_page.node = Some(left_node);
        self.unpin_page(left_page_id, true);

        let right_page = self.fetch_page(self.next_page_id).unwrap();
        let right_page_id = right_page.page_id.unwrap();
        right_page.node = Some(right_node);
        self.unpin_page(right_page_id, true);
    }

    pub fn insert_internal_node(&mut self, parent_page_num: usize, split_at_page_num: usize) {
        debug!("--- insert internal node {split_at_page_num} at parent {parent_page_num}");
        let parent_page = self.fetch_page(parent_page_num).unwrap();
        let parent_node = parent_page.node.as_ref().unwrap();
        let parent_right_child_offset = parent_node.right_child_offset as usize;
        self.unpin_page(parent_page_num, false);

        let new_node = self.fetch_node(split_at_page_num).unwrap();
        let new_child_max_key = new_node.get_max_key();
        new_node.parent_offset = parent_page_num as u32;
        self.unpin_page(split_at_page_num, true);

        let right_node = self.fetch_node(parent_right_child_offset).unwrap();
        let right_max_key = right_node.get_max_key();
        self.unpin_page(parent_right_child_offset, false);

        let parent_page = self.fetch_page(parent_page_num).unwrap();
        let parent_node = parent_page.node.as_mut().unwrap();
        parent_node.num_of_cells += 1;

        let index = parent_node.internal_search(new_child_max_key);
        if new_child_max_key > right_max_key {
            debug!("--- child max key: {new_child_max_key} > right_max_key: {right_max_key}");
            parent_node.right_child_offset = split_at_page_num as u32;
            parent_node.internal_insert(
                index,
                InternalCell::new(parent_right_child_offset as u32, right_max_key),
            );
        } else {
            debug!("--- child max key: {new_child_max_key} <= right_max_key: {right_max_key}");
            parent_node.internal_insert(
                index,
                InternalCell::new(split_at_page_num as u32, new_child_max_key),
            );
        }

        self.unpin_page(parent_page_num, true);
        self.maybe_split_internal_node(parent_page_num);
    }

    pub fn update_children_parent_offset(&mut self, page_num: usize) {
        let node = self.fetch_node(page_num).unwrap();

        let mut child_pointers = vec![node.right_child_offset as usize];
        for cell in &node.internal_cells {
            child_pointers.push(cell.child_pointer() as usize);
        }
        self.unpin_page(page_num, false);

        for i in child_pointers {
            let page = self.fetch_page(i).unwrap();
            let child = page.node.as_mut().unwrap();
            child.parent_offset = page_num as u32;
            self.unpin_page(i, true);
        }
    }

    pub fn maybe_split_internal_node(&mut self, page_num: usize) {
        let next_page_id = self.next_page_id;
        let left_page = self.fetch_page(page_num).unwrap();
        let left_node = left_page.node.as_ref().unwrap();

        if left_node.num_of_cells > INTERNAL_NODE_MAX_CELLS as u32 {
            let left_node = left_page.node.as_mut().unwrap();
            let split_at_index = left_node.num_of_cells as usize / 2;

            let mut right_node = Node::new(false, NodeType::Internal);
            right_node.right_child_offset = left_node.right_child_offset;
            right_node.parent_offset = left_node.parent_offset as u32;

            let ic = left_node.internal_cells.remove(split_at_index);
            left_node.num_of_cells -= 1;
            left_node.right_child_offset = ic.child_pointer();

            for i in 0..split_at_index - 1 {
                let ic = left_node.internal_cells.remove(split_at_index);
                left_node.num_of_cells -= 1;
                right_node.internal_insert(i, ic);
                right_node.num_of_cells += 1;
            }

            let left_node = left_page.node.as_ref().unwrap();
            if left_node.is_root {
                debug!("splitting root internal node...");
                self.unpin_page(page_num, true);
                self.create_new_root(page_num, right_node, ic.key());

                self.update_children_parent_offset(next_page_id as usize);
                self.update_children_parent_offset(next_page_id as usize + 1);
            } else {
                debug!("update internal node {page_num}, parent...");
                let parent_offset = left_node.parent_offset as usize;
                self.unpin_page(page_num, true);
                let parent = self.fetch_node(parent_offset).unwrap();

                let index = parent.internal_search_child_pointer(page_num as u32);

                if parent.num_of_cells == index as u32 {
                    debug!("update parent after split most right internal node");
                    parent.right_child_offset = next_page_id as u32;
                    parent.internal_insert(index, InternalCell::new(page_num as u32, ic.key()));
                    parent.num_of_cells += 1;
                } else {
                    debug!("update parent after split internal node");
                    parent.internal_insert(index, InternalCell::new(page_num as u32, ic.key()));

                    let internel_cell = parent.internal_cells.remove(index + 1);
                    parent.internal_insert(
                        index + 1,
                        InternalCell::new(next_page_id as u32, internel_cell.key()),
                    );
                    parent.num_of_cells += 1;
                }
                self.unpin_page(parent_offset, true);

                let right_page = self.fetch_page(next_page_id).unwrap();
                right_page.is_dirty = true;
                right_page.node = Some(right_node);
                self.unpin_page(next_page_id, true);
                self.update_children_parent_offset(next_page_id);
            }
        } else {
            self.unpin_page(page_num, true);
        }
    }

    fn insert_and_split_leaf_node(&mut self, cursor: &Cursor, row: &Row) {
        debug!("--- insert_and_split_leaf_node");
        // We can unwrap here since, this will be called by insert_record
        // which have already check if the page of cursor.page_num existed.
        let left_node = self.fetch_node(cursor.page_num).unwrap();
        let old_max = left_node.get_max_key();
        left_node.insert(row, cursor);

        let mut right_node = Node::new(false, left_node.node_type);
        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = left_node.cells.remove(LEAF_NODE_LEFT_SPLIT_COUNT);
            left_node.num_of_cells -= 1;

            right_node.cells.push(cell);
            right_node.num_of_cells += 1;
        }

        let left_max_key = left_node.get_max_key();

        if left_node.is_root {
            self.unpin_page(cursor.page_num, true);
            self.create_new_root(cursor.page_num, right_node, left_max_key);
        } else {
            debug!("--- split leaf node and update parent ---");
            self.unpin_page(cursor.page_num, true);

            let next_page_id = self.next_page_id as u32;
            let left_node = self.fetch_node(cursor.page_num).unwrap();
            right_node.next_leaf_offset = left_node.next_leaf_offset;
            left_node.next_leaf_offset = next_page_id;
            right_node.parent_offset = left_node.parent_offset;

            let parent_page_num = left_node.parent_offset as usize;
            let new_max = left_node.get_max_key();
            self.unpin_page(cursor.page_num, true);

            let parent_page = self.fetch_page(parent_page_num).unwrap();
            let parent_node = parent_page.node.as_mut().unwrap();
            parent_node.update_internal_key(old_max, new_max);
            self.unpin_page(parent_page_num, true);

            let right_page = self.fetch_page(self.next_page_id).unwrap();
            let right_page_id = right_page.page_id.unwrap();
            right_page.node = Some(right_node);
            self.unpin_page(right_page_id, true);

            self.insert_internal_node(parent_page_num, right_page_id);
        }
    }

    pub fn get_record(&mut self, cursor: &Cursor) -> Row {
        debug!(
            "--- get_record at page {}, cell {}",
            cursor.page_num, cursor.cell_num
        );
        if let Some(page) = self.fetch_page(cursor.page_num) {
            let node = page.node.as_mut().unwrap();
            let row = node.get(cursor.cell_num);
            self.unpin_page(cursor.page_num, false);
            return row;
        }

        panic!("row not found...");
    }

    pub fn delete_record(&mut self, cursor: &Cursor) {
        let page = self.fetch_page(cursor.page_num).unwrap();
        let node = page.node.as_mut().unwrap();
        node.delete(cursor.cell_num);
        self.unpin_page(cursor.page_num, true);
    }

    pub fn delete_row(&mut self, cursor: &Cursor) {
        self.get_page(cursor.page_num);
        self.tree.delete(cursor);
    }

    pub fn tree_len(&self) -> usize {
        self.tree.len()
    }

    pub fn debug_pages(&mut self) {
        println!("\n\n------ DEBUG ------");
        for i in 0..self.next_page_id {
            let bytes = self.disk_manager.read_page(i).unwrap();
            println!("--- Page {i} ---");
            println!("{:?}", Node::new_from_bytes(&bytes));
        }
        println!("------ END DEBUG ------\n\n");
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
    fn pager_create_or_replace_page_when_page_cache_is_not_full() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        let frame_id = pager.create_or_replace_page(0);
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
    fn pager_create_or_replace_page_when_page_cache_is_full_with_victims_in_replacer() {
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
        sleep(5);
        pager.unpin_page(5, false);

        let frame_id = pager.create_or_replace_page(7);
        assert!(frame_id.is_some());

        // Ensure that page table only record page metadata
        // in our pages
        assert_eq!(pager.page_table.get(&2), None);
        assert_eq!(pager.page_table.len(), 4);

        // TODO: test it flush dirty page

        let frame_id = frame_id.unwrap();
        let page = &pager.pages[frame_id];
        assert_eq!(page.page_id, Some(7));
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
        assert!(page.node.is_none());
        cleanup_test_db_file();
    }

    #[test]
    fn pager_create_or_replace_page_when_no_pages_can_be_freed() {
        setup_test_db_file();
        let mut pager = Pager::new("test.db");

        // Since our pool size is hardcoded to 4,
        // we just need to fetch 4 pages to fill
        // up the page cache.
        pager.fetch_page(0);
        pager.fetch_page(4);
        pager.fetch_page(2);
        pager.fetch_page(5);

        let frame_id = pager.create_or_replace_page(7);
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
