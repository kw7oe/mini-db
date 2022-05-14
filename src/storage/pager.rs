use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::debug;

use super::node::{
    InternalCell, Node, INTERNAL_NODE_MAX_CELLS, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_RIGHT_SPLIT_COUNT,
};
use crate::row::Row;
use crate::storage::{DiskManager, NodeType};
use std::time::Instant;

pub const PAGE_SIZE: usize = 4096;
const SLEEP_MS: u64 = 10;
const MAX_RETRY: usize = 2000 / SLEEP_MS as usize;

#[derive(Debug)]
pub struct Cursor {
    pub page_num: usize,
    pub cell_num: usize,
    pub key_existed: bool,
    pub end_of_table: bool,
}

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
    page_table: RwLock<Vec<PageMetadata>>,
}

impl LRUReplacer {
    pub fn new(pool_size: usize) -> Self {
        Self {
            page_table: RwLock::new(Vec::with_capacity(pool_size)),
        }
    }

    #[cfg(test)]
    /// Number of frames that are currently in the replacer.
    pub fn size(&self) -> usize {
        let page_table = self.page_table.read();
        page_table.len()
    }

    /// Return frame metadata that are accessed least recently
    /// as compared to the other frame.
    pub fn victim(&self) -> Option<PageMetadata> {
        let mut page_table = self.page_table.write();
        page_table.sort_by(|a, b| b.last_accessed_at.cmp(&a.last_accessed_at));
        page_table.pop()
    }

    /// This should be called after our Pager place the page into
    /// our memory. Here, pin a frame means removing it from our
    /// replacer. I guess this prevent it from the page being
    /// evicted
    pub fn pin(&self, frame_id: usize) {
        let mut page_table = self.page_table.write();
        if let Some(index) = page_table.iter().position(|md| md.frame_id == frame_id) {
            page_table.remove(index);
        }
    }

    /// This should be called by our Pager when the page pin_count
    /// becomes 0. Here, unpin a frame means adding it to our
    /// replacer. This allow the page to be evicted.
    pub fn unpin(&self, frame_id: usize) {
        let mut page_table = self.page_table.write();
        page_table.push(PageMetadata::new(frame_id));
    }
}

#[derive(Debug)]
pub struct Page {
    page_id: Option<usize>,
    is_dirty: bool,
    pin_count: usize,
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
}

#[derive(Debug)]
pub enum PagerError {
    NoFreePageAvailable,
    FailToAcquirePageLock,
}

// #[derive(Debug)]
pub struct Pager {
    disk_manager: DiskManager,
    replacer: LRUReplacer,
    pages: Arc<Vec<RwLock<Page>>>,
    next_page_id: AtomicUsize,
    // Indexes in our `pages` that are "free", which mean
    // it is uninitialize.
    free_list: Mutex<Vec<usize>>,
    // Mapping page id to frame id
    page_table: Arc<RwLock<HashMap<usize, usize>>>,
}

impl Pager {
    pub fn new(path: impl AsRef<Path>) -> Pager {
        let pool_size = 8;

        // Initialize free list.
        let mut free_list = Vec::with_capacity(pool_size);
        for i in (0..pool_size).rev() {
            free_list.push(i);
        }

        // Initialize pages.
        //
        // Okay, while we can dynamically allocate new page as we need, it would make
        // implementing latch crabbing really tricky. In order to mutate our Vec<Page>
        // dynamically, it means we need to have interior mutability, but to make it thread
        // safe, we can't just use RefCell, we need to use a RwLock, which means we need
        // to lock the whole "B Tree"...
        //
        // Hence, for the sake of simplicity, I'll preallocate empty page first....
        let mut pages = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pages.push(RwLock::new(Page::new(None)));
        }

        let disk_manager = DiskManager::new(path);
        let next_page_id = disk_manager.file_len / PAGE_SIZE;

        Pager {
            disk_manager,
            replacer: LRUReplacer::new(pool_size),
            pages: Arc::new(pages),
            next_page_id: AtomicUsize::new(next_page_id),
            free_list: Mutex::new(free_list),
            page_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn new_page(&self) -> Option<RwLockWriteGuard<Page>> {
        let mut page_table = self.page_table.write();

        // Pop unused page index from free list.
        let mut free_list = self.free_list.lock();
        let frame_id = free_list
            .pop()
            .or_else(|| self.replacer.victim().map(|md| md.frame_id));
        drop(free_list);

        if let Some(frame_id) = frame_id {
            let unlock_page = self.pages.get(frame_id).unwrap();
            let mut page = unlock_page.write();

            // Check if page is dirty. Flush page to disk
            // if needed
            if page.is_dirty {
                let dirty_page_id = page.page_id.unwrap();
                self.flush_write_page(dirty_page_id, &page);
            }

            let page_id = self.next_page_id.fetch_add(1, Ordering::Acquire);

            // Update page table
            page_table.retain(|_, &mut fid| fid != frame_id);
            page_table.insert(page_id, frame_id);

            // Reset page
            page.is_dirty = false;
            page.pin_count = 0;
            page.page_id = Some(page_id);
            page.node = None;

            if page_id == 0 {
                page.node = Some(Node::root());
            }

            page.pin_count += 1;
            self.replacer.pin(frame_id);
            drop(page_table);

            Some(page)
        } else {
            drop(page_table);

            let duration = std::time::Duration::from_millis(SLEEP_MS);
            std::thread::sleep(duration);

            self.new_page()
        }
    }

    pub fn flush_write_page(&self, page_id: usize, page: &RwLockWriteGuard<Page>) {
        let node = page.node.as_ref().unwrap();
        let bytes = node.to_bytes();
        self.disk_manager.write_page(page_id, &bytes).unwrap();
    }

    pub fn flush_all_pages(&self) {
        for page in self.pages.iter() {
            let page = page.read();
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

    pub fn delete_page_with_write_guard(&self, page: &mut RwLockWriteGuard<Page>) -> bool {
        let page_id = page.page_id.unwrap();
        let mut page_table = self.page_table.write();
        if let Some(&frame_id) = page_table.get(&page_id) {
            if page.pin_count == 0 {
                page.deallocate();
                page_table.remove(&page_id);
                self.free_list.lock().push(frame_id);
                drop(page_table);

                true
            } else {
                drop(page_table);
                false
            }
        } else {
            drop(page_table);
            true
        }
    }

    pub fn unpin_page_with_write_guard(&self, page: &mut RwLockWriteGuard<Page>, is_dirty: bool) {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page.page_id.unwrap()) {
            if !page.is_dirty {
                page.is_dirty = is_dirty;
            }
            page.pin_count -= 1;

            if page.pin_count == 0 {
                self.replacer.unpin(frame_id);
            };

            drop(page_table);
        } else {
            drop(page_table);
        }
    }

    pub fn unpin_page(&self, page_id: usize, is_dirty: bool) {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = &self.pages[frame_id];
            if let Some(mut page) = page.try_write() {
                if !page.is_dirty {
                    page.is_dirty = is_dirty;
                }
                page.pin_count -= 1;

                if page.pin_count == 0 {
                    self.replacer.unpin(frame_id);
                };
                drop(page_table);

                drop(page);
            } else {
                drop(page_table);
                let duration = std::time::Duration::from_millis(SLEEP_MS);
                std::thread::sleep(duration);

                self.unpin_page(page_id, is_dirty);
            }
        }
    }

    pub fn select(&self, root_page_num: usize) -> String {
        let mut output = String::new();

        let mut page = self.search_page(root_page_num, 0);

        let mut page_num = page.page_id.unwrap();
        let mut node = page.node.as_ref().unwrap();
        assert_eq!(node.node_type, NodeType::Leaf);

        if node.num_of_cells == 0 {
            drop(page);
            self.unpin_page(page_num, false);

            return output;
        };

        loop {
            for i in 0..node.num_of_cells as usize {
                let row = node.get(i);
                output.push_str(&row.to_string());
                output.push('\n');
            }

            if node.next_leaf_offset == 0 {
                drop(page);
                self.unpin_page(page_num, false);

                break;
            } else {
                let old_page_num = page_num;
                page_num = node.next_leaf_offset as usize;

                drop(page);
                self.unpin_page(old_page_num, false);

                page = self.fetch_read_page_with_retry(page_num);
                node = page.node.as_ref().unwrap();
            }
        }

        output
    }

    fn search_page(&self, page_num: usize, key: u32) -> RwLockReadGuard<Page> {
        match self.fetch_read_page_guard(page_num) {
            Err(_) => {
                let duration = std::time::Duration::from_millis(SLEEP_MS);
                std::thread::sleep(duration);

                self.search_page(0, key)
            }
            Ok(page) => {
                let node = page.node.as_ref().unwrap();

                if node.node_type == NodeType::Leaf {
                    return page;
                }

                let next_page_num = node.search(key).unwrap();
                drop(page);
                self.unpin_page(page_num, false);
                self.search_page(next_page_num, key)
            }
        }
    }

    pub fn find(
        &self,
        page_num: usize,
        parent_page_guard: Option<RwLockReadGuard<Page>>,
        key: u32,
    ) -> String {
        match self.fetch_read_page_guard(page_num) {
            Err(_) => {
                if let Some(page) = parent_page_guard {
                    drop(page);
                }

                let duration = std::time::Duration::from_millis(SLEEP_MS);
                std::thread::sleep(duration);

                self.find(0, None, key)
            }
            Ok(page) => {
                let page_id = page.page_id.unwrap();
                let node = page.node.as_ref().unwrap();

                if node.node_type == NodeType::Leaf {
                    if let Some(page) = parent_page_guard {
                        drop(page);
                    }

                    match node.search(key) {
                        Ok(index) => {
                            let row = node.get(index);
                            drop(page);
                            self.unpin_page(page_id, false);
                            format!("{}\n", row.to_string())
                        }
                        Err(_index) => {
                            drop(page);
                            self.unpin_page(page_id, false);
                            "".to_string()
                        }
                    }
                } else if let Ok(next_page_num) = node.search(key) {
                    if let Some(page) = parent_page_guard {
                        drop(page);
                    }

                    self.find(next_page_num, Some(page), key)
                } else {
                    unreachable!("this shouldn't happen!");
                }
            }
        }
    }

    fn min_key(&self, max_degree: usize) -> usize {
        let mut min_key = (max_degree / 2) - 1;

        if min_key == 0 {
            min_key = 1;
        }

        min_key
    }

    pub fn node_to_string(&self, node_index: usize, indent_level: usize) -> String {
        let page = self.fetch_read_page_guard(node_index).unwrap();
        let node = page.node.as_ref().unwrap();
        let mut result = String::new();

        if node.node_type == NodeType::Internal {
            for _ in 0..indent_level {
                result += "  ";
            }
            result += &format!("- internal (size {})\n", node.num_of_cells);
            let most_righ_child_index = node.right_child_offset as usize;

            let mut child_pointers = vec![];
            for c in &node.internal_cells {
                let child_index = c.child_pointer() as usize;
                child_pointers.push((child_index, c.key()));
            }
            drop(page);
            self.unpin_page(node_index, false);

            for (i, k) in child_pointers {
                result += &self.node_to_string(i, indent_level + 1);

                for _ in 0..indent_level + 1 {
                    result += "  ";
                }
                result += &format!("- key {}\n", k);
            }

            result += &self.node_to_string(most_righ_child_index, indent_level + 1);
        } else if node.node_type == NodeType::Leaf {
            for _ in 0..indent_level {
                result += "  ";
            }

            result += &format!("- leaf (size {})\n", node.num_of_cells);
            for c in &node.cells {
                for _ in 0..indent_level + 1 {
                    result += "  ";
                }
                result += &format!("- {}\n", c.key());
            }

            drop(page);
            self.unpin_page(node_index, false);
        }

        result
    }

    pub fn to_tree_string(&self) -> String {
        if self.next_page_id.load(Ordering::Acquire) != 0 {
            self.node_to_string(0, 0)
        } else {
            "Empty tree...".to_string()
        }
    }

    // ---------------------
    // Concurrent Operations
    // ---------------------
    fn retry<T, F>(&self, max_retry: usize, func: F) -> T
    where
        F: Fn() -> Result<T, PagerError>,
    {
        match func() {
            Err(_) => {
                if max_retry == 0 {
                    panic!("exceed max retry");
                }

                let duration = std::time::Duration::from_millis(SLEEP_MS);
                std::thread::sleep(duration);

                self.retry(max_retry - 1, func)
            }
            Ok(page) => page,
        }
    }

    fn fetch_write_page_guard_with_retry(&self, page_num: usize) -> RwLockWriteGuard<Page> {
        self.retry(MAX_RETRY, || self.fetch_write_page_guard(page_num))
    }

    fn fetch_read_page_with_retry(&self, page_num: usize) -> RwLockReadGuard<Page> {
        self.retry(MAX_RETRY, || self.fetch_read_page_guard(page_num))
    }

    pub fn fetch_write_page_guard(
        &self,
        page_id: usize,
    ) -> Result<RwLockWriteGuard<Page>, PagerError> {
        let page_table = self.page_table.upgradable_read();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = self.pages.get(frame_id).unwrap();

            if let Some(mut page) = page.try_write() {
                page.pin_count += 1;
                self.replacer.pin(frame_id);
                drop(page_table);

                return Ok(page);
            } else {
                drop(page_table);
                return Err(PagerError::FailToAcquirePageLock);
            }
        }

        self.replace_page(page_table, page_id)
    }

    pub fn fetch_read_page_guard(
        &self,
        page_id: usize,
    ) -> Result<RwLockReadGuard<Page>, PagerError> {
        let page_table = self.page_table.upgradable_read();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = self.pages.get(frame_id).unwrap();
            if let Some(mut page) = page.try_write() {
                page.pin_count += 1;
                self.replacer.pin(frame_id);
                drop(page_table);

                let page = RwLockWriteGuard::downgrade(page);
                return Ok(page);
            } else {
                drop(page_table);
                return Err(PagerError::FailToAcquirePageLock);
            }
        }

        self.replace_page(page_table, page_id)
            .map(RwLockWriteGuard::downgrade)
    }

    fn replace_page(
        &self,
        page_table: RwLockUpgradableReadGuard<HashMap<usize, usize>>,
        page_id: usize,
    ) -> Result<RwLockWriteGuard<Page>, PagerError> {
        let mut page_table = RwLockUpgradableReadGuard::upgrade(page_table);
        let mut free_list = self.free_list.lock();
        let frame_id = free_list
            .pop()
            .or_else(|| self.replacer.victim().map(|md| md.frame_id));
        drop(free_list);

        if let Some(frame_id) = frame_id {
            let unlock_page = self.pages.get(frame_id).unwrap();
            let mut page = unlock_page.write();

            // Update page table
            page_table.retain(|_, &mut fid| fid != frame_id);
            page_table.insert(page_id, frame_id);

            // Check if page is dirty. Flush page to disk
            // if needed
            if page.is_dirty {
                let dirty_page_id = page.page_id.unwrap();
                self.flush_write_page(dirty_page_id, &page);
            }

            // Reset page
            page.is_dirty = false;
            page.pin_count = 1;
            page.page_id = Some(page_id);

            match self.disk_manager.read_page(page_id) {
                Ok(bytes) => {
                    page.node = Some(Node::new_from_bytes(&bytes));
                }
                Err(_err) => {
                    // This either mean the file is corrupted or is a partial page
                    // or it's just a new file.
                    if page_id == 0 {
                        page.node = Some(Node::root());
                    }

                    self.next_page_id.fetch_add(1, Ordering::SeqCst);
                }
            };
            self.replacer.pin(frame_id);
            drop(page_table);

            Ok(page)
        } else {
            drop(page_table);
            Err(PagerError::NoFreePageAvailable)
        }
    }

    pub fn search_and_then<F, T>(
        &self,
        parent_page_guards: Vec<RwLockWriteGuard<Page>>,
        page_num: usize,
        key: u32,
        operation: &str,
        func: F,
    ) -> Option<T>
    where
        F: FnOnce(Cursor, Vec<RwLockWriteGuard<Page>>, RwLockWriteGuard<Page>) -> Option<T>,
    {
        match self.fetch_write_page_guard(page_num) {
            Ok(page) => {
                let node = page.node.as_ref().unwrap();
                let num_of_cells = node.num_of_cells as usize;
                if node.node_type == NodeType::Leaf {
                    match node.search(key) {
                        Ok(index) => func(
                            Cursor {
                                page_num,
                                cell_num: index,
                                key_existed: true,
                                end_of_table: index == num_of_cells,
                            },
                            parent_page_guards,
                            page,
                        ),
                        Err(index) => func(
                            Cursor {
                                page_num,
                                cell_num: index,
                                key_existed: false,
                                end_of_table: index == num_of_cells,
                            },
                            parent_page_guards,
                            page,
                        ),
                    }
                } else if let Ok(next_page_num) = node.search(key) {
                    // If our internal node might need to split, we'll continue to hold the
                    // lock.
                    let predicate = if operation == "insert" {
                        // node.num_of_cells + 1 > INTERNAL_NODE_MAX_CELLS as u32
                        true
                    } else {
                        // Okay, initially, we are planning to drop any parent lock if we find that
                        // a merge won't occur in my current node.
                        //
                        // However, wit this it will introduce a race condition when 2 thread under
                        // the same parent are merging at the same time. For example, let' say we
                        // have a Tree as follow:
                        //
                        //          [ 0 ]
                        //    8  /         \  9
                        // [2 | 4 | 5]  [6 | 7]
                        //
                        // Thread A will be deleting key under page 8, and Thread B will be
                        // deleting key under page 9. So here's how things might interleave
                        // and caused a deadlock:
                        //
                        // T1: A hold page 0 write lock. B wait.
                        // T2: A hold page 8 write lock. Release page 0 write lock due to this
                        // optimisation.
                        // T3: B hold page 0 write lock.
                        // T4: B hold page 9 write lock. Not releasing page 0 as merge might occur.
                        // T5: B continue with deletion. Merge leaf nodes.
                        // T6: B continue to merge internal nodes, which then require to fetch it's
                        // sibling page 8.
                        // T7: B waiting for page 8 write lock.
                        // ??? why A can't continue with it's operation?
                        // T8: A delete page N. However, since thread B is holding page table read
                        // lock. A can't get the page_table write lock. This caused a deadlock.
                        // TN: B acquire page 8 write lock and continue the merge.
                        //
                        // let min_key_length = self.min_key(INTERNAL_NODE_MAX_CELLS) as u32;
                        // node.num_of_cells - 1 <= min_key_length as u32
                        true
                    };

                    if predicate {
                        let mut parent_page_guards = parent_page_guards;
                        parent_page_guards.push(page);
                        self.search_and_then(
                            parent_page_guards,
                            next_page_num,
                            key,
                            operation,
                            func,
                        )
                    } else {
                        // Else, we drop the parent lock first since we know that any changes
                        // in our children will not caused a split in this node, and hence
                        // won't affect our parent node.
                        for mut page in parent_page_guards {
                            self.unpin_page_with_write_guard(&mut page, false);
                            drop(page);
                        }
                        self.search_and_then(vec![page], next_page_num, key, operation, func)
                    }
                } else {
                    unreachable!("this shouldn't happen!");
                }
            }
            Err(_) => {
                for mut page in parent_page_guards {
                    self.unpin_page_with_write_guard(&mut page, false);
                    drop(page);
                }

                let duration = std::time::Duration::from_millis(SLEEP_MS);
                std::thread::sleep(duration);

                // Restart at root
                self.search_and_then(vec![], 0, key, operation, func)
            }
        }
    }

    pub fn insert(&self, root_page_num: usize, row: &Row) -> Option<String> {
        self.search_and_then(
            vec![],
            root_page_num,
            row.id,
            "insert",
            |cursor, parent_page_guards, mut page| {
                if cursor.key_existed {
                    return Some("duplicate key\n".to_string());
                };

                let node = page.node.as_ref().unwrap();
                let num_of_cells = node.num_of_cells as usize;

                if num_of_cells >= LEAF_NODE_MAX_CELLS {
                    self.concurrent_insert_and_split_node(parent_page_guards, page, &cursor, row);
                } else {
                    let node = page.node.as_mut().unwrap();
                    node.insert(row, &cursor);

                    for mut page in parent_page_guards {
                        self.unpin_page_with_write_guard(&mut page, false);
                        drop(page);
                    }

                    self.unpin_page_with_write_guard(&mut page, true);
                    drop(page);
                }

                Some(format!(
                    "inserting into page: {}, cell: {}...\n",
                    cursor.page_num, cursor.cell_num
                ))
            },
        )
    }

    fn concurrent_insert_and_split_node(
        &self,
        parent_page_guards: Vec<RwLockWriteGuard<Page>>,
        mut left_page: RwLockWriteGuard<Page>,
        cursor: &Cursor,
        row: &Row,
    ) {
        let left_node = left_page.node.as_mut().unwrap();
        let old_max = left_node.get_max_key();
        left_node.insert(row, cursor);

        let mut right_node = Node::new(false, left_node.node_type);
        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = left_node.cells.remove(LEAF_NODE_LEFT_SPLIT_COUNT);
            left_node.num_of_cells -= 1;

            right_node.cells.push(cell);
            right_node.num_of_cells += 1;
        }

        if left_node.is_root {
            let left_max_key = left_node.get_max_key();

            // If left node is root it shouldn't have any parent.
            assert_eq!(parent_page_guards.len(), 0);

            self.concurrent_create_new_root(left_page, right_node, left_max_key);
        } else {
            self.concurrent_split_node_and_update_parent(
                parent_page_guards,
                left_page,
                right_node,
                old_max,
            );
        }
    }

    fn concurrent_split_node_and_update_parent(
        &self,
        mut parent_page_guards: Vec<RwLockWriteGuard<Page>>,
        mut left_page: RwLockWriteGuard<Page>,
        mut right_node: Node,
        max_key: u32,
    ) {
        let mut right_page = self.new_page().unwrap();
        let right_page_id = right_page.page_id.unwrap();
        let left_node = left_page.node.as_mut().unwrap();
        let new_max = left_node.get_max_key();

        right_node.next_leaf_offset = left_node.next_leaf_offset;
        left_node.next_leaf_offset = right_page_id as u32;

        let new_child_max_key = right_node.get_max_key();
        right_node.parent_offset = left_node.parent_offset;

        self.unpin_page_with_write_guard(&mut left_page, true);
        drop(left_page);

        right_page.node = Some(right_node);
        self.unpin_page_with_write_guard(&mut right_page, true);
        drop(right_page);

        assert!(!parent_page_guards.is_empty());
        let mut parent_page = parent_page_guards.pop().unwrap();
        let parent_node = parent_page.node.as_mut().unwrap();
        parent_node.update_internal_key(max_key, new_max);

        let split_at_page_num = right_page_id;

        let parent_node = parent_page.node.as_ref().unwrap();
        let parent_right_child_offset = parent_node.right_child_offset as usize;

        let mut most_right_page = self.fetch_write_page_guard_with_retry(parent_right_child_offset);
        let right_node = most_right_page.node.as_ref().unwrap();
        let right_max_key = right_node.get_max_key();
        self.unpin_page_with_write_guard(&mut most_right_page, false);
        drop(most_right_page);

        let parent_node = parent_page.node.as_mut().unwrap();
        parent_node.num_of_cells += 1;

        let index = parent_node.internal_search(new_child_max_key);
        if new_child_max_key > right_max_key {
            parent_node.right_child_offset = split_at_page_num as u32;
            parent_node.internal_insert(
                index,
                InternalCell::new(parent_right_child_offset as u32, right_max_key),
            );
        } else {
            parent_node.internal_insert(
                index,
                InternalCell::new(split_at_page_num as u32, new_child_max_key),
            );
        }

        if parent_node.num_of_cells > INTERNAL_NODE_MAX_CELLS as u32 {
            return self.concurrent_split_internal_node(parent_page, parent_page_guards);
        }

        for mut page in parent_page_guards {
            self.unpin_page_with_write_guard(&mut page, false);
            drop(page);
        }

        self.unpin_page_with_write_guard(&mut parent_page, true);
        drop(parent_page);
    }

    fn concurrent_create_new_root(
        &self,
        mut page: RwLockWriteGuard<Page>,
        mut right_node: Node,
        max_key: u32,
    ) {
        let mut left_page = self.new_page().unwrap();
        let left_page_id = left_page.page_id.unwrap() as u32;

        let mut right_page = self.new_page().unwrap();
        let right_page_id = right_page.page_id.unwrap() as u32;

        let mut root_node = Node::new(true, NodeType::Internal);
        root_node.num_of_cells += 1;
        root_node.right_child_offset = right_page_id;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        let mut left_node = page.node.take().unwrap();
        left_node.is_root = false;
        left_node.next_leaf_offset = right_page_id;
        left_node.parent_offset = 0;

        let cell = InternalCell::new(left_page_id, max_key);
        root_node.internal_cells.insert(0, cell);

        page.node = Some(root_node);
        left_page.node = Some(left_node);
        right_page.node = Some(right_node);

        self.concurrent_update_children_parent_offset(&mut left_page);
        self.unpin_page_with_write_guard(&mut left_page, true);
        drop(left_page);

        self.concurrent_update_children_parent_offset(&mut right_page);
        self.unpin_page_with_write_guard(&mut right_page, true);
        drop(right_page);

        self.unpin_page_with_write_guard(&mut page, true);
        drop(page);
    }

    pub fn concurrent_update_children_parent_offset(&self, page: &mut RwLockWriteGuard<Page>) {
        let node = page.node.as_ref().unwrap();
        let parent_page_id = page.page_id.unwrap();

        let mut child_pointers = vec![node.right_child_offset as usize];
        for cell in &node.internal_cells {
            child_pointers.push(cell.child_pointer() as usize);
        }

        child_pointers.retain(|&i| i != 0);
        for i in child_pointers {
            let mut page = self.fetch_write_page_guard_with_retry(i);
            let child = page.node.as_mut().unwrap();
            child.parent_offset = parent_page_id as u32;
            self.unpin_page_with_write_guard(&mut page, true);
            drop(page);
        }
    }

    pub fn concurrent_split_internal_node(
        &self,
        mut left_page: RwLockWriteGuard<Page>,
        mut parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
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
            assert_eq!(parent_page_guards.len(), 0);
            self.concurrent_create_new_root(left_page, right_node, ic.key());
        } else {
            let page_num = left_page.page_id.unwrap();

            assert!(!parent_page_guards.is_empty());
            let mut parent_page = parent_page_guards.pop().unwrap();
            let parent = parent_page.node.as_mut().unwrap();
            let index = parent.internal_search_child_pointer(page_num as u32);

            let mut right_page = self.new_page().unwrap();
            let right_page_id = right_page.page_id.unwrap() as u32;
            right_page.is_dirty = true;
            right_page.node = Some(right_node);

            if parent.num_of_cells == index as u32 {
                parent.right_child_offset = right_page_id;
                parent.internal_insert(index, InternalCell::new(page_num as u32, ic.key()));
                parent.num_of_cells += 1;
            } else {
                parent.internal_insert(index, InternalCell::new(page_num as u32, ic.key()));

                let internel_cell = parent.internal_cells.remove(index + 1);
                parent.internal_insert(
                    index + 1,
                    InternalCell::new(right_page_id, internel_cell.key()),
                );
                parent.num_of_cells += 1;
            }

            for mut page in parent_page_guards {
                self.unpin_page_with_write_guard(&mut page, false);
                drop(page);
            }

            self.unpin_page_with_write_guard(&mut parent_page, true);
            drop(parent_page);

            self.unpin_page_with_write_guard(&mut left_page, true);
            drop(left_page);

            self.concurrent_update_children_parent_offset(&mut right_page);
            self.unpin_page_with_write_guard(&mut right_page, true);
            drop(right_page);
        }
    }

    pub fn delete(&self, root_page_num: usize, row: &Row) -> Option<String> {
        self.search_and_then(
            vec![],
            root_page_num,
            row.id,
            "delete",
            |cursor, parent_page_guards, mut page| {
                if cursor.key_existed {
                    let node = page.node.as_mut().unwrap();
                    node.delete(cursor.cell_num);
                    self.concurrent_maybe_merge_nodes(page, parent_page_guards);

                    Some(format!("deleted {}", row.id))
                } else {
                    for mut page in parent_page_guards {
                        self.unpin_page_with_write_guard(&mut page, false);
                        drop(page);
                    }

                    self.unpin_page_with_write_guard(&mut page, false);
                    drop(page);

                    Some(format!("item not found with id {}", row.id))
                }
            },
        )
    }

    fn concurrent_maybe_merge_nodes(
        &self,
        mut page: RwLockWriteGuard<Page>,
        parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
        let node = page.node.as_ref().unwrap();

        if node.node_type == NodeType::Leaf
            && node.num_of_cells < LEAF_NODE_MAX_CELLS as u32 / 2
            && !node.is_root
        {
            return self.concurrent_merge_leaf_nodes(page, parent_page_guards);
        }

        for mut page in parent_page_guards {
            self.unpin_page_with_write_guard(&mut page, false);
            drop(page);
        }

        self.unpin_page_with_write_guard(&mut page, true);
        drop(page);
    }

    fn concurrent_merge_leaf_nodes(
        &self,
        mut page: RwLockWriteGuard<Page>,
        mut parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
        let page_id = page.page_id.unwrap();
        let node = page.node.as_ref().unwrap();
        let node_cells_len = node.cells.len();

        let mut parent_page = parent_page_guards.pop().unwrap();
        let parent = parent_page.node.as_ref().unwrap();
        let (left_child_pointer, right_child_pointer) = parent.siblings(page_id as u32);

        if let Some(cp) = left_child_pointer {
            let mut left_page = self.fetch_write_page_guard_with_retry(cp);
            let left_nb = left_page.node.as_ref().unwrap();

            if cp != page_id && left_nb.cells.len() + node_cells_len < LEAF_NODE_MAX_CELLS {
                debug!("Merging node {} with its left neighbour...", page_id);
                return self.concurrent_do_merge_leaf_nodes(
                    parent_page,
                    left_page,
                    page,
                    parent_page_guards,
                );
            }

            self.unpin_page_with_write_guard(&mut left_page, false);
            drop(left_page);
        }

        if let Some(cp) = right_child_pointer {
            let mut right_page = self.fetch_write_page_guard_with_retry(cp);
            let right_nb = right_page.node.as_ref().unwrap();

            if cp != page_id && right_nb.cells.len() + node_cells_len < LEAF_NODE_MAX_CELLS {
                debug!("Merging node {} with its right neighbour...", page_id);
                return self.concurrent_do_merge_leaf_nodes(
                    parent_page,
                    page,
                    right_page,
                    parent_page_guards,
                );
            }

            self.unpin_page_with_write_guard(&mut right_page, false);
            drop(right_page);
        }

        // Drop parent guards lock
        for mut page in parent_page_guards {
            self.unpin_page_with_write_guard(&mut page, false);
            drop(page);
        }

        self.unpin_page_with_write_guard(&mut parent_page, false);
        drop(parent_page);

        self.unpin_page_with_write_guard(&mut page, true);
        drop(page);
    }

    fn concurrent_do_merge_leaf_nodes(
        &self,
        mut parent_page: RwLockWriteGuard<Page>,
        mut left_page: RwLockWriteGuard<Page>,
        mut right_page: RwLockWriteGuard<Page>,
        parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
        let right_page_id = right_page.page_id.unwrap();
        let left_page_id = left_page.page_id.unwrap();

        // Take the node of right page and left page out of page.
        //
        // Free up the pages as we don't need it anymore.
        let left_node = left_page.node.as_mut().unwrap();
        let right_node = right_page.node.take().unwrap();

        // Merge the leaf nodes cells
        for c in right_node.cells {
            left_node.cells.push(c);
            left_node.num_of_cells += 1;
        }
        left_node.next_leaf_offset = right_node.next_leaf_offset;

        let parent = parent_page.node.as_mut().unwrap();

        if parent.num_of_cells == 1 && parent.is_root {
            self.concurrent_promote_node_to_root(parent_page, left_page, right_page);
        } else {
            self.unpin_page_with_write_guard(&mut right_page, true);
            self.delete_page_with_write_guard(&mut right_page);
            drop(right_page);

            let max_key = left_node.get_max_key();
            self.unpin_page_with_write_guard(&mut left_page, true);
            drop(left_page);

            let min_key_length = self.min_key(INTERNAL_NODE_MAX_CELLS) as u32;

            let index = parent.internal_search_child_pointer(right_page_id as u32);
            if index == parent.num_of_cells as usize {
                // The right_cp is our right child offset

                // Move last internal cell to become the right child offset
                let internal_cell = parent.internal_cells.remove(index - 1);
                parent.num_of_cells -= 1;
                parent.right_child_offset = internal_cell.child_pointer();
            } else {
                // Remove extra key, pointers cell as we now have one less child
                // after merge
                parent.num_of_cells -= 1;
                parent.internal_cells.remove(index);

                // Update the key for our existing child pointer pointing to our merged node
                // to use the new max key.
                if index != 0 {
                    parent.internal_cells[index - 1] =
                        InternalCell::new(left_page_id as u32, max_key);
                }
            }

            if parent.num_of_cells <= min_key_length && !parent.is_root {
                return self.concurrent_merge_internal_nodes(parent_page, parent_page_guards);
            }

            // Drop parent guards lock
            for mut page in parent_page_guards {
                self.unpin_page_with_write_guard(&mut page, false);
                drop(page);
            }

            self.unpin_page_with_write_guard(&mut parent_page, true);
            drop(parent_page);
        }
    }

    fn concurrent_promote_node_to_root(
        &self,
        mut parent_page: RwLockWriteGuard<Page>,
        mut left_page: RwLockWriteGuard<Page>,
        mut right_page: RwLockWriteGuard<Page>,
    ) {
        // Take left node out of left page as it will be used to replace
        // the node in our parent.
        let mut left_node = left_page.node.take().unwrap();

        // Replace the parent.node with our new combined left node
        left_node.is_root = true;
        left_node.next_leaf_offset = 0;
        parent_page.node = Some(left_node);

        self.concurrent_update_children_parent_offset(&mut parent_page);
        self.unpin_page_with_write_guard(&mut parent_page, true);
        drop(parent_page);

        self.unpin_page_with_write_guard(&mut left_page, true);
        self.delete_page_with_write_guard(&mut left_page);
        drop(left_page);

        self.unpin_page_with_write_guard(&mut right_page, true);
        self.delete_page_with_write_guard(&mut right_page);
        drop(right_page);
    }

    fn concurrent_merge_internal_nodes(
        &self,
        mut page: RwLockWriteGuard<Page>,
        mut parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
        let page_id = page.page_id.unwrap();
        let node = page.node.as_ref().unwrap();
        let node_num_of_cells = node.num_of_cells as usize;

        let mut parent_page = parent_page_guards.pop().unwrap();
        let parent = parent_page.node.as_ref().unwrap();

        let (left_child_pointer, right_child_pointer) = parent.siblings(page_id as u32);

        if let Some(cp) = left_child_pointer {
            let mut left_page = self.fetch_write_page_guard_with_retry(cp);
            let left_nb = left_page.node.as_ref().unwrap();

            if cp != page_id
                && left_nb.internal_cells.len() + node_num_of_cells <= INTERNAL_NODE_MAX_CELLS
            {
                debug!("Merging internal node {page_id} with left neighbour");
                self.concurrent_do_merge_internal_nodes(
                    parent_page,
                    left_page,
                    page,
                    parent_page_guards,
                );
                return;
            }

            self.unpin_page_with_write_guard(&mut left_page, false);
            drop(left_page);
        }

        if let Some(cp) = right_child_pointer {
            let mut right_page = self.fetch_write_page_guard_with_retry(cp);
            let right_nb = right_page.node.as_ref().unwrap();

            if cp != page_id
                && right_nb.internal_cells.len() + node_num_of_cells <= INTERNAL_NODE_MAX_CELLS
            {
                debug!("Merging internal node {page_id} with right neighbour");
                self.concurrent_do_merge_internal_nodes(
                    parent_page,
                    page,
                    right_page,
                    parent_page_guards,
                );
                return;
            }

            self.unpin_page_with_write_guard(&mut right_page, false);
            drop(right_page);
        }

        // Drop parent guards lock
        for mut page in parent_page_guards {
            self.unpin_page_with_write_guard(&mut page, false);
            drop(page);
        }

        self.unpin_page_with_write_guard(&mut page, true);
        drop(page);

        self.unpin_page_with_write_guard(&mut parent_page, false);
        drop(parent_page);
    }

    fn concurrent_do_merge_internal_nodes(
        &self,
        mut parent_page: RwLockWriteGuard<Page>,
        mut left_page: RwLockWriteGuard<Page>,
        mut right_page: RwLockWriteGuard<Page>,
        parent_page_guards: Vec<RwLockWriteGuard<Page>>,
    ) {
        let right_page_id = right_page.page_id.unwrap();
        let left_page_id = left_page.page_id.unwrap();

        let left_node = left_page.node.as_ref().unwrap();
        let left_node_right_child_offset = left_node.right_child_offset as usize;

        let mut left_most_right_child_page =
            self.fetch_write_page_guard_with_retry(left_node_right_child_offset);
        let left_most_right_child_node = left_most_right_child_page.node.as_ref().unwrap();
        let new_left_max_key = left_most_right_child_node.get_max_key();
        self.unpin_page_with_write_guard(&mut left_most_right_child_page, false);
        drop(left_most_right_child_page);

        let right_node = right_page.node.take().unwrap();

        let left_node = left_page.node.as_mut().unwrap();
        left_node.internal_cells.push(InternalCell::new(
            left_node.right_child_offset,
            new_left_max_key,
        ));
        left_node.num_of_cells += 1;

        for c in right_node.internal_cells {
            left_node.internal_cells.push(c);
            left_node.num_of_cells += 1;
        }

        left_node.right_child_offset = right_node.right_child_offset;

        // Update parent metadata
        let parent = parent_page.node.as_ref().unwrap();
        if parent.num_of_cells == 1 && parent.is_root {
            assert!(parent_page_guards.is_empty());
            self.concurrent_promote_node_to_root(parent_page, left_page, right_page);
        } else {
            let parent = parent_page.node.as_mut().unwrap();
            let parent_right_child_offset = parent.right_child_offset as usize;
            let index = parent.internal_search_child_pointer(left_page_id as u32);
            parent.internal_cells.remove(index);
            parent.num_of_cells -= 1;

            if right_page_id == parent_right_child_offset {
                debug!("  update parent after merging most right child");
                parent.right_child_offset = left_page_id as u32;
            } else {
                debug!("  update parent after merging child");
                parent.internal_cells[index].write_child_pointer(left_page_id as u32);
            }

            for mut page in parent_page_guards {
                self.unpin_page_with_write_guard(&mut page, false);
                drop(page);
            }

            self.unpin_page_with_write_guard(&mut right_page, true);
            self.delete_page_with_write_guard(&mut right_page);
            drop(right_page);

            self.concurrent_update_children_parent_offset(&mut left_page);
            self.unpin_page_with_write_guard(&mut left_page, true);
            drop(left_page);

            self.unpin_page_with_write_guard(&mut parent_page, true);
            drop(parent_page);
        }
    }

    pub fn debug_pages(&self) -> String {
        use std::fmt::Write;
        let mut result = String::new();
        for i in 0..self.next_page_id.load(Ordering::Relaxed) {
            let bytes = self.disk_manager.read_page(i).unwrap();
            writeln!(&mut result, "--- Page {} ---", i).unwrap();
            writeln!(&mut result, "{:?}", Node::new_from_bytes(&bytes)).unwrap();
        }
        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::table::Table;

    #[test]
    fn lru_replacer_evict_least_recently_accessed_page() {
        let replacer = LRUReplacer::new(4);

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
        let replacer = LRUReplacer::new(4);

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

    use std::sync::Arc;
    use std::thread;
    #[test]
    // I'm not really sure how to further verify
    // the behaviour when it being accessed concurrently.
    //
    // At least now it's "thread safe".
    fn lru_replacer_works_concurrently() {
        let replacer = Arc::new(LRUReplacer::new(4));

        let re = replacer.clone();
        let handle = thread::spawn(move || re.unpin(2));

        let re = replacer.clone();
        let handle2 = thread::spawn(move || re.unpin(3));

        handle.join().unwrap();
        handle2.join().unwrap();

        replacer.pin(2);

        let evicted_page = replacer.victim().unwrap();
        assert_eq!(evicted_page.frame_id, 3);
    }

    #[test]
    #[ignore]
    fn pager_create_or_replace_page_when_page_cache_is_not_full() {
        setup_test_db_file();
        let pager = setup_test_pager();

        // let frame_id = pager.create_or_replace_page(0);
        // assert!(frame_id.is_some());
        // let frame_id = frame_id.unwrap();

        // let page = &pager.pages[frame_id];
        // let page = page.read();
        // assert_eq!(page.page_id, Some(0));
        // assert!(!page.is_dirty);
        // assert_eq!(page.pin_count, 0);
        // assert!(page.node.is_none());
        // drop(page);
        cleanup_test_db_file();
    }

    // #[test]
    // fn pager_create_or_replace_page_when_page_cache_is_full_with_victims_in_replacer() {
    //     setup_test_db_file();
    //     let pager = setup_test_pager();

    //     // Since our pool size is hardcoded to 8,
    //     // we just need to fetch 8 pages to fill
    //     // up the page cache.
    //     pager.fetch_page(0);
    //     pager.fetch_page(4);
    //     pager.fetch_page(2);
    //     pager.fetch_page(5);
    //     pager.fetch_page(6);
    //     pager.fetch_page(8);
    //     pager.fetch_page(9);
    //     pager.fetch_page(10);

    //     // Unpin some of the pages so there's
    //     // victim from our replacer.
    //     pager.unpin_page(2, false);
    //     sleep(5);
    //     pager.unpin_page(5, false);

    //     // let frame_id = pager.create_or_replace_page(7);
    //     // assert!(frame_id.is_some());

    //     // Ensure that page table only record page metadata
    //     // in our pages
    //     // let page_table = pager.page_table.read();
    //     // assert_eq!(page_table.get(&2), None);
    //     // assert_eq!(page_table.len(), 8);
    //     // drop(page_table);

    //     // TODO: test it flush dirty page

    //     // let frame_id = frame_id.unwrap();
    //     // let page = &pager.pages[frame_id];
    //     // let page = page.read();
    //     // assert_eq!(page.page_id, Some(7));
    //     // assert!(!page.is_dirty);
    //     // assert_eq!(page.pin_count, 0);
    //     // assert!(page.node.is_none());
    //     // drop(page);
    //     cleanup_test_db_file();
    // }

    // #[test]
    // fn pager_create_or_replace_page_when_no_pages_can_be_freed() {
    //     setup_test_db_file();
    //     let pager = setup_test_pager();

    //     // Since our pool size is hardcoded to 8,
    //     // we just need to fetch 8 pages to fill
    //     // up the page cache.
    //     pager.fetch_page(0);
    //     pager.fetch_page(4);
    //     pager.fetch_page(2);
    //     pager.fetch_page(5);
    //     pager.fetch_page(6);
    //     pager.fetch_page(8);
    //     pager.fetch_page(9);
    //     pager.fetch_page(10);

    //     // let frame_id = pager.create_or_replace_page(7);
    //     // assert!(frame_id.is_none());
    //     cleanup_test_db_file();
    // }

    #[test]
    fn pager_fetch_page() {}

    #[test]
    fn pager_delete_page() {}

    #[test]
    fn pager_unpin_page() {
        setup_test_db_file();
        let pager = setup_test_pager();

        let page = pager.fetch_read_page_with_retry(0);
        drop(page);
        let page = pager.fetch_read_page_with_retry(0);
        drop(page);

        pager.unpin_page(0, true);
        assert_eq!(pager.replacer.size(), 0);

        let pages = pager.pages.clone();
        let page = &pages.get(0);
        assert!(page.is_some());

        let page = page.unwrap().read();
        assert_eq!(page.pin_count, 1);
        assert!(page.is_dirty);

        drop(page);
        drop(pages);
        pager.unpin_page(0, false);

        // If a page pin_count reach 0,
        // it should be place into replacer.
        assert_eq!(pager.replacer.size(), 1);

        let pages = pager.pages;
        let page = &pages.get(0);
        assert!(page.is_some());

        let page = page.unwrap().read();
        assert_eq!(page.pin_count, 0);

        // If a page is previously dirty, it should stay
        // dirty.
        assert!(page.is_dirty);

        cleanup_test_db_file();
    }

    // #[test]
    // fn pager_get_record() {
    //     setup_test_db_file();

    //     let pager = setup_test_pager();
    //     let cursor = Cursor {
    //         page_num: 1,
    //         cell_num: 0,
    //         key_existed: false,
    //         end_of_table: false,
    //     };

    //     let row = pager.get_record(&cursor);
    //     assert_eq!(row.id, 1);
    //     assert_eq!(row.username(), "user1");
    //     assert_eq!(row.email(), "user1@email.com");
    //     let page = pager.fetch_page(cursor.page_num).unwrap();
    //     let page = page.read();
    //     assert_eq!(page.pin_count, 1);
    //     drop(page);

    //     let cursor = Cursor {
    //         page_num: 2,
    //         cell_num: 1,
    //         key_existed: false,
    //         end_of_table: false,
    //     };

    //     let row = pager.get_record(&cursor);
    //     assert_eq!(row.id, 9);
    //     assert_eq!(row.username(), "user9");
    //     assert_eq!(row.email(), "user9@email.com");
    //     let page = pager.fetch_page(cursor.page_num).unwrap();
    //     let page = page.read();
    //     assert_eq!(page.pin_count, 1);

    //     cleanup_test_db_file();
    // }

    fn setup_test_table() -> Table {
        return Table::new(format!("test-{:?}.db", std::thread::current().id()));
    }

    fn setup_test_pager() -> Pager {
        return Pager::new(format!("test-{:?}.db", std::thread::current().id()));
    }

    fn setup_test_db_file() {
        let table = setup_test_table();

        for i in 1..50 {
            let row =
                Row::from_statement(&format!("insert {i} user{i} user{i}@email.com")).unwrap();
            table.insert(&row);
        }

        table.flush();
    }

    fn cleanup_test_db_file() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }

    fn sleep(duration_in_ms: u64) {
        let ten_millis = std::time::Duration::from_millis(duration_in_ms);
        std::thread::sleep(ten_millis);
    }
}
