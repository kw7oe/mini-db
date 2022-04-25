use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::node::{
    InternalCell, Node, INTERNAL_NODE_MAX_CELLS, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_RIGHT_SPLIT_COUNT,
};
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

// #[derive(Debug)]
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

// #[derive(Debug)]
pub struct Pager {
    disk_manager: DiskManager,
    replacer: LRUReplacer,
    pages: Arc<RwLock<Vec<Arc<RwLock<Page>>>>>,
    next_page_id: AtomicUsize,
    // Indexes in our `pages` that are "free", which mean
    // it is uninitialize.
    free_list: Mutex<Vec<usize>>,
    // Mapping page id to frame id
    page_table: Arc<RwLock<HashMap<usize, usize>>>,
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
            pages: Arc::new(RwLock::new(Vec::with_capacity(pool_size))),
            // This would probably need to be based on the number of pages we
            // already have in our disk.
            next_page_id: AtomicUsize::new(next_page_id),
            free_list: Mutex::new(free_list),
            page_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn create_or_replace_page(&self, page_id: usize) -> Option<usize> {
        let mut free_list = self.free_list.lock();
        let frame_id = if let Some(frame_id) = free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.victim().map(|md| md.frame_id)
        };
        drop(free_list);

        if let Some(frame_id) = frame_id {
            let mut pages = self.pages.write();
            if let Some(page) = pages.get(frame_id) {
                let page_arc = page.clone();
                let page = page.read();
                if page.is_dirty {
                    let dirty_page_id = page.page_id.unwrap();
                    drop(page);
                    self.flush_page_v2(dirty_page_id, page_arc);
                } else {
                    drop(page)
                }

                let page = pages.get(frame_id).unwrap();
                let mut page = page.write();
                page.is_dirty = false;
                page.pin_count = 0;
                page.page_id = Some(page_id);
                page.node = None;
                drop(page);

                let mut page_table = self.page_table.write();
                page_table.retain(|_, &mut fid| fid != frame_id);
                page_table.insert(page_id, frame_id);
            } else {
                let page = Page::new(page_id);
                pages.insert(frame_id, Arc::new(RwLock::new(page)));

                let mut page_table = self.page_table.write();
                page_table.insert(page_id, frame_id);
            }

            Some(frame_id)
        } else {
            None
        }
    }

    fn concurrent_create_or_replace_page(
        &self,
        pages: &mut RwLockWriteGuard<Vec<Arc<RwLock<Page>>>>,
        page_id: usize,
    ) -> Option<Arc<RwLock<Page>>> {
        let mut free_list = self.free_list.lock();
        let frame_id = if let Some(frame_id) = free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.victim().map(|md| md.frame_id)
        };
        drop(free_list);

        let thread_id = std::thread::current().id();
        if let Some(frame_id) = frame_id {
            if let Some(page) = pages.get(frame_id) {
                debug!("--- old page ---");
                let page_arc = page.clone();
                let page = page.read();
                debug!(
                    "--- create or replace page ---: {:?} passed through page.read()",
                    thread_id
                );
                if page.is_dirty {
                    let dirty_page_id = page.page_id.unwrap();
                    drop(page);
                    self.flush_page_v2(dirty_page_id, page_arc);
                } else {
                    drop(page)
                }

                let page = pages.get(frame_id).unwrap();
                let mut page = page.write();
                debug!(
                    "--- create or replace page ---: {:?} passed through page.write()",
                    thread_id
                );
                page.is_dirty = false;
                page.pin_count = 0;
                page.page_id = Some(page_id);
                page.node = None;
                drop(page);

                let mut page_table = self.page_table.write();
                debug!(
                    "--- create or replace page ---: {:?} passed through page_table.write()",
                    thread_id
                );
                page_table.retain(|_, &mut fid| fid != frame_id);
                page_table.insert(page_id, frame_id);
                drop(page_table);
            } else {
                debug!("--- new page ---");
                let page = Page::new(page_id);
                pages.insert(frame_id, Arc::new(RwLock::new(page)));

                let mut page_table = self.page_table.write();
                debug!(
                    "--- create or replace page ---: {:?} passed through page_table.write()",
                    thread_id
                );
                page_table.insert(page_id, frame_id);
                drop(page_table);
            }

            let page = &pages[frame_id];
            let mut page = page.write();
            debug!(
                "--- create or replace page ---: {:?} passed through page.write()",
                thread_id
            );
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
            }

            page.pin_count += 1;
            self.replacer.pin(frame_id);

            let page = &pages[frame_id];
            debug!("--- create or replace page (end) --- {:?}", thread_id);
            Some(page.to_owned())
        } else {
            None
        }
    }

    pub fn fetch_page_else_restart_at_root(&self, page_id: usize) -> Option<Arc<RwLock<Page>>> {
        let mut pages = self.pages.write();
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = pages.get(frame_id).unwrap();
            let page = page.try_write();

            if let Some(mut page) = page {
                page.pin_count += 1;
                self.replacer.pin(frame_id);
                drop(page);

                let page = &pages[frame_id];
                return Some(page.to_owned());
            } else {
                drop(page);
                drop(page_table);
                drop(pages);

                let mut rng = rand::thread_rng();
                let duration = std::time::Duration::from_millis(rng.gen_range(1..5));
                std::thread::sleep(duration);

                info!("restart at root when since we can't fetch page {page_id}");
                return self.fetch_page_else_restart_at_root(0);
            }
        }

        drop(page_table);
        self.concurrent_create_or_replace_page(&mut pages, page_id)
    }

    pub fn fetch_page(&self, page_id: usize) -> Option<Arc<RwLock<Page>>> {
        // Check if the page is already in memory. If yes,
        // just pin the page and return the node.

        let thread_id = std::thread::current().id();
        debug!(
            "--- fetch_page --- {:?}: S({:?}), X({:?})",
            thread_id,
            self.pages.is_locked(),
            self.pages.is_locked_exclusive()
        );
        let mut pages = self.pages.write();
        let page_table = self.page_table.read();
        debug!(
            "--- fetch page ---: {:?} passed through pages.write(), next fetch page {page_id}",
            thread_id
        );
        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = pages.get(frame_id).unwrap();
            // let mut page = page.write();
            // debug!(
            //     "--- fetch page ---: {:?} passed through page.write() for {page_id}",
            //     thread_id
            // );
            // page.pin_count += 1;
            // self.replacer.pin(frame_id);

            // let page = &pages[frame_id];
            // return Some(page.to_owned());

            let page = page.try_write();
            if let Some(mut page) = page {
                page.pin_count += 1;
                self.replacer.pin(frame_id);
                drop(page);

                let page = &pages[frame_id];
                return Some(page.to_owned());
            } else {
                drop(page);
                drop(page_table);
                drop(pages);

                let mut rng = rand::thread_rng();
                let duration = std::time::Duration::from_millis(rng.gen_range(1..5));
                debug!(
                    "--- fetch page --- {:?}: sleeping for {:?}",
                    thread_id, duration
                );
                std::thread::sleep(duration);

                return self.fetch_page(page_id);
            }
        }

        drop(page_table);
        debug!("--- concrruent page --- {:?}", thread_id);
        let result = self.concurrent_create_or_replace_page(&mut pages, page_id);

        debug!("--- fetch page (end) --- {:?}", thread_id);
        result
    }

    // Taking in a page so we don't have to lock the whole Vec<Page>
    pub fn flush_page_v2(&self, page_id: usize, page: Arc<RwLock<Page>>) {
        let page = page.read();
        let node = page.node.as_ref().unwrap();
        let bytes = node.to_bytes();
        drop(page);
        self.disk_manager.write_page(page_id, &bytes).unwrap();
    }

    pub fn flush_all_pages(&self) {
        for page in self.pages.read().iter() {
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

    pub fn delete_page(&self, page_id: usize) -> bool {
        debug!("--- delete page {page_id}");
        let mut page_table = self.page_table.write();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let pages = &self.pages.read();
            let page = &pages[frame_id];
            let mut page = page.write();
            if page.pin_count == 0 {
                debug!("--- page {page_id} found, deleting it...");
                page.deallocate();
                page_table.remove(&page_id);
                self.free_list.lock().push(frame_id);

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

    pub fn unpin_page_with_write_guard(&self, page: &mut RwLockWriteGuard<Page>, is_dirty: bool) {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page.page_id.unwrap()) {
            drop(page_table);

            if !page.is_dirty {
                page.is_dirty = is_dirty;
            }
            page.pin_count -= 1;

            if page.pin_count == 0 {
                self.replacer.unpin(frame_id);
            };
        }
    }

    pub fn unpin_page(&self, page_id: usize, is_dirty: bool) {
        let thread_id = std::thread::current().id();
        debug!("--- unpin page ---: {:?}", thread_id);
        let pages = self.pages.read();
        let page_table = self.page_table.read();
        debug!(
            "--- unpin page ---: {:?} passed through pages.read()",
            thread_id
        );
        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = &pages[frame_id];
            // let mut page = page.write();

            // debug!(
            //     "--- unpin page ---: {:?} passed through page.write()",
            //     thread_id
            // );
            // if !page.is_dirty {
            //     page.is_dirty = is_dirty;
            // }
            // page.pin_count -= 1;

            // if page.pin_count == 0 {
            //     self.replacer.unpin(frame_id);
            // };

            // drop(page);
            let page = page.try_write();
            if let Some(mut page) = page {
                if !page.is_dirty {
                    page.is_dirty = is_dirty;
                }
                page.pin_count -= 1;

                if page.pin_count == 0 {
                    self.replacer.unpin(frame_id);
                };
            } else {
                drop(page_table);
                drop(page);
                drop(pages);
                let mut rng = rand::thread_rng();
                let duration = std::time::Duration::from_millis(rng.gen_range(1..5));
                debug!(
                    "--- unpin page --- {:?}: sleeping for {:?}",
                    thread_id, duration
                );
                std::thread::sleep(duration);
                return self.unpin_page(page_id, is_dirty);
            }
        }
        drop(page_table);
        drop(pages);

        debug!("--- unpin page (end) --- {:?}", thread_id);
    }

    pub fn insert_record(&self, row: &Row, cursor: &Cursor) {
        if let Some(page) = self.fetch_page(cursor.page_num) {
            let page_lock = page.read();
            let node = page_lock.node.as_ref().unwrap();
            let num_of_cells = node.num_of_cells as usize;
            drop(page_lock);

            if num_of_cells >= LEAF_NODE_MAX_CELLS {
                self.unpin_page(cursor.page_num, true);
                self.insert_and_split_leaf_node(cursor, row);
            } else {
                let mut page = page.write();
                let node = page.node.as_mut().unwrap();
                node.insert(row, cursor);
                drop(page);
                self.unpin_page(cursor.page_num, true)
            }
        }
    }

    pub fn create_new_root(&self, left_node_page_num: usize, mut right_node: Node, max_key: u32) {
        debug!("--- create_new_root");
        let next_page_id = self.next_page_id.load(Ordering::Acquire);
        let root_page = self.fetch_page(left_node_page_num).unwrap();
        let mut root_page = root_page.write();
        let root_page_id = root_page.page_id.unwrap();

        let mut root_node = Node::new(true, NodeType::Internal);
        root_node.num_of_cells += 1;
        root_node.right_child_offset = next_page_id as u32 + 1;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        let mut left_node = root_page.node.take().unwrap();
        left_node.is_root = false;
        left_node.next_leaf_offset = next_page_id as u32 + 1;
        left_node.parent_offset = 0;

        let cell = InternalCell::new(next_page_id as u32, max_key);
        root_node.internal_cells.insert(0, cell);

        root_page.node = Some(root_node);
        drop(root_page);
        self.unpin_page(root_page_id, true);

        let left_page = self
            .fetch_page(self.next_page_id.load(Ordering::Acquire))
            .unwrap();
        let mut left_page = left_page.write();
        let left_page_id = left_page.page_id.unwrap();
        left_page.node = Some(left_node);
        drop(left_page);
        self.unpin_page(left_page_id, true);

        let right_page = self
            .fetch_page(self.next_page_id.load(Ordering::Acquire))
            .unwrap();
        let mut right_page = right_page.write();
        let right_page_id = right_page.page_id.unwrap();
        right_page.node = Some(right_node);
        drop(right_page);
        self.unpin_page(right_page_id, true);
    }

    pub fn insert_internal_node(&self, parent_page_num: usize, split_at_page_num: usize) {
        debug!("--- insert internal node {split_at_page_num} at parent {parent_page_num}");
        let parent_page = self.fetch_page(parent_page_num).unwrap();
        let parent_page = parent_page.read();
        let parent_node = parent_page.node.as_ref().unwrap();
        let parent_right_child_offset = parent_node.right_child_offset as usize;
        drop(parent_page);
        self.unpin_page(parent_page_num, false);

        let new_page = self.fetch_page(split_at_page_num).unwrap();
        let mut new_page = new_page.write();
        let new_node = new_page.node.as_mut().unwrap();
        let new_child_max_key = new_node.get_max_key();
        new_node.parent_offset = parent_page_num as u32;
        drop(new_page);
        self.unpin_page(split_at_page_num, true);

        let right_page = self.fetch_page(parent_right_child_offset).unwrap();
        let right_page = right_page.read();
        let right_node = right_page.node.as_ref().unwrap();
        let right_max_key = right_node.get_max_key();
        drop(right_page);
        self.unpin_page(parent_right_child_offset, false);

        let parent_page = self.fetch_page(parent_page_num).unwrap();
        let mut parent_page = parent_page.write();
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

        drop(parent_page);
        self.unpin_page(parent_page_num, true);
        self.maybe_split_internal_node(parent_page_num);
    }

    pub fn update_children_parent_offset(&self, page_num: usize) {
        let page = self.fetch_page(page_num).unwrap();
        let page = page.read();
        let node = page.node.as_ref().unwrap();

        let mut child_pointers = vec![node.right_child_offset as usize];
        for cell in &node.internal_cells {
            child_pointers.push(cell.child_pointer() as usize);
        }
        drop(page);
        self.unpin_page(page_num, false);

        for i in child_pointers {
            let page = self.fetch_page(i).unwrap();
            let mut page = page.write();
            let child = page.node.as_mut().unwrap();
            child.parent_offset = page_num as u32;
            drop(page);
            self.unpin_page(i, true);
        }
    }

    pub fn maybe_split_internal_node(&self, page_num: usize) {
        let next_page_id = self.next_page_id.load(Ordering::Acquire);
        let left_page = self.fetch_page(page_num).unwrap();
        let mut left_page = left_page.write();
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
                drop(left_page);
                self.unpin_page(page_num, true);
                self.create_new_root(page_num, right_node, ic.key());

                self.update_children_parent_offset(next_page_id as usize);
                self.update_children_parent_offset(next_page_id as usize + 1);
            } else {
                debug!("update internal node {page_num}, parent...");
                let parent_offset = left_node.parent_offset as usize;
                drop(left_page);
                self.unpin_page(page_num, true);
                let parent_page = self.fetch_page(parent_offset).unwrap();
                let mut parent_page = parent_page.write();
                let parent = parent_page.node.as_mut().unwrap();

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
                drop(parent_page);
                self.unpin_page(parent_offset, true);

                let right_page = self.fetch_page(next_page_id).unwrap();
                let mut right_page = right_page.write();
                right_page.is_dirty = true;
                right_page.node = Some(right_node);
                drop(right_page);
                self.unpin_page(next_page_id, true);
                self.update_children_parent_offset(next_page_id);
            }
        } else {
            drop(left_page);
            self.unpin_page(page_num, true);
        }
    }

    fn insert_and_split_leaf_node(&self, cursor: &Cursor, row: &Row) {
        debug!("--- insert_and_split_leaf_node");
        let left_page = self.fetch_page(cursor.page_num).unwrap();
        let mut left_page = left_page.write();
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

        let left_max_key = left_node.get_max_key();

        if left_node.is_root {
            drop(left_page);
            self.unpin_page(cursor.page_num, true);
            self.create_new_root(cursor.page_num, right_node, left_max_key);
        } else {
            debug!("--- split leaf node and update parent ---");
            drop(left_page);
            self.unpin_page(cursor.page_num, true);

            let next_page_id = self.next_page_id.load(Ordering::Acquire);
            let left_page = self.fetch_page(cursor.page_num).unwrap();
            let mut left_page = left_page.write();
            let left_node = left_page.node.as_mut().unwrap();
            right_node.next_leaf_offset = left_node.next_leaf_offset;
            left_node.next_leaf_offset = next_page_id as u32;
            right_node.parent_offset = left_node.parent_offset;

            let parent_page_num = left_node.parent_offset as usize;
            let new_max = left_node.get_max_key();
            drop(left_page);
            self.unpin_page(cursor.page_num, true);

            let parent_page = self.fetch_page(parent_page_num).unwrap();
            let mut parent_page = parent_page.write();
            let parent_node = parent_page.node.as_mut().unwrap();
            parent_node.update_internal_key(old_max, new_max);
            drop(parent_page);
            self.unpin_page(parent_page_num, true);

            let right_page = self
                .fetch_page(self.next_page_id.load(Ordering::Acquire))
                .unwrap();
            let mut right_page = right_page.write();
            let right_page_id = right_page.page_id.unwrap();
            right_page.node = Some(right_node);
            drop(right_page);
            self.unpin_page(right_page_id, true);

            self.insert_internal_node(parent_page_num, right_page_id);
        }
    }

    pub fn get_record(&self, cursor: &Cursor) -> Row {
        if let Some(page) = self.fetch_page(cursor.page_num) {
            let page = page.read();
            let node = page.node.as_ref().unwrap();
            let row = node.get(cursor.cell_num);
            drop(page);
            self.unpin_page(cursor.page_num, false);
            return row;
        }

        info!("{:?}, {:?}", cursor, self.pages);
        panic!("row not found...");
    }

    pub fn delete_record(&self, cursor: &Cursor) {
        debug!(
            "--- delete_record at page {}, cell {}",
            cursor.page_num, cursor.cell_num
        );

        let page = self.fetch_page(cursor.page_num).unwrap();
        let mut page = page.write();
        let node = page.node.as_mut().unwrap();
        node.delete(cursor.cell_num);
        drop(page);
        self.unpin_page(cursor.page_num, true);
        self.maybe_merge_nodes(cursor);
    }

    fn maybe_merge_nodes(&self, cursor: &Cursor) {
        let page = self.fetch_page(cursor.page_num).unwrap();
        let page = page.read();
        let node = page.node.as_ref().unwrap();

        if node.node_type == NodeType::Leaf
            && node.num_of_cells < LEAF_NODE_MAX_CELLS as u32 / 2
            && !node.is_root
        {
            drop(page);
            self.unpin_page(cursor.page_num, false);
            self.merge_leaf_nodes(cursor.page_num);
        } else {
            drop(page);
            self.unpin_page(cursor.page_num, false);
        }
    }

    fn merge_leaf_nodes(&self, page_num: usize) {
        let page = self.fetch_page(page_num).unwrap();
        let page = page.read();
        let node = page.node.as_ref().unwrap();
        let node_cells_len = node.cells.len();

        let parent_page_id = node.parent_offset as usize;
        drop(page);
        self.unpin_page(page_num, false);

        let parent_page = self.fetch_page(parent_page_id).unwrap();
        let parent_page = parent_page.read();
        let parent = parent_page.node.as_ref().unwrap();
        let (left_child_pointer, right_child_pointer) = parent.siblings(page_num as u32);
        drop(parent_page);
        self.unpin_page(parent_page_id, false);

        if let Some(cp) = left_child_pointer {
            let left_page = self.fetch_page(cp).unwrap();
            let left_page = left_page.read();
            let left_nb = left_page.node.as_ref().unwrap();

            if cp != page_num && left_nb.cells.len() + node_cells_len < LEAF_NODE_MAX_CELLS {
                drop(left_page);
                self.unpin_page(cp, false);
                debug!("Merging node {} with its left neighbour...", page_num);
                self.do_merge_leaf_nodes(cp, page_num);

                return;
            } else {
                drop(left_page);
                self.unpin_page(cp, false);
            }
        }

        if let Some(cp) = right_child_pointer {
            let right_page = self.fetch_page(cp).unwrap();
            let right_page = right_page.read();
            let right_nb = right_page.node.as_ref().unwrap();

            if cp != page_num && right_nb.cells.len() + node_cells_len < LEAF_NODE_MAX_CELLS {
                drop(right_page);
                self.unpin_page(cp, false);
                debug!("Merging node {} with its right neighbour...", page_num);
                self.do_merge_leaf_nodes(page_num, cp);
            } else {
                drop(right_page);
                self.unpin_page(cp, false);
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

    fn promote_last_node_to_root(&self, page_num: usize) {
        let page = self.fetch_page(page_num).unwrap();
        let mut page = page.write();
        let mut node = page.node.take().unwrap();

        let parent_offset = node.parent_offset as usize;
        drop(page);
        self.unpin_page(page_num, true);
        self.delete_page(page_num);
        node.is_root = true;
        node.next_leaf_offset = 0;

        let parent_page = self.fetch_page(parent_offset).unwrap();
        let mut parent_page = parent_page.write();
        parent_page.node = Some(node);
        drop(parent_page);
        self.unpin_page(parent_offset, true);
    }

    fn do_merge_leaf_nodes(&self, left_cp: usize, right_cp: usize) {
        let page = self.fetch_page(right_cp).unwrap();
        let mut page = page.write();
        let right_node = page.node.take().unwrap();
        drop(page);
        self.unpin_page(right_cp, true);
        self.delete_page(right_cp);

        let left_page = self.fetch_page(left_cp).unwrap();
        let mut left_page = left_page.write();
        let left_node = left_page.node.as_mut().unwrap();

        // Merge the leaf nodes cells
        for c in right_node.cells {
            left_node.cells.push(c);
            left_node.num_of_cells += 1;
        }

        left_node.next_leaf_offset = right_node.next_leaf_offset;

        let parent_offset = left_node.parent_offset as usize;

        let max_key = left_node.get_max_key();
        drop(left_page);
        self.unpin_page(left_cp, true);

        let min_key_length = self.min_key(INTERNAL_NODE_MAX_CELLS) as u32;

        // Update parent metadata
        let parent_page = self.fetch_page(parent_offset).unwrap();
        let mut parent_page = parent_page.write();
        let parent = parent_page.node.as_mut().unwrap();

        if parent.num_of_cells == 1 && parent.is_root {
            drop(parent_page);
            self.unpin_page(parent_offset, false);
            debug!("promote last leaf node to root");
            self.promote_last_node_to_root(left_cp);
        } else {
            let index = parent.internal_search_child_pointer(right_cp as u32);
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
                    parent.internal_cells[index - 1] = InternalCell::new(left_cp as u32, max_key);
                }
            }
            drop(parent_page);
            self.unpin_page(parent_offset, true);

            let parent_page = self.fetch_page(parent_offset).unwrap();
            let parent_page = parent_page.read();
            let parent = parent_page.node.as_ref().unwrap();
            if parent.num_of_cells <= min_key_length && !parent.is_root {
                drop(parent_page);
                self.unpin_page(parent_offset, false);
                self.merge_internal_nodes(parent_offset);
            } else {
                drop(parent_page);
                self.unpin_page(parent_offset, false);
            }
        }
    }

    fn merge_internal_nodes(&self, page_num: usize) {
        let page = self.fetch_page(page_num).unwrap();
        let page = page.read();
        let node = page.node.as_ref().unwrap();
        let node_num_of_cells = node.num_of_cells as usize;
        let parent_page_id = node.parent_offset as usize;
        drop(page);
        self.unpin_page(page_num, false);

        let parent_page = self.fetch_page(parent_page_id).unwrap();
        let parent_page = parent_page.read();
        let parent = parent_page.node.as_ref().unwrap();

        let (left_child_pointer, right_child_pointer) = parent.siblings(page_num as u32);
        drop(parent_page);
        self.unpin_page(parent_page_id, false);

        if let Some(cp) = left_child_pointer {
            let left_page = self.fetch_page(cp).unwrap();
            let left_page = left_page.read();
            let left_nb = left_page.node.as_ref().unwrap();

            if cp != page_num
                && left_nb.internal_cells.len() + node_num_of_cells <= INTERNAL_NODE_MAX_CELLS
            {
                drop(left_page);
                self.unpin_page(cp, false);
                debug!("Merging internal node {page_num} with left neighbour");
                self.do_merge_internal_nodes(cp, page_num);
                return;
            } else {
                drop(left_page);
                self.unpin_page(cp, false);
            }
        }

        if let Some(cp) = right_child_pointer {
            let right_page = self.fetch_page(cp).unwrap();
            let right_page = right_page.read();
            let right_nb = right_page.node.as_ref().unwrap();
            if cp != page_num
                && right_nb.internal_cells.len() + node_num_of_cells <= INTERNAL_NODE_MAX_CELLS
            {
                drop(right_page);
                self.unpin_page(cp, false);
                debug!("Merging internal node {page_num} with right neighbour");
                self.do_merge_internal_nodes(page_num, cp);
            } else {
                drop(right_page);
                self.unpin_page(cp, false);
            }
        }
    }

    fn do_merge_internal_nodes(&self, left_cp: usize, right_cp: usize) {
        // let min_key_length = self.min_key(3) as u32;

        let left_page = self.fetch_page(left_cp).unwrap();
        let left_page = left_page.read();
        let left_node = left_page.node.as_ref().unwrap();
        let left_node_right_child_offset = left_node.right_child_offset as usize;
        drop(left_page);
        self.unpin_page(left_cp, false);

        let left_most_right_child_page = self.fetch_page(left_node_right_child_offset).unwrap();
        let left_most_right_child_page = left_most_right_child_page.read();
        let left_most_right_child_node = left_most_right_child_page.node.as_ref().unwrap();
        let new_left_max_key = left_most_right_child_node.get_max_key();
        drop(left_most_right_child_page);
        self.unpin_page(left_node_right_child_offset, false);

        let right_page = self.fetch_page(right_cp).unwrap();
        let mut right_page = right_page.write();
        let right_node = right_page.node.take().unwrap();
        drop(right_page);
        self.unpin_page(right_cp, true);
        self.delete_page(right_cp);

        let left_page = self.fetch_page(left_cp).unwrap();
        let mut left_page = left_page.write();
        let left_node = left_page.node.as_mut().unwrap();
        left_node.internal_cells.push(InternalCell::new(
            left_node.right_child_offset,
            new_left_max_key,
        ));
        left_node.num_of_cells += 1;

        // Merge the leaf nodes cells
        for c in right_node.internal_cells {
            left_node.internal_cells.push(c);
            left_node.num_of_cells += 1;
        }

        left_node.right_child_offset = right_node.right_child_offset;

        // Update parent metadata
        let parent_offset = left_node.parent_offset as usize;
        drop(left_page);
        self.unpin_page(left_cp, true);

        let parent_page = self.fetch_page(parent_offset).unwrap();
        let mut parent_page = parent_page.write();
        let parent = parent_page.node.as_ref().unwrap();

        if parent.num_of_cells == 1 && parent.is_root {
            drop(parent_page);
            self.unpin_page(parent_offset, false);
            debug!("promote internal nodes to root");
            self.promote_last_node_to_root(left_cp);
            self.update_children_parent_offset(0);
        } else {
            debug!("update parent linked with childrens");
            let parent = parent_page.node.as_mut().unwrap();
            let parent_right_child_offset = parent.right_child_offset as usize;

            let index = parent.internal_search_child_pointer(left_cp as u32);
            parent.internal_cells.remove(index);
            parent.num_of_cells -= 1;

            if right_cp == parent_right_child_offset {
                debug!("  update parent after merging most right child");

                parent.right_child_offset = left_cp as u32;
            } else {
                debug!("  update parent after merging child");
                parent.internal_cells[index].write_child_pointer(left_cp as u32);
            }
            drop(parent_page);
            self.unpin_page(parent_offset, true);

            self.update_children_parent_offset(left_cp as usize);
        }
    }

    pub fn node_to_string(&self, node_index: usize, indent_level: usize) -> String {
        let page = self.fetch_page(node_index).unwrap();
        let page = page.read();
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
    pub fn fetch_page_with_pages_guard(
        &self,
        pages: &mut RwLockWriteGuard<Vec<Arc<RwLock<Page>>>>,
        page_id: usize,
    ) -> Option<Arc<RwLock<Page>>> {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let page = pages.get(frame_id).unwrap();

            // The reason we can do this safely is becaused,
            // we will be holding Pages for quite a while before
            // this function is getting called.
            //
            // So it should not be deadlock.
            let mut page = page.write();
            page.pin_count += 1;
            self.replacer.pin(frame_id);
            drop(page);

            let page = &pages[frame_id];
            return Some(page.to_owned());
        }

        drop(page_table);
        self.concurrent_create_or_replace_page(pages, page_id)
    }
    pub fn search_and_then<F, T>(
        &self,
        parent_page_guard: Option<RwLockWriteGuard<Page>>,
        page_num: usize,
        key: u32,
        func: F,
    ) -> Option<T>
    where
        F: FnOnce(Cursor, Option<RwLockWriteGuard<Page>>, RwLockWriteGuard<Page>) -> Option<T>,
    {
        let page = self.fetch_page_else_restart_at_root(page_num).unwrap();
        let mut page = page.write();
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
                    parent_page_guard,
                    page,
                ),
                Err(index) => func(
                    Cursor {
                        page_num,
                        cell_num: index,
                        key_existed: false,
                        end_of_table: index == num_of_cells,
                    },
                    parent_page_guard,
                    page,
                ),
            }
        } else if let Ok(next_page_num) = node.search(key) {
            // TODO: Implement latching on parent page later.
            //
            // Now latching a parent page will caused dead lock of
            // one thread holding Pages and the other holding Page N
            // while each thread need each other lock to proceed.
            //
            // Typically caused by unpin_page and fetch_page.
            self.unpin_page_with_write_guard(&mut page, false);
            drop(page);
            self.search_and_then(None, next_page_num, key, func)
        } else {
            unreachable!("this shouldn't happen!");
        }
    }

    pub fn insert(&self, root_page_num: usize, row: &Row) -> Option<String> {
        self.search_and_then(
            None,
            root_page_num,
            row.id,
            |cursor, parent_page, mut page| {
                let node = page.node.as_ref().unwrap();
                let num_of_cells = node.num_of_cells as usize;

                if num_of_cells >= LEAF_NODE_MAX_CELLS {
                    debug!(
                        "--- insert --- {:?}: require pages",
                        std::thread::current().id()
                    );

                    // We need to make sure we can whole the pages all the way
                    // until our split ended.
                    let pages = self.pages.write();
                    debug!(
                        "--- insert --- {:?}: pass through pages.write()",
                        std::thread::current().id()
                    );
                    self.concurrent_insert_and_split_node(pages, parent_page, page, &cursor, row);
                } else {
                    // if let Some(parent_page) = parent_page {
                    //     let parent_page_id = parent_page.page_id.unwrap();
                    //     drop(parent_page);
                    //     debug!("unpin parent");
                    //     self.unpin_page(parent_page_id, false);
                    // }

                    let node = page.node.as_mut().unwrap();
                    node.insert(row, &cursor);

                    self.unpin_page_with_write_guard(&mut page, true);
                    drop(page);
                    debug!("finish insert");
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
        pages: RwLockWriteGuard<Vec<Arc<RwLock<Page>>>>,
        parent_page: Option<RwLockWriteGuard<Page>>,
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
            self.concurrent_create_new_root(
                pages,
                parent_page,
                left_page,
                right_node,
                left_max_key,
            );
            debug!("--- create new root (end) ---");
        } else {
            self.concurrent_split_node_and_update_parent(
                pages,
                parent_page,
                left_page,
                right_node,
                old_max,
            );
        }
    }

    fn concurrent_split_node_and_update_parent(
        &self,
        mut pages: RwLockWriteGuard<Vec<Arc<RwLock<Page>>>>,
        parent_page: Option<RwLockWriteGuard<Page>>,
        mut left_page: RwLockWriteGuard<Page>,
        mut right_node: Node,
        max_key: u32,
    ) {
        let left_node = left_page.node.as_mut().unwrap();
        right_node.next_leaf_offset = left_node.next_leaf_offset;

        let next_page_id = self.next_page_id.load(Ordering::Acquire);
        left_node.next_leaf_offset = next_page_id as u32;
        right_node.parent_offset = left_node.parent_offset;

        let parent_page_num = left_node.parent_offset as usize;
        let new_max = left_node.get_max_key();
        self.unpin_page_with_write_guard(&mut left_page, true);
        drop(left_page);

        let parent_page = self
            .fetch_page_with_pages_guard(&mut pages, parent_page_num)
            .unwrap();
        let mut parent_page = parent_page.write();
        let parent_node = parent_page.node.as_mut().unwrap();
        parent_node.update_internal_key(max_key, new_max);

        self.unpin_page_with_write_guard(&mut parent_page, true);
        drop(parent_page);

        let right_page = self
            .concurrent_create_or_replace_page(
                &mut pages,
                self.next_page_id.load(Ordering::Acquire),
            )
            .unwrap();
        let mut right_page = right_page.write();
        let right_page_id = right_page.page_id.unwrap();
        right_page.node = Some(right_node);

        let new_node = right_page.node.as_mut().unwrap();
        let new_child_max_key = new_node.get_max_key();
        new_node.parent_offset = parent_page_num as u32;
        self.unpin_page_with_write_guard(&mut right_page, true);
        drop(right_page);

        let split_at_page_num = right_page_id;
        let parent_page = self
            .fetch_page_with_pages_guard(&mut pages, parent_page_num)
            .unwrap();

        let mut parent_page = parent_page.write();
        let parent_node = parent_page.node.as_ref().unwrap();
        let parent_right_child_offset = parent_node.right_child_offset as usize;

        let most_right_page = self
            .fetch_page_with_pages_guard(&mut pages, parent_right_child_offset)
            .unwrap();

        // This actually doesn't have to be a write lock.
        let mut most_right_page = most_right_page.write();
        let right_node = most_right_page.node.as_ref().unwrap();
        let right_max_key = right_node.get_max_key();
        self.unpin_page_with_write_guard(&mut most_right_page, false);

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

        if parent_node.num_of_cells > INTERNAL_NODE_MAX_CELLS as u32 {
            unimplemented!("split internal node");
        }

        self.unpin_page_with_write_guard(&mut parent_page, false);
    }

    fn concurrent_create_new_root(
        &self,
        mut pages: RwLockWriteGuard<Vec<Arc<RwLock<Page>>>>,
        parent_page: Option<RwLockWriteGuard<Page>>,
        mut page: RwLockWriteGuard<Page>,
        mut right_node: Node,
        max_key: u32,
    ) {
        debug!("--- create_new_root --- {:?}", std::thread::current().id());
        let next_page_id = self.next_page_id.load(Ordering::Acquire);

        let mut root_node = Node::new(true, NodeType::Internal);
        root_node.num_of_cells += 1;
        root_node.right_child_offset = next_page_id as u32 + 1;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        let mut left_node = page.node.take().unwrap();
        left_node.is_root = false;
        left_node.next_leaf_offset = next_page_id as u32 + 1;
        left_node.parent_offset = 0;

        let cell = InternalCell::new(next_page_id as u32, max_key);
        root_node.internal_cells.insert(0, cell);

        page.node = Some(root_node);

        let left_page_id = self.next_page_id.load(Ordering::Acquire);
        debug!("--- create left page --- ");
        // let left_page = self.fetch_page(left_page_id).unwrap();
        let left_page = self
            .concurrent_create_or_replace_page(&mut pages, left_page_id)
            .unwrap();
        let mut left_page = left_page.write();
        left_page.node = Some(left_node);
        drop(left_page);

        let right_page_id = self.next_page_id.load(Ordering::Acquire);
        debug!("--- create right page --- ");
        let right_page = self
            .concurrent_create_or_replace_page(&mut pages, right_page_id)
            .unwrap();
        let mut right_page = right_page.write();
        right_page.node = Some(right_node);
        drop(right_page);
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
    fn pager_create_or_replace_page_when_page_cache_is_not_full() {
        setup_test_db_file();
        let pager = Pager::new("test.db");

        let frame_id = pager.create_or_replace_page(0);
        assert!(frame_id.is_some());
        let frame_id = frame_id.unwrap();

        let page = &pager.pages.read()[frame_id];
        let page = page.read();
        assert_eq!(page.page_id, Some(0));
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
        assert!(page.node.is_none());
        drop(page);
        cleanup_test_db_file();
    }

    #[test]
    fn pager_create_or_replace_page_when_page_cache_is_full_with_victims_in_replacer() {
        setup_test_db_file();
        let pager = Pager::new("test.db");

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
        let page_table = pager.page_table.read();
        assert_eq!(page_table.get(&2), None);
        assert_eq!(page_table.len(), 4);
        drop(page_table);

        // TODO: test it flush dirty page

        let frame_id = frame_id.unwrap();
        let page = &pager.pages.read()[frame_id];
        let page = page.read();
        assert_eq!(page.page_id, Some(7));
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
        assert!(page.node.is_none());
        drop(page);
        cleanup_test_db_file();
    }

    #[test]
    fn pager_create_or_replace_page_when_no_pages_can_be_freed() {
        setup_test_db_file();
        let pager = Pager::new("test.db");

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
        let pager = Pager::new("test.db");

        pager.fetch_page(0);
        pager.fetch_page(0);

        pager.unpin_page(0, true);
        assert_eq!(pager.replacer.size(), 0);

        let pages = pager.pages.read();
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

        let pages = pager.pages.read();
        let page = &pages.get(0);
        assert!(page.is_some());

        let page = page.unwrap().read();
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

        let pager = Pager::new("test.db");
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
        let page = page.read();
        assert_eq!(page.pin_count, 1);
        drop(page);

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
        let page = page.read();
        assert_eq!(page.pin_count, 1);

        cleanup_test_db_file();
    }

    fn setup_test_db_file() {
        let table = Table::new("test.db");

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
