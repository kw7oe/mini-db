use std::collections::HashMap;
use std::path::PathBuf;

use super::node::{Node, NodeType, LEAF_NODE_MAX_CELLS};
use super::tree::Tree;
use crate::row::Row;
use crate::storage::DiskManager;
use crate::table::Cursor;

pub const PAGE_SIZE: usize = 4096;

struct PageMetadata {
    frame_id: usize,
    is_dirty: bool,
    pin_count: u32,
}

impl PageMetadata {
    pub fn new(frame_id: usize) -> Self {
        PageMetadata {
            frame_id,
            is_dirty: false,
            pin_count: 0,
        }
    }
}

// We are not implementing the Least Recently Used algorithm
// yet.
//
// Currently we just track pin count and replace
// the page where pin_count is 0.
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

    pub fn victim(&mut self) -> Option<PageMetadata> {
        for (i, md) in self.page_table.iter().enumerate() {
            if md.pin_count == 0 {
                return Some(self.page_table.remove(i));
            }
        }

        None
    }

    pub fn pin(&mut self, frame_id: usize) {
        if let Some(md) = self
            .page_table
            .iter_mut()
            .find(|md| md.frame_id == frame_id)
        {
            md.pin_count += 1;
        } else {
            let mut md = PageMetadata::new(frame_id);
            md.pin_count += 1;
            self.page_table.push(md);
        }
    }

    pub fn unpin(&mut self, frame_id: usize) {
        if let Some(md) = self
            .page_table
            .iter_mut()
            .find(|md| md.frame_id == frame_id)
        {
            md.pin_count -= 1;
        }
    }
}

pub struct Pager {
    disk_manager: DiskManager,
    pages: Vec<Node>,
    // Mapping page id to frame id
    page_table: HashMap<usize, usize>,
    tree: Tree,
}

impl Pager {
    pub fn new(path: impl Into<PathBuf>) -> Pager {
        Pager {
            disk_manager: DiskManager::new(path),
            pages: Vec::new(),
            page_table: HashMap::new(),
            tree: Tree::new(),
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
            let mut bytes = node.header();

            if node.node_type == NodeType::Leaf {
                for c in &node.cells {
                    let mut cell_bytes = bincode::serialize(c).unwrap();
                    bytes.append(&mut cell_bytes);
                }
            } else {
                for c in &node.internal_cells {
                    let mut cell_bytes = bincode::serialize(c).unwrap();
                    bytes.append(&mut cell_bytes);
                }
            }

            // Okay, we need to backfill the space because we are assuming
            // per page is always with PAGE_SIZE.
            //
            // If we didn't fill up the space, what would happen is when we read
            // from file, we will not have an accurate number of pages because file with
            // PAGE_SIZE might contain multiple pages. In theory, you can still keep
            // track of the number of pages in the file, tricky part would then be,
            // how do we identify the page offset of each page? We will have to read each
            // page to find out the next page offset.
            //
            // So long story short, let's just backfill the space...
            let remaining_space = PAGE_SIZE - bytes.len();
            let mut vec = vec![0; remaining_space];
            bytes.append(&mut vec);
            self.disk_manager.write_page(i, &bytes).unwrap();
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
