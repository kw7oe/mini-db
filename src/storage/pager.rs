use std::path::PathBuf;

use crate::node::{Node, NodeType, LEAF_NODE_MAX_CELLS};
use crate::row::Row;
use crate::storage::DiskManager;
use crate::table::Cursor;
use crate::tree::Tree;

pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    disk_manager: DiskManager,
    tree: Tree,
}

impl Pager {
    pub fn new(path: impl Into<PathBuf>) -> Pager {
        Pager {
            disk_manager: DiskManager::new(path),
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
