use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::PathBuf,
};

use crate::node::{Node, NodeType, LEAF_NODE_MAX_CELLS};
use crate::row::Row;
use crate::table::Cursor;
use crate::tree::Tree;

const PAGE_SIZE: usize = 4096;

pub struct Pager {
    write_file: File,
    read_file: File,
    file_len: usize,
    tree: Tree,
}

impl Pager {
    pub fn new(path: impl Into<PathBuf>) -> Pager {
        let path = path.into();

        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let read_file = File::open(&path).unwrap();
        let file_len = read_file.metadata().unwrap().len() as usize;

        Pager {
            write_file,
            read_file,
            file_len,
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
            let mut number_of_pages = self.file_len / PAGE_SIZE;
            if self.file_len % PAGE_SIZE != 0 {
                // We wrote a partial page
                number_of_pages += 1;
            }

            if page_num < number_of_pages {
                let offset = page_num as u64 * PAGE_SIZE as u64;

                if let Ok(_) = self.read_file.seek(SeekFrom::Start(offset)) {
                    let mut buffer = [0; PAGE_SIZE];
                    if let Ok(_read_len) = self.read_file.read(&mut buffer) {
                        node.from_bytes(&buffer);
                    };
                }
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
        for node in self.tree.nodes() {
            let header_bytes = node.header();
            let mut size = self.write_file.write(&header_bytes).unwrap();

            if node.node_type == NodeType::Leaf {
                for c in &node.cells {
                    let cell_bytes = bincode::serialize(c).unwrap();
                    size += self.write_file.write(&cell_bytes).unwrap();
                }
            } else {
                for c in &node.internal_cells {
                    let cell_bytes = bincode::serialize(c).unwrap();
                    size += self.write_file.write(&cell_bytes).unwrap();
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
            let remaining_space = PAGE_SIZE - size;
            let vec = vec![0; remaining_space];
            size += self.write_file.write(&vec).unwrap();

            debug!("flusing {size} bytes to file...");
        }
    }

    // pub fn flush(&mut self, cursor: &Cursor) {
    //     let node = self.get_page(cursor.page_num);
    //     let num_of_cells_bytes = &node.num_of_cells.to_le_bytes();

    //     self.write_file
    //         .write(&self.nodes[cursor.page_num].cells(cursor.cell_num))
    //         .unwrap();

    //     self.write_file
    //         .seek(SeekFrom::Start(COMMON_NODE_HEADER_SIZE as u64))
    //         .unwrap();

    //     self.write_file.write(num_of_cells_bytes).unwrap();
    //     self.write_file.seek(SeekFrom::End(0)).unwrap();
    // }

    pub fn serialize_row(&mut self, row: &Row, cursor: &Cursor) {
        let node = &mut self.tree.mut_nodes()[cursor.page_num];
        let num_of_cells = node.num_of_cells as usize;
        if num_of_cells >= LEAF_NODE_MAX_CELLS {
            self.tree.split_and_insert_leaf_node(cursor, row);
        } else {
            node.insert(row, cursor);
        }
        // self.flush(&cursor);
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

    pub fn to_string(&mut self) -> String {
        self.tree.to_string()
    }
}
