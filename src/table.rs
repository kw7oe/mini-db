use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::PathBuf,
};

use crate::node::{Node, NodeType, LEAF_NODE_MAX_CELLS};
use crate::row::Row;
use crate::tree::Tree;
const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Cursor {
    pub page_num: usize,
    pub cell_num: usize,
    end_of_table: bool,
}

impl Cursor {
    pub fn table_start(table: &mut Table) -> Self {
        let page_num = table.root_page_num;
        if let Ok(mut cursor) = Self::table_find(table, page_num, 0) {
            let num_of_cells = table.pager.get_page(cursor.page_num).num_of_cells as usize;

            cursor.end_of_table = num_of_cells == 0;
            cursor
        } else {
            let num_of_cells = table.pager.get_page(page_num).num_of_cells as usize;

            Cursor {
                page_num,
                cell_num: 0,
                end_of_table: num_of_cells == 0,
            }
        }
    }

    pub fn table_find(table: &mut Table, page_num: usize, key: u32) -> Result<Self, String> {
        let node = table.pager.get_page(page_num);
        let num_of_cells = node.num_of_cells as usize;

        if node.node_type == NodeType::Leaf {
            match node.search(key) {
                Ok(_index) => Err("duplicate key\n".to_string()),
                Err(index) => Ok(Cursor {
                    page_num,
                    cell_num: index,
                    end_of_table: index == num_of_cells,
                }),
            }
        } else {
            if let Ok(page_num) = node.search(key) {
                Self::table_find(table, page_num, key)
            } else {
                Err("something went wrong".to_string())
            }
        }
    }

    fn advance(&mut self, table: &mut Table) {
        self.cell_num += 1;
        let node = &mut table.pager.get_page(self.page_num);
        let num_of_cells = node.num_of_cells as usize;

        if self.cell_num >= num_of_cells {
            if node.next_leaf_offset == 0 {
                self.end_of_table = true;
            } else {
                self.page_num = node.next_leaf_offset as usize;
                self.cell_num = 0;
            }
        }
    }
}

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
                    self.tree.mut_nodes().insert(i, Node::unintialize());
                }
            }

            self.tree.mut_nodes().insert(page_num, Node::unintialize());
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

            println!("--- write {size} to disk");
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
        self.tree.mut_nodes()[cursor.page_num].get(cursor.cell_num)
    }
}

pub struct Table {
    root_page_num: usize,
    pager: Pager,
}

impl Table {
    pub fn new(path: impl Into<PathBuf>) -> Table {
        let pager = Pager::new(path);
        Table {
            root_page_num: 0,
            pager,
        }
    }

    pub fn flush(&mut self) {
        self.pager.flush_all();
    }

    pub fn select(&mut self) -> String {
        let mut cursor = Cursor::table_start(self);
        let mut output = String::new();

        while !cursor.end_of_table {
            let row = self.pager.deserialize_row(&cursor);
            output.push_str(&format!("{:?}\n", row));
            cursor.advance(self);
        }

        output
    }

    pub fn insert(&mut self, row: &Row) -> String {
        let page_num = self.root_page_num;
        match Cursor::table_find(self, page_num, row.id) {
            Ok(cursor) => {
                self.pager.serialize_row(row, &cursor);

                format!(
                    "inserting into page: {}, cell: {}...\n",
                    cursor.page_num, cursor.cell_num
                )
            }
            Err(message) => message,
        }
    }

    pub fn to_string(&mut self) -> String {
        self.pager.tree.to_string()
    }
}
