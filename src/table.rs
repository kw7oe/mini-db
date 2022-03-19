use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::PathBuf,
};

use crate::node::{
    InternalCell, Node, NodeType, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_RIGHT_SPLIT_COUNT,
};
use crate::BigArray;
use serde::{Deserialize, Serialize};

const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;

#[derive(Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub id: u32,
    #[serde(with = "BigArray")]
    username: [u8; USERNAME_SIZE],
    #[serde(with = "BigArray")]
    email: [u8; EMAIL_SIZE],
}

impl std::fmt::Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            self.id,
            // Since we are converting from a fixed size array, there will be NULL
            // characters at the end. Hence, we need to trim it.
            //
            // While it doesn't impact outputing to the display, it caused
            // issue with our test, as the result will have additional character while
            // our expectation don't.
            String::from_utf8_lossy(&self.username).trim_end_matches(char::from(0)),
            String::from_utf8_lossy(&self.email).trim_end_matches(char::from(0))
        )
    }
}

impl Row {
    // Alternatively can move to prepare_insert instead.
    pub fn from_statement(statement: &str) -> Result<Row, String> {
        let insert_statement: Vec<&str> = statement.split(" ").collect();
        match insert_statement[..] {
            ["insert", id, name, email] => {
                if let Ok(id) = id.parse::<u32>() {
                    if name.len() > USERNAME_SIZE {
                        return Err("Name is too long.".to_string());
                    }

                    if email.len() > EMAIL_SIZE {
                        return Err("Email is too long.".to_string());
                    }

                    Ok(Self::create(id, name, email))
                } else {
                    Err("ID must be positive.".to_string())
                }
            }
            _ => Err(format!("Unrecognized keyword at start of '{statement}'.")),
        }
    }

    pub fn create(id: u32, u: &str, m: &str) -> Row {
        let mut username: [u8; USERNAME_SIZE] = [0; USERNAME_SIZE];
        let mut email: [u8; EMAIL_SIZE] = [0; EMAIL_SIZE];

        let mut index = 0;
        for c in u.bytes() {
            username[index] = c;
            index += 1;
        }

        index = 0;
        for c in m.bytes() {
            email[index] = c;
            index += 1;
        }

        Row {
            id,
            username,
            email,
        }
    }
}

pub const ROW_SIZE: usize = USERNAME_SIZE + EMAIL_SIZE + 4; // u32 is 4 x u8;
const PAGE_SIZE: usize = 4096;
// const TABLE_MAX_PAGE: usize = 100;

#[derive(Debug)]
pub struct Cursor {
    page_num: usize,
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
    num_pages: usize,
    nodes: Vec<Node>,
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
            num_pages: file_len / PAGE_SIZE,
            nodes: Vec::new(),
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> &Node {
        if self.nodes.get(page_num).is_none() {
            let mut number_of_pages = self.file_len / PAGE_SIZE;
            println!("number_of_pages: {number_of_pages}");
            if self.file_len % PAGE_SIZE != 0 {
                // We wrote a partial page
                number_of_pages += 1;
            }

            self.nodes.insert(page_num, Node::new(true, NodeType::Leaf));

            if page_num < number_of_pages {
                let offset = page_num as u64 * PAGE_SIZE as u64;

                if let Ok(_) = self.read_file.seek(SeekFrom::Start(offset)) {
                    let mut buffer = [0; PAGE_SIZE];
                    if let Ok(_read_len) = self.read_file.read(&mut buffer) {
                        let node = self.nodes.get_mut(page_num).unwrap();
                        node.from_bytes(&buffer);
                    };
                }
            }
        }

        &self.nodes[page_num]
    }

    pub fn flush_all(&mut self) {
        // Again, the reason why we can't just deserialize whole node
        // with bincode is because we are tracking our own num_of_cells.
        //
        // So, if we just use deserialize directly, it will also include
        // the node.cells len by Vec<Cell>.
        //
        // Ideally, we should have just need to call bincode deserialize.
        for node in &self.nodes {
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
        let node = &mut self.nodes[cursor.page_num];
        let num_of_cells = node.num_of_cells as usize;
        if num_of_cells >= LEAF_NODE_MAX_CELLS {
            self.split_and_insert_leaf_node(cursor, row);
        } else {
            node.insert(row, cursor);
        }
        // self.flush(&cursor);
    }

    pub fn create_new_root(&mut self, cursor: &Cursor, mut old_node: Node, mut new_node: Node) {
        println!("--- create_new_root: cursor.page_num: {}", cursor.page_num);
        let mut root_node = Node::new(true, NodeType::Internal);
        old_node.is_root = false;

        root_node.num_of_cells += 1;
        root_node.right_child_offset = cursor.page_num as u32 + 2;

        old_node.parent_offset = 0;
        old_node.next_leaf_offset = cursor.page_num as u32 + 2;

        new_node.parent_offset = 0;

        let left_max_key = old_node.get_max_key();
        let cell = InternalCell::new(cursor.page_num as u32 + 1, left_max_key);
        root_node.internal_cells.insert(0, cell);

        self.nodes.insert(0, root_node);
        self.nodes.insert(cursor.page_num + 1, old_node);
        self.nodes.insert(cursor.page_num + 2, new_node);
    }

    pub fn split_and_insert_leaf_node(&mut self, cursor: &Cursor, row: &Row) {
        println!("--- split_and_insert_leaf_node: {}", row.id);
        let mut old_node = self.nodes.remove(cursor.page_num);
        let old_max = old_node.get_max_key();
        old_node.insert(row, cursor);

        let mut new_node = Node::new(false, old_node.node_type);

        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = old_node.cells.remove(LEAF_NODE_LEFT_SPLIT_COUNT);
            old_node.num_of_cells -= 1;

            new_node.cells.push(cell);
            new_node.num_of_cells += 1;
        }

        if old_node.is_root {
            self.create_new_root(cursor, old_node, new_node);
        } else {
            new_node.next_leaf_offset = old_node.next_leaf_offset + 1;
            old_node.next_leaf_offset = cursor.page_num as u32 + 1;

            let parent_page_num = old_node.parent_offset as usize;
            let parent = &mut self.nodes[parent_page_num];
            let new_max = old_node.get_max_key();
            parent.update_internal_key(old_max, new_max);
            self.nodes.insert(cursor.page_num, new_node);
            self.nodes.insert(cursor.page_num, old_node);
            self.insert_internal_node(parent_page_num, cursor.page_num + 1);
            println!("{:?}", self.nodes);
        }
    }

    pub fn insert_internal_node(&mut self, parent_page_num: usize, new_child_page_num: usize) {
        let parent_right_child_offset = self.nodes[parent_page_num].right_child_offset as usize;
        let new_node = &self.nodes[new_child_page_num];
        let new_child_max_key = new_node.get_max_key();

        let right_child = &self.nodes[parent_right_child_offset];
        let right_max_key = right_child.get_max_key();

        let parent = &mut self.nodes[parent_page_num];
        let original_num_keys = parent.num_of_cells;
        parent.num_of_cells += 1;

        if original_num_keys >= 3 {
            panic!("Need to split internal node\n");
        }

        let index = parent.internal_search(new_child_max_key);
        if new_child_max_key > right_max_key {
            parent.right_child_offset = new_child_page_num as u32;
            parent.internal_insert(
                index,
                InternalCell::new(parent_right_child_offset as u32, right_max_key),
            );
        } else {
            parent.right_child_offset += 1;
            parent.internal_insert(
                index,
                InternalCell::new(new_child_page_num as u32, new_child_max_key),
            );
        }
    }

    pub fn deserialize_row(&mut self, cursor: &Cursor) -> Row {
        self.get_page(cursor.page_num);
        self.nodes[cursor.page_num].get(cursor.cell_num)
    }

    pub fn print_node(&self, node: &Node, indent_level: usize) {
        if node.node_type == NodeType::Internal {
            indent(indent_level);
            println!("- internal (size {})", node.num_of_cells);

            for c in &node.internal_cells {
                let child_index = c.child_pointer() as usize;
                let node = &self.nodes[child_index];
                self.print_node(&node, indent_level + 1);

                indent(indent_level + 1);
                println!("- key {}", c.key());
            }

            let child_index = node.right_child_offset as usize;
            let node = &self.nodes[child_index];
            self.print_node(&node, indent_level + 1);
        } else if node.node_type == NodeType::Leaf {
            indent(indent_level);
            println!("- leaf (size {})", node.num_of_cells);
            for c in &node.cells {
                indent(indent_level + 1);
                println!("- {}", c.key());
            }
        }
    }

    pub fn print_tree(&self) {
        let node = &self.nodes[0];
        self.print_node(node, 0);
    }
}

pub fn indent(level: usize) {
    for _ in 0..level {
        print!("  ");
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

    pub fn debug(&mut self) {
        println!("{:?}", self.pager.nodes);
    }

    pub fn print(&mut self) {
        self.pager.print_tree();
    }
}
