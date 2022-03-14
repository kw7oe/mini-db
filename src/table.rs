use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    io::{Read, Seek, Write},
    path::PathBuf,
};

use crate::node::{
    Node, NodeType, COMMON_NODE_HEADER_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_MAX_CELLS,
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
    pub fn table_start(table: &mut Table) -> Cursor {
        let page_num = table.root_page_num;
        let num_of_cells = table.pager.get_page(page_num).num_of_cells as usize;

        Cursor {
            page_num,
            cell_num: 0,
            end_of_table: num_of_cells == 0,
        }
    }

    pub fn table_find(table: &mut Table, key: u32) -> Result<Cursor, String> {
        let page_num = table.root_page_num;
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
            panic!("need to implement search for internal node");
        }
    }

    fn advance(&mut self, table: &mut Table) {
        self.cell_num += 1;

        let num_of_cells = table.pager.get_page(self.page_num).num_of_cells as usize;

        if self.cell_num >= num_of_cells {
            self.end_of_table = true;
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

            if self.file_len % PAGE_SIZE != 0 {
                // We wrote a partial page
                number_of_pages += 1;
            }

            self.nodes.insert(page_num, Node::new(true, NodeType::Leaf));

            if page_num < number_of_pages {
                let offset = page_num as i64 * PAGE_SIZE as i64;

                if let Ok(_) = self.read_file.seek(SeekFrom::Current(offset)) {
                    let mut buffer = [0; PAGE_SIZE];
                    if let Ok(_read_len) = self.read_file.read(&mut buffer) {
                        let node = self.nodes.get_mut(page_num).unwrap();
                        node.set_header(&buffer[0..LEAF_NODE_HEADER_SIZE]);
                        node.set_cells(&buffer[LEAF_NODE_HEADER_SIZE..]);
                    };
                }
            } else {
                let bytes = &self.nodes[page_num].header();
                self.write_file.write(bytes).unwrap();
            }
        }

        &self.nodes[page_num]
    }

    pub fn flush(&mut self, cursor: &Cursor) {
        let node = self.get_page(cursor.page_num);
        let num_of_cells_bytes = &node.num_of_cells.to_le_bytes();

        self.write_file
            .write(&self.nodes[cursor.page_num].cells(cursor.cell_num))
            .unwrap();

        self.write_file
            .seek(SeekFrom::Start(COMMON_NODE_HEADER_SIZE as u64))
            .unwrap();

        self.write_file.write(num_of_cells_bytes).unwrap();
        self.write_file.seek(SeekFrom::End(0)).unwrap();
    }

    pub fn serialize_row(&mut self, row: &Row, cursor: &Cursor) {
        self.nodes[cursor.page_num].insert(row, cursor);
        self.flush(&cursor);
    }

    pub fn deserialize_row(&mut self, cursor: &Cursor) -> Row {
        self.nodes[cursor.page_num].get(cursor.cell_num)
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
        let node = self.pager.get_page(self.root_page_num);
        if node.num_of_cells as usize >= LEAF_NODE_MAX_CELLS {
            panic!("table full")
        }

        match Cursor::table_find(self, row.id) {
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
}
