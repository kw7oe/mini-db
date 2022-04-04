use crate::node::NodeType;
use crate::pager::Pager;
use crate::query::Statement;
use crate::row::Row;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Cursor {
    pub page_num: usize,
    pub cell_num: usize,
    pub key_existed: bool,
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
            panic!("Oops, I'm a bug!");
        }
    }

    pub fn table_find(table: &mut Table, page_num: usize, key: u32) -> Result<Self, String> {
        let node = table.pager.get_page(page_num);
        let num_of_cells = node.num_of_cells as usize;

        if node.node_type == NodeType::Leaf {
            match node.search(key) {
                Ok(index) => Ok(Cursor {
                    page_num,
                    cell_num: index,
                    key_existed: true,
                    end_of_table: index == num_of_cells,
                }),
                Err(index) => Ok(Cursor {
                    page_num,
                    cell_num: index,
                    key_existed: false,
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

    pub fn select(&mut self, statement: &Statement) -> String {
        let mut cursor: Cursor;
        let mut output = String::new();

        if let Some(row) = &statement.row {
            cursor = Cursor::table_find(self, self.root_page_num, row.id).unwrap();
            if cursor.key_existed {
                let row = self.pager.deserialize_row(&cursor);
                output.push_str(&format!("{:?}\n", row));
            }
        } else {
            cursor = Cursor::table_start(self);
            while !cursor.end_of_table {
                let row = self.pager.deserialize_row(&cursor);
                output.push_str(&format!("{:?}\n", row));
                cursor.advance(self);
            }
        }

        output
    }

    pub fn insert(&mut self, row: &Row) -> String {
        let page_num = self.root_page_num;
        match Cursor::table_find(self, page_num, row.id) {
            Ok(cursor) => {
                if cursor.key_existed {
                    return "duplicate key\n".to_string();
                }
                self.pager.serialize_row(row, &cursor);

                format!(
                    "inserting into page: {}, cell: {}...\n",
                    cursor.page_num, cursor.cell_num
                )
            }
            Err(message) => message,
        }
    }

    pub fn delete(&mut self, row: &Row) -> String {
        debug!("deleting row with id {}", row.id);
        let cursor = Cursor::table_find(self, self.root_page_num, row.id).unwrap();
        if cursor.key_existed {
            self.pager.delete_row(&cursor);
            format!("deleted {}", row.id)
        } else {
            format!("item not found with id {}", row.id)
        }
    }

    pub fn to_string(&mut self) -> String {
        self.pager.to_string()
    }
}
