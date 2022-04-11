use crate::query::Statement;
use crate::row::Row;
// Ideally this mod shouldn't need to know about NodeType.
// TODO: remove this couping by restructuring the code.
use crate::storage::NodeType;
use crate::storage::Pager;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Cursor {
    pub page_num: usize,
    pub cell_num: usize,
    pub key_existed: bool,
    pub end_of_table: bool,
}

impl Cursor {
    pub fn table_start_v2(table: &mut Table) -> Self {
        let page_num = table.root_page_num;
        if let Ok(mut cursor) = Self::table_find_v2(table, page_num, 0) {
            let num_of_cells = table
                .pager
                .fetch_node(cursor.page_num)
                .as_ref()
                .unwrap()
                .num_of_cells as usize;

            table.pager.unpin_page(cursor.page_num, false);
            cursor.end_of_table = num_of_cells == 0;
            cursor
        } else {
            panic!("Oops, I'm a bug!");
        }
    }

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

    pub fn table_find_v2(table: &mut Table, page_num: usize, key: u32) -> Result<Self, String> {
        let node = table.pager.fetch_node(page_num).unwrap();
        let num_of_cells = node.num_of_cells as usize;
        if node.node_type == NodeType::Leaf {
            match node.search(key) {
                Ok(index) => {
                    table.pager.unpin_page(page_num, false);
                    Ok(Cursor {
                        page_num,
                        cell_num: index,
                        key_existed: true,
                        end_of_table: index == num_of_cells,
                    })
                }
                Err(index) => {
                    table.pager.unpin_page(page_num, false);
                    Ok(Cursor {
                        page_num,
                        cell_num: index,
                        key_existed: false,
                        end_of_table: index == num_of_cells,
                    })
                }
            }
        } else if let Ok(next_page_num) = node.search(key) {
            table.pager.unpin_page(page_num, false);
            Self::table_find_v2(table, next_page_num, key)
        } else {
            table.pager.unpin_page(page_num, false);
            Err("something went wrong".to_string())
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
        } else if let Ok(page_num) = node.search(key) {
            Self::table_find(table, page_num, key)
        } else {
            Err("something went wrong".to_string())
        }
    }

    fn advance_v2(&mut self, table: &mut Table) {
        self.cell_num += 1;
        let old_page_num = self.page_num;
        let node = table.pager.get_node(self.page_num).unwrap();
        let num_of_cells = node.num_of_cells as usize;

        if self.cell_num >= num_of_cells {
            if node.next_leaf_offset == 0 {
                self.end_of_table = true;
            } else {
                self.page_num = node.next_leaf_offset as usize;
                self.cell_num = 0;
            }
        }

        table.pager.unpin_page(old_page_num, false);
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

    pub fn flush_v2(&mut self) {
        self.pager.flush_all_pages();
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

    pub fn select_v2(&mut self, statement: &Statement) -> String {
        let mut cursor: Cursor;
        let mut output = String::new();

        if let Some(row) = &statement.row {
            cursor = Cursor::table_find_v2(self, self.root_page_num, row.id).unwrap();
            if cursor.key_existed {
                let row = self.pager.get_record(&cursor);
                output.push_str(&format!("{:?}\n", row));
            }
        } else {
            cursor = Cursor::table_start_v2(self);
            while !cursor.end_of_table {
                let row = self.pager.get_record(&cursor);
                output.push_str(&format!("{:?}\n", row));
                cursor.advance_v2(self);
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

    pub fn insert_v2(&mut self, row: &Row) -> String {
        let page_num = self.root_page_num;
        match Cursor::table_find_v2(self, page_num, row.id) {
            Ok(cursor) => {
                if cursor.key_existed {
                    return "duplicate key\n".to_string();
                }
                self.pager.insert_record(row, &cursor);

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
}

impl std::string::ToString for Table {
    fn to_string(&self) -> String {
        self.pager.to_string()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::query::prepare_statement;

    use super::*;

    #[test]
    fn select_with_new_buffer_pool_impl() {
        setup_test_db_file();
        let mut table = Table::new("test.db");
        let statement = prepare_statement("select").unwrap();
        let result = table.select_v2(&statement);

        assert_eq!(result, expected_output(1..50));
        assert_eq!(table.pager.tree_len(), 0);

        cleanup_test_db_file();
    }

    #[test]
    fn insert_row_into_leaf_root_node_with_new_buffer_pool_impl() {
        insertion_test(10);
    }

    #[test]
    fn insert_and_split_root_leaf_node_with_new_buffer_pool_impl() {
        insertion_test(15);
    }

    #[test]
    fn insert_internal_node_with_new_buffer_pool_impl() {
        insertion_test(22)
    }

    #[test]
    fn insert_and_split_internal_node_with_new_buffer_pool_impl() {
        env_logger::init();
        insertion_test(29)
    }

    fn insertion_test(row_count: usize) {
        let mut table = Table::new("test.db");
        for i in 1..row_count {
            let query = format!("insert {i} user{i} user{i}@email.com");
            let statement = prepare_statement(&query).unwrap();
            table.insert_v2(&statement.row.unwrap());
            assert_eq!(table.pager.tree_len(), 0);
        }

        let expected_output = expected_output(1..row_count);
        let statement = prepare_statement("select").unwrap();
        let result = table.select_v2(&statement);
        assert_eq!(result, expected_output);

        table.flush_v2();

        // Testing select after we flush all pages
        //
        // So this make sure that our code work as expected
        // even reading from a file that we have just wrote to.
        let mut table = Table::new("test.db");
        let statement = prepare_statement("select").unwrap();
        let result = table.select_v2(&statement);
        assert_eq!(result, expected_output);
        cleanup_test_db_file();
    }

    fn expected_output(range: Range<usize>) -> String {
        range
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("")
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
}
