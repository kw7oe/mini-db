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
    pub fn table_start(table: &Table) -> Self {
        let page_num = table.root_page_num;
        if let Ok(mut cursor) = Self::table_find(table, page_num, 0) {
            let page = table.pager.fetch_read_page_guard(cursor.page_num).unwrap();
            let num_of_cells = page.node.as_ref().unwrap().num_of_cells as usize;

            drop(page);
            table.pager.unpin_page(cursor.page_num, false);
            cursor.end_of_table = num_of_cells == 0;
            cursor
        } else {
            panic!("Oops, I'm a bug!");
        }
    }

    pub fn table_find(table: &Table, page_num: usize, key: u32) -> Result<Self, String> {
        let page = table.pager.fetch_read_page_guard(page_num).unwrap();
        let node = page.node.as_ref().unwrap();
        let num_of_cells = node.num_of_cells as usize;

        if node.node_type == NodeType::Leaf {
            match node.search(key) {
                Ok(index) => {
                    drop(page);
                    table.pager.unpin_page(page_num, false);
                    Ok(Cursor {
                        page_num,
                        cell_num: index,
                        key_existed: true,
                        end_of_table: index == num_of_cells,
                    })
                }
                Err(index) => {
                    drop(page);
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
            drop(page);
            table.pager.unpin_page(page_num, false);
            Self::table_find(table, next_page_num, key)
        } else {
            drop(page);
            table.pager.unpin_page(page_num, false);
            Err("something went wrong".to_string())
        }
    }

    fn advance(&mut self, table: &Table) {
        self.cell_num += 1;
        let old_page_num = self.page_num;
        let page = table.pager.fetch_read_page_guard(self.page_num).unwrap();
        let node = page.node.as_ref().unwrap();
        let num_of_cells = node.num_of_cells as usize;

        if self.cell_num >= num_of_cells {
            if node.next_leaf_offset == 0 {
                self.end_of_table = true;
            } else {
                self.page_num = node.next_leaf_offset as usize;
                self.cell_num = 0;
            }
        }

        drop(page);
        table.pager.unpin_page(old_page_num, false);
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

    pub fn flush(&self) {
        self.pager.flush_all_pages();
    }

    pub fn select(&self, statement: &Statement) -> String {
        let mut cursor: Cursor;
        let mut output = String::new();

        if let Some(row) = &statement.row {
            cursor = Cursor::table_find(self, self.root_page_num, row.id).unwrap();
            if cursor.key_existed {
                let row = self.pager.get_record(&cursor);
                output.push_str(&format!("{:?}\n", row));
            }
        } else {
            cursor = Cursor::table_start(self);
            while !cursor.end_of_table {
                let row = self.pager.get_record(&cursor);
                output.push_str(&format!("{:?}\n", row));
                cursor.advance(self);
            }
        }

        output
    }

    pub fn insert(&self, row: &Row) -> String {
        let page_num = self.root_page_num;
        self.pager.insert(page_num, row).unwrap()
    }

    pub fn delete(&self, row: &Row) -> String {
        let cursor = Cursor::table_find(self, self.root_page_num, row.id).unwrap();
        if cursor.key_existed {
            self.pager.delete_record(&cursor);
            format!("deleted {}", row.id)
        } else {
            format!("item not found with id {}", row.id)
        }
    }

    pub fn concurrent_delete(&self, row: &Row) -> String {
        let page_num = self.root_page_num;
        self.pager.delete(page_num, row).unwrap()
    }

    pub fn pages(&self) -> String {
        self.pager.debug_pages()
    }
}

impl std::string::ToString for Table {
    fn to_string(&self) -> String {
        self.pager.to_tree_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::query::prepare_statement;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::thread;
    use threadpool::ThreadPool;
    use tracing::info;

    #[test]
    fn select_with_new_buffer_pool_impl() {
        setup_test_db_file();
        let table = setup_test_table();
        let statement = prepare_statement("select").unwrap();
        let result = table.select(&statement);

        assert_eq!(result, expected_output(1..50));

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
    fn insert_with_replace_pages_and_flush_dirty_pages_with_new_buffer_pool_impl() {
        insertion_test(29)
    }

    #[test]
    fn insert_and_split_root_internal_node_with_new_buffer_pool_impl() {
        insertion_test(36)
    }

    #[test]
    fn insert_and_split_internal_node_with_new_buffer_pool_impl() {
        insertion_test(57)
    }

    #[test]
    fn insert_ensure_pin_count_is_updated_correctly() {
        // Previous incorrect implementation will error due to
        // not having enough pages as pin count is not updated correctly.
        insertion_test(65);
        insertion_test(165);
        insertion_test(365);
    }

    #[test]
    fn insert_ensure_siblings_and_children_page_id_is_updated_correctly() {
        // Previous implementation did not update the child pointer of the parent
        // correctly when we split or create new root node.
        //
        // Also, we didn't update right_node next_leaf page id correctly as well.
        let ids = UniqueIDs(vec![
            163, 91, 7, 79, 208, 225, 157, 237, 234, 142, 45, 22, 201, 156, 43, 119, 1, 30, 252,
            47, 169, 52, 120, 75, 189, 24, 230, 210, 103, 98, 150, 112, 100, 255, 72, 58, 29, 232,
            11, 126, 154, 85, 20, 176, 125, 118, 83, 73, 76, 105, 130, 124, 220, 211, 114, 88, 10,
            244, 203, 49,
        ]);

        insert_and_select_prop(ids);
    }

    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    #[derive(Clone, Debug)]
    struct UniqueIDs(pub Vec<u8>);

    impl Arbitrary for UniqueIDs {
        fn arbitrary(g: &mut Gen) -> UniqueIDs {
            let mut vec = Vec::<u8>::arbitrary(g);
            vec.sort_unstable();
            vec.dedup();
            vec.shuffle(&mut thread_rng());
            UniqueIDs(vec)
        }
    }

    #[test]
    fn quickcheck_insert_and_select() {
        // Change the Gen::new(size) to have quickcheck
        // generate larger size vector.
        let gen = Gen::new(100);

        QuickCheck::new()
            .gen(gen)
            .quickcheck(insert_and_select_prop as fn(UniqueIDs));
    }

    fn insert_and_select_prop(mut ids: UniqueIDs) {
        let table = setup_test_table();
        for i in &ids.0 {
            let query = format!("insert {i} user{i} user{i}@email.com");
            let statement = prepare_statement(&query).unwrap();
            table.insert(&statement.row.unwrap());
        }

        ids.0.sort_unstable();
        let expected_output = expected_output(ids.0);
        let statement = prepare_statement("select").unwrap();
        let result = table.select(&statement);
        assert_eq!(result, expected_output);

        table.flush();

        // Testing select after we flush all pages
        //
        // So this make sure that our code work as expected
        // even reading from a file that we have just wrote to.
        let table = setup_test_table();
        let statement = prepare_statement("select").unwrap();
        let result = table.select(&statement);
        assert_eq!(result, expected_output);

        cleanup_test_db_file();
    }

    fn insertion_test(row_count: usize) {
        let table = setup_test_table();
        for i in 1..row_count {
            let query = format!("insert {i} user{i} user{i}@email.com");
            let statement = prepare_statement(&query).unwrap();
            table.insert(&statement.row.unwrap());
        }

        let expected_output = expected_output(1..row_count);
        let statement = prepare_statement("select").unwrap();
        let result = table.select(&statement);
        assert_eq!(result, expected_output);

        table.flush();

        // Testing select after we flush all pages
        //
        // So this make sure that our code work as expected
        // even reading from a file that we have just wrote to.
        let table = setup_test_table();
        let statement = prepare_statement("select").unwrap();
        let result = table.select(&statement);
        assert_eq!(result, expected_output);
        cleanup_test_db_file();
    }

    #[test]
    fn delete_cells_from_root_node() {
        deletion_test(10);
    }

    #[test]
    fn delete_cells_from_leaf_node_up_to_root_node() {
        deletion_test(15);
    }

    #[test]
    fn delete_cells_and_ensure_new_merged_node_next_leaf_offset_is_updated_correctly() {
        deletion_test(22)
    }

    #[test]
    fn delete_cells_and_merge_internal_nodes() {
        deletion_test(36)
    }

    #[test]
    fn delete_ensure_pages_is_unpin_correctly() {
        // Previous impl doesn not unpin left page during merge operation.
        deletion_test(57);

        // Previous impl does not unpin left page most right node during merge operation.
        deletion_test(165);

        // Testing on a bigger db.
        deletion_test(365);
    }

    fn deletion_test(row_count: usize) {
        let table = setup_test_table();
        for i in 1..row_count {
            let query = format!("insert {i} user{i} user{i}@email.com");
            let statement = prepare_statement(&query).unwrap();
            table.insert(&statement.row.unwrap());
        }

        let mut remaining: Vec<usize> = (1..row_count).collect();
        for i in 1..row_count {
            let query = format!("delete {i}");
            let statement = prepare_statement(&query).unwrap();
            table.delete(&statement.row.unwrap());

            let index = remaining.iter().position(|&x| x == i).unwrap();
            remaining.remove(index);

            let statement = prepare_statement("select").unwrap();
            let result = table.select(&statement);
            assert_eq!(result, expected_output(&remaining));
        }

        cleanup_test_db_file();
    }

    #[derive(Clone, Debug)]
    struct DeleteInputs {
        pub insertion_ids: Vec<u8>,
        pub deletion_ids: Vec<u8>,
    }

    impl Arbitrary for DeleteInputs {
        fn arbitrary(g: &mut Gen) -> DeleteInputs {
            let mut insertion_ids = Vec::<u8>::arbitrary(g);
            insertion_ids.sort_unstable();
            insertion_ids.dedup();
            insertion_ids.shuffle(&mut thread_rng());

            let mut deletion_ids = insertion_ids.clone();
            deletion_ids.shuffle(&mut thread_rng());

            Self {
                insertion_ids,
                deletion_ids,
            }
        }
    }

    #[test]
    fn quickcheck_insert_delete_and_select() {
        // Change the Gen::new(size) to have quickcheck
        // generate larger size vector.
        let gen = Gen::new(100);

        QuickCheck::new()
            .gen(gen)
            .quickcheck(insert_delete_and_select_prop as fn(DeleteInputs));
    }

    fn insert_delete_and_select_prop(delete_input: DeleteInputs) {
        let table = setup_test_table();

        for i in &delete_input.insertion_ids {
            let query = format!("insert {i} user{i} user{i}@email.com");
            let statement = prepare_statement(&query).unwrap();
            table.insert(&statement.row.unwrap());
        }

        let mut remaining: Vec<u8> = delete_input.insertion_ids.clone();
        remaining.sort_unstable();

        for i in &delete_input.deletion_ids {
            let query = format!("delete {i}");
            let statement = prepare_statement(&query).unwrap();
            table.delete(&statement.row.unwrap());

            let index = remaining.iter().position(|x| x == i).unwrap();
            remaining.remove(index);

            let statement = prepare_statement("select").unwrap();
            let result = table.select(&statement);
            assert_eq!(result, expected_output(&remaining));
        }

        cleanup_test_db_file();
    }

    #[test]
    fn concurrent_insert_into_root_leaf_node() {
        test_concurrent_insert(100, 12)
    }

    #[test]
    fn concurrent_insert_and_split_into_level_2() {
        test_concurrent_insert(100, 20)
    }

    #[test]
    fn concurrent_insert_and_split_leaf_node_and_update_parent_at_level_2() {
        test_concurrent_insert(100, 30)
    }

    #[test]
    fn concurrent_insert_and_split_root_internal_node() {
        test_concurrent_insert(100, 40)
    }

    #[test]
    fn concurrent_insert_and_split_internal_node() {
        test_concurrent_insert(100, 75)
    }

    #[test]
    fn concurrent_insert_lots_of_records() {
        // With tracing lib
        // let format = tracing_subscriber::fmt::format().with_thread_ids(true);
        // tracing_subscriber::fmt().event_format(format).init();

        // The reason we need to use a thread pool is because it might failed
        // occasionally due to insufficient buffer pool page if we spawn 1 thread
        // per record.  We are essentially  spawning 1000 threads to be
        // executed concurrently and it will caused our buffer pool not having
        // enough pages to hold those page and caused a panic.
        //
        // And having more threads != better performance.
        test_concurrent_insert_with_thread_pool(16, 10, 1000)
    }

    fn test_concurrent_insert_with_thread_pool(
        thread_pool_size: usize,
        frequency: usize,
        row: usize,
    ) {
        std::panic::set_hook(Box::new(|p| {
            cleanup_test_db_file();
            println!("{p}");
        }));

        let pool = ThreadPool::new(thread_pool_size);

        for i in 0..frequency {
            info!("--- test concurrent insert {i} ---");
            let table = Arc::new(setup_test_table());
            let (tx, rx) = std::sync::mpsc::channel();

            for i in 1..row {
                let table = Arc::clone(&table);
                let tx = tx.clone();
                pool.execute(move || {
                    let row = Row::from_statement(&format!("insert {i} user{i} user{i}@email.com"))
                        .unwrap();
                    table.insert(&row);
                    tx.send(1)
                        .expect("channel will be there waiting for the pool");
                });
            }

            // Wait for rx, similar to calling handle.join()
            for _ in 1..row {
                rx.recv().unwrap();
            }

            let statement = prepare_statement("select").unwrap();
            let result = table.select(&statement);
            assert_eq!(result, expected_output(1..row));

            cleanup_test_db_file();
        }
    }

    fn test_concurrent_insert(frequency: usize, row: usize) {
        std::panic::set_hook(Box::new(|p| {
            cleanup_test_db_file();
            println!("{p}");
        }));

        for i in 0..frequency {
            info!("--- test concurrent insert {i} ---");
            let table = Arc::new(setup_test_table());

            let mut handles = vec![];
            for i in 1..row {
                let table = Arc::clone(&table);
                let handle = thread::spawn(move || {
                    let row = Row::from_statement(&format!("insert {i} user{i} user{i}@email.com"))
                        .unwrap();
                    table.insert(&row);
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.join().unwrap();
            }

            let statement = prepare_statement("select").unwrap();
            let result = table.select(&statement);
            assert_eq!(result, expected_output(1..row));

            cleanup_test_db_file();
        }
    }

    #[test]
    fn concurrent_select() {
        let thread_pool_size = 16;
        let request_count = 24;
        let frequency = 50;
        let row = 250;

        let pool = ThreadPool::new(thread_pool_size);
        let table = Arc::new(setup_test_table());

        for i in 1..row {
            let row =
                Row::from_statement(&format!("insert {i} user{i} user{i}@email.com")).unwrap();
            table.insert(&row);
        }

        for i in 0..frequency {
            info!("--- concurrent select: {i} ---");
            for _ in 1..request_count {
                let table = Arc::clone(&table);
                pool.execute(move || {
                    let statement = prepare_statement("select").unwrap();
                    let result = table.select(&statement);
                    assert_eq!(result, expected_output(1..row));
                });
            }

            pool.join();
            assert_eq!(pool.panic_count(), 0);
        }

        cleanup_test_db_file();
    }

    #[test]
    fn concurrent_delete_on_root_leaf_node() {
        test_concurrent_delete(100, 13);
    }

    #[test]
    fn concurrent_delete_from_level_2() {
        test_concurrent_delete(100, 20);
    }

    #[test]
    fn concurrent_delete_and_merge_leaf_node_and_update_parent_from_level_2() {
        test_concurrent_delete(100, 30);
    }

    #[test]
    fn concurrent_delete_and_merge_root_internal_node() {
        test_concurrent_delete(100, 40);
    }

    // #[test]
    fn concurrent_delete_and_merge_internal_node() {
        // tracing_subscriber::fmt()
        //     .with_max_level(tracing::Level::INFO)
        //     .with_thread_ids(true)
        //     .init();
        test_concurrent_delete(100, 75);
    }

    fn test_concurrent_delete(frequency: usize, row: usize) {
        for i in 0..frequency {
            info!("--- concurrent delete: {i} ---");
            let table = Arc::new(setup_test_table());

            for i in 1..row {
                let row =
                    Row::from_statement(&format!("insert {i} user{i} user{i}@email.com")).unwrap();
                table.insert(&row);
            }

            let mut handles = vec![];
            for i in 1..row {
                let table = Arc::clone(&table);
                let handle = std::thread::spawn(move || {
                    let statement = prepare_statement(&format!("delete {i}")).unwrap();
                    let result = table.concurrent_delete(&statement.row.unwrap());
                    assert_eq!(result, format!("deleted {i}"));
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let statement = prepare_statement("select").unwrap();
            let result = table.select(&statement);
            assert_eq!(result, "");
        }

        cleanup_test_db_file();
    }

    fn expected_output<I>(range: I) -> String
    where
        I: IntoIterator,
        I::Item: std::fmt::Display,
    {
        range
            .into_iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("")
    }

    fn setup_test_table() -> Table {
        return Table::new(format!("test-{:?}.db", std::thread::current().id()));
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
}
