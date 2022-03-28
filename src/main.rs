use crate::node::LEAF_NODE_CELL_SIZE;
use crate::query::*;
use crate::table::*;
use std::io::Write;
use std::process::exit;

#[macro_use]
extern crate serde_big_array;
big_array! {
    BigArray;
    32, 255, LEAF_NODE_CELL_SIZE
}

mod node;
mod query;
mod row;
mod table;
mod tree;

fn main() -> std::io::Result<()> {
    let mut table = Table::new("data.db".to_string());
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        let output = handle_input(&mut table, &input);
        if output == "Exit" {
            table.flush();
            exit(0);
        }

        println!("{}", output);

        println!("Executed.");
        buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = std::io::stdout().flush();
}

fn handle_input(table: &mut Table, input: &str) -> String {
    if input.starts_with(".") {
        match handle_meta_command(&input) {
            MetaCommand::Exit => return "Exit".to_string(),
            MetaCommand::Unrecognized => return format!("Unrecognized command '{input}'."),
        }
    }

    match prepare_statement(&input) {
        Ok(statement) => execute_statement(table, &statement),
        Err(reason) => reason,
    }
}

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn exit_command() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, ".exit");
        assert_eq!(output, "Exit");

        clean_test();
    }

    #[test]
    fn unrecognized_command() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, ".dfaskfd");
        assert_eq!(output, "Unrecognized command '.dfaskfd'.");

        clean_test();
    }

    #[test]
    fn invalid_statement() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, "insert 1 apple apple apple");
        assert_eq!(
            output,
            "Unrecognized keyword at start of 'insert 1 apple apple apple'."
        );

        clean_test();
    }

    #[test]
    fn select_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "select");
        assert_eq!(output, "");

        handle_input(&mut table, "insert 1 john john@email.com");
        handle_input(&mut table, "insert 2 wick wick@email.com");

        let output = handle_input(&mut table, "select");
        assert_eq!(
            output,
            "(1, john, john@email.com)\n(2, wick, wick@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn insert_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "insert 2 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 3 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 2...\n");

        clean_test();
    }

    #[test]
    fn insert_up_to_3_leaf_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..15 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        handle_input(&mut table, &format!("insert 15 user15 user15@email.com"));

        let expected_output = "- internal (size 1)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 8)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
    - 14
    - 15
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_up_to_4_leaf_node_split_when_child_max_key_larger_than_right_max_key() {
        let mut table = Table::new("test.db".to_string());
        let inputs = [
            "insert 18 user18 person18@example.com",
            "insert 7 user7 person7@example.com",
            "insert 10 user10 person10@example.com",
            "insert 29 user29 person29@example.com",
            "insert 23 user23 person23@example.com",
            "insert 4 user4 person4@example.com",
            "insert 14 user14 person14@example.com",
            "insert 30 user30 person30@example.com",
            "insert 15 user15 person15@example.com",
            "insert 26 user26 person26@example.com",
            "insert 22 user22 person22@example.com",
            "insert 19 user19 person19@example.com",
            "insert 2 user2 person2@example.com",
            "insert 1 user1 person1@example.com",
            "insert 21 user21 person21@example.com",
            "insert 11 user11 person11@example.com",
            "insert 6 user6 person6@example.com",
            "insert 20 user20 person20@example.com",
            "insert 5 user5 person5@example.com",
            "insert 8 user8 person8@example.com",
            "insert 9 user9 person9@example.com",
            "insert 3 user3 person3@example.com",
            "insert 12 user12 person12@example.com",
            "insert 27 user27 person27@example.com",
            "insert 17 user17 person17@example.com",
            "insert 16 user16 person16@example.com",
            "insert 13 user13 person13@example.com",
            "insert 24 user24 person24@example.com",
            "insert 25 user25 person25@example.com",
            "insert 28 user28 person28@example.com",
        ];

        for input in inputs {
            handle_input(&mut table, input);
        }

        let expected_output = "- internal (size 3)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 8)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
    - 14
    - 15
  - key 15
  - leaf (size 7)
    - 16
    - 17
    - 18
    - 19
    - 20
    - 21
    - 22
  - key 22
  - leaf (size 8)
    - 23
    - 24
    - 25
    - 26
    - 27
    - 28
    - 29
    - 30
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_up_to_4_leaf_node_split_when_child_max_key_not_larger_than_right_max_key() {
        let mut table = Table::new("test.db".to_string());
        let inputs = [
            "insert 1 user18 person18@example.com",
            "insert 4 user7 person7@example.com",
            "insert 7 user10 person10@example.com",
            "insert 10 user29 person29@example.com",
            "insert 13 user23 person23@example.com",
            "insert 14 user4 person4@example.com",
            "insert 19 user14 person14@example.com",
            "insert 24 user30 person30@example.com",
            "insert 27 user15 person15@example.com",
            "insert 30 user26 person26@example.com",
            "insert 40 user22 person22@example.com",
            "insert 55 user19 person19@example.com",
            "insert 41 user2 person2@example.com",
            "insert 34 user1 person1@example.com",
            "insert 21 user21 person21@example.com",
            "insert 60 user11 person11@example.com",
            "insert 64 user6 person6@example.com",
            "insert 58 user20 person20@example.com",
            "insert 76 user5 person5@example.com",
            "insert 88 user8 person8@example.com",
            "insert 90 user9 person9@example.com",
            "insert 70 user3 person3@example.com",
            "insert 5 user12 person12@example.com",
            "insert 2 user27 person27@example.com",
            "insert 72 user17 person17@example.com",
            "insert 66 user16 person16@example.com",
            "insert 53 user13 person13@example.com",
            "insert 34 user24 person24@example.com",
            "insert 22 user25 person25@example.com",
            "insert 23 user25 person25@example.com",
            "insert 25 user25 person25@example.com",
            "insert 26 user25 person25@example.com",
            "insert 28 user25 person25@example.com",
            "insert 31 user25 person25@example.com",
            "insert 32 user25 person25@example.com",
        ];

        for input in inputs {
            handle_input(&mut table, input);
        }

        let expected_output = "- internal (size 3)
  - leaf (size 9)
    - 1
    - 2
    - 4
    - 5
    - 7
    - 10
    - 13
    - 14
    - 19
  - key 19
  - leaf (size 7)
    - 21
    - 22
    - 23
    - 24
    - 25
    - 26
    - 27
  - key 27
  - leaf (size 7)
    - 28
    - 30
    - 31
    - 32
    - 34
    - 40
    - 41
  - key 41
  - leaf (size 11)
    - 53
    - 55
    - 58
    - 60
    - 64
    - 66
    - 70
    - 72
    - 76
    - 88
    - 90
";
        let output = table.to_string();
        assert_eq!(output, expected_output);
        clean_test()
    }

    #[test]
    fn insert_and_split_internal_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..36 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let expected_output = "- internal (size 1)
  - internal (size 2)
    - leaf (size 7)
      - 1
      - 2
      - 3
      - 4
      - 5
      - 6
      - 7
    - key 7
    - leaf (size 7)
      - 8
      - 9
      - 10
      - 11
      - 12
      - 13
      - 14
    - key 14
    - leaf (size 7)
      - 15
      - 16
      - 17
      - 18
      - 19
      - 20
      - 21
  - key 21
  - internal (size 1)
    - leaf (size 7)
      - 22
      - 23
      - 24
      - 25
      - 26
      - 27
      - 28
    - key 28
    - leaf (size 7)
      - 29
      - 30
      - 31
      - 32
      - 33
      - 34
      - 35
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_string_at_max_length() {
        let mut table = Table::new("test.db".to_string());
        let mut username = String::new();
        for _ in 0..32 {
            username.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let mut email = String::new();
        for _ in 0..255 {
            email.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 2 john {email}"));
        assert_eq!(output, "inserting into page: 0, cell: 1...\n");

        clean_test();
    }

    #[test]
    fn error_when_duplicate_key() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "duplicate key\n");

        clean_test();
    }

    #[test]
    fn error_when_id_is_negative() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, "insert -1 john john@email.com");
        assert_eq!(output, "ID must be positive.");

        clean_test();
    }

    #[test]
    fn error_when_string_are_too_long() {
        let mut table = Table::new("test.db".to_string());
        let mut username = String::new();
        for _ in 0..33 {
            username.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "Name is too long.");

        let mut email = String::new();
        for _ in 0..256 {
            email.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 john {email}"));
        assert_eq!(output, "Email is too long.");

        clean_test();
    }

    #[test]
    fn persist_data_to_file() {
        let mut table = Table::new("test.db".to_string());

        handle_input(&mut table, "insert 2 john john@email.com");
        handle_input(&mut table, "insert 1 wick wick@email.com");
        let output = handle_input(&mut table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );
        table.flush();

        let mut reopen_table = Table::new("test.db".to_string());
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn persist_leaf_and_internal_node_to_file() {
        let mut table = Table::new("test.db".to_string());
        let row_count = 1000;

        for i in 1..row_count {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        let expected_output: Vec<String> = (1..row_count)
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));

        // To test it doesn't go stack overflow.
        table.to_string();
        table.flush();

        let mut reopen_table = Table::new("test.db".to_string());
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(output, expected_output.join(""));

        clean_test();
    }

    #[test]
    // This is an edge case where previously our table_find method will return
    // Err("duplicate key") when an existed key is found. This work as expected when
    // we are using table_find for insertion.
    //
    // However, there's rare cases where 0 is part of the input, during select,
    // we called table_start, which then called table_find to find the 0 element to get
    // the first cell.
    //
    // Since the 0 element exist, instead of returning the cursor,
    // we return error, which our impl fallback to our previous incorrect
    // implementation and caused a panic as our code attempt to access cells that is empty.
    //
    // This test inputs is generated by quickcheck.
    fn edge_case_1() {
        let mut table = Table::new("test.db".to_string());
        let mut ids = vec![2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 0, 11, 12, 13];
        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    use rand::seq::SliceRandom;
    use rand::thread_rng;

    quickcheck! {
        fn insert_and_select_prop_test(ids: Vec<u32>) -> bool {
            let mut generated_ids = ids.clone();
            generated_ids.sort();
            generated_ids.dedup();
            generated_ids.shuffle(&mut thread_rng());
            println!("quickcheck: {:?}", generated_ids);

            let mut table = Table::new("test.db".to_string());

            for i in &generated_ids {
                handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
            }

            let output = handle_input(&mut table, "select");

            generated_ids.sort();
            let expected_output: Vec<String> = generated_ids
                .iter()
                .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
                .collect();

            output == expected_output.join("")
        }
    }

    fn clean_test() {
        let _ = std::fs::remove_file("test.db");
    }
}
