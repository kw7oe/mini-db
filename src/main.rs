use crate::query::*;
use crate::table::*;
use std::io::Write;
use std::process::exit;

#[macro_use]
extern crate serde_big_array;
big_array! {
    BigArray;
    32, 255
}

mod query;
mod table;

fn main() -> std::io::Result<()> {
    let mut table = Table::new("data.db".to_string());
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        let output = handle_input(&mut table, &input);
        if output == "Exit" {
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
        println!("{}", output);
        assert_eq!(
            output,
            "(1, john, john@email.com)\n(2, wick, wick@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn insert_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(
            output,
            "inserting to page 0 with row offset 0 and byte offset 0...\n"
        );

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(
            output,
            "inserting to page 0 with row offset 1 and byte offset 291...\n"
        );

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
        assert_eq!(
            output,
            "inserting to page 0 with row offset 0 and byte offset 0...\n"
        );

        let mut email = String::new();
        for _ in 0..255 {
            email.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 john {email}"));
        assert_eq!(
            output,
            "inserting to page 0 with row offset 1 and byte offset 291...\n"
        );

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

        handle_input(&mut table, "insert 1 john john@email.com");
        let output = handle_input(&mut table, "select");
        assert_eq!(output, "(1, john, john@email.com)\n");
        drop(table);

        let mut reopen_table = Table::new("test.db".to_string());
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(output, "(1, john, john@email.com)\n");

        clean_test();
    }

    fn clean_test() {
        let _ = std::fs::remove_file("test.db");
    }
}
