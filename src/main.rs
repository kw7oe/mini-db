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
    let mut table = Table::new();
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        let output = handle_input(&mut table, &input);
        if output == "Exit" {
            exit(0);
        }

        print!("{}", output);

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
        Err(_reason) => format!("Unrecognized keyword at start of '{input}'."),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn exit_command() {
        let mut table = Table::new();
        let output = handle_input(&mut table, ".exit");
        assert_eq!(output, "Exit");
    }

    #[test]
    fn unrecognized_command() {
        let mut table = Table::new();
        let output = handle_input(&mut table, ".dfaskfd");
        assert_eq!(output, "Unrecognized command '.dfaskfd'.");
    }
}
