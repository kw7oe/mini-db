use crate::query::*;
use crate::table::*;
use std::io::Write;

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
        handle_input(&mut table, &input);

        println!("Executed.");
        buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = std::io::stdout().flush();
}

fn handle_input(table: &mut Table, input: &str) {
    if input.starts_with(".") {
        match handle_meta_command(&input) {
            MetaCommand::Unrecognized => println!("Unrecognized command '{input}'."),
        }
    }

    match prepare_statement(&input) {
        Ok(statement) => execute_statement(table, &statement),
        Err(_reason) => println!("Unrecognized keyword at start of '{input}'."),
    }
}
