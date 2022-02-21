use std::{io::Write, process::exit};

#[derive(Debug)]
enum MetaCommand {
    // Success,
    Unrecognized,
}

#[derive(Debug)]
enum StatementType {
    Select,
    Insert,
}

#[derive(Debug)]
struct Statement {
    statement_type: StatementType,
}

fn main() -> std::io::Result<()> {
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();

        if input.starts_with(".") {
            match handle_meta_command(&input) {
                MetaCommand::Unrecognized => println!("Unrecognized command '{input}'."),
            }
        }

        match prepare_statement(&input) {
            Ok(statement) => execute_statement(&statement),
            Err(_reason) => println!("Unrecognized keyword at start of '{input}'."),
        }

        println!("Executed.");
        buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = std::io::stdout().flush();
}

fn handle_meta_command(command: &str) -> MetaCommand {
    if command.eq(".exit") {
        exit(0)
    } else {
        return MetaCommand::Unrecognized;
    }
}

fn prepare_statement(input: &str) -> Result<Statement, &str> {
    if input.starts_with("select") {
        return Ok(Statement {
            statement_type: StatementType::Select,
        });
    }

    if input.starts_with("insert") {
        return Ok(Statement {
            statement_type: StatementType::Insert,
        });
    }

    return Err("unrecognized statement");
}

fn execute_statement(statement: &Statement) {
    match statement.statement_type {
        StatementType::Select => println!("do select"),
        StatementType::Insert => println!("do insert"),
    }
}
