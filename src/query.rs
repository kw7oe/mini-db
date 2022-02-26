use crate::table::*;
use std::process::exit;

#[derive(Debug)]
pub enum MetaCommand {
    // Success,
    Unrecognized,
}

#[derive(Debug)]
pub enum StatementType {
    Select,
    Insert,
}

#[derive(Debug)]
pub struct Statement {
    statement_type: StatementType,
    row: Option<Row>,
}

pub fn handle_meta_command(command: &str) -> MetaCommand {
    if command.eq(".exit") {
        exit(0)
    } else {
        return MetaCommand::Unrecognized;
    }
}

pub fn prepare_statement(input: &str) -> Result<Statement, &str> {
    if input.starts_with("select") {
        return Ok(Statement {
            statement_type: StatementType::Select,
            row: None,
        });
    }

    if input.starts_with("insert") {
        if let Ok(row) = Row::from_statement(&input) {
            return Ok(Statement {
                statement_type: StatementType::Insert,
                row: Some(row),
            });
        } else {
            return Err("invalid insert statement");
        }
    }

    return Err("unrecognized statement");
}

pub fn execute_statement(table: &mut Table, statement: &Statement) {
    match statement.statement_type {
        StatementType::Select => {
            table.select();
        }
        StatementType::Insert => {
            println!("{:?}", statement.row);
            table.insert(statement.row.as_ref().unwrap());
        }
    }
}
