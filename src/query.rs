use crate::row::Row;
use crate::table::*;

#[derive(Debug)]
pub enum MetaCommand {
    // Success,
    Unrecognized,
    Exit,
    PrintTree,
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
        MetaCommand::Exit
    } else if command.eq(".tree") {
        MetaCommand::PrintTree
    } else {
        MetaCommand::Unrecognized
    }
}

pub fn prepare_statement(input: &str) -> Result<Statement, String> {
    if input.starts_with("select") {
        return Ok(Statement {
            statement_type: StatementType::Select,
            row: None,
        });
    }

    if input.starts_with("insert") {
        match Row::from_statement(&input) {
            Ok(row) => {
                return Ok(Statement {
                    statement_type: StatementType::Insert,
                    row: Some(row),
                })
            }
            Err(e) => return Err(e),
        }
    }

    return Err("unrecognized statement".to_string());
}

pub fn execute_statement(table: &mut Table, statement: &Statement) -> String {
    match statement.statement_type {
        StatementType::Select => table.select(),
        StatementType::Insert => table.insert(statement.row.as_ref().unwrap()),
    }
}
