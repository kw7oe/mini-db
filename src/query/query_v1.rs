use crate::row::Row;
use crate::table::*;
use std::str::FromStr;

#[derive(Debug)]
pub enum MetaCommand {
    // Success,
    Unrecognized,
    Exit,
    PrintTree,
    PrintPages,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Delete,
}

impl FromStr for StatementType {
    type Err = String;
    fn from_str(action: &str) -> Result<Self, Self::Err> {
        match action {
            "select" => Ok(StatementType::Select),
            "insert" => Ok(StatementType::Insert),
            "delete" => Ok(StatementType::Delete),
            _ => Err("unrecognized statement".into()),
        }
    }
}

#[derive(Debug)]
pub struct Statement {
    statement_type: StatementType,
    pub row: Option<Row>,
}

pub fn handle_meta_command(command: &str) -> MetaCommand {
    if command.eq(".exit") {
        MetaCommand::Exit
    } else if command.eq(".tree") {
        MetaCommand::PrintTree
    } else if command.eq(".pages") {
        MetaCommand::PrintPages
    } else {
        MetaCommand::Unrecognized
    }
}

pub fn prepare_statement(input: &str) -> Result<Statement, String> {
    match input.split_once(' ') {
        None => {
            let statement_type = StatementType::from_str(input)?;

            if statement_type == StatementType::Insert {
                Err("missing row value for insert".to_string())
            } else {
                Ok(Statement {
                    statement_type,
                    row: None,
                })
            }
        }
        Some((action, rest)) => Ok(Statement {
            statement_type: StatementType::from_str(action)?,
            row: Some(Row::from_str(rest)?),
        }),
    }
}

pub fn execute_statement(table: &mut Table, statement: &Statement) -> String {
    match statement.statement_type {
        StatementType::Select => table.select(statement),
        StatementType::Insert => table.insert(statement.row.as_ref().unwrap()),
        StatementType::Delete => table.delete(statement.row.as_ref().unwrap()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_select_without_id() {
        let result = prepare_statement("select");
        assert!(result.is_ok());

        let statement = result.unwrap();
        assert_eq!(statement.statement_type, StatementType::Select);
        assert_eq!(statement.row, None);
    }

    #[test]
    fn parse_select_with_id() {
        let result = prepare_statement("select 1");
        assert!(result.is_ok());

        let statement = result.unwrap();
        assert_eq!(statement.statement_type, StatementType::Select);
        assert_eq!(statement.row, Some(Row::new("1", "", "").unwrap()));
    }

    #[test]
    fn parse_delete_with_id() {
        let result = prepare_statement("delete 1");
        assert!(result.is_ok());

        let statement = result.unwrap();
        assert_eq!(statement.statement_type, StatementType::Delete);
        assert_eq!(statement.row, Some(Row::new("1", "", "").unwrap()));
    }

    #[test]
    fn error_when_parse_action_with_non_u32_id() {
        let result = prepare_statement("select apple");
        assert!(result.is_err());

        let message = result.unwrap_err();
        assert_eq!(message, "invalid id provided");

        let result = prepare_statement("delete apple");
        assert!(result.is_err());

        let message = result.unwrap_err();
        assert_eq!(message, "invalid id provided");
    }
}
