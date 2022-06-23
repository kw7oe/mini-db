use parking_lot::RwLockWriteGuard;

use super::query_plan::SeqScanPlanNode;
use crate::{
    concurrency::{RowID, Table, Transaction},
    row::Row,
};

pub struct ExecutionContext<'a> {
    table: &'a Table,
    transaction: RwLockWriteGuard<'a, Transaction>,
}

pub struct SequenceScanExecutor<'a> {
    execution_context: ExecutionContext<'a>,
    plan_node: SeqScanPlanNode,
}

impl<'a> SequenceScanExecutor<'a> {
    pub fn new(ctx: ExecutionContext<'a>, plan_node: SeqScanPlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
        }
    }

    pub fn next(&mut self) -> Option<(RowID, String)> {
        // TODO: implement iterator in Table.rs
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        concurrency::{IsolationLevel, TransactionManager},
        query::query_plan::SeqScanPlanNode,
    };
    use std::str::FromStr;

    #[test]
    fn test() {
        let predicate = "name = 'user2'".to_string();
        let plan_node = SeqScanPlanNode { predicate };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = ExecutionContext {
            table: &table,
            transaction: transaction.write(),
        };

        let mut executor = SequenceScanExecutor::new(ctx, plan_node);
        while let Some(t) = executor.next() {
            println!("{:?}", t);
        }

        cleanup_table();
    }

    fn setup_table(tm: &TransactionManager) -> Table {
        let table = Table::new(format!("test-{:?}.db", std::thread::current().id()), 4);
        // let transaction = tm.begin(IsolationLevel::ReadCommited);
        // let mut t = transaction.write();
        // for i in 1..50 {
        //     let row = Row::from_str(&format!("{i} user{i} user{i}@email.com")).unwrap();
        //     table.insert(&row, &mut t);
        // }
        // tm.commit(&table, &mut t);

        table
    }

    fn cleanup_table() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }
}
