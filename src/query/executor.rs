use parking_lot::RwLockWriteGuard;

use super::query_plan::{SeqScanPlanNode, DeletePlanNode};
use crate::{
    concurrency::{RowID, Table, Transaction, TableIntoIter},
    row::Row,
};

pub struct ExecutionContext<'a> {
    table: &'a Table,
    transaction: RwLockWriteGuard<'a, Transaction>,
}

pub struct SequenceScanExecutor<'a> {
    execution_context: &'a ExecutionContext<'a>,
    plan_node: &'a SeqScanPlanNode,
    iter: Option<TableIntoIter<'a>>
}

impl<'a> SequenceScanExecutor<'a> {
    pub fn new(ctx: &'a ExecutionContext<'a>, plan_node: &'a SeqScanPlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            iter: None
        }
    }

    pub fn next(&mut self) -> Option<(RowID, Row)> {
        let table = self.execution_context.table;
        if self.iter.is_none() {
            self.iter =  Some(table.iter());
        };

        let iter = self.iter.as_mut().unwrap();
        iter.next()
    }
}

pub struct DeleteExecutor<'a> {
    execution_context: &'a mut ExecutionContext<'a>,
    plan_node: &'a DeletePlanNode,
    affected_row: usize,
}

impl<'a> DeleteExecutor<'a> {
    pub fn new(ctx: &'a mut ExecutionContext<'a>, plan_node: &'a DeletePlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            affected_row: 0
        }
    }

    pub fn next(&mut self) -> Option<usize> {
        let mut executor = SequenceScanExecutor::new(&self.execution_context, &self.plan_node.child);
        while let Some((rid, row)) = executor.next() {
            self.execution_context.table.delete(&row, &rid, &mut self.execution_context.transaction);
            self.affected_row += 1;
        }

        Some(self.affected_row)
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
    fn seq_scan() {
        // Okay, this is just sample, we would need to implement
        // expression evaluation for it to work.
        let predicate = "name = 'user2'".to_string();
        let plan_node = SeqScanPlanNode { predicate };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = ExecutionContext {
            table: &table,
            transaction: transaction.write(),
        };

        let mut executor = SequenceScanExecutor::new(&ctx, &plan_node);

        let mut id = 1;
        while let Some((_rid, row)) = executor.next() {
            assert_eq!(row.id, id);
            id += 1;
        }

        cleanup_table();
    }

    #[test]
    fn delete() {
        let predicate = "".to_string();
        let seq_plan_node = SeqScanPlanNode { predicate };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = ExecutionContext {
            table: &table,
            transaction: transaction.write(),
        };

        let plan_node = DeletePlanNode { child: seq_plan_node };
        let mut executor = DeleteExecutor::new(&ctx, plan_node);

        let mut count = 0;
        while let Some(affected_row) = executor.next() {
            assert_eq!(count, affected_row);
            count += 1;
        }

        let seq_plan_node = SeqScanPlanNode { predicate };
        let mut executor = SequenceScanExecutor::new(&ctx, &seq_plan_node);
        assert!(executor.next().is_none());

        cleanup_table();
    }

    fn setup_table(tm: &TransactionManager) -> Table {
        let table = Table::new(format!("test-{:?}.db", std::thread::current().id()), 4);
        let transaction = tm.begin(IsolationLevel::ReadCommited);
        let mut t = transaction.write();
        for i in 1..50 {
            let row = Row::from_str(&format!("{i} user{i} user{i}@email.com")).unwrap();
            table.insert(&row, &mut t);
        }
        tm.commit(&table, &mut t);

        table
    }

    fn cleanup_table() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }
}
