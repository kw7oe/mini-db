use parking_lot::RwLock;

use super::query_plan::{DeletePlanNode, PlanNode, SeqScanPlanNode};
use crate::{
    concurrency::{RowID, Table, TableIntoIter, Transaction},
    row::Row,
};
use std::sync::Arc;

pub struct ExecutionContext {
    table: Arc<Table>,
    transaction: Arc<RwLock<Transaction>>,
}

pub struct ExecutionEngine {
    execution_context: Arc<ExecutionContext>,
}

impl ExecutionEngine {
    pub fn new(ctx: Arc<ExecutionContext>) -> Self {
        Self {
            execution_context: ctx,
        }
    }

    pub fn execute(&self, plan_node: PlanNode) -> Vec<(RowID, Row)> {
        let mut result_set = Vec::new();
        let mut executor: Box<dyn Executor> = match plan_node {
            PlanNode::SeqScan(plan_node) => Box::new(SequenceScanExecutor::new(
                self.execution_context.clone(),
                plan_node,
            )),
            PlanNode::Delete(plan_node) => Box::new(DeleteExecutor::new(
                self.execution_context.clone(),
                plan_node,
            )),
            _ => unimplemented!("oops"),
        };

        while let Some(result) = executor.next() {
            result_set.push(result);
        }

        result_set
    }
}

pub trait Executor {
    fn next(&mut self) -> Option<(RowID, Row)>;
}

pub struct SequenceScanExecutor {
    execution_context: Arc<ExecutionContext>,
    plan_node: SeqScanPlanNode,
    iter: Option<TableIntoIter>,
}

impl SequenceScanExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan_node: SeqScanPlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            iter: None,
        }
    }
}

impl Executor for SequenceScanExecutor {
    fn next(&mut self) -> Option<(RowID, Row)> {
        let table = &self.execution_context.table;
        if self.iter.is_none() {
            self.iter = Some(table.iter());
        };

        let iter = self.iter.as_mut().unwrap();
        iter.next()
    }
}

pub struct DeleteExecutor {
    execution_context: Arc<ExecutionContext>,
    plan_node: DeletePlanNode,
    affected_row: usize,
    iter: Option<SequenceScanExecutor>,
}

impl DeleteExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan_node: DeletePlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            affected_row: 0,
            iter: None,
        }
    }
}

impl Executor for DeleteExecutor {
    fn next(&mut self) -> Option<(RowID, Row)> {
        if self.iter.is_none() {
            self.iter = Some(SequenceScanExecutor::new(
                self.execution_context.clone(),
                self.plan_node.child.clone(),
            ));
        }

        let executor = self.iter.as_mut().unwrap();

        if let Some((rid, row)) = executor.next() {
            let mut t = self.execution_context.transaction.write();
            self.execution_context.table.delete(&row, &rid, &mut t);
            drop(t);
            self.affected_row += 1;
            Some((rid, row))
        } else {
            None
        }
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
    fn execution_engine() {
        let plan_node = SeqScanPlanNode {
            predicate: "".to_string(),
        };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            transaction,
        });

        let execution_engine = ExecutionEngine::new(ctx);
        let result = execution_engine.execute(PlanNode::SeqScan(plan_node));
        assert_eq!(result.len(), 49);
        let mut id = 1;

        for (_, row) in result {
            assert_eq!(row.id, id);
            id += 1;
        }

        cleanup_table();
    }

    #[test]
    fn seq_scan_executor() {
        // Okay, this is just sample, we would need to implement
        // expression evaluation for it to work.
        let predicate = "name = 'user2'".to_string();
        let plan_node = SeqScanPlanNode { predicate };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            transaction,
        });

        let mut executor = SequenceScanExecutor::new(ctx, plan_node);

        let mut id = 1;
        while let Some((_rid, row)) = executor.next() {
            assert_eq!(row.id, id);
            id += 1;
        }

        cleanup_table();
    }

    #[test]
    fn delete_executor() {
        let predicate = "".to_string();
        let seq_plan_node = SeqScanPlanNode {
            predicate: predicate.clone(),
        };
        let tm = TransactionManager::new();
        let table = setup_table(&tm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            transaction,
        });

        let plan_node = DeletePlanNode {
            child: seq_plan_node,
        };
        let mut executor = DeleteExecutor::new(ctx.clone(), plan_node);

        let mut count = 0;
        while executor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 49);

        let mut t = ctx.transaction.write();
        tm.commit(&ctx.table, &mut t);
        drop(t);

        let seq_plan_node = SeqScanPlanNode { predicate };
        let mut executor = SequenceScanExecutor::new(ctx, seq_plan_node);
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
