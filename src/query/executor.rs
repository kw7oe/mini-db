use parking_lot::RwLock;

use super::query_plan::{
    DeletePlanNode, IndexScanPlanNode, PlanNode, SeqScanPlanNode, UpdatePlanNode,
};
use crate::{
    concurrency::{LockManager, RowID, Table, TableIntoIter, Transaction},
    row::Row,
};
use std::sync::Arc;

pub struct ExecutionContext {
    table: Arc<Table>,
    lock_manager: Arc<LockManager>,
    transaction: Arc<RwLock<Transaction>>,
}

impl ExecutionContext {
    pub fn new(
        table: Arc<Table>,
        lock_manager: Arc<LockManager>,
        transaction: Arc<RwLock<Transaction>>,
    ) -> Self {
        Self {
            table,
            lock_manager,
            transaction,
        }
    }
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
            PlanNode::IndexScan(plan_node) => Box::new(IndexScanExecutor::new(
                self.execution_context.clone(),
                plan_node,
            )),
            PlanNode::SeqScan(plan_node) => Box::new(SequenceScanExecutor::new(
                self.execution_context.clone(),
                plan_node,
            )),
            PlanNode::Update(plan_node) => Box::new(UpdateExecutor::new(
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

// Currently our index scan executor only support getting
// 1 row. and index scan by row.id.
pub struct IndexScanExecutor {
    execution_context: Arc<ExecutionContext>,
    plan_node: IndexScanPlanNode,
    ended: bool,
}

impl IndexScanExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan_node: IndexScanPlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            ended: false,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn next(&mut self) -> Option<(RowID, Row)> {
        if self.ended {
            None
        } else {
            let table = &self.execution_context.table;
            let mut t = self.execution_context.transaction.write();
            self.ended = true;

            // Get Row ID first, so we could ask for a lock from the lock manager.
            //
            // We can only get the row after lock manager grant us the lock.
            table
                .get_row_id(self.plan_node.key, &mut t)
                .and_then(|row_id| {
                    // For the simplicity of implementation,
                    // let's always take an exclusive lock.
                    //
                    // Later on, we'll use lock_upgrade to
                    // upgrade our shared lock to exclusive lock
                    // in update/delete exectuor.
                    if !(t.is_shared_lock(&row_id) || t.is_exclusive_lock(&row_id)) {
                        self.execution_context
                            .lock_manager
                            // TODO: We should pass &row_id
                            .lock_shared(&mut t, row_id);
                    }

                    // TODO: we should probably just pass &row_id as well
                    table.get(row_id, &mut t).map(|row| (row_id, row))
                })
        }
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

pub struct UpdateExecutor {
    execution_context: Arc<ExecutionContext>,
    plan_node: UpdatePlanNode,
    affected_row: usize,
    iter: Option<Box<dyn Executor>>,
}

impl UpdateExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan_node: UpdatePlanNode) -> Self {
        Self {
            plan_node,
            execution_context: ctx,
            affected_row: 0,
            iter: None,
        }
    }
}

impl Executor for UpdateExecutor {
    fn next(&mut self) -> Option<(RowID, Row)> {
        if self.iter.is_none() {
            match self.plan_node.child.as_ref() {
                PlanNode::IndexScan(plan_node) => {
                    self.iter = Some(Box::new(IndexScanExecutor::new(
                        self.execution_context.clone(),
                        plan_node.clone(),
                    )));
                }
                PlanNode::SeqScan(plan_node) => {
                    self.iter = Some(Box::new(SequenceScanExecutor::new(
                        self.execution_context.clone(),
                        plan_node.clone(),
                    )));
                }
                _ => panic!("unsupported plan node for child"),
            }
        }

        let executor = self.iter.as_mut().unwrap();

        if let Some((rid, row)) = executor.next() {
            let mut t = self.execution_context.transaction.write();
            self.execution_context.table.update(
                &row,
                &self.plan_node.new_row,
                &self.plan_node.columns,
                &rid,
                &mut t,
            );
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
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
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
    fn index_scan_executor() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
            transaction,
        });
        let execution_engine = ExecutionEngine::new(ctx);

        let plan_node = IndexScanPlanNode { key: 15 };
        let result = execution_engine.execute(PlanNode::IndexScan(plan_node));
        assert_eq!(result.len(), 1);
        let (_, row) = &result[0];
        assert_eq!(row.id, 15);

        cleanup_table();
    }

    #[test]
    fn seq_scan_executor() {
        // Okay, this is just sample, we would need to implement
        // expression evaluation for it to work.
        let predicate = "name = 'user2'".to_string();
        let plan_node = SeqScanPlanNode { predicate };
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
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
    fn delete_executor_with_seq_scan() {
        let predicate = "".to_string();
        let seq_plan_node = SeqScanPlanNode {
            predicate: predicate.clone(),
        };
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
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

    #[test]
    fn update_executor_with_seq_scan() {
        let predicate = "".to_string();
        let seq_plan_node = SeqScanPlanNode {
            predicate: predicate.clone(),
        };
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
            transaction,
        });

        let new_row = Row::new("0", "user1", "email").unwrap();
        let columns = vec!["username".to_string()];
        let plan_node = UpdatePlanNode {
            child: Box::new(PlanNode::SeqScan(seq_plan_node)),
            new_row,
            columns,
        };
        let mut executor = UpdateExecutor::new(ctx.clone(), plan_node);

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
        while let Some((_, row)) = executor.next() {
            assert_eq!(row.username(), "user1");
            assert!(row.id != 0);
        }

        cleanup_table();
    }

    #[test]
    fn update_executor_with_index_scan() {
        let lm = Arc::new(LockManager::new());
        let tm = TransactionManager::new(lm.clone());
        let table = setup_table(&tm, lm.clone());
        let transaction = tm.begin(IsolationLevel::ReadCommited);

        let ctx = Arc::new(ExecutionContext {
            table: Arc::new(table),
            lock_manager: lm.clone(),
            transaction,
        });
        let execution_engine = ExecutionEngine::new(ctx);

        let child_plan_node = IndexScanPlanNode { key: 15 };
        let update_plan_node = UpdatePlanNode {
            child: Box::new(PlanNode::IndexScan(child_plan_node.clone())),
            columns: vec!["email".to_string()],
            new_row: Row::new("0", "0", "new@email.com").unwrap(),
        };

        let result = execution_engine.execute(PlanNode::Update(update_plan_node));
        assert_eq!(result.len(), 1);
        let (_, row) = &result[0];
        assert_eq!(row.id, 15);
        // We can't assert email here since, our current implementation doesn't return
        // the updated row.

        let result = execution_engine.execute(PlanNode::IndexScan(child_plan_node));
        assert_eq!(result.len(), 1);
        let (_, row) = &result[0];
        assert_eq!(row.id, 15);
        assert_eq!(row.email(), "new@email.com");

        cleanup_table();
    }

    fn setup_table(tm: &TransactionManager, lm: Arc<LockManager>) -> Table {
        let table = Table::new(format!("test-{:?}.db", std::thread::current().id()), 4, lm);
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
