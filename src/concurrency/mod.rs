mod lock_manager;
mod table;
mod transaction;
mod transaction_manager;

pub use {
    lock_manager::LockManager,
    table::{RowID, Table, TableIntoIter},
    transaction::{IsolationLevel, Transaction},
    transaction_manager::TransactionManager,
};

#[cfg(test)]
mod test {
    use super::lock_manager::LockManager;
    use super::transaction_manager::TransactionManager;
    use super::{IsolationLevel, Table};
    use crate::query::{
        ExecutionContext, ExecutionEngine, IndexScanPlanNode, PlanNode, UpdatePlanNode,
    };
    use crate::row::Row;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn repeatable_read() {
        // A bit of fuzzing.
        for _ in 0..100 {
            // Repeatable read
            //  T1           T2
            // BEGIN
            // R(A) -> 10
            //              BEGIN
            //              R(A) -> 10
            //              W(A) -> =20
            //              COMMIT
            // R(A) -> 20
            // COMMIT
            let lock_manager = Arc::new(LockManager::new());
            let transaction_manager = Arc::new(TransactionManager::new(lock_manager.clone()));
            let table = Arc::new(setup_table(&transaction_manager, lock_manager.clone()));

            // Transaction 1
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle = std::thread::spawn(move || {
                let t1 = tm.begin(IsolationLevel::ReadCommited);
                let ctx1 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t1.clone()));
                let execution_engine = ExecutionEngine::new(ctx1);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });
                let result = execution_engine.execute(index_scan_plan_node.clone());
                let (_rid, row) = &result[0];
                assert_eq!(row.id, 5);
                assert_eq!(row.username(), "user5");

                // Make sure that T2 finish it's read write first before we attempt to read again.
                std::thread::sleep(std::time::Duration::from_millis(15));
                let (_, row) = &execution_engine.execute(index_scan_plan_node)[0];
                assert_eq!(row.id, 5);
                assert_eq!(row.username(), "user5");

                let mut t1 = t1.write();
                tm.commit(&tb, &mut t1);
            });

            // Transaction 2
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle2 = std::thread::spawn(move || {
                let t2 = tm.begin(IsolationLevel::ReadCommited);
                let ctx2 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t2.clone()));
                let execution_engine = ExecutionEngine::new(ctx2);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });
                let update_plan_node = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["username".to_string()],
                    new_row: Row::new("0", "new_name", "").unwrap(),
                });

                // Make sure that T2 start later than T1..
                std::thread::sleep(std::time::Duration::from_millis(10));
                execution_engine.execute(index_scan_plan_node);
                execution_engine.execute(update_plan_node);
                let mut t2 = t2.write();
                tm.commit(&tb, &mut t2);
            });

            handle.join().unwrap();
            handle2.join().unwrap();

            cleanup_table();
        }
    }

    #[test]
    fn dirty_read() {
        // A bit of fuzzing.
        for _ in 0..1 {
            // Dirty reads (Read of uncommited data)
            //  T1            T2
            // BEGIN
            // R(A) -> 10
            // W(A) -> =20
            //
            //               BEGIN
            //               R(A) -> 20
            //               W(A) -> +2 = 22
            //               COMMIT
            // COMMIT
            let lock_manager = Arc::new(LockManager::new());
            let transaction_manager = Arc::new(TransactionManager::new(lock_manager.clone()));
            let table = Arc::new(setup_table(&transaction_manager, lock_manager.clone()));

            // Transaction 1
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle = std::thread::spawn(move || {
                let t1 = tm.begin(IsolationLevel::ReadCommited);
                let ctx1 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t1.clone()));
                let execution_engine = ExecutionEngine::new(ctx1);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });
                let update_plan_node = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["username".to_string()],
                    new_row: Row::new("0", "new_name", "").unwrap(),
                });

                let result = execution_engine.execute(index_scan_plan_node.clone());
                let (_rid, row) = &result[0];
                assert_eq!(row.id, 5);
                assert_eq!(row.username(), "user5");

                execution_engine.execute(update_plan_node);

                let result = execution_engine.execute(index_scan_plan_node);
                let (_rid, row) = &result[0];
                assert_eq!(row.id, 5);
                assert_eq!(row.username(), "new_name");

                // Make sure that T2 finish it's transaction before we abort
                std::thread::sleep(std::time::Duration::from_millis(20));

                let mut t1 = t1.write();
                tm.abort(&tb, &mut t1);
            });

            // Transaction 2
            //
            // By right, T2, will be blocked by T1 if we implemented 2 phase locking correctly.
            // So, this mean that, T2 will keep trying to acquire the lock for the row.
            //
            // So the R(A) will only be executed after T1 abort it transactions.
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle2 = std::thread::spawn(move || {
                let t2 = tm.begin(IsolationLevel::ReadCommited);
                let ctx2 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t2.clone()));
                let execution_engine = ExecutionEngine::new(ctx2);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });

                // Make sure T1 started first
                std::thread::sleep(std::time::Duration::from_millis(10));

                let result = execution_engine.execute(index_scan_plan_node);
                let (_rid, row) = &result[0];
                assert_eq!(row.id, 5);
                assert_eq!(row.username(), "user5");

                let mut t2 = t2.write();
                tm.commit(&tb, &mut t2);
            });

            handle.join().unwrap();
            handle2.join().unwrap();

            cleanup_table();
        }
    }

    #[test]
    fn write_write() {
        // A bit of fuzzing.
        for _ in 0..100 {
            // Overwritign uncommitted data
            //  T1            T2
            // BEGIN
            // W(A) -> 10
            //
            //               BEGIN
            //               W(A) -> 20
            //               W(B) -> Lin
            //               COMMIT
            //
            // W(B) -> And
            // COMMIT
            //
            // Result: 20, And
            // Corrrct result: 10, And | 20, Lin (Depending on which transaction commit last)
            let lock_manager = Arc::new(LockManager::new());
            let transaction_manager = Arc::new(TransactionManager::new(lock_manager.clone()));
            let table = Arc::new(setup_table(&transaction_manager, lock_manager.clone()));

            // Transaction 1
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle = std::thread::spawn(move || {
                let t1 = tm.begin(IsolationLevel::ReadCommited);
                let ctx1 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t1.clone()));
                let execution_engine = ExecutionEngine::new(ctx1);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });
                let update_plan_node_a = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["username".to_string()],
                    new_row: Row::new("0", "t1_name", "").unwrap(),
                });
                let update_plan_node_b = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["email".to_string()],
                    new_row: Row::new("0", "", "t1_email").unwrap(),
                });

                execution_engine.execute(update_plan_node_a);

                // Make sure that T2 finish before we continue
                std::thread::sleep(std::time::Duration::from_millis(20));
                execution_engine.execute(update_plan_node_b);

                let result = execution_engine.execute(index_scan_plan_node);
                let (_, row) = &result[0];
                assert_eq!(row.username(), "t1_name");
                assert_eq!(row.email(), "t1_email");

                let mut t1 = t1.write();
                tm.abort(&tb, &mut t1);
            });

            // Transaction 2
            let tm = transaction_manager.clone();
            let lm = lock_manager.clone();
            let tb = table.clone();
            let handle2 = std::thread::spawn(move || {
                let t2 = tm.begin(IsolationLevel::ReadCommited);
                let ctx2 = Arc::new(ExecutionContext::new(tb.clone(), lm.clone(), t2.clone()));
                let execution_engine = ExecutionEngine::new(ctx2);
                let index_scan_plan_node = PlanNode::IndexScan(IndexScanPlanNode { key: 5 });
                let update_plan_node_a = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["username".to_string()],
                    new_row: Row::new("0", "t2_name", "").unwrap(),
                });
                let update_plan_node_b = PlanNode::Update(UpdatePlanNode {
                    child: Box::new(index_scan_plan_node.clone()),
                    columns: vec!["email".to_string()],
                    new_row: Row::new("0", "", "t2_email").unwrap(),
                });

                // Make sure that T1 start first before continue:
                std::thread::sleep(std::time::Duration::from_millis(10));
                execution_engine.execute(update_plan_node_a);
                execution_engine.execute(update_plan_node_b);
                let result = execution_engine.execute(index_scan_plan_node);
                let (_, row) = &result[0];
                assert_eq!(row.username(), "t2_name");
                assert_eq!(row.email(), "t2_email");

                let mut t2 = t2.write();
                tm.commit(&tb, &mut t2);
            });

            handle.join().unwrap();
            handle2.join().unwrap();

            cleanup_table();
        }
    }

    fn setup_table(tm: &TransactionManager, lm: Arc<LockManager>) -> Table {
        let table = Table::new(format!("test-{:?}.db", std::thread::current().id()), 4, lm);
        let transaction = tm.begin(IsolationLevel::ReadCommited);
        let mut t = transaction.write();
        for i in 1..10 {
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
