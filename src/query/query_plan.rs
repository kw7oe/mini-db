use crate::row::Row;

pub enum PlanNode {
    SeqScan(SeqScanPlanNode),
    IndexScan(IndexScanPlanNode),
    Insert(InsertPlanNode),
    Update(UpdatePlanNode),
    Delete(DeletePlanNode),
}

#[derive(Clone)]
pub struct SeqScanPlanNode {
    pub predicate: String,
}

#[derive(Clone)]
pub struct IndexScanPlanNode {
    pub predicate: String,
}

#[derive(Clone)]
pub struct InsertPlanNode {
    pub row: Row,
}

// Currently, we are hardcoding both
// delete and update node to use sequence scan
// to walk through all of our records.
//
// We need to make the child type Generic or Trait
// so it could use different access methods to
// retrive the affected rows.
#[derive(Clone)]
pub struct UpdatePlanNode {
    pub child: SeqScanPlanNode,
    pub new_row: Row,
}

#[derive(Clone)]
pub struct DeletePlanNode {
    pub child: SeqScanPlanNode,
}
