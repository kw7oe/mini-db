#[derive(Clone)]
pub struct SeqScanPlanNode {
    pub predicate: String,
}

#[derive(Clone)]
pub struct DeletePlanNode {
    pub child: SeqScanPlanNode,
}
