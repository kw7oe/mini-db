pub struct SeqScanPlanNode {
    pub predicate: String,
}

pub struct DeletePlanNode {
    pub child: SeqScanPlanNode
}
