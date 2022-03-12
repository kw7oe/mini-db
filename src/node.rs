use crate::table::{Row, ROW_SIZE};
use crate::BigArray;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[serde(into = "u8")]
pub enum NodeType {
    Internal,
    Leaf,
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> u8 {
        value as u8
    }
}

const MAX_NODE_SIZE: usize = 4096;
const COMMON_NODE_HEADER_SIZE: usize =
    std::mem::size_of::<NodeType>() + std::mem::size_of::<bool>() + std::mem::size_of::<u32>();

const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + std::mem::size_of::<u32>();
const LEAF_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - LEAF_NODE_HEADER_SIZE;

const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    // Header
    // Common
    node_type: NodeType,
    is_root: bool,
    parent_offset: u32,

    // Leaf
    num_of_cells: u32,
    // Body
    #[serde(with = "BigArray")]
    cells: [u8; LEAF_NODE_CELL_SIZE],
}

pub fn print_constant() {
    println!(
        "
    ROW_SIZE: {ROW_SIZE},
    COMMON_NODE_HEADER_SIZE: {COMMON_NODE_HEADER_SIZE},
    LEAF_NODE_HEADER_SIZE: {LEAF_NODE_HEADER_SIZE},
    LEAF_NODE_CELL_SIZE: {LEAF_NODE_CELL_SIZE},
    LEAF_NODE_SPACE_FOR_CELLS: {LEAF_NODE_SPACE_FOR_CELLS},
    LEAF_NODE_MAX_CELLS: {LEAF_NODE_MAX_CELLS},

    LEAF_NODE_KEY_SIZE: {LEAF_NODE_KEY_SIZE},
    LEAF_NODE_VALUE_SIZE: {LEAF_NODE_VALUE_SIZE},
    MAX_NODE_SIZE: {MAX_NODE_SIZE},
    "
    );
}

impl Node {
    fn new(is_root: bool, node_type: NodeType) -> Node {
        let num_of_cells = 10;
        Node {
            node_type,
            is_root,
            parent_offset: 0,
            num_of_cells,
            cells: [0; LEAF_NODE_CELL_SIZE],
        }
    }

    fn insert(&mut self, row: Row) {
        if self.num_of_cells as usize >= LEAF_NODE_MAX_CELLS {
            println!("Need to implement split leaf node");
            return;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        print_constant();
        let node = Node::new(true, NodeType::Leaf);
        let bytes = bincode::serialize(&node).unwrap();
        println!("{:?}", node);
        println!("{:?}", bytes);
    }
}
