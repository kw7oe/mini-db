use crate::table::{Cursor, Row, ROW_SIZE};
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
pub const LEAF_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - LEAF_NODE_HEADER_SIZE;

const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
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
    cells: [u8; LEAF_NODE_SPACE_FOR_CELLS],
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
            cells: [0; LEAF_NODE_SPACE_FOR_CELLS],
        }
    }

    fn write_key(&mut self, key: u32, cell_num: usize) {
        let offset = cell_num * LEAF_NODE_CELL_SIZE;

        let mut j = 0;
        for i in key.to_le_bytes() {
            self.cells[offset + j] = i;
            j += 1;
        }
    }

    fn read_value(&mut self, cell_num: usize) -> Row {
        let offset = cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        bincode::deserialize(&self.cells[offset..offset + LEAF_NODE_VALUE_SIZE]).unwrap()
    }

    fn write_value(&mut self, row: &Row, cell_num: usize) {
        let offset = cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        println!("cell_num: {cell_num}, offset: {offset}");
        let row_in_bytes = bincode::serialize(row).unwrap();

        for i in 0..ROW_SIZE {
            self.cells[offset + i] = row_in_bytes[i];
        }
    }

    fn insert(&mut self, row: &Row, cursor: &Cursor) {
        if self.num_of_cells as usize >= LEAF_NODE_MAX_CELLS {
            println!("Need to implement split leaf node");
            return;
        } else {
            self.num_of_cells += 1;
            self.write_key(row.id, cursor.cell_num);
            self.write_value(row, cursor.cell_num);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::table::Table;

    #[test]
    fn test() {
        print_constant();
        let mut node = Node::new(true, NodeType::Leaf);
        let bytes = bincode::serialize(&node).unwrap();
        println!("{:?}", bytes);

        let table = Table::new("test.db");
        let row = Row::from_statement("insert 1 john john@email.com").unwrap();
        let cursor = Cursor::table_end(&table);
        node.insert(&row, &cursor);
        println!("{:?}", node);

        let row = node.read_value(cursor.cell_num);
        println!("{:?}", row);
    }
}
