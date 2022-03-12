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
const LEAF_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - LEAF_NODE_HEADER_SIZE;

const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// We have to define a custom type in order to have a  define
// serde attributes in Vec<T>.
//
// See: https://github.com/serde-rs/serde/issues/723
#[derive(Serialize, Deserialize, Debug)]
struct Cell(#[serde(with = "BigArray")] [u8; LEAF_NODE_CELL_SIZE]);

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    // Header
    // Common
    node_type: NodeType,
    is_root: bool,
    parent_offset: u32,

    // Leaf
    pub num_of_cells: u32,
    // Body
    cells: Vec<Cell>,
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
    pub fn new(is_root: bool, node_type: NodeType) -> Node {
        Node {
            node_type,
            is_root,
            parent_offset: 0,
            num_of_cells: 0,
            cells: Vec::new(),
        }
    }

    fn write_key(&mut self, key: u32, cell_num: usize) {
        if self.cells.get(cell_num).is_none() {
            self.cells.insert(cell_num, Cell([0; LEAF_NODE_CELL_SIZE]))
        }

        let mut j = 0;
        for i in key.to_le_bytes() {
            self.cells[cell_num].0[j] = i;
            j += 1;
        }
    }

    fn write_value(&mut self, row: &Row, cell_num: usize) {
        let offset = LEAF_NODE_KEY_SIZE;
        let row_in_bytes = bincode::serialize(row).unwrap();

        for i in 0..ROW_SIZE {
            self.cells[cell_num].0[offset + i] = row_in_bytes[i];
        }
    }

    pub fn read_value(&mut self, cell_num: usize) -> &[u8] {
        let offset = LEAF_NODE_KEY_SIZE;
        &self.cells[cell_num].0[offset..offset + LEAF_NODE_VALUE_SIZE]
    }

    pub fn get(&mut self, cell_num: usize) -> Row {
        let bytes = self.read_value(cell_num);
        bincode::deserialize(bytes).unwrap()
    }

    pub fn insert(&mut self, row: &Row, cursor: &Cursor) {
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

        let mut table = Table::new("test.db");
        let row = Row::from_statement("insert 1 john john@email.com").unwrap();
        let cursor = Cursor::table_end(&mut table);
        node.insert(&row, &cursor);
        println!("{:?}", node);

        let row = node.read_value(cursor.cell_num);
        println!("{:?}", row);
    }
}
