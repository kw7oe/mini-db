use crate::table::{Cursor, Row, ROW_SIZE};
use crate::BigArray;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(into = "u8", from = "u8")]
pub enum NodeType {
    Internal,
    Leaf,
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> u8 {
        value as u8
    }
}

impl From<u8> for NodeType {
    fn from(value: u8) -> NodeType {
        match value {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => unreachable!(),
        }
    }
}

const MAX_NODE_SIZE: usize = 4096;
pub const COMMON_NODE_HEADER_SIZE: usize =
    std::mem::size_of::<NodeType>() + std::mem::size_of::<bool>() + std::mem::size_of::<u32>();

pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + std::mem::size_of::<u32>();
const LEAF_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - LEAF_NODE_HEADER_SIZE;

const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// We have to define a custom type in order to have a  define
// serde attributes in Vec<T>.
//
// See: https://github.com/serde-rs/serde/issues/723
#[derive(Serialize, Deserialize, Debug)]
pub struct Cell(#[serde(with = "BigArray")] [u8; LEAF_NODE_CELL_SIZE]);

impl Cell {
    fn key(&self) -> u32 {
        let key_bytes = &self.0[0..4];
        bincode::deserialize(&key_bytes).unwrap()
    }

    pub fn value(&mut self) -> &[u8] {
        let offset = LEAF_NODE_KEY_SIZE;
        &self.0[offset..offset + LEAF_NODE_VALUE_SIZE]
    }

    fn write_key(&mut self, key: u32) {
        let mut j = 0;
        for i in key.to_le_bytes() {
            self.0[j] = i;
            j += 1;
        }
    }

    fn write_value(&mut self, row: &Row) {
        let offset = LEAF_NODE_KEY_SIZE;
        let row_in_bytes = bincode::serialize(row).unwrap();

        for i in 0..ROW_SIZE {
            self.0[offset + i] = row_in_bytes[i];
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    // Header
    // Common
    pub node_type: NodeType,
    pub is_root: bool,
    parent_offset: u32,

    // Leaf
    pub num_of_cells: u32,
    // Body
    pub cells: Vec<Cell>,
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

    pub fn set_header(&mut self, bytes: &[u8]) {
        let node_type_bytes = [bytes[0]];
        self.node_type = bincode::deserialize(&node_type_bytes).unwrap();

        let is_root_bytes = [bytes[1]];
        self.is_root = bincode::deserialize(&is_root_bytes).unwrap();

        let parent_offset_bytes = &bytes[2..6];
        self.parent_offset = bincode::deserialize(parent_offset_bytes).unwrap();

        let num_of_cells_bytes = &bytes[6..10];
        self.num_of_cells = bincode::deserialize(num_of_cells_bytes).unwrap();
    }

    pub fn set_cells(&mut self, cell_bytes: &[u8]) {
        // The reason we can't use bincode to directly deserialize our bytes
        // into Vec<Cell> is because Vec<Cell> binary format will includes a
        // u64 (8 bytes) column that (seems like) representing the length of the
        // Vec<Cell>.
        //
        // However, when we persist a cell to disk, we are only writing Cell
        // binary format and we are tracking our own number of cells by using a
        // separate column with a u32 (4 bytes) data type.
        //
        // Hence, to deserialize with bincode directly into Vec<Cell>,  we append
        // the num_of_cells as 8 bytes into our bytes first.
        let max_size = self.num_of_cells as usize * LEAF_NODE_CELL_SIZE;
        let mut bytes = (self.num_of_cells as u64).to_le_bytes().to_vec();
        let cell_bytes = &mut cell_bytes[0..max_size].to_vec();
        bytes.append(cell_bytes);
        self.cells = bincode::deserialize_from(&bytes[..]).unwrap();

        // Alternatively, we could just deserialize a cell bytes into Cell
        // and insert into Vec<Cell> manually.
        // for i in 0..self.num_of_cells as usize {
        //     let offset = i * LEAF_NODE_CELL_SIZE;
        //     let cell = bincode::deserialize(&bytes[offset..offset + LEAF_NODE_CELL_SIZE]).unwrap();
        //     self.cells.insert(i, cell);
        // }
    }

    pub fn header(&self) -> [u8; LEAF_NODE_HEADER_SIZE] {
        let mut result = [0; LEAF_NODE_HEADER_SIZE];
        let bytes = bincode::serialize(self).unwrap();

        for i in 0..LEAF_NODE_HEADER_SIZE {
            result[i] = bytes[i];
        }

        result
    }

    pub fn cells(&self, cell_num: usize) -> &[u8] {
        &self.cells[cell_num].0
    }

    pub fn search(&self, key: u32) -> Result<usize, usize> {
        self.cells.binary_search_by(|cell| cell.key().cmp(&key))
    }

    pub fn get(&mut self, cell_num: usize) -> Row {
        let bytes = self.cells[cell_num].value();
        bincode::deserialize(bytes).unwrap()
    }

    pub fn insert(&mut self, row: &Row, cursor: &Cursor) {
        let num_of_cells = self.num_of_cells as usize;
        if num_of_cells >= LEAF_NODE_MAX_CELLS {
            println!("Need to implement split leaf node");
            return;
        }

        // Make room for new cell.
        //
        // Else, it will be the current cell at cell_num will be override by
        // new cell.
        if cursor.cell_num < num_of_cells {
            self.cells
                .insert(cursor.cell_num, Cell([0; LEAF_NODE_CELL_SIZE]));
        }

        if self.cells.get(cursor.cell_num).is_none() {
            self.cells
                .insert(cursor.cell_num, Cell([0; LEAF_NODE_CELL_SIZE]))
        }

        self.num_of_cells += 1;
        self.cells[cursor.cell_num].write_key(row.id);
        self.cells[cursor.cell_num].write_value(row);
    }

    pub fn print(&self, indentation_level: usize) {
        if self.node_type == NodeType::Leaf {
            indent(indentation_level);
            println!("- leaf (size {})", self.num_of_cells);
            for c in &self.cells {
                indent(indentation_level + 1);
                println!("- {}", c.key());
            }
        }

        if self.node_type == NodeType::Internal {
            indent(indentation_level);
            println!("- internal (size {})", self.num_of_cells);

            // for c in &self.cells {
            //     let child = get_child(&c);
            //     child.print(indentation_level + 1);

            //     indent(indentation_level + 1);
            //     println!("- key {}", get_key(&c));
            // }

            // let child = self.right_child();
            // child.print(indentation_level + 1);
        }
    }
}

pub fn indent(level: usize) {
    for _ in 0..level {
        print!("  ");
    }
}

#[cfg(test)]
mod test {}
