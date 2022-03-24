use crate::row::{Row, ROW_SIZE};
use crate::table::Cursor;
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

pub const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + std::mem::size_of::<u32>() + std::mem::size_of::<u32>();
const LEAF_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - LEAF_NODE_HEADER_SIZE;

const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
pub const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
pub const LEAF_NODE_LEFT_SPLIT_COUNT: usize =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

pub const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE;
pub const INTERNAL_NODE_CELL_SIZE: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u32>();

// We have to define a custom type in order to have a  define
// serde attributes in Vec<T>.
//
// See: https://github.com/serde-rs/serde/issues/723
#[derive(Serialize, Deserialize)]
pub struct Cell(#[serde(with = "BigArray")] [u8; LEAF_NODE_CELL_SIZE]);

#[derive(Serialize, Deserialize)]
pub struct InternalCell([u8; INTERNAL_NODE_CELL_SIZE]);

impl Cell {
    pub fn key(&self) -> u32 {
        let key_bytes = &self.0[0..4];
        bincode::deserialize(&key_bytes).unwrap()
    }

    pub fn value(&self) -> &[u8] {
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

impl std::fmt::Debug for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key())
    }
}

impl InternalCell {
    pub fn new(pointer: u32, key: u32) -> Self {
        let mut cell = Self([0; INTERNAL_NODE_CELL_SIZE]);
        cell.write_child_pointer(pointer);
        cell.write_key(key);
        cell
    }

    pub fn child_pointer(&self) -> u32 {
        let bytes = &self.0[0..4];
        bincode::deserialize(&bytes).unwrap()
    }

    fn write_child_pointer(&mut self, pointer: u32) {
        let mut j = 0;
        for i in pointer.to_le_bytes() {
            self.0[j] = i;
            j += 1;
        }
    }

    pub fn key(&self) -> u32 {
        let bytes = &self.0[4..8];
        bincode::deserialize(&bytes).unwrap()
    }

    fn write_key(&mut self, key: u32) {
        let mut j = 4;
        for i in key.to_le_bytes() {
            self.0[j] = i;
            j += 1;
        }
    }
}

impl std::fmt::Debug for InternalCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "InternalCell(key: {}, child_pointer: {})",
            self.key(),
            self.child_pointer()
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    // Header
    // Common
    pub node_type: NodeType,
    pub is_root: bool,
    pub parent_offset: u32,

    // Leaf
    pub num_of_cells: u32,

    // Internal
    pub right_child_offset: u32,
    pub next_leaf_offset: u32,

    // Body
    pub cells: Vec<Cell>,
    pub internal_cells: Vec<InternalCell>,

    pub has_initialize: bool,
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
    pub fn new(is_root: bool, node_type: NodeType) -> Self {
        Node {
            node_type,
            is_root,
            parent_offset: 0,
            right_child_offset: 0,
            next_leaf_offset: 0,
            num_of_cells: 0,
            has_initialize: true,
            cells: Vec::new(),
            internal_cells: Vec::new(),
        }
    }

    pub fn unintialize() -> Self {
        let mut node = Self::new(true, NodeType::Leaf);
        node.has_initialize = false;
        node
    }

    pub fn from_bytes(&mut self, bytes: &[u8]) {
        self.set_common_header(&bytes[0..COMMON_NODE_HEADER_SIZE]);

        if self.node_type == NodeType::Leaf {
            println!("--- form leaf node from bytes");
            self.set_leaf_header(&bytes[COMMON_NODE_HEADER_SIZE..LEAF_NODE_HEADER_SIZE]);
            self.set_leaf_cells(&bytes[LEAF_NODE_HEADER_SIZE..]);
        }

        if self.node_type == NodeType::Internal {
            println!("--- form internal node from bytes");
            self.set_internal_header(&bytes[COMMON_NODE_HEADER_SIZE..INTERNAL_NODE_HEADER_SIZE]);
            self.set_internal_cells(&bytes[INTERNAL_NODE_HEADER_SIZE..]);
        }
    }

    pub fn set_common_header(&mut self, bytes: &[u8]) {
        let node_type_bytes = [bytes[0]];
        self.node_type = bincode::deserialize(&node_type_bytes).unwrap();

        let is_root_bytes = [bytes[1]];
        self.is_root = bincode::deserialize(&is_root_bytes).unwrap();

        let parent_offset_bytes = &bytes[2..6];
        self.parent_offset = bincode::deserialize(parent_offset_bytes).unwrap();
    }

    pub fn set_leaf_header(&mut self, bytes: &[u8]) {
        let num_of_cells_bytes = &bytes[0..4];
        self.num_of_cells = bincode::deserialize(num_of_cells_bytes).unwrap();

        let next_leaf_offset_bytes = &bytes[4..8];
        self.next_leaf_offset = bincode::deserialize(next_leaf_offset_bytes).unwrap();
    }

    pub fn set_internal_header(&mut self, bytes: &[u8]) {
        let num_of_cells_bytes = &bytes[0..4];
        self.num_of_cells = bincode::deserialize(num_of_cells_bytes).unwrap();

        let right_child_offset_bytes = &bytes[4..8];
        self.right_child_offset = bincode::deserialize(right_child_offset_bytes).unwrap();
    }

    pub fn set_leaf_cells(&mut self, cell_bytes: &[u8]) {
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

    pub fn set_internal_cells(&mut self, cell_bytes: &[u8]) {
        let max_size = self.num_of_cells as usize * INTERNAL_NODE_CELL_SIZE;
        let mut bytes = (self.num_of_cells as u64).to_le_bytes().to_vec();
        let cell_bytes = &mut cell_bytes[0..max_size].to_vec();
        bytes.append(cell_bytes);
        self.internal_cells = bincode::deserialize_from(&bytes[..]).unwrap();
    }

    pub fn header(&self) -> Vec<u8> {
        let mut result = Vec::new();

        if self.node_type == NodeType::Leaf {
            let bytes = bincode::serialize(self).unwrap();

            for i in 0..COMMON_NODE_HEADER_SIZE {
                result.insert(i, bytes[i]);
            }

            let num_of_cells_bytes = bincode::serialize(&self.num_of_cells).unwrap();
            for byte in num_of_cells_bytes {
                result.push(byte);
            }

            let next_leaf_offset_bytes = bincode::serialize(&self.next_leaf_offset).unwrap();
            for byte in next_leaf_offset_bytes {
                result.push(byte);
            }
        } else {
            let bytes = bincode::serialize(self).unwrap();

            for i in 0..INTERNAL_NODE_HEADER_SIZE {
                result.insert(i, bytes[i]);
            }
        }

        result
    }

    pub fn cells(&self, cell_num: usize) -> &[u8] {
        if self.node_type == NodeType::Leaf {
            &self.cells[cell_num].0
        } else {
            &self.internal_cells[cell_num].0
        }
    }

    pub fn get_max_key(&self) -> u32 {
        match self.node_type {
            NodeType::Leaf => {
                let cell = &self.cells[self.num_of_cells as usize - 1];
                cell.key()
            }
            NodeType::Internal => {
                let internal_cell = &self.internal_cells[self.num_of_cells as usize - 1];
                internal_cell.key()
            }
        }
    }

    pub fn search(&self, key: u32) -> Result<usize, usize> {
        if self.node_type == NodeType::Leaf {
            return self.cells.binary_search_by(|cell| cell.key().cmp(&key));
        }

        let index = match self
            .internal_cells
            .binary_search_by(|cell| cell.key().cmp(&key))
        {
            Ok(index) => index,
            Err(index) => index,
        };

        let child_pointer = if index == self.num_of_cells as usize {
            self.right_child_offset
        } else {
            self.internal_cells[index].child_pointer()
        };

        Ok(child_pointer as usize)
    }

    pub fn get(&mut self, cell_num: usize) -> Row {
        let bytes = self.cells[cell_num].value();
        bincode::deserialize(bytes).unwrap()
    }

    pub fn insert(&mut self, row: &Row, cursor: &Cursor) {
        let num_of_cells = self.num_of_cells as usize;

        // Make room for new cell.
        //
        // Else, the current cell at cell_num will be override by
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

    pub fn internal_insert(&mut self, index: usize, cell: InternalCell) {
        self.internal_cells.insert(index, cell);
    }

    pub fn internal_search(&self, key: u32) -> usize {
        match self.internal_cells.binary_search_by(|c| c.key().cmp(&key)) {
            Ok(index) => index,
            Err(index) => index,
        }
    }

    pub fn update_internal_key(&mut self, old_key: u32, new_key: u32) {
        let index = self.internal_search(old_key);
        if index < self.internal_cells.len() {
            self.internal_cells[index].write_key(new_key);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        print_constant();
    }
}
