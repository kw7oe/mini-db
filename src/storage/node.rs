use super::page::PAGE_HEADER_BYTES;
use super::{Cursor, PAGE_SIZE};
use crate::row::{Row, ROW_SIZE};
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

const MAX_NODE_SIZE: usize = PAGE_SIZE - PAGE_HEADER_BYTES;
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
// const INTERNAL_NODE_SPACE_FOR_CELLS: usize = MAX_NODE_SIZE - INTERNAL_NODE_HEADER_SIZE;
// pub const INTERNAL_NODE_MAX_CELLS: usize = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

// Hardcoded to 3 for testing
pub const INTERNAL_NODE_MAX_CELLS: usize = 3;

// We have to define a custom type in order to have a  define
// serde attributes in Vec<T>.
//
// See: https://github.com/serde-rs/serde/issues/723
#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct Cell(#[serde(with = "BigArray")] [u8; LEAF_NODE_CELL_SIZE]);

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct InternalCell([u8; INTERNAL_NODE_CELL_SIZE]);

impl Cell {
    pub fn key(&self) -> u32 {
        let key_bytes = &self.0[0..4];
        bincode::deserialize(key_bytes).unwrap()
    }

    pub fn value(&self) -> &[u8] {
        let offset = LEAF_NODE_KEY_SIZE;
        &self.0[offset..offset + LEAF_NODE_VALUE_SIZE]
    }

    fn write_key(&mut self, key: u32) {
        for (i, byte) in key.to_le_bytes().into_iter().enumerate() {
            self.0[i] = byte;
        }
    }

    pub fn mark_as_deleted(&mut self) {
        let offset = LEAF_NODE_KEY_SIZE;
        self.0[offset + ROW_SIZE - 1] = 1;
    }

    pub fn mark_as_undeleted(&mut self) {
        let offset = LEAF_NODE_KEY_SIZE;
        self.0[offset + ROW_SIZE - 1] = 0;
    }

    // TRADEOFF: We are a clustered table.
    //
    // Where our rows is not stored in a separate heap file but together
    // with the B+ Tree file.
    pub fn write_value(&mut self, row: &Row) {
        let offset = LEAF_NODE_KEY_SIZE;
        let row_in_bytes = bincode::serialize(row).unwrap();

        self.0[offset..(ROW_SIZE + offset)].clone_from_slice(&row_in_bytes[..ROW_SIZE]);
    }

    pub fn update(&mut self, columns: &Vec<String>, new_row: &Row) {
        let offset = LEAF_NODE_KEY_SIZE;
        let row_in_bytes = &self.0[offset..(ROW_SIZE + offset)];
        let mut row: Row = bincode::deserialize(row_in_bytes).unwrap();

        for column in columns {
            row.update(column, new_row);
        }

        self.write_value(&row);
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
        bincode::deserialize(bytes).unwrap()
    }

    pub fn write_child_pointer(&mut self, pointer: u32) {
        for (i, byte) in pointer.to_le_bytes().into_iter().enumerate() {
            self.0[i] = byte;
        }
    }

    pub fn key(&self) -> u32 {
        let bytes = &self.0[4..8];
        bincode::deserialize(bytes).unwrap()
    }

    pub fn write_key(&mut self, key: u32) {
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Node {
    // Header
    // Common
    pub node_type: NodeType,
    pub is_root: bool,

    // TRADEOFF: We are tracking parent pointer of a node here.
    //
    // It's actually quite expensive to maintain the parent pointer
    // as a split or merge. As you will need to hold the locks for a longer
    // time, and have multiple page in/out of child nodes.
    //
    // It probably incurs more disk I/O.
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

#[allow(dead_code)]
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

    pub fn root() -> Self {
        Node {
            node_type: NodeType::Leaf,
            is_root: true,
            parent_offset: 0,
            right_child_offset: 0,
            next_leaf_offset: 0,
            num_of_cells: 0,
            has_initialize: true,
            cells: Vec::new(),
            internal_cells: Vec::new(),
        }
    }

    pub fn uninitialize() -> Self {
        let mut node = Self::new(true, NodeType::Leaf);
        node.has_initialize = false;
        node
    }

    pub fn new_from_bytes(bytes: &[u8]) -> Self {
        let mut node = Node::uninitialize();
        node.set_common_header(&bytes[0..COMMON_NODE_HEADER_SIZE]);

        if node.node_type == NodeType::Leaf {
            node.set_leaf_header(&bytes[COMMON_NODE_HEADER_SIZE..LEAF_NODE_HEADER_SIZE]);
            node.set_leaf_cells(&bytes[LEAF_NODE_HEADER_SIZE..]);
        }

        if node.node_type == NodeType::Internal {
            node.set_internal_header(&bytes[COMMON_NODE_HEADER_SIZE..INTERNAL_NODE_HEADER_SIZE]);
            node.set_internal_cells(&bytes[INTERNAL_NODE_HEADER_SIZE..]);
        }

        node
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header();

        if self.node_type == NodeType::Leaf {
            for c in &self.cells {
                let mut cell_bytes = bincode::serialize(c).unwrap();
                bytes.append(&mut cell_bytes);
            }
        } else {
            for c in &self.internal_cells {
                let mut cell_bytes = bincode::serialize(c).unwrap();
                bytes.append(&mut cell_bytes);
            }
        }

        // Outdated a bit:
        // Okay, we need to backfill the space because we are assuming
        // per page is always with PAGE_SIZE.
        //
        // If we didn't fill up the space, what would happen is when we read
        // from file, we will not have an accurate number of pages because file with
        // PAGE_SIZE might contain multiple pages. In theory, you can still keep
        // track of the number of pages in the file, tricky part would then be,
        // how do we identify the page offset of each page? We will have to read each
        // page to find out the next page offset.
        //
        // So long story short, let's just backfill the space...
        let remaining_space = MAX_NODE_SIZE - bytes.len();
        let mut vec = vec![0; remaining_space];
        bytes.append(&mut vec);

        bytes
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

            for (i, bytes_slice) in bytes.iter().enumerate().take(COMMON_NODE_HEADER_SIZE) {
                result.insert(i, *bytes_slice);
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

            for (i, bytes_slice) in bytes.iter().enumerate().take(INTERNAL_NODE_HEADER_SIZE) {
                result.insert(i, *bytes_slice);
            }
        }

        result
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

    pub fn get_mut_cell(&mut self, cell_num: usize) -> Option<&mut Cell> {
        self.cells.get_mut(cell_num)
    }

    pub fn get_row(&self, cell_num: usize) -> Option<Row> {
        self.cells.get(cell_num).map(|cell| {
            let bytes = cell.value();
            bincode::deserialize(bytes).unwrap()
        })
    }

    pub fn get(&self, cell_num: usize) -> Row {
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

    pub fn delete(&mut self, cell_num: usize) {
        if self.node_type == NodeType::Leaf {
            self.cells.remove(cell_num);
            self.num_of_cells -= 1;
        } else {
            unimplemented!("implement delete for internal node")
        }
    }

    pub fn internal_insert(&mut self, index: usize, cell: InternalCell) {
        self.internal_cells.insert(index, cell);
    }

    /// Return the index of the given key.
    pub fn internal_search(&self, key: u32) -> usize {
        match self.internal_cells.binary_search_by(|c| c.key().cmp(&key)) {
            Ok(index) => index,
            Err(index) => index,
        }
    }

    /// Return the index of the given child_pointer.
    pub fn internal_search_child_pointer(&self, child_pointer: u32) -> usize {
        for i in 0..self.internal_cells.len() {
            let cell = &self.internal_cells[i];
            if cell.child_pointer() == child_pointer {
                return i;
            }
        }

        self.internal_cells.len()
    }

    pub fn update_internal_key(&mut self, old_key: u32, new_key: u32) {
        let index = self.internal_search(old_key);
        if index < self.internal_cells.len() {
            self.internal_cells[index].write_key(new_key);
        }
    }

    pub fn siblings(&self, child_offset: u32) -> (Option<usize>, Option<usize>) {
        let index = self.internal_search_child_pointer(child_offset);

        if self.node_type == NodeType::Leaf {
            return (None, None);
        }

        if index == 0 {
            // No left neighbour if we are the first one
            let right_cp = if index + 1 < self.internal_cells.len() {
                self.internal_cells[index + 1].child_pointer() as usize
            } else {
                self.right_child_offset as usize
            };

            (None, Some(right_cp))
        } else if index == self.internal_cells.len() - 1 {
            // Right neighbour would be at right_child_offset  if we are the last one
            let left_cp = self.internal_cells[index - 1].child_pointer() as usize;
            (Some(left_cp), Some(self.right_child_offset as usize))
        } else {
            // We might also be the most right child, where our index would be larger
            // than internal_cells.len().
            //
            // In that case, we won't have a right neighbour as well.
            if index >= self.internal_cells.len() {
                let left_cp = self.internal_cells[index - 1].child_pointer() as usize;
                (Some(left_cp), None)
            } else {
                let left_cp = self.internal_cells[index - 1].child_pointer() as usize;
                let right_cp = self.internal_cells[index + 1].child_pointer() as usize;
                (Some(left_cp), Some(right_cp))
            }
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
