use crate::node::{
    InternalCell, Node, NodeType, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_RIGHT_SPLIT_COUNT,
};
use crate::row::Row;
use crate::Cursor;

pub struct Tree(Vec<Node>);
impl Tree {
    pub fn new() -> Self {
        Tree(Vec::new())
    }

    pub fn nodes(&self) -> &Vec<Node> {
        &self.0
    }

    pub fn mut_nodes(&mut self) -> &mut Vec<Node> {
        &mut self.0
    }

    pub fn create_new_root(&mut self, cursor: &Cursor, mut old_node: Node, mut new_node: Node) {
        println!("--- create_new_root: cursor.page_num: {}", cursor.page_num);
        let mut root_node = Node::new(true, NodeType::Internal);
        old_node.is_root = false;

        root_node.num_of_cells += 1;
        root_node.right_child_offset = cursor.page_num as u32 + 2;

        old_node.parent_offset = 0;
        old_node.next_leaf_offset = cursor.page_num as u32 + 2;

        new_node.parent_offset = 0;

        let left_max_key = old_node.get_max_key();
        let cell = InternalCell::new(cursor.page_num as u32 + 1, left_max_key);
        root_node.internal_cells.insert(0, cell);

        self.0.insert(0, root_node);
        self.0.insert(cursor.page_num + 1, old_node);
        self.0.insert(cursor.page_num + 2, new_node);
    }

    pub fn split_and_insert_leaf_node(&mut self, cursor: &Cursor, row: &Row) {
        println!("--- split_and_insert_leaf_node: {}", row.id);
        let mut old_node = self.0.remove(cursor.page_num);
        let old_max = old_node.get_max_key();
        old_node.insert(row, cursor);

        let mut new_node = Node::new(false, old_node.node_type);

        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = old_node.cells.remove(LEAF_NODE_LEFT_SPLIT_COUNT);
            old_node.num_of_cells -= 1;

            new_node.cells.push(cell);
            new_node.num_of_cells += 1;
        }

        if old_node.is_root {
            self.create_new_root(cursor, old_node, new_node);
        } else {
            new_node.next_leaf_offset = old_node.next_leaf_offset + 1;
            old_node.next_leaf_offset = cursor.page_num as u32 + 1;

            let parent_page_num = old_node.parent_offset as usize;
            let parent = &mut self.0[parent_page_num];
            let new_max = old_node.get_max_key();
            parent.update_internal_key(old_max, new_max);
            self.0.insert(cursor.page_num, new_node);
            self.0.insert(cursor.page_num, old_node);
            self.insert_internal_node(parent_page_num, cursor.page_num + 1);
            println!("{:?}", self.0);
        }
    }

    pub fn insert_internal_node(&mut self, parent_page_num: usize, new_child_page_num: usize) {
        let parent_right_child_offset = self.0[parent_page_num].right_child_offset as usize;
        let new_node = &self.0[new_child_page_num];
        let new_child_max_key = new_node.get_max_key();

        let right_child = &self.0[parent_right_child_offset];
        let right_max_key = right_child.get_max_key();

        let parent = &mut self.0[parent_page_num];
        let original_num_keys = parent.num_of_cells;
        parent.num_of_cells += 1;

        if original_num_keys >= 3 {
            panic!("Need to split internal node\n");
        }

        let index = parent.internal_search(new_child_max_key);
        if new_child_max_key > right_max_key {
            parent.right_child_offset = new_child_page_num as u32;
            parent.internal_insert(
                index,
                InternalCell::new(parent_right_child_offset as u32, right_max_key),
            );
        } else {
            parent.right_child_offset += 1;
            parent.internal_insert(
                index,
                InternalCell::new(new_child_page_num as u32, new_child_max_key),
            );
        }
    }

    pub fn print_node(&self, node: &Node, indent_level: usize) {
        if node.node_type == NodeType::Internal {
            indent(indent_level);
            println!("- internal (size {})", node.num_of_cells);

            for c in &node.internal_cells {
                let child_index = c.child_pointer() as usize;
                let node = &self.0[child_index];
                self.print_node(&node, indent_level + 1);

                indent(indent_level + 1);
                println!("- key {}", c.key());
            }

            let child_index = node.right_child_offset as usize;
            let node = &self.0[child_index];
            self.print_node(&node, indent_level + 1);
        } else if node.node_type == NodeType::Leaf {
            indent(indent_level);
            println!("- leaf (size {})", node.num_of_cells);
            for c in &node.cells {
                indent(indent_level + 1);
                println!("- {}", c.key());
            }
        }
    }

    pub fn print(&self) {
        let node = &self.0[0];
        self.print_node(node, 0);
    }
}

pub fn indent(level: usize) {
    for _ in 0..level {
        print!("  ");
    }
}
