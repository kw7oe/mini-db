use crate::node::{InternalCell, Node, NodeType, LEAF_NODE_MAX_CELLS, LEAF_NODE_RIGHT_SPLIT_COUNT};
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

    pub fn create_new_root(&mut self, right_node_page_num: usize, mut left_node: Node) {
        debug!("--- create_new_root");
        let right_node = self.0.get_mut(right_node_page_num).unwrap();
        let mut root_node = Node::new(true, NodeType::Internal);
        right_node.is_root = false;

        // The only reason we could hardcode all of the offsets
        // is becuase we always insert root node to 0 and it's children to index
        // 1 and 2, when we create a new root node.
        root_node.num_of_cells += 1;
        root_node.right_child_offset = 2;

        right_node.parent_offset = 0;
        right_node.next_leaf_offset = 0;

        left_node.next_leaf_offset = 2;
        left_node.parent_offset = 0;

        let left_max_key = left_node.get_max_key();
        let cell = InternalCell::new(1, left_max_key);
        root_node.internal_cells.insert(0, cell);

        self.0.insert(0, root_node);
        self.0.insert(1, left_node);
    }

    pub fn split_and_insert_leaf_node(&mut self, cursor: &Cursor, row: &Row) {
        let mut right_node = self.0.get_mut(cursor.page_num).unwrap();
        let old_max = right_node.get_max_key();
        right_node.insert(row, cursor);

        let mut left_node = Node::new(false, right_node.node_type);

        for _i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let cell = right_node.cells.remove(0);
            right_node.num_of_cells -= 1;

            left_node.cells.push(cell);
            left_node.num_of_cells += 1;
        }

        if right_node.is_root {
            self.create_new_root(cursor.page_num, left_node);
        } else {
            debug!("--- split leaf node and update parent ---");
            let right_node = &self.0[cursor.page_num];
            left_node.next_leaf_offset = (cursor.page_num + 1) as u32;
            left_node.parent_offset = right_node.parent_offset;

            let parent_page_num = right_node.parent_offset as usize;
            let new_max = left_node.get_max_key();

            let parent = &mut self.0[parent_page_num];
            parent.update_internal_key(old_max, new_max);

            self.0.insert(cursor.page_num, left_node);

            let parent_page_num = if cursor.page_num < parent_page_num {
                parent_page_num + 1
            } else {
                parent_page_num
            };

            self.increment_pointers(cursor.page_num);
            self.insert_internal_node(parent_page_num, cursor.page_num + 1);
            self.maybe_split_internal_node(parent_page_num);
        }
    }

    pub fn increment_pointers(&mut self, page_num: usize) {
        for i in 0..self.0.len() {
            let node = &mut self.0[i];

            if node.node_type == NodeType::Internal {
                node.increment_internal_child_pointers(page_num);
                self.update_children_parent_offset(i as u32);
            } else if node.node_type == NodeType::Leaf && node.next_leaf_offset != 0 && page_num < i
            {
                node.next_leaf_offset += 1
            }
        }
    }

    pub fn decrement_pointers(&mut self, page_num: usize) {
        for i in 0..self.0.len() {
            let node = &mut self.0[i];

            if node.node_type == NodeType::Internal {
                node.decrement_internal_child_pointers(page_num);
                self.update_children_parent_offset(i as u32);
            } else if node.node_type == NodeType::Leaf
                && node.next_leaf_offset != 0
                && page_num < node.next_leaf_offset as usize
            {
                node.next_leaf_offset -= 1
            }
        }
    }

    pub fn insert_internal_node(&mut self, parent_page_num: usize, new_child_page_num: usize) {
        let parent_right_child_offset = self.0[parent_page_num].right_child_offset as usize;

        let new_child_page_num = if new_child_page_num == parent_right_child_offset {
            new_child_page_num - 1
        } else {
            new_child_page_num
        };

        let new_node = &mut self.0[new_child_page_num];
        let new_child_max_key = new_node.get_max_key();
        new_node.parent_offset = parent_page_num as u32;

        let right_child = &self.0[parent_right_child_offset];
        let right_max_key = right_child.get_max_key();

        let parent = &mut self.0[parent_page_num];
        parent.num_of_cells += 1;

        let index = parent.internal_search(new_child_max_key);
        if new_child_max_key > right_max_key {
            debug!("--- child max key: {new_child_max_key} > right_max_key: {right_max_key}");
            parent.right_child_offset = new_child_page_num as u32;
            parent.internal_insert(
                index,
                InternalCell::new(parent_right_child_offset as u32, right_max_key),
            );
        } else {
            debug!("--- child max key: {new_child_max_key} <= right_max_key: {right_max_key}");
            parent.internal_insert(
                index,
                InternalCell::new(new_child_page_num as u32, new_child_max_key),
            );
        }
    }

    pub fn maybe_split_internal_node(&mut self, page_num: usize) {
        let max_num_cells_for_internal_node = 3;
        let last_unused_page_num = self.0.len() as u32;
        let left_node = &self.0[page_num];

        if left_node.num_of_cells > max_num_cells_for_internal_node {
            let mut left_node = self.0.remove(page_num);
            let split_at_index = left_node.num_of_cells as usize / 2;

            let mut right_node = Node::new(false, left_node.node_type);
            right_node.right_child_offset = left_node.right_child_offset;
            right_node.parent_offset = left_node.parent_offset as u32;

            let ic = left_node.internal_cells.remove(split_at_index);
            left_node.num_of_cells -= 1;
            left_node.right_child_offset = ic.child_pointer();

            for i in 0..split_at_index - 1 {
                let ic = left_node.internal_cells.remove(split_at_index);
                left_node.num_of_cells -= 1;
                right_node.internal_insert(i, ic);
                right_node.num_of_cells += 1;
            }

            if left_node.is_root {
                // Notice that we are using last_unused_page_num and last_unused_page_num + 1
                // as the child pointers for our new nodes.
                //
                // Initially, last_unused_page_num = the Vec<Node> len, however since we
                // remove our left_node from the Vec, the up to date value should be -1.
                //
                // Hence, in this section, the removal of the left node is filled by the
                // new root node.
                debug!("splitting root internal node...");
                let mut root_node = Node::new(true, NodeType::Internal);
                left_node.is_root = false;

                root_node.num_of_cells += 1;
                root_node.right_child_offset = last_unused_page_num + 1;

                left_node.parent_offset = 0;
                right_node.parent_offset = 0;

                let cell = InternalCell::new(last_unused_page_num, ic.key());
                root_node.internal_cells.insert(0, cell);

                self.0.insert(0, root_node);
                self.0.push(left_node);
                self.0.push(right_node);
                self.update_children_parent_offset(last_unused_page_num);
                self.update_children_parent_offset(last_unused_page_num + 1);
            } else {
                debug!("update internal node {page_num}, parent...");
                let parent = &mut self.0[left_node.parent_offset as usize];
                let index = parent.internal_search_child_pointer(page_num as u32);

                if page_num < parent.right_child_offset as usize {
                    parent.right_child_offset -= 1;
                }

                if page_num < left_node.right_child_offset as usize {
                    left_node.right_child_offset -= 1;
                }

                if page_num < right_node.right_child_offset as usize {
                    right_node.right_child_offset -= 1;
                }

                if parent.num_of_cells == index as u32 {
                    parent.right_child_offset = last_unused_page_num;
                    parent.internal_insert(
                        index,
                        InternalCell::new(last_unused_page_num - 1, ic.key()),
                    );
                    parent.num_of_cells += 1;
                } else {
                    parent.internal_insert(
                        index,
                        InternalCell::new(last_unused_page_num - 1, ic.key()),
                    );

                    let internel_cell = parent.internal_cells.remove(index + 1);
                    parent.internal_insert(
                        index + 1,
                        InternalCell::new(last_unused_page_num, internel_cell.key()),
                    );
                    parent.num_of_cells += 1;
                }

                self.0.push(left_node);
                self.0.push(right_node);

                // Notice that here, we also update the children offset of page_num.
                //
                // Let's say we have 3 internal node at position 8 and 9. We
                // split at node 8, which will become 2 new internal nodes. After removal
                // of our initial node 8, node 9 now become the node 8, and the two new
                // nodes become node 9 and 10.
                //
                // This mean that the childrens of our new node 8 will need to have their
                // parent offset updated as well.
                //
                // So any internal nodes between page_numm to last_unused_page_num - 1, will
                // be affected by this changes, as we have a removal.
                for i in page_num..=last_unused_page_num as usize {
                    self.update_children_parent_offset(i as u32);
                }
            }
        }
    }

    pub fn update_children_parent_offset(&mut self, page_num: u32) {
        let node = &self.0[page_num as usize];

        let mut child_pointers = vec![node.right_child_offset as usize];
        for cell in &node.internal_cells {
            child_pointers.push(cell.child_pointer() as usize);
        }

        for i in child_pointers {
            let child = &mut self.0[i];
            child.parent_offset = page_num;
        }
    }

    pub fn delete(&mut self, cursor: &Cursor) {
        let node = &mut self.0[cursor.page_num];
        node.delete(cursor.cell_num);
        self.maybe_merge_nodes(&cursor);
    }

    fn maybe_merge_nodes(&mut self, cursor: &Cursor) {
        let node = &self.0[cursor.page_num];

        if node.node_type == NodeType::Leaf
            && node.num_of_cells < LEAF_NODE_MAX_CELLS as u32 / 2
            && !node.is_root
        {
            self.merge_leaf_nodes(cursor.page_num);
        }
    }

    fn merge_leaf_nodes(&mut self, page_num: usize) {
        let node = &self.0[page_num];
        let parent = &self.0[node.parent_offset as usize];
        let (left_child_pointer, right_child_pointer) = parent.siblings(page_num as u32);

        if let Some(cp) = left_child_pointer {
            let left_nb = &self.0[cp];

            if left_nb.cells.len() + node.cells.len() < LEAF_NODE_MAX_CELLS {
                debug!("Merging node {} with its left neighbour...", page_num);

                // Another reason to move it into a function
                // is so that borrow checker did'nt complain borrow
                // about mutable after having immutable borrow above.
                self.do_merge_leaf_nodes(cp, page_num);
                return;
            }
        }

        if let Some(cp) = right_child_pointer {
            let right_nb = &self.0[cp];
            if right_nb.cells.len() + node.cells.len() < LEAF_NODE_MAX_CELLS {
                debug!("Merging node {} with its right neighbour...", page_num);
                self.do_merge_leaf_nodes(page_num, cp);
            }
            return;
        }
    }

    fn promote_to_last_node_to_root(&mut self, page_num: usize) {
        // Remove old root node.
        let mut node = self.0.remove(page_num);
        node.is_root = true;
        node.next_leaf_offset = 0;
        self.0[0] = node;
    }

    fn do_merge_leaf_nodes(&mut self, left_cp: usize, right_cp: usize) {
        let node = self.0.remove(right_cp);
        let left_node = self.0.get_mut(left_cp).unwrap();

        // Merge the leaf nodes cells
        for c in node.cells {
            left_node.cells.push(c);
            left_node.num_of_cells += 1;
        }

        if node.next_leaf_offset == 0 {
            left_node.next_leaf_offset = 0;
        }

        if right_cp < left_node.parent_offset as usize {
            left_node.parent_offset -= 1;
        }

        let parent_offset = left_node.parent_offset as usize;

        let max_key = left_node.get_max_key();
        let min_key_length = self.min_key(3) as u32;
        // Update parent metadata
        let parent = self.0.get_mut(parent_offset).unwrap();

        if parent.num_of_cells == 1 && parent.is_root {
            debug!("promote last leaf node to root");
            self.promote_to_last_node_to_root(left_cp);
        } else {
            let index = parent.internal_search_child_pointer(right_cp as u32);
            if index == parent.num_of_cells as usize {
                // The right_cp is our right child offset

                // Move last internal cell to become the right child offset
                let internal_cell = parent.internal_cells.remove(index - 1);
                parent.num_of_cells -= 1;
                parent.right_child_offset = internal_cell.child_pointer();
            } else {
                // Remove extra key, pointers cell as we now have one less child
                // after merge
                parent.num_of_cells -= 1;
                // if left_cp < parent.right_child_offset as usize {
                //     parent.right_child_offset -= 1;
                // }
                parent.internal_cells.remove(index);

                // Update the key for our existing child pointer pointing to our merged node
                // to use the new max key.
                if index != 0 {
                    parent.internal_cells[index - 1] = InternalCell::new(left_cp as u32, max_key);
                }
            }

            self.decrement_pointers(right_cp);

            let parent = self.0.get(parent_offset).unwrap();
            if parent.num_of_cells <= min_key_length && !parent.is_root {
                self.merge_internal_nodes(parent_offset);
            }
        }
    }

    fn min_key(&self, max_degree: usize) -> usize {
        let mut min_key = (max_degree / 2) - 1;

        if min_key == 0 {
            min_key = 1;
        }

        min_key
    }

    fn merge_internal_nodes(&mut self, page_num: usize) {
        let node = &self.0[page_num];
        let parent = &self.0[node.parent_offset as usize];

        let (left_child_pointer, right_child_pointer) = parent.siblings(page_num as u32);

        if let Some(cp) = left_child_pointer {
            let left_nb = &self.0[cp];
            if left_nb.internal_cells.len() + node.internal_cells.len() < 3 {
                debug!("Merging internal node {page_num} with left neighbour");
                self.do_merge_internal_nodes(cp, page_num);
            }
            return;
        }

        if let Some(cp) = right_child_pointer {
            let right_nb = &self.0[cp];
            if right_nb.internal_cells.len() + node.internal_cells.len() < 3 {
                debug!("Merging internal node {page_num} with right neighbour");
                self.do_merge_internal_nodes(page_num, cp);
            }
            return;
        }
    }

    fn do_merge_internal_nodes(&mut self, left_cp: usize, right_cp: usize) {
        // let min_key_length = self.min_key(3) as u32;

        let left_node = self.0.get(left_cp).unwrap();
        let new_left_max_key = self.0[left_node.right_child_offset as usize].get_max_key();
        let node = self.0.remove(right_cp);

        let left_node = self.0.get_mut(left_cp).unwrap();
        left_node.internal_cells.push(InternalCell::new(
            left_node.right_child_offset,
            new_left_max_key,
        ));
        left_node.num_of_cells += 1;

        // Merge the leaf nodes cells
        for c in node.internal_cells {
            left_node.internal_cells.push(c);
            left_node.num_of_cells += 1;
        }

        left_node.right_child_offset = node.right_child_offset;

        // if right_cp < left_node.parent_offset as usize {
        //     left_node.parent_offset -= 1;
        // }

        let parent_offset = left_node.parent_offset as usize;

        // Update parent metadata
        let parent = self.0.get(parent_offset).unwrap();

        if self.0.len() <= 4 + 1 + 1 && parent.num_of_cells == 1 && parent.is_root {
            debug!("promote internal nodes to root");
            self.promote_to_last_node_to_root(left_cp);
            // self.decrement_pointers(right_cp);
            self.update_children_parent_offset(0);
        } else {
            debug!("update parent linked with childrens");
        }
    }

    pub fn node_to_string(&self, node: &Node, indent_level: usize) -> String {
        let mut result = String::new();

        if node.node_type == NodeType::Internal {
            for _ in 0..indent_level {
                result += "  ";
            }
            result += &format!("- internal (size {})\n", node.num_of_cells);

            for c in &node.internal_cells {
                let child_index = c.child_pointer() as usize;
                let node = &self.0[child_index];
                result += &self.node_to_string(&node, indent_level + 1);

                for _ in 0..indent_level + 1 {
                    result += "  ";
                }
                result += &format!("- key {}\n", c.key());
            }

            let child_index = node.right_child_offset as usize;
            let node = &self.0[child_index];
            result += &self.node_to_string(&node, indent_level + 1);
        } else if node.node_type == NodeType::Leaf {
            for _ in 0..indent_level {
                result += "  ";
            }

            result += &format!("- leaf (size {})\n", node.num_of_cells);
            for c in &node.cells {
                for _ in 0..indent_level + 1 {
                    result += "  ";
                }
                result += &format!("- {}\n", c.key());
            }
        }

        result
    }

    pub fn to_string(&self) -> String {
        if let Some(node) = &self.0.get(0) {
            self.node_to_string(node, 0)
        } else {
            "Empty tree...".to_string()
        }
    }
}

impl std::fmt::Debug for Tree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tree {{\n").unwrap();
        for (i, c) in self.0.iter().enumerate() {
            write!(f, "  {i}: {:?}\n", c).unwrap();
        }
        write!(f, "}}")
    }
}
