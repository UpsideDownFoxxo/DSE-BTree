use std::{fmt::Debug, marker::PhantomData, ptr::NonNull, u16, usize};

use crate::{
    btree::{InsertResultIntern, Node},
    flex::{Flex, FlexHead, SlotNode, DATA_LEN},
    slotted_leaf::SlottedLeaf,
    PTR_SIZE,
};

pub struct SlottedBranch<T: Debug> {
    pub header: FlexHead,
    pub data: Flex,
    boo: PhantomData<T>,
}

impl<T: Debug> SlottedBranch<T> {
    pub fn new(left: Node, right: Node, separator: &str) -> Self {
        let mut new_self = Self {
            header: FlexHead::new(std::ptr::NonNull::new(Some(right).unwrap())),
            data: Flex::new(),
            boo: PhantomData,
        };

        let node = new_self
            .data
            .add_heap_entry(&mut new_self.header, separator, left);

        new_self.data.insert_stack(&mut new_self.header, 0, node);

        let (nodes, _) = new_self.data.interpret_mut(&new_self.header);

        nodes[0] = node;
        new_self
    }

    fn new_from_range(
        range: &[SlotNode],
        src: &Self,
        right: Option<NonNull<()>>,
        extra_node: Option<&SlotNode>,
        extra_slot: (&str, Node),
    ) -> Self {
        let mut new_self = Self {
            header: FlexHead::new(right),
            data: Flex::new(),
            boo: PhantomData,
        };

        for node in range {
            let (key, value) =
                src.data
                    .get_overflow_heap_entry(&mut new_self.header, node, extra_slot);

            let new_node = new_self
                .data
                .add_heap_entry(&mut new_self.header, key, value as Node);

            let count = new_self.header.node_count.into();
            new_self
                .data
                .insert_stack(&mut new_self.header, count, new_node);
        }

        if let Some(node) = extra_node {
            let (str, ptr) = src
                .data
                .get_overflow_heap_entry(&src.header, node, extra_slot);

            let new_node = new_self.data.add_heap_entry(&mut new_self.header, str, ptr);

            let count = new_self.header.node_count.into();
            new_self
                .data
                .insert_stack(&mut new_self.header, count, new_node);
        }

        new_self
    }

    pub fn size(&self) -> usize {
        self.header.node_count as usize
    }

    pub fn unused_bytes(&self) -> usize {
        self.header.key_pos as usize - self.header.node_count as usize * size_of::<SlotNode>()
    }

    pub fn payload_bytes(&self) -> usize {
        DATA_LEN - self.header.node_count as usize
    }

    pub fn key_at(&self, index: usize) -> &str {
        self.data.key_at(&self.header, index)
    }

    pub fn child_at(&self, index: usize) -> *mut () {
        if index == self.header.node_count as usize {
            return self.header.pointer.expect("Invalid Branch Layout").as_ptr();
        }

        self.data.value_at(&self.header, index)
    }

    pub fn can_fit(&self, key: &str) -> bool {
        let new_entry_size = key.len() + PTR_SIZE + size_of::<SlotNode>();

        self.unused_bytes() >= new_entry_size
    }

    pub fn get_raw(&mut self, at: u16) -> &u8 {
        self.data.get_raw(at as usize - size_of::<FlexHead>())
    }

    fn get_upper_bound(&self, key: &str) -> usize {
        let (nodes, _) = self.data.interpret(&self.header);
        let mut slot_nr = 0;

        let mut u32_bytes = [0; 4];
        let key_slice = if key.len() <= 4 { key } else { &key[0..4] };

        for (i, byte) in key_slice.bytes().enumerate() {
            u32_bytes[i] = byte;
        }

        let hint = u32::from_be_bytes(u32_bytes);

        for node in nodes {
            let (node_key, _) = self.data.get_heap_entry(&self.header, node);

            if node.first_bytes >= hint {
                if node_key > key {
                    return slot_nr;
                }
            }

            slot_nr += 1;
        }

        slot_nr
    }

    pub fn insert(&mut self, key: &str, value: Node, height: usize) -> InsertResultIntern {
        let i = self.get_upper_bound(key);

        let ptr = {
            if i == self.header.node_count as usize {
                self.header.pointer.expect("Invalid Branch Layout").as_ptr()
            } else {
                let (nodes, _) = self.data.interpret(&self.header);
                self.data.get_heap_entry(&self.header, &nodes[i]).1
            }
        };

        if height == 1 {
            // we have reached the bottom, this is a leaf
            let leaf_ptr = unsafe { &mut *(ptr as *mut SlottedLeaf<T>) };
            let res = leaf_ptr.insert(key, value);

            let InsertResultIntern::Split(separator, node) = res else {
                return res;
            };
            return self.insert_leaf_at(i, &separator, node);
        }

        // further down we go...
        let branch_ptr = unsafe { &mut *(ptr as *mut SlottedBranch<T>) };
        let res = branch_ptr.insert(key, value, height - 1);

        let InsertResultIntern::Split(separator, node) = res else {
            return res;
        };
        return self.insert_at(i, &separator, node);
    }

    fn fix_leaf_insert(&mut self, index: usize, value: Node) -> Node {
        if self.header.node_count as usize > index {
            self.data.swap_ptr_at(&self.header, index, value)
        } else {
            self.header
                .pointer
                .replace(NonNull::new(value).unwrap())
                .expect("Invalid Branch Layout")
                .as_ptr()
        }
    }

    fn insert_leaf_at(&mut self, index: usize, key: &str, value: Node) -> InsertResultIntern {
        let value = self.fix_leaf_insert(index, value);
        self.insert_at(index, key, value)
    }

    fn get_split<'a>(
        &'a mut self,
        overflow_node: &SlotNode,
        new_slot: (&'a str, Node),
    ) -> (usize, &'a str) {
        let midpoint = (&self.header.node_count + 1) / 2;

        return (
            midpoint as usize,
            self.data
                .key_at_overflow(&self.header, midpoint as usize, new_slot, overflow_node),
        );
    }

    fn insert_at(&mut self, index: usize, key: &str, value: Node) -> InsertResultIntern {
        if self.can_fit(key) {
            let node = self
                .data
                .add_heap_entry(&mut self.header, key, value as Node);

            self.data.insert_stack(&mut self.header, index, node);

            return InsertResultIntern::Inserted;
        }

        let mut u32_bytes = [0; 4];
        let key_slice = if key.len() <= 4 { key } else { &key[0..4] };

        for (i, byte) in key_slice.bytes().enumerate() {
            u32_bytes[i] = byte;
        }

        let end_node = self.data.insert_stack_overflow(
            &self.header,
            index,
            SlotNode::new(u16::MAX, u16::MAX, u32::from_be_bytes(u32_bytes)),
        );

        let (index, separator) = self.get_split(&end_node, (key, value));

        // save a copy of separator, since it currently lives inside self which will be destroyed
        // later on
        let separator = separator.to_owned();

        let (nodes, _) = self.data.interpret(&self.header);

        let (left_nodes, mid_node) = nodes.split_at(index);
        let (mid_node, right_nodes) = mid_node.split_at(1);

        // the key here is our separator. we already got that, so ignore
        let (_, mid_val) =
            self.data
                .get_overflow_heap_entry(&self.header, &mid_node[0], (key, value));

        let left = SlottedBranch::new_from_range(
            left_nodes,
            self,
            Some(std::ptr::NonNull::new(mid_val as Node).unwrap()),
            None,
            (key, value),
        );

        let right = SlottedBranch::new_from_range(
            right_nodes,
            self,
            self.header.pointer,
            Some(&end_node),
            (key, value),
        );

        // become the right subtree
        let _ = std::mem::replace(self, right);

        let left_pointer = Box::into_raw(Box::new(left));

        InsertResultIntern::Split(separator, left_pointer as Node)
    }

    pub fn get(&self, key: &str, height: usize) -> Option<*mut T> {
        let index = self.get_upper_bound(key);

        let child = if index == self.size() {
            self.header.pointer.expect("Invalid Branch Layout").as_ptr()
        } else {
            self.data.value_at(&self.header, index)
        };

        if height == 1 {
            let leaf = unsafe { &*(child as *mut SlottedLeaf<T>) };
            leaf.get(key)
        } else {
            let branch = unsafe { &*(child as *mut SlottedBranch<T>) };
            branch.get(key, height - 1)
        }
    }

    pub fn print(&self) -> String {
        let mut contents = String::new();
        let mut vertices = String::new();
        let self_ptr = std::ptr::from_ref(self) as usize;

        let (nodes, _) = self.data.interpret(&self.header);
        for node in nodes {
            let (key, ptr) = self.data.get_heap_entry(&self.header, node);
            let value_box_id = ptr as usize;

            vertices.push_str(&format!("{self_ptr}:s{value_box_id} -> {value_box_id}\n"));
            contents.push_str(&format!(
                "<s{}> | '{}' | ",
                value_box_id,
                key.replace("\n", "\\n").replace("\"", "\\\"")
            ));
        }

        let Some(last) = self.header.pointer else {
            panic!("Invalid Branch Layout");
        };

        let last_id = last.as_ptr() as usize;

        vertices.push_str(&format!("{self_ptr}:s{last_id} -> {last_id}\n",));

        contents.push_str(&format!("<s{last_id}>"));

        format!(
            "{}[label=\"{contents}\"]\n{vertices}\n",
            std::ptr::from_ref(self) as usize,
        )
    }
}

// no drop implementation here, the node itself has no idea what height it's on. So we need to rely
// on the BTree Drop
