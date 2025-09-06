use std::{fmt::Debug, iter, marker::PhantomData, ptr::NonNull};

use crate::{
    btree::{InsertResultIntern, Node},
    flex::{Flex, FlexHead, SlotNode, DATA_LEN},
    PTR_SIZE,
};

pub struct SlottedLeaf<T: Debug> {
    header: FlexHead,
    data: Flex,
    boo: PhantomData<T>,
}

impl<T: Debug> SlottedLeaf<T> {
    pub fn new() -> Self {
        let new_self = Self {
            header: FlexHead::new(None),
            data: Flex::new(),
            boo: PhantomData,
        };

        new_self
    }

    fn new_from_range(
        range: &[SlotNode],
        src: &Self,
        extra_node: Option<&SlotNode>,
        extra_slot: (&str, Node),
    ) -> Self {
        // initialize with empty pointer slot, the caller will have to re-bend the pointers
        let mut new_self = Self {
            header: FlexHead::new(None),
            data: Flex::new(),
            boo: PhantomData,
        };

        for node in range {
            let (key, value) = src
                .data
                .get_overflow_heap_entry(&src.header, node, extra_slot);

            let new_node = new_self
                .data
                .add_heap_entry(&mut new_self.header, key, value);

            let count = new_self.header.node_count.into();
            new_self
                .data
                .insert_stack(&mut new_self.header, count, new_node);
        }

        if let Some(node) = extra_node {
            let (key, value) = src
                .data
                .get_overflow_heap_entry(&src.header, node, extra_slot);

            let new_node = new_self
                .data
                .add_heap_entry(&mut new_self.header, key, value);

            let count = new_self.header.node_count.into();
            new_self
                .data
                .insert_stack(&mut new_self.header, count, new_node);
        }

        new_self
    }

    #[inline(always)]
    pub fn size(&self) -> usize {
        self.header.node_count as usize
    }

    pub fn unused_bytes(&self) -> usize {
        self.header.key_pos as usize - self.header.node_count as usize * size_of::<SlotNode>()
    }

    pub fn payload_bytes(&self) -> usize {
        DATA_LEN - self.header.key_pos as usize
    }

    pub fn key_at(&self, index: usize) -> &str {
        self.data.key_at(&self.header, index)
    }

    pub fn value_at(&self, index: usize) -> *mut T {
        self.data.value_at(&self.header, index) as *mut T
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
            if node.first_bytes >= hint {
                let (node_key, _) = self.data.get_heap_entry(&self.header, node);
                if node_key >= key {
                    return slot_nr;
                }
            }

            slot_nr += 1;
        }

        slot_nr
    }

    // (more or less) shamelessly taken from https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/4
    fn common_prefix<const N: usize>(xs: &[u8], ys: &[u8]) -> usize {
        let off = iter::zip(xs.chunks_exact(N), ys.chunks_exact(N))
            .take_while(|(x, y)| x == y)
            .count()
            * N;
        off + iter::zip(&xs[off..], &ys[off..])
            .take_while(|(x, y)| x == y)
            .count()
    }

    fn get_smallest_separator<'a>(
        last_key: &'a str,
        key: &'a str,
        current_best: u16,
    ) -> Option<(&'a str, u16)> {
        let sep_len = Self::common_prefix::<128>(last_key.as_bytes(), key.as_bytes()) + 1;

        if sep_len >= current_best as usize {
            return None;
        }

        // due to string comparison rules a prefix will always be less than its origin
        // we can guarantee that key has enough characters because
        // str1 !< str2 and |str1| = |str2| and str1 <= str2 would imply str1 == str2, which is
        // explicitly not allowed
        let separator = &key[0..sep_len];

        return Some((separator, sep_len as u16));
    }

    fn get_split<'a>(
        &'a mut self,
        overflow_node: &SlotNode,
        new_slot: (&'a str, Node),
    ) -> (usize, &'a str) {
        let midpoint = (&self.header.node_count + 1) / 2;

        let (nodes, _) = self.data.interpret(&self.header);

        let split_range = &nodes[u16::max(midpoint - 1, 0) as usize
            ..u16::min(midpoint + 2, self.header.node_count) as usize];

        let mut key = self
            .data
            .get_overflow_heap_entry(&self.header, &split_range[0], new_slot)
            .0;

        let mut i = u16::max(midpoint - 1, 0) as usize;
        let mut split_index = 0;
        let mut separator_length = u16::MAX;
        let mut separator = None;

        for node in &split_range[1..] {
            let (next_key, _) = self
                .data
                .get_overflow_heap_entry(&self.header, node, new_slot);

            if let Some((sep, len)) = Self::get_smallest_separator(key, next_key, separator_length)
            {
                separator_length = len;
                separator = Some(sep);
                split_index = i + 1;
            };

            key = next_key;
            i += 1;
        }

        // one last run for the  node if we excluded it before
        if midpoint + 2 > self.header.node_count {
            {
                let (next_key, _) =
                    self.data
                        .get_overflow_heap_entry(&self.header, overflow_node, new_slot);

                if let Some((sep, _)) =
                    Self::get_smallest_separator(key, next_key, separator_length)
                {
                    separator = Some(sep);
                    split_index = i + 1;
                };
            }
        }

        let Some(separator) = separator else {
            panic!("Could not find a suitable separator");
        };

        (split_index, separator)
    }

    pub fn insert(&mut self, key: &str, value: Node) -> InsertResultIntern {
        let index = self.get_upper_bound(key);

        let (nodes, _) = self.data.interpret(&self.header);

        if index < self.header.node_count as usize
            && self.data.get_heap_entry(&self.header, &nodes[index]).0 == key
        {
            return InsertResultIntern::Inserted;
        }

        if self.can_fit(key) {
            let node = self.data.add_heap_entry(&mut self.header, key, value);

            self.data.insert_stack(&mut self.header, index, node);

            return InsertResultIntern::Inserted;
        }

        let key_slice = if key.len() <= 4 { key } else { &key[0..4] };

        let mut u32_bytes = [0; 4];
        for (i, byte) in key_slice.bytes().enumerate() {
            u32_bytes[i] = byte;
        }

        let end_node = self.data.insert_stack_overflow(
            &self.header,
            index,
            SlotNode::new(u16::MAX, u16::MAX, u32::from_be_bytes(u32_bytes)),
        );
        let (index, separator) = self.get_split(&end_node, (key, value));

        // save a copy of separator, since it currently lives inside self, which will be replaced
        let separator = separator.to_owned();

        let (nodes, _) = self.data.interpret(&self.header);

        let (left_nodes, right_nodes) = nodes.split_at(index);

        let left = Self::new_from_range(left_nodes, self, None, (key, value));

        let mut right = Self::new_from_range(right_nodes, self, Some(&end_node), (key, value));

        right.header.pointer = self.header.pointer;

        // become the left leaf.
        // this creates some extra work in the branch that points to this, but saves us having to
        // search the tree for the node that points to this and re-bend ITS pointer to the new
        // location.
        let _ = std::mem::replace(self, left);

        let right_pointer = Box::into_raw(Box::new(right));
        self.header.pointer = NonNull::new(right_pointer as Node);

        InsertResultIntern::Split(separator, right_pointer as Node)
    }

    pub fn get(&self, key: &str) -> Option<*mut T> {
        let index = self.get_upper_bound(key);

        if index == self.size() {
            return None;
        }

        let node = self.data.interpret(&self.header).0[index];
        let (entry_key, entry_value) = self.data.get_heap_entry(&self.header, &node);

        if entry_key == key {
            Some(entry_value as *mut T)
        } else {
            None
        }
    }

    pub fn print(&self) -> String {
        let mut contents = String::new();
        let self_ptr = std::ptr::from_ref(self) as usize;

        let (nodes, _) = self.data.interpret(&self.header);
        for node in nodes {
            let (key, ptr) = self.data.get_heap_entry(&self.header, node);
            let value_ptr = ptr as usize;

            contents.push_str(&format!(
                "<s{}> | {} | ",
                value_ptr,
                key[0..usize::min(key.len(), 10)]
                    .replace("\n", "\\n")
                    .replace("\"", "\\\"")
            ));
        }

        format!(
            "{}[label=\"{contents}<next>\"]\n{}:next -> {}\n",
            std::ptr::from_ref(self) as usize,
            self_ptr,
            self.header
                .pointer
                .map(|p| p.as_ptr() as usize)
                .or(Some(0))
                .unwrap()
        )
    }
}

// we don't need this drop to free our contents, because they are pointers to data we don't own. so
// whoever actually owns it will clean up

// impl<T: Debug> Drop for SlottedLeaf<T> {
//     fn drop(&mut self) {
//         // we set the size to u16::MAX to indicate we don't want to drop children. So skip if we
//         // see that special size
//         if self.header.node_count != u16::MAX {
//             let (nodes, _) = self.data.interpret(&self.header);
//             for node in nodes {
//                 let (_, ptr) = self.data.get_heap_entry(&self.header, node);
//                 // re-box and drop
//                 let _boxed = unsafe { Box::from_raw(ptr as *mut T) };
//             }
//         }
//     }
// }

#[cfg(test)]
mod leaf_tests {

    use crate::{btree::InsertResultIntern, PTR_SIZE};

    use super::SlottedLeaf;

    #[test]
    fn hello_world() {
        let mut leaf: SlottedLeaf<()> = SlottedLeaf::new();

        assert_eq!(
            leaf.insert("hello", std::ptr::null_mut()),
            InsertResultIntern::Inserted
        );

        assert_ne!(leaf.get("hello"), None);
    }

    #[test]
    fn split() {
        for _ in 0..100 {
            let mut leaf: SlottedLeaf<()> = SlottedLeaf::new();
            let stream = rand::random_iter::<u64>();
            let mut overflow_key = String::new();

            for el in stream {
                let str = format!("{:08}", el);

                if !leaf.can_fit(&str) {
                    overflow_key = str;
                    break;
                }

                leaf.insert(&str, std::ptr::null_mut());
            }
            // leaf is now before splitting
            let page_size = leaf.size();
            let page_bytes = leaf.payload_bytes();

            let res = leaf.insert(&overflow_key, std::ptr::null_mut());
            let (_, left_tree) = match res {
                InsertResultIntern::Inserted => panic!("Leaf did not split"),
                InsertResultIntern::Replaced(_) => {
                    panic!("Either you got insanely lucky, or you just replaced some random value");
                }
                InsertResultIntern::Split(a, b) => (a, b),
            };

            let left = unsafe { &mut *(left_tree as *mut SlottedLeaf<()>) };

            assert_eq!(left.size() + leaf.size(), page_size + 1);
            assert_eq!(
                left.payload_bytes() + leaf.payload_bytes(),
                page_bytes + overflow_key.len() + PTR_SIZE
            );
        }
    }

    #[test]
    fn sort_order() {
        for _ in 0..100 {
            let mut leaf: SlottedLeaf<()> = SlottedLeaf::new();
            let stream = rand::random_iter::<u64>();

            for el in stream {
                let str = format!("{:08}", el);

                if !leaf.can_fit(&str) {
                    break;
                }

                leaf.insert(&str, std::ptr::null_mut());
            }
            // leaf is now before splitting
            let mut prev_key = leaf.key_at(0);
            for i in 1..leaf.size() {
                let key = leaf.key_at(i);
                assert!(prev_key < key);
                prev_key = key;
            }
        }
    }
}
