use crate::btree::Node;
use crate::PTR_SIZE;
use bytemuck::{cast_slice, cast_slice_mut};

use bytemuck::{Pod, Zeroable};

use crate::PAGE_SIZE;
use std::{fmt::Debug, ptr::NonNull};

pub const DATA_LEN: usize = PAGE_SIZE - size_of::<FlexHead>();

#[repr(C)]
#[derive(Pod, Copy, Clone, Zeroable, Debug)]
pub struct SlotNode {
    pub start: u16,
    pub end: u16,
    pub first_bytes: u32,
}

impl SlotNode {
    pub fn new(start: u16, end: u16, first_bytes: u32) -> Self {
        SlotNode {
            start,
            end,
            first_bytes,
        }
    }
}

#[derive(Debug)]
pub struct FlexHead {
    pub node_count: u16,
    pub key_pos: u16,
    pub pointer: Option<std::ptr::NonNull<()>>,
}

impl FlexHead {
    pub fn new(pointer: Option<NonNull<()>>) -> Self {
        Self {
            node_count: 0,
            key_pos: DATA_LEN as u16,
            pointer,
        }
    }
}

pub struct Flex {
    raw: [u8; PAGE_SIZE - size_of::<FlexHead>()],
}

impl Flex {
    pub fn new() -> Self {
        Self { raw: [0; DATA_LEN] }
    }

    pub fn get_raw(&self, index: usize) -> &u8 {
        return &self.raw[index];
    }

    pub fn interpret(&self, header: &FlexHead) -> (&[SlotNode], &[u8]) {
        let split = header.node_count as usize * size_of::<SlotNode>();

        let (nodes, data) = self.raw.split_at(split);
        (cast_slice(nodes), data)
    }

    pub fn interpret_mut(&mut self, header: &FlexHead) -> (&mut [SlotNode], &mut [u8]) {
        let split = header.node_count as usize * size_of::<SlotNode>();

        let (nodes, data) = self.raw.split_at_mut(split);
        (cast_slice_mut(nodes), data)
    }

    pub fn key_at(&self, header: &FlexHead, index: usize) -> &str {
        self.entry_at(header, index).0
    }

    pub fn key_at_overflow<'a>(
        &'a self,
        header: &'a FlexHead,
        index: usize,
        extra_slot: (&'a str, Node),
        extra_node: &SlotNode,
    ) -> &'a str {
        if index == header.node_count as usize {
            self.get_overflow_heap_entry(header, &extra_node, extra_slot)
                .0
        } else {
            let node = self.interpret(header).0[index];
            self.get_overflow_heap_entry(header, &node, extra_slot).0
        }
    }

    pub fn value_at(&self, header: &FlexHead, index: usize) -> *mut () {
        self.entry_at(header, index).1
    }

    pub fn entry_at(&self, header: &FlexHead, index: usize) -> (&str, *mut ()) {
        let (nodes, _) = self.interpret(header);
        self.get_heap_entry(header, &nodes[index])
    }

    pub fn swap_ptr_at(&mut self, header: &FlexHead, index: usize, ptr: *mut ()) -> *mut () {
        let (nodes, data) = self.interpret_mut(header);
        let node = nodes[index];

        let data_offset = header.node_count as usize * size_of::<SlotNode>();

        let (ptr_slot, _) = data
            [node.start as usize - data_offset..node.end as usize - data_offset]
            .split_at_mut(PTR_SIZE);

        let old_ptr = usize::from_ne_bytes(ptr_slot.try_into().ok().unwrap()) as *mut ();

        ptr_slot.copy_from_slice(&(ptr as usize).to_ne_bytes());

        old_ptr
    }

    pub fn swap_ptr_at_overflow<'a>(
        &mut self,
        header: &FlexHead,
        mut extra_slot: (&'a str, Node),
        index: usize,
        ptr: *mut (),
    ) -> *mut () {
        if index == header.node_count as usize {
            let old_ptr = extra_slot.1;
            extra_slot.1 = ptr;
            return old_ptr;
        }

        self.swap_ptr_at(header, index, ptr)
    }

    pub fn add_heap_entry(&mut self, header: &mut FlexHead, key: &str, value: *mut ()) -> SlotNode {
        let (_, data) = self.interpret_mut(header);

        let slot_end = header.key_pos as usize;
        let slot_len = key.len() + PTR_SIZE;

        let slot_start = slot_end - slot_len;

        let data_offset = header.node_count as usize * size_of::<SlotNode>();

        let data_slot = &mut data[slot_start - data_offset..slot_end - data_offset];

        let (ptr_slot, key_slot) = data_slot.split_at_mut(PTR_SIZE);

        ptr_slot.copy_from_slice(&(value as usize).to_ne_bytes());

        debug_assert_eq!(key_slot.len(), key.len());

        key_slot.copy_from_slice(key.as_bytes());

        header.key_pos = slot_start as u16;

        let mut u32_bytes = [0; 4];

        let key_slice = if key.len() <= 4 { key } else { &key[0..4] };

        for (i, byte) in key_slice.bytes().enumerate() {
            u32_bytes[i] = byte;
        }

        SlotNode::new(
            slot_start as u16,
            slot_end as u16,
            u32::from_be_bytes(u32_bytes),
        )
    }

    #[inline(always)]
    pub fn get_heap_entry(&self, header: &FlexHead, node: &SlotNode) -> (&str, Node) {
        let data_offset = header.node_count as usize * size_of::<SlotNode>();
        let (_, data) = self.interpret(header);

        let data_slot = &data[node.start as usize - data_offset..node.end as usize - data_offset];

        let (ptr_slot, key_slot) = data_slot.split_at(PTR_SIZE);

        let ptr = usize::from_ne_bytes(ptr_slot.try_into().ok().unwrap()) as *mut ();
        let key = unsafe { std::str::from_utf8_unchecked(key_slot) };

        (key, ptr)
    }

    pub fn get_upper_bound<'a>(&self, key: &str, header: &'a FlexHead) -> usize {
        let (nodes, _) = self.interpret(header);
        let mut slot_nr = 0;

        for node in nodes {
            let (node_key, _) = self.get_heap_entry(&header, node);
            if node_key >= key {
                return slot_nr;
            }

            slot_nr += 1;
        }

        slot_nr
    }

    pub fn get_overflow_heap_entry<'a>(
        &'a self,
        header: &FlexHead,
        node: &SlotNode,
        extra_slot: (&'a str, Node),
    ) -> (&'a str, Node) {
        if node.start == u16::MAX {
            return extra_slot;
        }
        self.get_heap_entry(header, node)
    }

    pub fn insert_stack(&mut self, header: &mut FlexHead, index: usize, entry: SlotNode) {
        header.node_count += 1;
        let (nodes, _) = self.interpret_mut(header);
        let mut cur_node = entry;

        for node in &mut nodes[index..] {
            std::mem::swap(node, &mut cur_node);
        }

        // the node at node_count - 1 had garbage inside from growing the array. So it's ok just
        // drop cur_entry here
    }

    pub fn insert_stack_overflow(
        &mut self,
        header: &FlexHead,
        index: usize,
        entry: SlotNode,
    ) -> SlotNode {
        // do not increase size, return last node instead
        let (nodes, _) = self.interpret_mut(header);
        let mut cur_node = entry;

        for node in &mut nodes[index..] {
            std::mem::swap(node, &mut cur_node);
        }

        cur_node
    }
}
