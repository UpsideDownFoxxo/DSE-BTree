#![feature(test)]
pub mod bees;
pub mod btree;
pub mod ffi;
pub mod flex;
pub mod slotted_branch;
pub mod slotted_leaf;
pub mod visualize;

pub static PAGE_SIZE: usize = 4096;

pub static PTR_SIZE: usize = std::mem::size_of::<*const u8>();

#[cfg(test)]
mod btree_test {

    use crate::{bees::BEES, slotted_branch::SlottedBranch, slotted_leaf::SlottedLeaf};
    // only used for deduplication. I am aware of the irony
    use std::collections::HashSet;

    use super::btree::BTree;

    #[test]
    fn sizes() {
        assert_eq!(size_of::<SlottedLeaf<()>>(), 4096);
        assert_eq!(size_of::<SlottedBranch<()>>(), 4096);
    }

    #[test]
    fn hello_world() {
        let mut tree = BTree::new();

        let location = 6942 as *mut ();

        tree.insert("hello", location);

        assert_eq!(tree.get("hello"), Some(location));
    }

    #[test]
    fn integers() {
        let mut tree = BTree::new();

        for i in 0..10_000 {
            let key_value = format!("{i:016}");
            tree.insert(&key_value, i as *mut ());
        }

        for i in 0..1_000 {
            let key_value = format!("{i:016}");
            assert_eq!(tree.get(&key_value), Some(i as *mut ()));
        }
    }

    #[test]
    fn the_bee_movie() {
        let mut tree = BTree::new();

        let mut set = HashSet::new();

        for line in BEES.lines() {
            set.insert(line);
        }

        for line in set.iter() {
            tree.insert(line, line.as_ptr() as *mut u8);
        }

        for line in set.iter() {
            let entry = tree.get(line);
            assert_eq!(entry, Some(line.as_ptr() as *mut u8))
        }
    }

    #[test]
    fn random() {
        let mut strings = vec![];

        let chars: Vec<_> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,/#?!"
            .chars()
            .collect();

        for _ in 0..10_000 {
            let len = rand::random_range(0..128);
            let mut str = String::new();

            for _ in 0..len {
                str.push(chars[rand::random_range(0..chars.len())]);
            }

            strings.push(str);
        }

        let mut tree: BTree<()> = BTree::new();

        for string in &strings {
            tree.insert(string, std::ptr::null_mut());
        }

        for string in &strings {
            assert_ne!(tree.get(string), None)
        }
    }
}
