use std::{fmt::Debug, io, marker::PhantomData};

use crate::{slotted_branch::SlottedBranch, slotted_leaf::SlottedLeaf, visualize::Graphviz};

// this is mainly cosmetic, since we just interpret based on tree height
// Values stored in the tree are nodes too.. I cannot be asked to properly type this if I have to cast
// between them all the time anyways
pub type Node = *mut ();

#[derive(Debug, PartialEq)]
pub enum InsertResultIntern {
    Inserted,
    Split(String, Node),
    Replaced(Node),
}

impl Into<InsertResult> for InsertResultIntern {
    fn into(self) -> InsertResult {
        match self {
            InsertResultIntern::Replaced(e) => InsertResult::Replaced(e),
            InsertResultIntern::Inserted => InsertResult::Inserted,
            InsertResultIntern::Split(_, _) => {
                panic!("Cannot convert split result into InsertResult")
            }
        }
    }
}

#[derive(Debug)]
pub enum InsertResult {
    Inserted,
    Replaced(Node),
}

#[derive(Debug)]
pub struct BTree<T: Debug> {
    height: usize,
    root: Node,
    boo: PhantomData<T>,
}

impl<T: Debug> BTree<T> {
    pub fn new() -> Self {
        // create a boxed node for the root, then immediately unbox into raw
        let root = Box::into_raw(Box::new(SlottedLeaf::<T>::new()));
        Self {
            height: 0,
            root: root as Node,
            boo: PhantomData,
        }
    }

    fn grow_leaf(&mut self, separator: &str, right: Node) {
        let new_root = Box::into_raw(Box::new(SlottedBranch::<T>::new(
            self.root, right, separator,
        )));
        self.root = new_root as Node;
        self.height += 1;
    }

    fn grow_branch(&mut self, separator: &str, left: Node) {
        let new_root = Box::into_raw(Box::new(SlottedBranch::<T>::new(
            left, self.root, separator,
        )));
        self.root = new_root as Node;
        self.height += 1;
    }

    pub fn insert(&mut self, key: &str, value: *mut T) -> InsertResult {
        // we handle data behind opaque pointers. It's not interesting for us what is actually
        // inside
        let value = value as Node;

        if self.height == 0 {
            // root is a leaf
            let leaf_ptr = unsafe { &mut *(self.root as *mut SlottedLeaf<T>) };
            let res = leaf_ptr.insert(key, value);

            let InsertResultIntern::Split(separator, node) = res else {
                return res.into();
            };

            self.grow_leaf(&separator, node);
            return InsertResult::Inserted;
        }

        let branch_ptr = unsafe { &mut *(self.root as *mut SlottedBranch<T>) };
        let res = branch_ptr.insert(key, value, self.height);

        let InsertResultIntern::Split(separator, node) = res else {
            return res.into();
        };

        self.grow_branch(&separator, node);
        InsertResult::Inserted
    }

    pub fn get(&self, key: &str) -> Option<*mut T> {
        if self.height == 0 {
            let leaf = unsafe { &*(self.root as *mut SlottedLeaf<T>) };
            leaf.get(key)
        } else {
            let branch = unsafe { &*(self.root as *mut SlottedBranch<T>) };
            branch.get(key, self.height)
        }
    }

    fn drop_branch(branch: &mut SlottedBranch<T>, height: usize) {
        let (nodes, _) = branch.data.interpret(&branch.header);

        let last_ptr = branch
            .header
            .pointer
            .expect("Invalid Branch Layout")
            .as_ptr();

        if height == 1 {
            for node in nodes {
                let (_, ptr) = branch.data.get_heap_entry(&branch.header, node);
                let _boxed = unsafe { Box::from_raw(ptr as *mut SlottedLeaf<T>) };
            }

            let _boxed = unsafe { Box::from_raw(last_ptr as *mut SlottedLeaf<T>) };
        } else {
            for node in nodes {
                let (_, ptr) = branch.data.get_heap_entry(&branch.header, node);
                let branch_ref = unsafe { &mut *(ptr as *mut SlottedBranch<T>) };
                BTree::drop_branch(branch_ref, height - 1);
                let _boxed = unsafe { Box::from_raw(ptr as *mut SlottedBranch<T>) };
            }

            let branch_ref = unsafe { &mut *(last_ptr as *mut SlottedBranch<T>) };
            BTree::drop_branch(branch_ref, height - 1);
            let _boxed = unsafe { Box::from_raw(last_ptr as *mut SlottedBranch<T>) };
        }
    }

    pub fn get_height(&self) -> usize {
        self.height
    }

    fn count_branch(branch: &SlottedBranch<T>, height: usize) -> usize {
        if height == 1 {
            return branch.size() + 1;
        }

        let mut cnt = 0;
        for index in 0..branch.size() + 1 {
            let branch = unsafe { &*(branch.child_at(index) as *mut SlottedBranch<T>) };
            cnt += BTree::count_branch(branch, height - 1);
        }

        return cnt;
    }

    pub fn count_nodes(&self) -> usize {
        if self.height == 0 {
            return 1;
        }

        let branch = unsafe { &*(self.root as *mut SlottedBranch<T>) };
        return BTree::count_branch(branch, self.height);
    }
}

impl<T: Debug> Drop for BTree<T> {
    fn drop(&mut self) {
        if self.height == 0 {
            // just drop in place, leaves can clean up themselves
            let _boxed = unsafe { Box::from_raw(self.root as *mut SlottedLeaf<T>) };
        } else {
            let branch_ref = unsafe { &mut *(self.root as *mut SlottedBranch<T>) };
            BTree::drop_branch(branch_ref, self.height);
            let _boxed = unsafe { Box::from_raw(self.root as *mut SlottedBranch<T>) };
        }
    }
}

fn serialize_branch<T: Debug>(
    branch: &SlottedBranch<T>,
    height: usize,
    leaves: &mut String,
    branches: &mut String,
) {
    branches.push_str(&branch.print());

    if height == 1 {
        let (nodes, _) = branch.data.interpret(&branch.header);

        for node in nodes {
            let (_, ptr) = branch.data.get_heap_entry(&branch.header, node);
            let leaf = unsafe { &*(ptr as *mut SlottedLeaf<T>) };
            leaves.push_str(&leaf.print());
        }

        let leaf = unsafe {
            &*(branch
                .header
                .pointer
                .expect("Invalid Branch Layout")
                .as_ptr() as *mut SlottedLeaf<T>)
        };

        leaves.push_str(&leaf.print());

        return;
    }

    let (nodes, _) = branch.data.interpret(&branch.header);

    for node in nodes {
        let (_, ptr) = branch.data.get_heap_entry(&branch.header, node);
        let new_branch = unsafe { &*(ptr as *mut SlottedBranch<T>) };
        serialize_branch(new_branch, height - 1, leaves, branches);
    }

    let last_branch = unsafe {
        &*(branch
            .header
            .pointer
            .expect("Invalid Branch Layout")
            .as_ptr() as *mut SlottedBranch<T>)
    };

    serialize_branch(last_branch, height - 1, leaves, branches);
}

impl<T: Debug> Graphviz for BTree<T> {
    fn serialize(&self, f: &mut dyn io::Write) -> io::Result<()> {
        if self.height == 0 {
            let leaf = unsafe { &mut *(self.root as *mut SlottedLeaf<T>) };
            write!(f, "{}", leaf.print())?;
            return Ok(());
        }

        let branch = unsafe { &mut *(self.root as *mut SlottedBranch<T>) };
        let mut branches = String::new();
        let mut leaves = String::new();
        serialize_branch(branch, self.height, &mut leaves, &mut branches);

        // DOT has a *few* problems trying to draw our tree. If I put rank=same in there, the arrows
        // between the leaves don't draw, but the alternative is worse. You can remove it to see
        // them, but the tree hierarchy will be completely f*cked.
        write!(f, "{}\n{{rank=same\n{}\n}}", branches, leaves)
    }
}
