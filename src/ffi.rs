use std::usize;

use crate::btree::BTree;

type FfiBTree = *mut BTree<()>;

fn get_rust_string(string: *const u8, len: usize) -> &'static str {
    let slice = unsafe { std::slice::from_raw_parts(string, len) };
    unsafe { std::str::from_utf8_unchecked(slice) }
}

#[no_mangle]
pub extern "C" fn ffi_btree_new() -> FfiBTree {
    let boxed_tree = Box::new(BTree::new());

    Box::into_raw(boxed_tree)
}

#[no_mangle]
pub extern "C" fn ffi_btree_drop(tree: FfiBTree) {
    let _ = unsafe { Box::from_raw(tree) };
}

#[no_mangle]
pub extern "C" fn ffi_btree_insert(tree: FfiBTree, string: *const u8, len: usize, value: *mut ()) {
    let key = get_rust_string(string, len);

    let tree = unsafe { &mut *tree };
    let _res = tree.insert(key, value);
}

#[no_mangle]
pub extern "C" fn ffi_btree_get(tree: FfiBTree, string: *const u8, len: usize) -> *mut () {
    let key = get_rust_string(string, len);

    let tree = unsafe { &mut *tree };
    match tree.get(key) {
        Some(p) => p,
        None => std::ptr::null_mut(),
    }
}
