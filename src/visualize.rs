use std::io;

pub trait Graphviz {
    fn serialize(&self, f: &mut dyn io::Write) -> io::Result<()>;
}
