use byteorder;

mod read;
mod write;


pub use self::read::*;
pub use self::write::CDBWriter;

const ENTRIES: usize = 256;
const PAIR_SIZE: usize = 8;

#[derive(Copy, Clone)]
pub struct PosLen {
    pos: usize,
    len: usize,
}

impl PosLen {
    fn valid(&self, datalen: usize) -> bool {
        (self.pos + self.len) <= datalen
    }
}

#[derive(Copy, Clone)]
struct CDBHash(u32);

impl CDBHash {
    fn new(d: &[u8]) -> Self {
        let h = d
            .iter()
            .fold(5381u32, |h, &c| (h << 5).wrapping_add(h) ^ u32::from(c));
        CDBHash(h)
    }

    fn table(&self) -> usize {
        self.0 as usize % ENTRIES
    }

    fn slot(&self, tlen: usize) -> usize {
        (self.0 as usize >> 8) % tlen
    }
}

impl std::fmt::Debug for CDBHash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CDBHash(0x{:08x})", self.0)
    }
}

impl<'a> From<&'a CDBHash> for u32 {
    fn from(h: &'a CDBHash) -> Self {
        h.0
    }
}
