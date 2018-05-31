#![cfg_attr(feature = "python", feature(specialization, proc_macro))]

extern crate byteorder;

mod read;
mod write;

#[cfg(feature = "python")]
#[macro_use]
extern crate pyo3;

#[cfg(feature = "python")]
mod pymod;

#[cfg(feature = "python")]
pub use pymod::PyInit_cordoba;

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
    fn valid(&self, datalen: u64) -> bool {
        (self.pos + self.len) as u64 <= datalen
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
