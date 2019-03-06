#![cfg_attr(not(feature="std"), no_std)]

mod read;

#[cfg(feature = "std")]
mod write;
#[cfg(feature = "std")]
pub use self::write::CDBWriter;

pub use self::read::*;

const ENTRIES: usize = 256;
const PAIR_SIZE: usize = 8;

#[derive(Copy, Clone)]
pub struct PosLen {
    pos: usize,
    len: usize,
}

impl PosLen {
    fn valid(&self, datalen: usize) -> bool {
        let data_sz = self.len.checked_mul(PAIR_SIZE);
        let data_end = data_sz.and_then(|sz| sz.checked_add(self.pos));

        match data_end {
            None => false,
            Some(end) => end <= datalen
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub struct CDBHash(pub u32);

impl CDBHash {
    fn new(d: &[u8]) -> Self {
        let h = d
            .iter()
            .fold(5381u32, |h, &c| (h << 5).wrapping_add(h) ^ u32::from(c));
        CDBHash(h)
    }

    #[inline]
    fn table(self) -> usize {
        self.0 as usize % ENTRIES
    }

    #[inline]
    fn slot(self, tlen: usize) -> usize {
        (self.0 as usize >> 8) % tlen
    }
}

impl core::fmt::Debug for CDBHash {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "CDBHash(0x{:08x})", self.0)
    }
}

impl From<CDBHash> for u32 {
    fn from(h: CDBHash) -> Self {
        h.0
    }
}
