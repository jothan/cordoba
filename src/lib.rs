#![cfg_attr(not(feature="std"), no_std)]
#![warn(rust_2018_idioms)]

mod read;

#[cfg(feature = "std")]
mod write;
#[cfg(feature = "std")]
pub use self::write::Writer;

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
pub struct Hash(pub u32);

impl Hash {
    fn new(d: &[u8]) -> Self {
        let h = d
            .iter()
            .fold(5381u32, |h, &c| (h << 5).wrapping_add(h) ^ u32::from(c));
        Hash(h)
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

impl core::fmt::Debug for Hash {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Hash(0x{:08x})", self.0)
    }
}

impl From<Hash> for u32 {
    fn from(h: Hash) -> Self {
        h.0
    }
}
