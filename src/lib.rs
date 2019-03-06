#![cfg_attr(not(feature="std"), no_std)]
#![feature(try_from)]

use core::hash::Hasher;

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

pub trait CDBFormat
    where Self::Hash: CDBHasher + Into<usize> + Copy + Clone + PartialEq,
          Self::Hash: Into<u32> + From<u32>,
{
    const NB_TABLES: usize;
    type Hash;

    #[inline]
    fn table(hash: Self::Hash) -> usize {
        Into::<usize>::into(hash) % Self::NB_TABLES
    }

    #[inline]
    fn slot(hash: Self::Hash, tlen: usize) -> usize {
        (Into::<usize>::into(hash) >> 8) % tlen
    }
}


#[derive(Copy, Clone)]
pub struct ClassicFormat();

impl CDBFormat for ClassicFormat {
    const NB_TABLES: usize = 256;
    type Hash = CDBHash;
}

#[derive(Copy, Clone, PartialEq)]
pub struct CDBHash(u32);

pub trait CDBHasher: Hasher {
    type Output;

    fn hash(data: &[u8]) -> Self;
    fn value(self) -> Self::Output;
    fn zero() -> Self;
}

impl CDBHasher for CDBHash {
    type Output = u32;

    fn hash(data: &[u8]) -> Self {
        let mut h = Self::default();
        h.write(data);
        h
    }

    #[inline]
    fn value(self) -> Self::Output {
        self.0
    }

    fn zero() -> Self {
        Self(0)
    }
}

impl Into<usize> for CDBHash {
    #[inline]
    fn into(self) -> usize {
        self.0 as usize
    }
}

impl Hasher for CDBHash {
    fn write(&mut self, data: &[u8]) {
        self.0 = data.iter()
            .fold(self.0, |h, &c| (h << 5).wrapping_add(h) ^ u32::from(c));
    }

    fn finish(&self) -> u64 {
        self.0.into()
    }
}

impl Default for CDBHash {
    fn default() -> Self {
        CDBHash(5381)
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

impl From<u32> for CDBHash {
    fn from(h: u32) -> Self {
        CDBHash(h)
    }
}
