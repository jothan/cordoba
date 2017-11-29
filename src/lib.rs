mod read;
mod write;

pub use self::read::{CDBReader, FileIter, LookupIter};
pub use self::write::{CDBWriter};

const ENTRIES: usize = 256;
const INT_SIZE: usize = 4;
const PAIR_SIZE: usize = 8;

#[derive(Copy, Clone)]
struct PosLen {
    pos: usize,
    len: usize,
}

impl PosLen {
    fn valid(&self, datalen : usize) -> bool
    {
        self.pos + self.len <= datalen
    }
}

struct CDBHash(u32);

impl CDBHash
{
    fn new(d: &[u8]) -> Self
    {
        let h = d.iter().fold(5381u32, |h, &c| (h << 5).wrapping_add(h) ^ u32::from(c));
        CDBHash(h)
    }

    fn table(&self) -> usize
    {
        self.0 as usize % ENTRIES
    }

    fn slot(&self, tlen: usize) -> usize
    {
        (self.0 as usize >> 8) % tlen
    }
}

impl std::fmt::Debug for CDBHash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CDBHash(0x{:08x})", self.0)
    }
}

impl PartialEq for CDBHash
{
    fn eq(&self, other: &Self) -> bool
    {
        self.0 == other.0
    }
}

impl<'a> From<&'a CDBHash> for u32
{
    fn from(h : &'a CDBHash) -> Self
    {
        h.0
    }
}

fn read_cdb_int(d: &[u8]) -> u32
{
    u32::from_le(u32::from(d[0]) |
                 u32::from(d[1]) << 8 |
                 u32::from(d[2]) << 16 |
                 u32::from(d[3]) << 24)
}

fn read_cdb_usize(d: &[u8]) -> usize
{
    read_cdb_int(d) as usize
}
