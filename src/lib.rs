mod read;

pub use self::read::{CDBReader, FileIter, LookupIter};

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
        let h = d.iter().fold(5381u32, |h, &c| (h << 5).wrapping_add(h) ^ (c as u32));
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

impl PartialEq for CDBHash
{
    fn eq(&self, other: &Self) -> bool
    {
        return self.0 == other.0;
    }
}

impl<'a> From<&'a CDBHash> for u32
{
    fn from(h : &'a CDBHash) -> Self
    {
        return h.0;
    }
}

fn read_cdb_int(d: &[u8]) -> u32
{
    u32::from_le((d[0] as u32) | (d[1] as u32) << 8 |  (d[2] as u32) << 16 | (d[3] as u32) << 24)
}

fn read_cdb_usize(d: &[u8]) -> usize
{
    read_cdb_int(d) as usize
}
