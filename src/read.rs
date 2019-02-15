use std::io;
use std::io::{Cursor, ErrorKind};
use std::iter::Chain;
use std::ops::Range;
use core::ops::Deref;

use super::*;
use byteorder::{ReadBytesExt, LE};

pub trait CDBAccess: Deref<Target=[u8]> {
    fn read_pair(&self, pos: usize) -> io::Result<(usize, usize)> {
        let data = self.get_data(pos, PAIR_SIZE)?;
        let mut cur = Cursor::new(data);

        Ok((
            cur.read_u32::<LE>()? as usize,
            cur.read_u32::<LE>()? as usize,
        ))
    }

    fn read_header(&self) -> io::Result<[PosLen; ENTRIES]> {
        let mut tables: [PosLen; ENTRIES] = [PosLen { pos: 0, len: 0 }; ENTRIES];
        let mut cur = Cursor::new(self.get_data(0, PAIR_SIZE * ENTRIES)?);

        for table in tables.iter_mut() {
            let (pos, len) = (
                cur.read_u32::<LE>()? as usize,
                cur.read_u32::<LE>()? as usize,
            );

            *table = PosLen { pos, len };
            if !table.valid(self.len()) {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "a hash table is beyond the end of this file",
                ));
            }
        }
        Ok(tables)
    }

    fn get_data(&self, pos: usize, len: usize) -> io::Result<&[u8]> {
        let res = self.get(pos..pos + len).ok_or_else(|| {
            io::Error::new(ErrorKind::UnexpectedEof, "tried to read beyond buffer")
        })?;
        Ok(res)
    }
}

use memmap::Mmap;
impl CDBAccess for Mmap {}
impl CDBAccess for Vec<u8> {}
impl CDBAccess for &[u8] {}

pub struct CDBReader<A> {
    access: A,
    tables: [PosLen; ENTRIES],
}

#[derive(Clone, Copy)]
pub struct IterState(usize);

impl Default for IterState {
    fn default() -> Self {
        IterState(ENTRIES * PAIR_SIZE)
    }
}

#[derive(Clone)]
struct FileIter<'a, A>
{
    cdb: &'a CDBReader<A>,
    state: IterState,
}

impl IterState
{
    #[inline]
    pub fn next<'c, A: CDBAccess>(&mut self, cdb: &'c CDBReader<A>) -> Option<io::Result<(&'c [u8], &'c [u8])>> {
        if self.0 < cdb.tables[0].pos {
            match cdb.get_data(self.0) {
                Ok((k, v, newpos)) => {
                    self.0 = newpos;
                    Some(Ok((k, v)))
                }
                Err(e) => {
                    self.0 = cdb.tables[0].pos;
                    Some(Err(e))
                }
            }
        } else {
            None
        }
    }
}

impl <'a, A: CDBAccess> Iterator for FileIter<'a, A> {
    type Item = io::Result<(&'a [u8], &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.next(self.cdb)
    }
}

#[derive(Clone)]
struct LookupIter<'c,  A>
{
    cdb: &'c CDBReader<A>,
    key: &'c [u8],
    state: LookupState,
}

impl <'c, A> LookupIter<'c, A> {
    fn new(cdb: &'c CDBReader<A>, key: &'c [u8]) -> Self {
        LookupIter {
            cdb,
            key,
            state: LookupState::new(cdb, key)
        }
    }
}

impl <'c, A: CDBAccess> Iterator for LookupIter<'c, A> {
    type Item = io::Result<&'c [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.next(self.cdb, self.key)
    }
}

#[derive(Clone)]
pub struct LookupState {
    table_pos: usize,
    khash: CDBHash,
    iter: Chain<Range<usize>, Range<usize>>,
    done: bool,
}

impl LookupState {
    #[inline]
    pub fn new<A>(cdb: &CDBReader<A>, key: &[u8]) -> Self {
        let khash = CDBHash::new(key);
        let table = cdb.tables[khash.table()];

        let start_pos = if table.len != 0 {
            khash.slot(table.len)
        } else {
            0
        };
        let iter = (start_pos..(table.len)).chain(0..start_pos);

        LookupState {
            khash,
            iter,
            table_pos: table.pos,
            done: false,
        }
    }

    #[inline]
    pub fn next<'a, A: CDBAccess>(&mut self, cdb: &'a CDBReader<A>, key: &[u8]) -> Option<io::Result<&'a [u8]>> {
        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = self.table_pos + tableidx * PAIR_SIZE;

            let (hash, ptr) = match cdb.access.read_pair(pos) {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            if ptr == 0 {
                self.done = true;
                return None;
            }

            if hash != self.khash.0 as usize {
                continue;
            }

            let (k, v, _) = match cdb.get_data(ptr as usize) {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            if k == key {
                return Some(Ok(v));
            }
        }
        self.done = true;
        None
    }
}

type KeyValueNext<'c> = (&'c [u8], &'c [u8], usize);

impl<A: CDBAccess> CDBReader<A> {
    pub fn new(access: A) -> io::Result<CDBReader<A>> {
        let tables = access.read_header()?;
        Ok(CDBReader { access, tables })
    }

    fn get_data(&self, pos: usize) -> io::Result<KeyValueNext<'_>> {
        let (klen, vlen) = self.access.read_pair(pos)?;

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        Ok((
            self.access.get_data(keystart, klen)?,
            self.access.get_data(keyend, vlen)?,
            valend,
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item=io::Result<(&'_ [u8], &'_ [u8])>>
    {
        FileIter{cdb: self, state: Default::default()}
    }

    pub fn lookup<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item=io::Result<&'_ [u8]>>
    {
        LookupIter::new(self, key)
    }

    pub fn get<'a>(&'a self, key: &'a [u8]) -> Option<io::Result<&'a[u8]>>
    {
        self.lookup(key).nth(0)
    }
}
