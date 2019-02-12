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

#[derive(Clone)]
pub struct FileIter<A, B>
    where B: Deref<Target=CDBReader<A>>
{
    cdb: B,
    pos: usize,
}

#[derive(Clone)]
pub struct LookupIter<A, B>
    where B: Deref<Target=CDBReader<A>>
{
    cdb: B,
    table_pos: usize,
    key: Vec<u8>, // FIXME
    khash: CDBHash,
    iter: Chain<Range<usize>, Range<usize>>,
    done: bool,
}

impl<A: CDBAccess, B> FileIter<A, B>
    where B: Deref<Target=CDBReader<A>>
{
    pub fn next<'a>(&'a mut self) -> Option<io::Result<(&'a [u8], &'a [u8])>> {
        if self.pos < self.cdb.tables[0].pos {
            match self.cdb.get_data(self.pos) {
                Ok((k, v, newpos)) => {
                    self.pos = newpos;
                    Some(Ok((k, v)))
                }
                Err(e) => {
                    self.pos = self.cdb.tables[0].pos;
                    Some(Err(e))
                }
            }
        } else {
            None
        }
    }
}

impl<A: CDBAccess, B> LookupIter<A, B>
    where B: Deref<Target=CDBReader<A>>,
{
    fn new(cdb: B, key: &[u8]) -> Self {
        let khash = CDBHash::new(key);
        let table = cdb.tables[khash.table()];

        let start_pos = if table.len != 0 {
            khash.slot(table.len)
        } else {
            0
        };
        let iter = (start_pos..(table.len)).chain(0..start_pos);

        LookupIter {
            cdb,
            key: key.to_vec(),
            khash,
            iter,
            table_pos: table.pos,
            done: false,
        }
    }

    pub fn next<'a>(&'a mut self) -> Option<io::Result<&'a [u8]>> {
        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = self.table_pos + tableidx * PAIR_SIZE;

            let (hash, ptr) = match self.cdb.access.read_pair(pos) {
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

            let (k, v, _) = match self.cdb.get_data(ptr as usize) {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            if k == self.key.as_slice() {
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

    fn get_data<'a>(&'a self, pos: usize) -> io::Result<KeyValueNext<'a>> {
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

    pub fn iter<B>(self: &B) -> FileIter<A, B>
        where B: Deref<Target=Self>,
              B: Clone
    {
        FileIter{
            cdb: self.clone(),
            pos: ENTRIES * PAIR_SIZE,
        }
    }

    pub fn lookup<B>(self: B, key: &[u8]) -> LookupIter<A, B>
        where B: Deref<Target=Self>,
    {
        LookupIter::new(self, key)
    }
}
