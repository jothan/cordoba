use std::borrow::{Borrow};
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
pub struct FileIter<'c, A> {
    cdb: &'c CDBReader<A>,
    pos: usize,
}

#[derive(Clone)]
struct LookupIter<'c, 'k, A>
{
    cdb: &'c CDBReader<A>,
    table_pos: usize,
    key: &'k [u8],
    khash: CDBHash,
    iter: Chain<Range<usize>, Range<usize>>,
    done: bool,
}

impl<'c, A: CDBAccess + 'c> Iterator for FileIter<'c, A> {
    type Item = io::Result<(&'c [u8], &'c [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
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

impl<'c, 'k, A: CDBAccess> LookupIter<'c, 'k, A>
    where A: CDBAccess,
{
    fn new(cdb: &'c CDBReader<A>, key: &'k [u8]) -> Self {
        let cdb_ref = cdb.borrow();
        let khash = CDBHash::new(key);
        let table = &cdb_ref.tables[khash.table()];

        let start_pos = if table.len != 0 {
            khash.slot(table.len)
        } else {
            0
        };
        let iter = (start_pos..(table.len)).chain(0..start_pos);

        LookupIter {
            cdb,
            key,
            khash,
            iter,
            table_pos: table.pos,
            done: false,
        }
    }
}

impl<'c, 'k, A: CDBAccess> Iterator for LookupIter<'c, 'k, A>
{
    type Item = io::Result<&'c [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let cdb_ref = self.cdb.borrow();

        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = self.table_pos + tableidx * PAIR_SIZE;

            let (hash, ptr) = match cdb_ref.access.read_pair(pos) {
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

            let (k, v, _) = match cdb_ref.get_data(ptr as usize) {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            if k.as_ref() == self.key {
                return Some(Ok(v));
            }
        }
        self.done = true;
        None
    }
}

impl<'c, A: CDBAccess> IntoIterator for &'c CDBReader<A> {
    type IntoIter = FileIter<'c, A>;
    type Item = <FileIter<'c, A> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        FileIter {
            cdb: self,
            pos: ENTRIES * PAIR_SIZE,
        }
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

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = io::Result<(&'a [u8], &'a [u8])>> {
        self.into_iter()
    }

    pub fn lookup<'k, 'c: 'k>(&'c self, key: &'k [u8]) -> impl Iterator<Item = io::Result<&'c [u8]>> + 'k
    {
        LookupIter::new(self, key)
    }

    pub fn get<'c, 'k>(&'c self, key: &'k [u8]) -> Option<io::Result<&'c [u8]>> {
        self.lookup(key).nth(0)
    }
}
