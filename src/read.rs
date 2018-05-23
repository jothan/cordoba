use std::cell::RefCell;
use std::io;
use std::io::{Cursor, SeekFrom};
use std::io::prelude::*;
use std::borrow::Cow;

use byteorder::{LE, ReadBytesExt};
use super::*;

pub trait CDBAccess {
    fn read_pair_u32(&self, pos: u64) -> io::Result<(u32, u32)>;

    fn read_pair(&self, pos: u64) -> io::Result<(usize, usize)> {
        let r = self.read_pair_u32(pos)?;
        Ok((r.0 as usize, r.1 as usize))
    }

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Cow<[u8]>>;

    fn len(&self) -> u64;
}

impl <'c> CDBAccess for &'c[u8] {
    fn read_pair_u32(&self, pos: u64) -> io::Result<(u32, u32)> {
        let mut rdr = Cursor::new(&self[pos as usize..]);
        Ok((rdr.read_u32::<LE>()?, rdr.read_u32::<LE>()?))
    }

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Cow<[u8]>> {
        let pos = pos as usize;
        Ok(Cow::from(&self[pos..pos+len]))
    }

    fn len(&self) -> u64 {
        (*self).len() as u64
    }
}

struct CDBFile<T> {
    file: T,
    pos: u64,
    size: u64,
}

impl <T: Read + Seek> CDBFile<T> {
    fn new(mut file: T) -> io::Result<Self> {
        let size = file.seek(SeekFrom::End(0))?;
        Ok(CDBFile{file, pos: size, size})
    }

    fn read(&mut self, pos: u64, out: &mut [u8]) -> io::Result<()> {
        if pos != self.pos {
            self.file.seek(SeekFrom::Start(pos as u64))?;
            self.pos = pos;
        }

        match self.file.read_exact(out) {
            Ok(_) => { self.pos += out.len() as u64; Ok(()) }
            Err(err) => Err(err),
        }
    }
}

pub struct CDBFileAccess<T> (
    RefCell<CDBFile<T>>
);

impl <T: Read + Seek> CDBFileAccess<T> {
    pub fn new(file: T) -> io::Result<Self> {
        Ok(CDBFileAccess(RefCell::new(CDBFile::new(file)?)))
    }
}

impl <T: Read + Seek> CDBAccess for CDBFileAccess<T> {
    fn read_pair_u32(&self, pos: u64) -> io::Result<(u32, u32)> {
        let mut file = self.0.borrow_mut();
        let mut buf : [u8; PAIR_SIZE] = [0; PAIR_SIZE];
        file.read(pos, &mut buf)?;

        let mut cur = Cursor::new(&buf);
        Ok((cur.read_u32::<LE>()?, cur.read_u32::<LE>()?))
    }

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Cow<[u8]>> {
        let mut out = Vec::with_capacity(len);
        out.resize(len, 0);
        let mut file = self.0.borrow_mut();
        file.read(pos, &mut out).map(|_| Cow::from(out))
    }

    fn len(&self) -> u64 {
        self.0.borrow().size
    }
}

pub struct CDBReader<T> {
    pub access: T,
    tables: [PosLen; ENTRIES],
}

pub struct FileIter<'c, T: 'c> {
    pub cdb: &'c CDBReader<T>,
    pos: usize,
}

pub struct LookupIter<'c, 'k, T: 'c> {
    pub cdb: &'c CDBReader<T>,
    table: &'c PosLen,
    key: &'k [u8],
    khash: CDBHash,
    start_pos: usize,
    nlookups: usize,
    done: bool,
}

impl<'c, T: CDBAccess> Iterator for FileIter<'c, T> {
    type Item = (Cow<'c, [u8]>, Cow<'c, [u8]>);

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.pos < self.cdb.tables[0].pos {
            let (k, v, newpos) = self.cdb.get_data(self.pos).unwrap();
            self.pos = newpos;
            Some((k, v))
        } else {
            None
        }
    }
}

impl<'c, 'k, T: CDBAccess> LookupIter<'c, 'k, T> {
    fn new(cdb: &'c CDBReader<T>, key: &'k [u8]) -> Self
    {
        let khash = CDBHash::new(key);
        let table = &cdb.tables[khash.table()];

        let start_pos = if table.len != 0 {
            khash.slot(table.len)
        } else {
            0
        };

        LookupIter{cdb, table, key, khash, start_pos, nlookups: 0, done: false}
    }
}

impl<'c, 'k, T: CDBAccess> Iterator for LookupIter<'c, 'k, T> {
    type Item = Cow<'c, [u8]>;

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.done {
            return None;
        }

        if self.table.len == 0 {
            self.done = true;
            return None;
        }

        loop {
            let tableidx = (self.start_pos + self.nlookups) % self.table.len;
            let pos = self.table.pos + tableidx*PAIR_SIZE;

            if tableidx == self.start_pos && self.nlookups != 0 {
                self.done = true;
                return None;
            }
            self.nlookups += 1;

            let (hash, ptr) = self.cdb.access.read_pair(pos as u64).unwrap();
            if ptr == 0 {
                self.done = true;
                return None;
            }

            if hash != self.khash.0 as usize {
                continue;
            }

            let (k, v, _) = self.cdb.get_data(ptr as usize).unwrap();
            if k == self.key {
                return Some(v);
            }
        }
    }
}

impl<'c, T: CDBAccess> IntoIterator for &'c CDBReader<T>
{
    type IntoIter = FileIter<'c, T>;
    type Item = <FileIter<'c, T> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter
    {
        self.iter()
    }
}


type KeyValueNext<'a> = (Cow<'a, [u8]>, Cow<'a, [u8]>, usize);

impl<A: CDBAccess> CDBReader<A> {
    pub fn new(access: A) -> io::Result<CDBReader<A>>
    {
        let mut tables: [PosLen; ENTRIES] = [PosLen{pos: 0,  len: 0}; ENTRIES];

        for (x, table) in tables.iter_mut().enumerate() {
            let (pos, len) = access.read_pair(x as u64*PAIR_SIZE as u64)?;
            *table = PosLen{pos, len};
            if !table.valid(access.len()) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, ""));
            }
        }

        Ok(CDBReader{access, tables})
    }

    fn get_data(&self, pos: usize) -> io::Result<KeyValueNext>
    {
        let (klen, vlen) = self.access.read_pair(pos as u64)?;

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        Ok((self.access.get_data(keystart as u64, klen)?,
            self.access.get_data(keyend as u64, vlen)?, valend))
    }

    pub fn iter(&self) -> FileIter<A>
    {
        FileIter{cdb: self, pos: ENTRIES*PAIR_SIZE}
    }

    pub fn lookup<'c, 'k>(&'c self, key: &'k [u8]) -> LookupIter<'c, 'k, A>
    {
        LookupIter::new(self, key)
    }

    pub fn get<'c, 'k>(&'c self, key: &'k [u8]) -> Option<Cow<'c, [u8]>>
    {
        self.lookup(key).nth(0)
    }
}

impl <B: Read + Seek> CDBReader<CDBFileAccess<B>>
{
    pub fn from_file(file: B) -> io::Result<Self>
    {
        CDBReader::new(CDBFileAccess::new(file)?)
    }
}
