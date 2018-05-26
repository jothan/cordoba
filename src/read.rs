use std::cell::RefCell;
use std::io;
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};
use std::iter::Chain;
use std::ops::Range;

use super::*;
use byteorder::{ReadBytesExt, LE};

pub trait CDBAccess
where
    Self::Output: AsRef<[u8]>,
{
    type Output;
    fn read_pair(&self, pos: u64) -> io::Result<(usize, usize)> {
        let data = self.get_data(pos, PAIR_SIZE)?;
        let mut cur = Cursor::new(data);

        Ok((
            cur.read_u32::<LE>()? as usize,
            cur.read_u32::<LE>()? as usize,
        ))
    }

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Self::Output>;
    fn len(&self) -> u64;
}

impl<'c> CDBAccess for &'c [u8] {
    type Output = &'c [u8];

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Self::Output> {
        let pos = pos as usize;
        Ok(&self[pos..pos + len])
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

impl<T: Read + Seek> CDBFile<T> {
    fn new(mut file: T) -> io::Result<Self> {
        let size = file.seek(SeekFrom::End(0))?;
        Ok(CDBFile {
            file,
            pos: size,
            size,
        })
    }

    fn read(&mut self, pos: u64, out: &mut [u8]) -> io::Result<()> {
        if pos != self.pos {
            self.file.seek(SeekFrom::Start(pos as u64))?;
            self.pos = pos;
        }

        match self.file.read_exact(out) {
            Ok(_) => {
                self.pos += out.len() as u64;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

pub struct CDBFileAccess<T>(RefCell<CDBFile<T>>);

impl<T: Read + Seek> CDBFileAccess<T> {
    pub fn new(file: T) -> io::Result<Self> {
        Ok(CDBFileAccess(RefCell::new(CDBFile::new(file)?)))
    }
}

impl<T: Read + Seek> CDBAccess for CDBFileAccess<T> {
    type Output = Vec<u8>;

    fn get_data(&self, pos: u64, len: usize) -> io::Result<Self::Output> {
        let mut out = Vec::with_capacity(len);
        out.resize(len, 0);
        let mut file = self.0.borrow_mut();
        file.read(pos, &mut out).map(|_| out)
    }

    fn len(&self) -> u64 {
        self.0.borrow().size
    }
}

pub struct CDBReader<A> {
    pub access: A,
    tables: [PosLen; ENTRIES],
}

pub struct FileIter<'c, A: 'c> {
    pub cdb: &'c CDBReader<A>,
    pos: usize,
}

pub struct LookupIter<'c, 'k, A: 'c> {
    pub cdb: &'c CDBReader<A>,
    table_pos: usize,
    key: &'k [u8],
    khash: CDBHash,
    iter: Chain<Range<usize>, Range<usize>>,
    done: bool,
}

impl<'c, A: CDBAccess> Iterator for FileIter<'c, A> {
    type Item = io::Result<(A::Output, A::Output)>;

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

impl<'c, 'k, A: CDBAccess> LookupIter<'c, 'k, A> {
    fn new(cdb: &'c CDBReader<A>, key: &'k [u8]) -> Self {
        let khash = CDBHash::new(key);
        let table = &cdb.tables[khash.table()];

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

impl<'c, 'k, A: CDBAccess> Iterator for LookupIter<'c, 'k, A> {
    type Item = io::Result<A::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = (self.table_pos + tableidx * PAIR_SIZE) as u64;

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
        self.iter()
    }
}

type KeyValueNext<O> = (O, O, usize);

impl<A: CDBAccess> CDBReader<A> {
    pub fn new(access: A) -> io::Result<CDBReader<A>> {
        let mut tables: [PosLen; ENTRIES] = [PosLen { pos: 0, len: 0 }; ENTRIES];

        for (x, table) in tables.iter_mut().enumerate() {
            let (pos, len) = access.read_pair(x as u64 * PAIR_SIZE as u64)?;
            *table = PosLen { pos, len };
            if !table.valid(access.len()) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, ""));
            }
        }

        Ok(CDBReader { access, tables })
    }

    fn get_data(&self, pos: usize) -> io::Result<KeyValueNext<A::Output>> {
        let (klen, vlen) = self.access.read_pair(pos as u64)?;

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        Ok((
            self.access.get_data(keystart as u64, klen)?,
            self.access.get_data(keyend as u64, vlen)?,
            valend,
        ))
    }

    pub fn iter(&self) -> FileIter<A> {
        FileIter {
            cdb: self,
            pos: ENTRIES * PAIR_SIZE,
        }
    }

    pub fn lookup<'c, 'k>(&'c self, key: &'k [u8]) -> LookupIter<'c, 'k, A> {
        LookupIter::new(self, key)
    }

    pub fn get(&self, key: &[u8]) -> Option<io::Result<A::Output>> {
        self.lookup(key).nth(0)
    }
}

impl<B: Read + Seek> CDBReader<CDBFileAccess<B>> {
    pub fn from_file(file: B) -> io::Result<Self> {
        CDBReader::new(CDBFileAccess::new(file)?)
    }
}
