use core::convert::TryInto;
use core::iter::Chain;
use core::ops::Range;

use super::*;

#[derive(Debug)]
pub enum CDBReadError {
    OutOfBounds,
    InvalidFile,
}

type CDBResult<T> = Result<T, CDBReadError>;

pub trait CDBAccess: AsRef<[u8]> {
    #[inline]
    fn read_pair(&self, pos: usize) -> CDBResult<(u32, u32)> {
        let data = self.get_data(pos, PAIR_SIZE)?;
        Ok((
            u32::from_le_bytes(data[0..4].try_into().unwrap()),
            u32::from_le_bytes(data[4..8].try_into().unwrap())
        ))
    }

    fn read_hash_pos(&self, pos: usize) -> CDBResult<(CDBHash, usize)> {
        let (hash, pos) = self.read_pair(pos)?;
        Ok((CDBHash(hash), pos as usize))
    }

    fn read_value_length(&self, pos: usize) -> CDBResult<(usize, usize)> {
        let (klen, vlen) = self.read_pair(pos)?;
        Ok((klen as usize, vlen as usize))
    }

    fn read_header(&self) -> CDBResult<[PosLen; ENTRIES]> {
        let mut tables: [PosLen; ENTRIES] = [PosLen { pos: 0, len: 0 }; ENTRIES];
        let header = self.get_data(0, PAIR_SIZE * ENTRIES)?;
        let mut header_chunks = header.chunks_exact(core::mem::size_of::<u32>());
        let mut empty = true;

        for table in tables.iter_mut() {
            *table = PosLen {
                pos: u32::from_le_bytes(header_chunks.next().unwrap().try_into().unwrap()) as usize,
                len: u32::from_le_bytes(header_chunks.next().unwrap().try_into().unwrap()) as usize,
            );

            if !table.valid(self.as_ref().len()) {
                return Err(CDBReadError::InvalidFile);
            }
            empty &= table.len == 0;
        }

        if empty {
            Err(CDBReadError::InvalidFile)
        } else {
            Ok(tables)
        }
    }

    fn get_data(&self, pos: usize, len: usize) -> CDBResult<&[u8]> {
        let res = self.as_ref().get(pos..pos + len).ok_or_else(|| {
            CDBReadError::OutOfBounds
        })?;
        Ok(res)
    }
}

impl <T: AsRef<[u8]>> CDBAccess for T {}

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
pub struct FileIter<'a, A>
{
    cdb: &'a CDBReader<A>,
    state: IterState,
}

impl IterState
{
    #[inline]
    pub fn next<'c, A: CDBAccess>(&mut self, cdb: &'c CDBReader<A>) -> Option<CDBResult<(&'c [u8], &'c [u8])>> {
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
    type Item = CDBResult<(&'a [u8], &'a [u8])>;

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
    type Item = CDBResult<&'c [u8]>;

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
    pub fn next<'a, A: CDBAccess>(&mut self, cdb: &'a CDBReader<A>, key: &[u8]) -> Option<CDBResult<&'a [u8]>> {
        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = self.table_pos + tableidx * PAIR_SIZE;

            let (hash, ptr) = match cdb.access.read_hash_pos(pos) {
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

            if hash != self.khash {
                continue;
            }

            let (k, v, _) = match cdb.get_data(ptr) {
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
    pub fn new(access: A) -> CDBResult<CDBReader<A>> {
        let tables = access.read_header()?;
        Ok(CDBReader { access, tables })
    }

    fn get_data(&self, pos: usize) -> CDBResult<KeyValueNext<'_>> {
        let (klen, vlen) = self.access.read_value_length(pos)?;

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        Ok((
            self.access.get_data(keystart, klen)?,
            self.access.get_data(keyend, vlen)?,
            valend,
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item=CDBResult<(&'_ [u8], &'_ [u8])>>
    {
        FileIter{cdb: self, state: Default::default()}
    }

    pub fn lookup<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item=CDBResult<&'_ [u8]>>
    {
        LookupIter::new(self, key)
    }

    pub fn get<'a>(&'a self, key: &'a [u8]) -> Option<CDBResult<&'a[u8]>>
    {
        self.lookup(key).nth(0)
    }
}

impl <'a, A: CDBAccess> IntoIterator for &'a CDBReader<A> {
    type IntoIter = FileIter<'a, A>;
    type Item = <FileIter<'a, A> as Iterator>::Item;

    fn into_iter(self) -> FileIter<'a, A>
    {
        FileIter{cdb: self, state: Default::default()}
    }
}

#[cfg(feature = "python")]
impl core::convert::From<CDBReadError> for pyo3::PyErr {
    fn from(error: CDBReadError) -> Self {
        match error {
            CDBReadError::OutOfBounds => pyo3::exceptions::EOFError::py_err("Tried to read beyond end of file."),
            CDBReadError::InvalidFile => pyo3::exceptions::IOError::py_err("Invalid file data."),
        }
    }
}

#[cfg(feature = "std")]
impl std::convert::From<CDBReadError> for std::io::Error {
    fn from(error: CDBReadError) -> Self {
        match error {
            CDBReadError::OutOfBounds => std::io::ErrorKind::UnexpectedEof,
            CDBReadError::InvalidFile => std::io::ErrorKind::InvalidData,
        }.into()
    }
}
