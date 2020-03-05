use core::convert::TryInto;
use core::iter::Chain;
use core::ops::Range;

use super::*;

#[derive(Debug)]
pub enum ReadError {
    OutOfBounds,
    InvalidFile,
}

type CDBResult<T> = Result<T, ReadError>;
pub trait CDBAccess: AsRef<[u8]> {}
impl <T: AsRef<[u8]>> CDBAccess for T {}

pub struct Reader<A> {
    access: A,
    tables: [PosLen; ENTRIES],
}

impl<A> std::fmt::Debug for Reader<A> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "Reader {{}}")
    }
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
    cdb: &'a Reader<A>,
    state: IterState,
}

impl <'a, A> FileIter<'a, A> {
    fn new(cdb: &'a Reader<A>) -> Self {
        FileIter{cdb, state: Default::default()}
    }
}

impl IterState
{
    #[inline]
    pub fn next<'c, A: CDBAccess>(&mut self, cdb: &'c Reader<A>) -> Option<CDBResult<(&'c [u8], &'c [u8])>> {
        if self.0 < cdb.tables[0].pos {
            match cdb.get_key_and_value(self.0) {
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
    cdb: &'c Reader<A>,
    key: &'c [u8],
    state: LookupState,
}

impl <'c, A> LookupIter<'c, A> {
    fn new(cdb: &'c Reader<A>, key: &'c [u8]) -> Self {
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
    khash: Hash,
    iter: Chain<Range<usize>, Range<usize>>,
    done: bool,
}

impl LookupState {
    #[inline]
    pub fn new<A>(cdb: &Reader<A>, key: &[u8]) -> Self {
        let khash = Hash::new(key);
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
    pub fn next<'a, A: CDBAccess>(&mut self, cdb: &'a Reader<A>, key: &[u8]) -> Option<CDBResult<&'a [u8]>> {
        if self.done {
            return None;
        }

        while let Some(tableidx) = self.iter.next() {
            let pos = self.table_pos + tableidx * PAIR_SIZE;

            let (hash, ptr) = match cdb.read_hash_pos(pos) {
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

            let (k, v, _) = match cdb.get_key_and_value(ptr) {
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

impl<A: CDBAccess> Reader<A> {
    pub fn new(access: A) -> CDBResult<Reader<A>> {
        let tables = Self::read_header(&access)?;
        Ok(Reader { access, tables })
    }

    fn get_key_and_value(&self, pos: usize) -> CDBResult<KeyValueNext<'_>> {
        let (klen, vlen) = self.read_value_length(pos)?;

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        Ok((
            Self::get_data(&self.access, keystart, klen)?,
            Self::get_data(&self.access, keyend, vlen)?,
            valend,
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item=CDBResult<(&'_ [u8], &'_ [u8])>>
    {
        FileIter::new(self)
    }

    pub fn lookup<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item=CDBResult<&'_ [u8]>>
    {
        LookupIter::new(self, key)
    }

    pub fn get<'a>(&'a self, key: &'a [u8]) -> Option<CDBResult<&'a[u8]>>
    {
        self.lookup(key).nth(0)
    }

    #[inline]
    fn read_pair(&self, pos: usize) -> CDBResult<(u32, u32)> {
        let data = Self::get_data(&self.access, pos, PAIR_SIZE)?;
        Ok((
            u32::from_le_bytes(data[0..4].try_into().unwrap()),
            u32::from_le_bytes(data[4..8].try_into().unwrap())
        ))
    }

    fn read_hash_pos(&self, pos: usize) -> CDBResult<(Hash, usize)> {
        let (hash, pos) = self.read_pair(pos)?;
        Ok((Hash(hash), pos as usize))
    }

    fn read_value_length(&self, pos: usize) -> CDBResult<(usize, usize)> {
        let (klen, vlen) = self.read_pair(pos)?;
        Ok((klen as usize, vlen as usize))
    }

    fn read_header(access: &A) -> CDBResult<[PosLen; ENTRIES]> {
        let mut tables: [PosLen; ENTRIES] = [PosLen { pos: 0, len: 0 }; ENTRIES];
        let header = Self::get_data(&access, 0, PAIR_SIZE * ENTRIES)?;
        let mut header_chunks = header.chunks_exact(core::mem::size_of::<u32>());
        let mut empty = true;

        for table in tables.iter_mut() {
            *table = PosLen {
                pos: u32::from_le_bytes(header_chunks.next().unwrap().try_into().unwrap()) as usize,
                len: u32::from_le_bytes(header_chunks.next().unwrap().try_into().unwrap()) as usize,
            };

            if !table.valid(access.as_ref().len()) {
                return Err(ReadError::InvalidFile);
            }
            empty &= table.len == 0;
        }

        if empty {
            Err(ReadError::InvalidFile)
        } else {
            Ok(tables)
        }
    }

    fn get_data(access: &A, pos: usize, len: usize) -> CDBResult<&[u8]> {
        let res = access.as_ref().get(pos..pos + len).ok_or_else(|| {
            ReadError::OutOfBounds
        })?;
        Ok(res)
    }
}

#[cfg(feature = "std")]
pub trait CDBRead
{
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=CDBResult<(&'a [u8], &'a [u8])>> + 'a>;
    fn lookup<'a>(&'a self, key: &'a [u8]) -> Box<dyn Iterator<Item=CDBResult<&'a [u8]>> + 'a>;
    fn get<'a>(&'a self, key: &'a [u8]) -> Option<CDBResult<&'a[u8]>>;
}

#[cfg(feature = "std")]
impl <A: CDBAccess> CDBRead for Reader<A> {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=CDBResult<(&'a [u8], &'a [u8])>> + 'a>
    {
        Box::new(Reader::iter(self))
    }

    fn lookup<'a>(&'a self, key: &'a [u8]) -> Box<dyn Iterator<Item=CDBResult<&'a [u8]>> + 'a>
    {
        Box::new(Reader::lookup(self, key))
    }

    fn get<'a>(&'a self, key: &'a [u8]) -> Option<CDBResult<&'a[u8]>>
    {
        Reader::get(self, key)
    }
}

impl <'a, A: CDBAccess> IntoIterator for &'a Reader<A> {
    type IntoIter = FileIter<'a, A>;
    type Item = <FileIter<'a, A> as Iterator>::Item;

    fn into_iter(self) -> FileIter<'a, A>
    {
        FileIter::new(self)
    }
}

#[cfg(feature = "python")]
impl core::convert::From<ReadError> for pyo3::PyErr {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::OutOfBounds => pyo3::exceptions::EOFError::py_err("Tried to read beyond end of file."),
            ReadError::InvalidFile => pyo3::exceptions::IOError::py_err("Invalid file data."),
        }
    }
}

#[cfg(feature = "std")]
impl std::convert::From<ReadError> for std::io::Error {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::OutOfBounds => std::io::ErrorKind::UnexpectedEof,
            ReadError::InvalidFile => std::io::ErrorKind::InvalidData,
        }.into()
    }
}

#[cfg(feature = "std")]
impl std::fmt::Display for ReadError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            ReadError::OutOfBounds => write!(fmt, "Index out of bounds"),
            ReadError::InvalidFile => write!(fmt, "Invalid CDB file"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ReadError {}
