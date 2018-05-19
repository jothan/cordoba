use super::*;

pub struct CDBReader<'c> {
    pub data: &'c [u8],
    tables: [PosLen; ENTRIES],
}

pub struct FileIter<'c> {
    pub cdb: &'c CDBReader<'c>,
    pos: usize,
}

pub struct LookupIter<'c, 'k> {
    pub cdb: &'c CDBReader<'c>,
    table: &'c PosLen,
    key: &'k [u8],
    khash: CDBHash,
    start_pos: usize,
    nlookups: usize,
    done: bool,
}

impl<'c> Iterator for FileIter<'c> {
    type Item = (&'c [u8], &'c [u8]);

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.pos < self.cdb.tables[0].pos {
            let (k, v, newpos) = self.cdb.get_data(self.pos);
            self.pos = newpos;
            Some((k, v))
        } else {
            None
        }
    }
}

impl<'c, 'k> LookupIter<'c, 'k> {
    fn new(cdb: &'c CDBReader, key: &'k [u8]) -> Self
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

impl<'c, 'k> Iterator for LookupIter<'c, 'k> {
    type Item = (&'c [u8]);

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

            let (hash, ptr) = read_cdb_pair(&self.cdb.data[pos..]);
            if ptr == 0 {
                self.done = true;
                return None;
            }

            if hash != self.khash.0 {
                continue;
            }

            let (k, v, _) = self.cdb.get_data(ptr as usize);
            if k == self.key {
                return Some(v);
            }
        }
    }
}

impl<'c> IntoIterator for &'c CDBReader<'c>
{
    type IntoIter = FileIter<'c>;
    type Item = <FileIter<'c> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter
    {
        self.iter()
    }
}

impl<'c> CDBReader<'c> {
    pub fn new(data: &[u8]) -> Result<CDBReader, &str>
    {
        let mut tables: [PosLen; ENTRIES] = [PosLen{pos: 0,  len: 0}; ENTRIES];

        for x in 0..ENTRIES {
            let (pos, len) = read_cdb_usize(&data[x*PAIR_SIZE..]);
            tables[x] = PosLen{pos, len};
            if !tables[x].valid(data.len()) {
                return Err("Invalid CDB file.");
            }
        }

        Ok(CDBReader{data, tables})
    }

    fn get_data(&self, pos: usize) -> (&[u8], &[u8], usize)
    {
        let (klen, vlen) = read_cdb_usize(&self.data[pos..]);

        let keystart = pos + PAIR_SIZE;
        let keyend = keystart + klen;
        let valend = keyend + vlen;

        (&self.data[keystart..keyend], &self.data[keyend..valend], valend)
    }

    pub fn iter(&self) -> FileIter
    {
        FileIter{cdb: self, pos: ENTRIES*PAIR_SIZE}
    }

    pub fn lookup<'k>(&'c self, key: &'k [u8]) -> LookupIter<'c, 'k>
    {
        LookupIter::new(self, key)
    }

    pub fn get<'k>(&'c self, key: &'k [u8]) -> Option<&[u8]>
    {
        self.lookup(key).nth(0)
    }
}
