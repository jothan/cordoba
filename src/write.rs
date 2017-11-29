use std::io::{Write, Seek, SeekFrom};

use super::*;

//trait WriteFile : Write + Seek;

#[derive(Debug)]
struct HashPos(CDBHash, u32);

const FILLFACTOR: usize = 2;

pub struct CDBWriter<'c, T: 'c> {
    pub file : &'c mut T,
    pos: u64,
    tables: Vec<Vec<HashPos>>,
    header: [PosLen; ENTRIES],
}

fn usize_to_bytes(sz: usize) -> [u8; 4]
{
    let mut sz = u32::to_le(sz as u32);
    let mut v : [u8; 4] = [0; 4];

    v[0] = sz as u8;
    sz >>= 8;
    v[1] = sz as u8;
    sz >>= 8;
    v[2] = sz as u8;
    sz >>= 8;
    v[3] = sz as u8;

    v
}

impl<'c, T: Write + Seek> CDBWriter<'c, T>
{
    pub fn new(file: &'c mut T) -> Self
    {
        let pos = (ENTRIES*PAIR_SIZE) as u64;
        let mut tables = Vec::with_capacity(ENTRIES);
        file.seek(SeekFrom::Start(pos)).unwrap();

        for _ in 0..ENTRIES {
            tables.push(Vec::new());
        }
        CDBWriter{file, pos, tables, header: [PosLen{pos:0, len:0}; ENTRIES]}
    }

    fn write_kv(&mut self, k: &[u8], v: &[u8]) -> Result<(), std::io::Error>
    {
        self.file.write_all(&usize_to_bytes(k.len())[..])?;
        self.file.write_all(&usize_to_bytes(v.len())[..])?;
        self.file.write_all(k)?;
        self.file.write_all(v)?;

        self.pos += (PAIR_SIZE + k.len() + v.len()) as u64;

        Ok(())
    }

    pub fn write(&mut self, k: &[u8], v: &[u8]) -> Result<(), std::io::Error>
    {
        let hash = CDBHash::new(k);
        let tableidx = hash.table();

        self.tables[tableidx].push(HashPos(hash, self.pos as u32));

        self.write_kv(k, v)?;

        Ok(())
    }

    fn write_header(&mut self) -> Result<(), std::io::Error>
    {
        self.file.seek(SeekFrom::Start(0))?;

        for header in self.header.iter() {
            self.file.write_all(&usize_to_bytes(header.pos as usize)[..])?;
            self.file.write_all(&usize_to_bytes(header.len as usize)[..])?;
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), std::io::Error>
    {
        let mut tout = Vec::<[u32; 2]>::new();

        for (i, table) in self.tables.iter().enumerate() {
            let tlen = table.len() * FILLFACTOR;
            tout.resize(tlen, [0, 0]);
            for x in &mut tout { *x = [0, 0]; }

            for hp in table {
                let startpos = hp.0.slot(tlen);
                for try in 0..tlen {
                    let idx = (startpos + try) % tlen;
                    if tout[idx][1] == 0 {
                        tout[idx][0] = u32::from(&hp.0);
                        tout[idx][1] = hp.1;
                        break;
                    }
                }
            }
            self.header[i] = PosLen{pos: self.pos as usize, len: tout.len()};
            self.header[i] = PosLen{pos: self.pos as usize, len: tout.len()};
            for row in &tout {
                self.file.write_all(&usize_to_bytes(row[0] as usize)[..])?;
                self.file.write_all(&usize_to_bytes(row[1] as usize)[..])?;
            }
            self.pos += (PAIR_SIZE * tout.len()) as u64;
        }
        self.write_header()?;
        self.file.flush()?;

        Ok(())
    }
}
