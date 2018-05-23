use std::collections::BTreeSet;
use std::io::{Write, Seek, SeekFrom};

use byteorder::{LE, WriteBytesExt};

use super::*;

#[derive(Debug)]
struct HashPos(CDBHash, u32);

const FILLFACTOR: usize = 2;

pub struct CDBWriter<T> {
    file : T,
    pos: u64,
    tables: Vec<Vec<HashPos>>,
    header: [PosLen; ENTRIES],
}

impl<T> CDBWriter<T> where T: Write + Seek
{
    pub fn new(mut file: T) -> Result<Self, std::io::Error>
    {
        let pos = (ENTRIES*PAIR_SIZE) as u64;
        let mut tables = Vec::with_capacity(ENTRIES);
        file.seek(SeekFrom::Start(pos))?;

        for _ in 0..ENTRIES {
            tables.push(Vec::new());
        }
        Ok(CDBWriter{file, pos, tables, header: [PosLen{pos:0, len:0}; ENTRIES]})
    }

    fn write_kv(&mut self, k: &[u8], v: &[u8]) -> Result<(), std::io::Error>
    {
        self.file.write_u32::<LE>(k.len() as u32)?;
        self.file.write_u32::<LE>(v.len() as u32)?;
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
            self.file.write_u32::<LE>(header.pos as u32)?;
            self.file.write_u32::<LE>(header.len as u32)?;
        }

        Ok(())
    }

    fn finish_generic<F: Fn(&Vec<HashPos>, &mut Vec<(u32, u32)>)>(mut self, filler: F) -> Result<(), std::io::Error> {
        let mut tout = Vec::new();

        for (i, table) in self.tables.iter().enumerate() {
            filler(&table, &mut tout);
            self.header[i] = PosLen{pos: self.pos as usize, len: tout.len()};
            for row in &tout {
                self.file.write_u32::<LE>(row.0 as u32)?;
                self.file.write_u32::<LE>(row.1 as u32)?;
            }
            self.pos += (PAIR_SIZE * tout.len()) as u64;
        }
        self.write_header()?;
        self.file.flush()?;

        Ok(())
    }

    pub fn finish(self) -> Result<(), std::io::Error> {
        self.finish_btree()
    }

    pub fn finish_naive(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_naive)
    }

    pub fn finish_btree(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_btree)
    }

    pub fn get_file(self) -> T {
        self.file
    }
}

fn fill_table_naive(input: &Vec<HashPos>, output: &mut Vec<(u32, u32)>) {
    let tlen = input.len() * FILLFACTOR;
    output.resize(tlen, (0, 0));
    for x in output.iter_mut() { *x = (0, 0); }

    for hp in input {
        let startpos = hp.0.slot(tlen);
        for try in 0..tlen {
            let idx = (startpos + try) % tlen;
            if output[idx].1 == 0 {
                output[idx] = (u32::from(&hp.0), hp.1);
                break;
            }
        }
    }
}

fn fill_table_btree(input: &Vec<HashPos>, output: &mut Vec<(u32, u32)>) {
    let mut cache = BTreeSet::new();
    let tlen = input.len() * FILLFACTOR;
    output.resize(tlen, (0, 0));
    for x in output.iter_mut() { *x = (0, 0); }

    for i in 0..tlen { cache.insert(i); }

    for hp in input {
        let startpos = hp.0.slot(tlen);
        let idx = *cache.range(startpos..).chain(cache.range(0..startpos)).nth(0).unwrap();
        cache.take(&idx);

        debug_assert!(output[idx].1 == 0);
        output[idx] = (u32::from(&hp.0), hp.1);
    }
}
