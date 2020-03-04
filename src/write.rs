use std::mem;
use std::collections::BTreeSet;
use std::io::{Seek, SeekFrom, Write};

use super::*;

#[derive(Copy, Clone, Debug)]
struct HashPos(Hash, u32);

impl HashPos {
    #[inline]
    fn distance(self, tlen: usize, pos: usize) -> usize {
        let startslot = self.0.slot(tlen);
        pos.checked_sub(startslot).unwrap_or_else(|| pos + tlen - startslot)
    }
}

const FILLFACTOR: usize = 2;

pub struct Writer<T> {
    file: T,
    pos: u64,
    tables: Vec<Vec<HashPos>>,
    header: [PosLen; ENTRIES],
}

impl<T> Writer<T>
where
    T: Write + Seek,
{
    pub fn new(mut file: T) -> Result<Self, std::io::Error> {
        let pos = (ENTRIES * PAIR_SIZE) as u64;
        let mut tables = Vec::with_capacity(ENTRIES);
        file.seek(SeekFrom::Start(pos))?;

        for _ in 0..ENTRIES {
            tables.push(Vec::new());
        }
        Ok(Writer {
            file,
            pos,
            tables,
            header: [PosLen { pos: 0, len: 0 }; ENTRIES],
        })
    }

    fn write_kv(&mut self, k: &[u8], v: &[u8]) -> Result<(), std::io::Error> {
        self.file.write_all(&(k.len() as u32).to_le_bytes())?;
        self.file.write_all(&(v.len() as u32).to_le_bytes())?;
        self.file.write_all(k)?;
        self.file.write_all(v)?;

        self.pos += (PAIR_SIZE + k.len() + v.len()) as u64;

        Ok(())
    }

    pub fn write(&mut self, k: &[u8], v: &[u8]) -> Result<(), std::io::Error> {
        let hash = Hash::new(k);
        let tableidx = hash.table();

        self.tables[tableidx].push(HashPos(hash, self.pos as u32));

        self.write_kv(k, v)?;

        Ok(())
    }

    fn write_header(&mut self) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(0))?;

        for header in self.header.iter() {
            self.file.write_all(&(header.pos as u32).to_le_bytes())?;
            self.file.write_all(&(header.len as u32).to_le_bytes())?;
        }

        Ok(())
    }

    fn finish_generic<F>(mut self, fill: F) -> Result<(), std::io::Error>
    where
        F: Fn(&[HashPos], &mut Vec<HashPos>),
    {
        let mut tout = Vec::new();

        for (i, table) in self.tables.iter().enumerate() {
            fill(&table, &mut tout);
            self.header[i] = PosLen {
                pos: self.pos as usize,
                len: tout.len(),
            };
            for row in &tout {
                let hash : u32 = row.0.into();
                self.file.write_all(&hash.to_le_bytes())?;
                self.file.write_all(&row.1.to_le_bytes())?;
            }
            self.pos += (PAIR_SIZE * tout.len()) as u64;
        }
        self.write_header()?;
        self.file.flush()?;

        Ok(())
    }

    pub fn finish(self) -> Result<(), std::io::Error> {
        self.finish_robinhood()
    }

    pub fn finish_naive(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_naive)
    }

    pub fn finish_btree(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_btree)
    }

    pub fn finish_robinhood(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_robinhood)
    }

    pub fn into_file(self) -> T {
        self.file
    }

    pub fn get_file(&self) -> &T {
        &self.file
    }
}

fn fill_table_naive(input: &[HashPos], output: &mut Vec<HashPos>) {
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(Hash(0), 0));

    for hp in input {
        let (left, right) = output.split_at_mut(hp.0.slot(tlen));

        for slot in right.iter_mut().chain(left.iter_mut()) {
            if slot.1 == 0 {
                *slot = *hp;
                break;
            }
        }
    }
}

fn fill_table_btree(input: &[HashPos], output: &mut Vec<HashPos>) {
    let mut cache = BTreeSet::new();
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(Hash(0), 0));

    cache.extend(0..tlen);

    for hp in input {
        let startpos = hp.0.slot(tlen);
        let idx = *cache
            .range(startpos..)
            .chain(cache.range(0..startpos))
            .nth(0)
            .unwrap();
        cache.take(&idx);

        debug_assert_eq!(output[idx].1, 0);
        output[idx] = *hp;
    }
}

fn fill_table_robinhood(input: &[HashPos], output: &mut Vec<HashPos>) {
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(Hash(0), 0));

    for mut hp in input.iter().cloned() {
        let startslot = hp.0.slot(tlen);
        let (left, right) = output.split_at_mut(startslot);
        let mut slotnum = startslot;
        let mut distance = 0;

        for slot in right.iter_mut().chain(left.iter_mut()) {
            if slot.1 == 0 {
                *slot = hp;
                break;
            } else if slot.distance(tlen, slotnum) < distance {
                mem::swap(slot, &mut hp);
                distance = hp.distance(tlen, slotnum);
            }
            distance += 1;
            slotnum = (slotnum + 1) % tlen;
        }
    }
}
