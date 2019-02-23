use std::mem;
use std::collections::BTreeSet;
use std::io::{Seek, SeekFrom, Write};

use super::*;

#[derive(Copy, Clone, Debug)]
pub struct HashPos<F: CDBFormat> (F::Hash, u32);

impl <F: CDBFormat> HashPos<F>
{
    #[inline]
    fn distance(self, tlen: usize, pos: usize) -> usize {
        let startslot = F::slot(self.0, tlen);
        pos.checked_sub(startslot).unwrap_or_else(|| pos + tlen - startslot)
    }
}

const FILLFACTOR: usize = 2;

pub struct CDBWriter<T, F=ClassicFormat>
    where F: CDBFormat,
{
    file: T,
    pos: u64,
    tables: Vec<Vec<HashPos<F>>>,
    header: [PosLen; ENTRIES],
}

impl<T, F> CDBWriter<T, F>
where
    T: Write + Seek,
    F: CDBFormat,
{
    pub fn new(mut file: T) -> Result<Self, std::io::Error> {
        let pos = (ENTRIES * PAIR_SIZE) as u64;
        let mut tables = Vec::with_capacity(ENTRIES);
        file.seek(SeekFrom::Start(pos))?;

        for _ in 0..ENTRIES {
            tables.push(Vec::new());
        }
        Ok(CDBWriter {
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
        let hash = F::Hash::hash(k);
        let tableidx = F::table(hash);

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

    fn finish_generic<A>(mut self, fill: A) -> Result<(), std::io::Error>
    where
        A: Fn(&[HashPos<F>], &mut Vec<HashPos<F>>),
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
        self.finish_generic(fill_table_naive::<F>)
    }

    pub fn finish_btree(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_btree::<F>)
    }

    pub fn finish_robinhood(self) -> Result<(), std::io::Error> {
        self.finish_generic(fill_table_robinhood::<F>)
    }

    pub fn into_file(self) -> T {
        self.file
    }

    pub fn get_file(&self) -> &T {
        &self.file
    }
}

fn fill_table_naive<F: CDBFormat>(input: &[HashPos<F>], output: &mut Vec<HashPos<F>>) {
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(F::Hash::zero(), 0));

    for hp in input {
        let (left, right) = output.split_at_mut(F::slot(hp.0, tlen));

        for slot in right.iter_mut().chain(left.iter_mut()) {
            if slot.1 == 0 {
                *slot = *hp;
                break;
            }
        }
    }
}

fn fill_table_btree<F: CDBFormat>(input: &[HashPos<F>], output: &mut Vec<HashPos<F>>) {
    let mut cache = BTreeSet::new();
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(F::Hash::zero(), 0));

    cache.extend(0..tlen);

    for hp in input {
        let startpos = F::slot(hp.0, tlen);
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

fn fill_table_robinhood<F: CDBFormat>(input: &[HashPos<F>], output: &mut Vec<HashPos<F>>) {
    let tlen = input.len() * FILLFACTOR;
    output.clear();
    output.resize(tlen, HashPos(F::Hash::zero(), 0));

    for mut hp in input.iter().cloned() {
        let startslot = F::slot(hp.0, tlen);
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
