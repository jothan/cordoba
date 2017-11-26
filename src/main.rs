#![feature(iterator_step_by)]

extern crate memmap;
extern crate cdb;

use cdb::CDBReader;

use std::env;
use std::str;
//use std::fmt;
//use std::mem::size_of;
use std::fs::File;
//use std::io::prelude::*;

use memmap::Mmap;
use memmap::MmapOptions;

fn open(fname: &str) -> Mmap
{
    let file = File::open(fname).expect("file not found");
    unsafe { MmapOptions::new().map(&file).expect("mmap error") }
}

fn main() {
    let args : Vec<String> = env::args().collect();
    let fname = args.get(1).expect("First argument must be a CDB file.");
    let mmap = open(&fname);

    let cdb = CDBReader::new(&mmap).unwrap();

    for k in args[2..].iter() {
        let ks = k.as_bytes();
        for v in cdb.lookup(ks) {
            println!("v: {}", str::from_utf8(&v).unwrap());
        }

        match cdb.get(ks) {
            Some(v) => println!("{} = {}", k, str::from_utf8(&v).unwrap()),
            None => println!("{} not found", k),
        }
    }
    for (k, v) in &cdb {
        println!("{}: {}", str::from_utf8(&k).unwrap(), str::from_utf8(&v).unwrap());
    }
}
