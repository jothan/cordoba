extern crate cdb;
extern crate memmap;

use std::fs::File;
use std::io::BufWriter;

use cdb::CDBWriter;

fn main()
{
    let mut file = BufWriter::new(File::create("truc.cdb").unwrap());
    let mut cdb = CDBWriter::new(&mut file).unwrap();

    for x in 0..10000 {
        let k = format!("#{:05} potato", x);
        let v = format!("patate #{:05}", 10000-x);
        cdb.write(k.as_bytes(), v.as_bytes()).unwrap();
    }
    cdb.finish().unwrap();

}
