use std::fs::File;
use std::io::BufWriter;

use cordoba::Writer;

fn main() {
    let mut file = BufWriter::new(File::create("truc.cdb").unwrap());
    let mut cdb = Writer::new(&mut file).unwrap();

    for x in 0..10000 {
        let k = format!("#{:05} potato", x / 2);
        let v = format!("patate #{:05}", 10000 - x);
        cdb.write(k.as_bytes(), v.as_bytes()).unwrap();
    }
    cdb.finish().unwrap();
}
