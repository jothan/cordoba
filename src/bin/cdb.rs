use std::fs::File;
use std::io::Write;

use cordoba::Reader;
use memmap::Mmap;

use clap::{App, Arg, ArgMatches, SubCommand};

fn cdb_open(fname: &str) -> std::io::Result<Reader<Mmap>>
{
    let f = File::open(fname)?;
    let map = unsafe { Mmap::map(&f) }?;
    Ok(Reader::new(map)?)
}

fn cmd_query(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = cdb_open(matches.value_of("cdbfile").unwrap())?;
    let key = matches.value_of("key").unwrap().as_bytes();
    let recno = matches.value_of("recno");
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    if let Some(recno) = recno {
        let recno : usize = recno.parse().unwrap();
        if recno == 0 {
            return Ok(())
        }

        if let Some(v) = reader.lookup(key).nth(recno - 1) {
            let v = v?;
            handle.write_all(&v)?;
            handle.write_all(b"\n")?;
            return Ok(());
        }
        return Ok(());
    }

    for v in reader.lookup(key) {
        let v = v?;
        handle.write_all(&v)?;
        handle.write_all(b"\n")?;
    }

    Ok(())
}

fn cmd_dump(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = cdb_open(matches.value_of("cdbfile").unwrap())?;

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    for res in &reader {
        let (k, v) = res?;
        handle.write_all(&k)?;
        handle.write_all(b" = ")?;
        handle.write_all(&v)?;
        handle.write_all(b"\n")?;
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let newline_arg = Arg::with_name("newline").short("m");
    let cdbfile_arg = Arg::with_name("cdbfile").index(1).required(true);

    let matches = App::new("cdb")
        .subcommand(
            SubCommand::with_name("-q")
                .about("query")
                .arg(newline_arg.clone())
                .arg(Arg::with_name("recno").short("n").takes_value(true))
                .arg(cdbfile_arg.clone())
                .arg(Arg::with_name("key").index(2).required(true)),
        )
        .subcommand(
            SubCommand::with_name("-d")
                .about("dump")
                .arg(newline_arg.clone())
                .arg(cdbfile_arg.clone()),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("-q") {
        cmd_query(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("-d") {
        cmd_dump(matches)?;
    }

    Ok(())
}
