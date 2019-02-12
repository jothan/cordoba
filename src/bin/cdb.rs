use std::fs::File;
use std::io::Write;

use cordoba::CDBReader;
use memmap::Mmap;

use clap::{App, Arg, ArgMatches, SubCommand};

fn cdb_open(fname: &str) -> std::io::Result<CDBReader<Mmap>>
{
    let f = File::open(fname)?;
    let map = unsafe { Mmap::map(&f) }?;
    CDBReader::new(map)
}

fn cmd_query(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = cdb_open(matches.value_of("cdbfile").unwrap())?;
    let key = matches.value_of("key").unwrap().as_bytes();
    let recno = matches.value_of("recno");
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let mut lu = (&reader).lookup(key);

    if let Some(recno) = recno {
        let recno = recno.parse().unwrap();
        let mut n = 1;

        while let Some(v) = lu.next(key) {
            let v = v?;
            if n == recno {
                handle.write_all(&v)?;
                handle.write_all(b"\n")?;
                return Ok(());
            }
            n += 1
        }
        return Ok(());
    }

    while let Some(v) = lu.next(key) {
        let v = v?;
        handle.write_all(&v)?;
        handle.write_all(b"\n")?;
    }

    Ok(())
}

fn cmd_dump(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = cdb_open(matches.value_of("cdbfile").unwrap())?;
    let mut iter = (&reader).iter();

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    while let Some(res) = iter.next() {
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
