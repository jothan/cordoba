use std::fs::File;
use std::io::Write;
use std::sync::Arc;

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

    if let Some(recno) = recno {
        let recno = recno.parse::<usize>().unwrap() - 1;
        if let Some(value) = reader.lookup(key).nth(recno) {
            std::io::stdout().write_all(value?)?;
            std::io::stdout().write_all(b"\n")?;
        }
        return Ok(());
    }

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    for v in reader.lookup(key) {
        let v = v?;
        handle.write_all(&v)?;
        handle.write_all(b"\n")?;
    }

    Ok(())
}

fn cmd_dump(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = Arc::new(cdb_open(matches.value_of("cdbfile").unwrap())?);
    let mut iter = reader.iter();

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
    let access_arg = Arg::with_name("access")
        .long("access")
        .takes_value(true)
        .possible_value("mmap")
        .possible_value("reader")
        .possible_value("bufreader");
    let cdbfile_arg = Arg::with_name("cdbfile").index(1).required(true);

    let matches = App::new("cdb")
        .subcommand(
            SubCommand::with_name("-q")
                .about("query")
                .arg(newline_arg.clone())
                .arg(access_arg.clone())
                .arg(Arg::with_name("recno").short("n").takes_value(true))
                .arg(cdbfile_arg.clone())
                .arg(Arg::with_name("key").index(2).required(true)),
        )
        .subcommand(
            SubCommand::with_name("-d")
                .about("dump")
                .arg(newline_arg.clone())
                .arg(access_arg.clone())
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
