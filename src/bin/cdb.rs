use std::fs::File;
use std::io::Write;

extern crate cordoba;

extern crate clap;
extern crate memmap;

use cordoba::{CDBReader, CDBFileAccess};

use clap::{Arg, App, SubCommand, ArgMatches};

fn open_reader(fname: &str, access_type: Option<&str>) -> std::io::Result<CDBReader<CDBFileAccess<File>>>
{
    // FIXME: Make this generic.
    let file = File::open(fname)?;

    Ok(match access_type {
        _ => CDBReader::from_file(file)?,
    })
}

fn cmd_query(matches: &ArgMatches) -> std::io::Result<()> {
    let reader = open_reader(matches.value_of("cdbfile").unwrap(), matches.value_of("access"))?;
    let key = matches.value_of("key").unwrap().as_bytes();
    let recno = matches.value_of("recno");

    if let Some(recno) = recno {
        let recno = recno.parse::<usize>().unwrap() - 1;
        if let Some(value) = reader.lookup(key).nth(recno) {
            std::io::stdout().write_all(&value?)?;
            std::io::stdout().write_all(b"\n")?;
        }
        return Ok(())
    }

    for v in reader.lookup(key) {
        let v = v?;
        std::io::stdout().write_all(&v)?;
        std::io::stdout().write_all(b"\n")?;
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let newline_arg = Arg::with_name("newline").short("m");
    let access_arg = Arg::with_name("access")
        .long("access").takes_value(true)
        .possible_value("mmap")
        .possible_value("reader")
        .possible_value("bufreader");

    let matches = App::new("cdb")
        .subcommand(SubCommand::with_name("-q")
                    .about("query")
                    .arg(newline_arg)
                    .arg(access_arg)
                    .arg(Arg::with_name("recno").short("n").takes_value(true))
                    .arg(Arg::with_name("cdbfile").index(1).required(true))
                    .arg(Arg::with_name("key").index(2).required(true))
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("-q") {
        cmd_query(matches)?;
    }
    Ok(())
}
