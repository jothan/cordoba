use std::fs::File;
use std::io::{BufReader, Write};

extern crate cordoba;

extern crate clap;
extern crate memmap;

use cordoba::{CDBLookup, CDBReader};
use memmap::Mmap;

use clap::{App, Arg, ArgMatches, SubCommand};

enum AccessType {
    Mmap,
    Reader,
    BufReader,
}

impl<'a> From<&'a str> for AccessType {
    fn from(s: &str) -> Self {
        match s {
            "reader" => AccessType::Reader,
            "bufreader" => AccessType::BufReader,
            _ => AccessType::Mmap,
        }
    }
}

impl AccessType {
    fn open<'a>(&self, fname: &str) -> std::io::Result<Box<CDBLookup + 'a>> {
        let file = File::open(fname)?;

        Ok(match self {
            AccessType::Mmap => {
                let map = unsafe { Mmap::map(&file) }?;
                Box::new(CDBReader::new(map)?)
            }
            AccessType::Reader => Box::new(CDBReader::from_file(file)?),
            AccessType::BufReader => Box::new(CDBReader::from_file(BufReader::new(file))?),
        })
    }
}

fn access_type(matches: &ArgMatches) -> AccessType {
    matches.value_of("access").unwrap_or("mmap").into()
}

fn cmd_query(matches: &ArgMatches) -> std::io::Result<()> {
    let access = access_type(matches);
    let reader = access.open(matches.value_of("cdbfile").unwrap())?;

    let key = matches.value_of("key").unwrap().as_bytes();
    let recno = matches.value_of("recno");

    if let Some(recno) = recno {
        let recno = recno.parse::<usize>().unwrap() - 1;
        if let Some(value) = reader.lookup(key).nth(recno) {
            std::io::stdout().write_all(&value?)?;
            std::io::stdout().write_all(b"\n")?;
        }
        return Ok(());
    }

    for v in reader.lookup(key) {
        let v = v?;
        std::io::stdout().write_all(&v)?;
        std::io::stdout().write_all(b"\n")?;
    }

    Ok(())
}

fn cmd_dump(matches: &ArgMatches) -> std::io::Result<()> {
    let access = access_type(matches);
    let reader = access.open(matches.value_of("cdbfile").unwrap())?;

    for res in reader.iter() {
        let (k, v) = res?;
        std::io::stdout().write_all(&k)?;
        std::io::stdout().write_all(b" = ")?;
        std::io::stdout().write_all(&v)?;
        std::io::stdout().write_all(b"\n")?;
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
