use std::fs::File;
use std::io::{BufReader, Write};

extern crate cordoba;

extern crate clap;
extern crate memmap;

use memmap::Mmap;
use cordoba::{CDBReader, CDBLookup};

use clap::{App, Arg, ArgMatches, SubCommand};


/*    fn get_lookup<'c>(&'c self, access_type: Option<&str>) -> std::io::Result<Box<CDBLookup<'c, '_> + 'c>> {
        println!("at: {:?}", access_type);
    }
}*/

/*fn open_reader (
    fname: &str,
) -> std::io::Result<Reader>
{
    // FIXME: Make this generic.

    Ok(Reader{file, map})
}*/

fn cmd_query(matches: &ArgMatches) -> std::io::Result<()> {
    let file = File::open(matches.value_of("cdbfile").unwrap())?;
    let map = unsafe { Mmap::map(&file)? };

    let reader: Box<CDBLookup> = match matches.value_of("access") {
        Some("bufreader") => Box::new(CDBReader::from_file(BufReader::new(&file))?),
        Some("reader") => Box::new(CDBReader::from_file(&file)?),
        _ => Box::new(CDBReader::new(&map[..])?),
    };

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

fn main() -> std::io::Result<()> {
    let newline_arg = Arg::with_name("newline").short("m");
    let access_arg = Arg::with_name("access")
        .long("access")
        .takes_value(true)
        .possible_value("mmap")
        .possible_value("reader")
        .possible_value("bufreader");

    let matches = App::new("cdb")
        .subcommand(
            SubCommand::with_name("-q")
                .about("query")
                .arg(newline_arg)
                .arg(access_arg)
                .arg(Arg::with_name("recno").short("n").takes_value(true))
                .arg(Arg::with_name("cdbfile").index(1).required(true))
                .arg(Arg::with_name("key").index(2).required(true)),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("-q") {
        cmd_query(matches)?;
    }
    Ok(())
}
