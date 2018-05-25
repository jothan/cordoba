cordoba zero-copy CDB access library
====================================

The CDBReader is mostly intended for operating over an mmaped byte
slice. For small files, loading the whole file in memory is also an
option.

The file access implemented on any type that is Read + Seek does no
optimization beyond removing redundant seeks. If BufReader is used, a
rather small buffer size is recommended as hash table reads are done
on every value access.

Planned features
----------------

In order of priority.

 * Python interface with PyO3.
 * Generic cdb executable mostly compatible with tinycdb.
 * Make the CDB layout generic and customizable allowing files greater
   than 4 GiB or alignement for keys and values.
