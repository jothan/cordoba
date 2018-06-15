#![feature(specialization, proc_macro)]

extern crate cordoba;
extern crate pyo3;
extern crate memmap;

use std::fs::File;
use std::io;

use memmap::Mmap;

use pyo3::prelude::*;
use pyo3::class::{PyIterProtocol, PyMappingProtocol};
use pyo3::exc;
use pyo3::py::class as pyclass;
use pyo3::py::methods as pymethods;
use pyo3::py::modinit as pymodinit;
use pyo3::py::proto as pyproto;

use cordoba::{CDBLookup, CDBReader};

#[pyclass]
struct Reader {
    reader: CDBReader<Mmap>,
    token: PyToken,
}

#[pymethods]
impl Reader {
    #[new]
    fn __new__(obj: &PyRawObject, fname: &str) -> PyResult<()> {
        let file = File::open(fname)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = CDBReader::new(map)?;
        obj.init(|token| Reader { reader, token })
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<PyObject> {
        let py = self.py();
        match self.reader.get(key.data()) {
            Some(Ok(r)) => Ok(PyBytes::new(py, &r).into()),
            Some(Err(e)) => Err(e.into()),
            None => Err(PyErr::new::<exc::IndexError, _>(key.to_object(py))),
        }
    }
}


#[pyclass]
struct FileIter {
    iter: Box<Iterator<Item=io::Result<(Vec<u8>, Vec<u8>)>>>,
    token: PyToken,
}

#[pyproto]
impl PyIterProtocol for FileIter {
    fn __iter__(& mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((k, v).to_object(self.py()))),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

#[pyproto]
impl PyIterProtocol for Reader
{
    fn __iter__(&mut self) -> PyResult<PyObject> {
        let lookup : Box<CDBLookup> = Box::new(self.reader);
        let iter = lookup.iter().map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())));

        Ok(FileIter{iter: Box::new(iter), token: self.token }.to_object(self.py()))
    }
}

#[pymodinit(cordoba)]
fn init_mod(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<FileIter>()?;

    Ok(())
}
