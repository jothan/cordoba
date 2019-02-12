#![feature(specialization, nll)]

extern crate cordoba;
extern crate pyo3;
extern crate memmap;

use std::fs::File;
use std::sync::Arc;

use memmap::Mmap;

use pyo3::prelude::*;
use pyo3::{IntoPyTuple};
use pyo3::{PyIterProtocol, PyMappingProtocol, PyRawObject};
use pyo3::types::{PyBytes};
use pyo3::types::exceptions as exc;

use cordoba::{CDBReader, FileIter};

#[pyclass]
struct Reader {
    reader: Arc<CDBReader<Mmap>>,
}

#[pymethods]
impl Reader {
    #[new]
    fn __new__(obj: &PyRawObject, fname: &str) -> PyResult<()> {
        let file = File::open(fname)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = Arc::new(CDBReader::new(map)?);
        obj.init(|| Reader { reader })
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.reader.get(key.as_bytes()) {
            Some(Ok(r)) => Ok(PyBytes::new(py, &r).into()),
            Some(Err(e)) => Err(e.into()),
            None => Err(PyErr::new::<exc::KeyError, _>(key.to_object(py))),
        }
    }
}


#[pyclass]
struct PyFileIter {
    iter: FileIter<Mmap>,
}

#[pyproto]
impl PyIterProtocol for PyFileIter {
    fn __iter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.iter.next() {
            Some(Ok((k, v))) => {
                Ok(Some((PyBytes::new(py, k), PyBytes::new(py, v)).into_tuple(py).into()))
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

#[pyproto]
impl PyIterProtocol for Reader
{
    fn __iter__(&mut self) -> PyResult<PyFileIter> {
        Ok(PyFileIter{iter: self.reader.clone().iter() })
    }
}

#[pymodule]
fn cordoba(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<PyFileIter>()?;

    Ok(())
}
