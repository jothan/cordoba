extern crate cordoba;
extern crate pyo3;
extern crate memmap;

use std::fs::File;
use std::rc::Rc;

use memmap::Mmap;

use pyo3::prelude::*;
use pyo3::{IntoPyTuple};
use pyo3::{PyIterProtocol, PyMappingProtocol, PyRawObject};
use pyo3::types::{PyBytes};
use pyo3::types::exceptions as exc;

use cordoba::{CDBReader, FileIter, LookupIter};

#[pyclass]
struct Reader {
    reader: Rc<CDBReader<Mmap>>,
}

#[pymethods]
impl Reader {
    #[new]
    fn __new__(obj: &PyRawObject, fname: &str) -> PyResult<()> {
        let file = File::open(fname)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = Rc::new(CDBReader::new(map)?);
        obj.init(|| Reader { reader })
    }

    fn get_all(&self, key: &PyBytes) -> PyLookupIter {
        PyLookupIter{iter: self.reader.clone().lookup(key.as_bytes())}
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let mut lu = self.reader.clone().lookup(key.as_bytes());

        match lu.next() {
            Some(Ok(r)) => Ok(PyBytes::new(py, &r).into()),
            Some(Err(e)) => Err(e.into()),
            None => Err(PyErr::new::<exc::KeyError, _>(key.to_object(py))),
        }
    }
}


#[pyclass]
struct PyFileIter {
    iter: FileIter<Mmap, Rc<CDBReader<Mmap>>>,
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
#[pyclass]
struct PyLookupIter {
    iter: LookupIter<Mmap, Rc<CDBReader<Mmap>>>,
}

#[pyproto]
impl PyIterProtocol for PyLookupIter {
    fn __iter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.iter.next() {
            Some(Ok(v)) => {
                Ok(Some(PyBytes::new(py, v).into()))
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
        Ok(PyFileIter{iter: self.reader.iter() })
    }
}

#[pymodule]
fn cordoba(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<PyFileIter>()?;
    m.add_class::<PyLookupIter>()?;

    Ok(())
}
