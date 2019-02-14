use std::fs::File;

use memmap::Mmap;

use pyo3::prelude::*;
use pyo3::{IntoPyTuple};
use pyo3::{PyIterProtocol, PyMappingProtocol, PyRawObject};
use pyo3::types::{PyBytes};
use pyo3::types::exceptions as exc;

use cordoba::{CDBReader, IterState, LookupState};

#[pyclass]
pub struct Reader {
    inner: CDBReader<Mmap>,
}

#[pymethods]
impl Reader {
    #[new]
    fn __new__(obj: &PyRawObject, fname: &str) -> PyResult<()> {
        let file = File::open(fname)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = CDBReader::new(map)?;
        obj.init(|| Reader { inner: reader })
    }

    fn get_all(&self, key: &PyBytes) -> LookupIter {
        let gil = Python::acquire_gil();
        let py = gil.python();

        LookupIter{reader: self.into_object(py), key: key.into(), state: LookupState::new(&self.inner, key.as_bytes())}
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let key_bytes = key.as_bytes();

        match self.inner.get(key_bytes) {
            Some(Ok(r)) => Ok(PyBytes::new(py, &r).into()),
            Some(Err(e)) => Err(e.into()),
            None => Err(PyErr::new::<exc::KeyError, _>(key.to_object(py))),
        }
    }
}


#[pyclass]
pub struct FileIter {
    reader: PyObject,
    state: IterState,
}

#[pyproto]
impl PyIterProtocol for FileIter {
    fn __iter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let reader : &Reader = self.reader.cast_as(py)?;

        match self.state.next(&reader.inner) {
            Some(Ok((k, v))) => {
                Ok(Some((PyBytes::new(py, k), PyBytes::new(py, v)).into_tuple(py).into()))
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}
#[pyclass]
struct LookupIter {
    reader: PyObject,
    key: PyObject,
    state: LookupState,
}

#[pyproto]
impl PyIterProtocol for LookupIter {
    fn __iter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let reader : &Reader = self.reader.cast_as(py)?;
        let key_pybytes : &PyBytes = self.key.cast_as(py)?;

        match self.state.next(&reader.inner, key_pybytes.as_bytes()) {
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
    fn __iter__(&mut self) -> PyResult<FileIter> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        Ok(FileIter{reader: self.to_object(py), state: Default::default() })
    }
}

#[pymodule]
fn cordoba(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<FileIter>()?;
    m.add_class::<LookupIter>()?;

    Ok(())
}
