#![feature(specialization, try_from)]
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Write, Seek};

use memmap::Mmap;

use pyo3::prelude::*;
use pyo3::{IntoPyTuple, PyRawObject};
use pyo3::{PyIterProtocol, PyMappingProtocol, PyContextProtocol};
use pyo3::{PyGCProtocol, PyVisit, PyTraverseError};
use pyo3::types::{PyBytes, PyType, PyObjectRef};
use pyo3::types::exceptions as exc;

use cordoba::{CDBReader, CDBWriter, IterState, LookupState, ClassicFormat};

#[pyclass]
pub struct Reader {
    inner: CDBReader<Mmap>,
}

#[pymethods]
impl Reader {
    #[new]
    fn __new__(obj: &PyRawObject, fname: PyObject, py: Python) -> PyResult<()> {
        let path : &str = py.import("os")?.call1("fsdecode", (fname,))?.extract()?;
        let file = File::open(path)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = CDBReader::new(map)?;
        obj.init(|| Reader { inner: reader })
    }

    fn get_all(&self, key: &PyBytes) -> LookupIter {
        LookupIter{reader: self.into(),
                   key: key.into(),
                   state: LookupState::new(&self.inner, key.as_bytes())}
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.inner.get(key.as_bytes()) {
            Some(Ok(r)) => Ok(PyBytes::new(py, &r).into()),
            Some(Err(e)) => Err(e.into()),
            None => Err(exc::KeyError::py_err(key.to_object(py))),
        }
    }
}


#[pyclass]
pub struct FileIter {
    reader: Py<Reader>,
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

        match self.state.next(&self.reader.as_ref(py).inner) {
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
    reader: Py<Reader>,
    key: Py<PyBytes>,
    state: LookupState<ClassicFormat>,
}

#[pyproto]
impl PyIterProtocol for LookupIter {
    fn __iter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __next__(&mut self) -> PyResult<Option<PyObject>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.state.next(&self.reader.as_ref(py).inner, self.key.as_ref(py).as_bytes()) {
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
        Ok(FileIter{reader: self.into(), state: Default::default() })
    }
}


#[pyclass(gc)]
pub struct Writer {
    inner: Option<CDBWriter<PyFile>>,
}

struct PyFile(PyObject);

impl Write for PyFile {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let written = self.0.call_method1(py, "write", (PyBytes::new(py, data),))?;
        let res: usize = written.extract(py).map_err(|_| ErrorKind::InvalidData)?;
        Ok(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        self.0.call_method0(py, "flush")?;
        Ok(())
    }
}

impl Seek for PyFile {
    fn seek(&mut self, from: io::SeekFrom) -> io::Result<u64> {
        let seek_args = match from {
            io::SeekFrom::Start(pos) => {
                (i64::try_from(pos).map_err(|_| ErrorKind::InvalidData)?, 0)
            }
            io::SeekFrom::Current(pos) => (pos, 1),
            io::SeekFrom::End(pos) => (pos, 2),
        };

        let gil = Python::acquire_gil();
        let py = gil.python();

        let cur_pos = self.0.call_method1(py, "seek", seek_args)?;
        let res : u64 = cur_pos.extract(py).map_err(|_| ErrorKind::InvalidData)?;
        Ok(res)
    }
}

#[pymethods]
impl Writer {
    #[new]
    fn __new__(obj: &PyRawObject, file: PyObject) -> PyResult<()> {
        let writer = CDBWriter::new(PyFile(file))?;
        obj.init(|| Writer { inner: Some(writer) })
    }

    fn close(&mut self) -> PyResult<()> {
        let writer = self.inner.take().ok_or_else(Self::closed_exc)?;

        writer.finish()?;
        Ok(())
    }
}

impl Writer {
    #[inline]
    fn closed_exc() -> PyErr {
        exc::ValueError::py_err("Writer is closed")
    }
}

#[pyproto]
impl PyMappingProtocol for Writer {
    fn __setitem__(&mut self, key: &PyBytes, value: &PyBytes) -> PyResult<()> {
        match &mut self.inner {
            Some(ref mut w) => w.write(key.as_bytes(), value.as_bytes()).map_err(Into::into),
            None => Err(Self::closed_exc())
        }
    }
}

#[pyproto]
impl PyGCProtocol for Writer {
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        if let Some(ref writer) = self.inner {
            visit.call(&writer.get_file().0)?
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.inner.take();
    }
}

#[pyproto]
impl<'p> PyContextProtocol<'p> for Writer {
    fn __enter__(&mut self) -> PyResult<PyObject> {
        Ok(self.into())
    }

    fn __exit__(&mut self,
                ty: Option<&'p PyType>,
                _value: Option<&'p PyObjectRef>,
                _traceback: Option<&'p PyObjectRef>,
    ) -> PyResult<bool> {
        if ty.is_none() {
            self.close()?;
        }
        Ok(false)
    }
}

#[pymodule]
fn cordoba(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<Writer>()?;
    m.add_class::<FileIter>()?;
    m.add_class::<LookupIter>()?;

    Ok(())
}
