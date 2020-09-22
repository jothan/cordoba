
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::io::AsRawFd;

use memmap::Mmap;

use pyo3::exceptions::{KeyError, ValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{PyIterProtocol, PyMappingProtocol, PySequenceProtocol};

use crate::{IterState, LookupState, Reader as CDBReader, Writer as CDBWriter};

#[pyclass]
pub struct Reader {
    inner: CDBReader<Mmap>,
}

#[pymethods]
impl Reader {
    #[new]
    fn new(fname: PyObject, py: Python<'_>) -> PyResult<Self> {
        let path: &str = py.import("os")?.call1("fsdecode", (fname,))?.extract()?;
        let file = File::open(path)?;
        let map = unsafe { Mmap::map(&file) }?;
        let reader = CDBReader::new(map)?;
        Ok(Reader { inner: reader })
    }

    fn get_all(slf: PyRef<'_, Self>, key: &PyBytes) -> LookupIter {
        let state = RefCell::new(LookupState::new(&slf.inner, key.as_bytes()));

        LookupIter {
            reader: slf.into(),
            key: key.into(),
            state,
        }
    }
}

#[pyproto]
impl PyMappingProtocol for Reader {
    fn __getitem__(&self, key: &PyBytes) -> PyResult<Py<PyBytes>> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        match self.inner.get(key.as_bytes())? {
            Some(r) => Ok(PyBytes::new(py, &r).into()),
            None => Err(KeyError::py_err(key.to_object(py))),
        }
    }
}

#[pyproto]
impl PySequenceProtocol for Reader {
    fn __contains__(&self, key: &PyBytes) -> PyResult<bool> {
        Ok(self.inner.get(key.as_bytes())?.is_some())
    }
}

#[pyproto]
impl PyIterProtocol for Reader {
    fn __iter__(slf: PyRef<Self>) -> PyResult<FileIter> {
        Ok(FileIter {
            reader: slf.into(),
            state: Default::default(),
        })
    }
}

#[pyclass]
pub struct FileIter {
    reader: Py<Reader>,
    state: IterState,
}

#[pyproto]
impl PyIterProtocol for FileIter {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<Self>> {
        Ok(slf.into())
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<(Py<PyBytes>, Py<PyBytes>)>> {
        let reader = slf.reader.borrow(slf.py());
        let mut state: IterState = slf.state;
        match state.next(&reader.inner) {
            Some(Ok((k, v))) => {
                let ret = Ok(Some((
                    PyBytes::new(slf.py(), &k).into(),
                    PyBytes::new(slf.py(), &v).into(),
                )));
                drop(reader);

                slf.state = state;
                ret
            }
            Some(Err(e)) => {
                drop(reader);
                slf.state = state;
                Err(e.into())
            }
            None => Ok(None),
        }
    }
}

#[pyclass]
struct LookupIter {
    reader: Py<Reader>,
    key: Py<PyBytes>,
    state: RefCell<LookupState>,
}

#[pyproto]
impl PyIterProtocol for LookupIter {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<Self>> {
        Ok(slf.into())
    }

    fn __next__(slf: PyRef<Self>) -> PyResult<Option<Py<PyBytes>>> {
        let py = slf.py();

        match slf
            .state
            .borrow_mut()
            .next(&slf.reader.borrow(py).inner, slf.key.as_ref(py).as_bytes())
        {
            Some(Ok(v)) => Ok(Some(PyBytes::new(py, v).into())),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

#[pyclass]
pub struct Writer {
    inner: Option<CDBWriter<BufWriter<File>>>,
    sync: bool,
}

#[pymethods]
impl Writer {
    #[new]
    #[args(sync = "true", exclusive = "true")]
    fn new(fname: PyObject, sync: bool, exclusive: bool, py: Python<'_>) -> PyResult<Self> {
        let path: &str = py.import("os")?.call1("fsdecode", (fname,))?.extract()?;
        let file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .create_new(exclusive)
                .open(path)?,
        );

        let writer = CDBWriter::new(file)?;

        Ok(Writer {
            inner: Some(writer),
            sync,
        })
    }

    fn fileno(&self) -> PyResult<i32> {
        let writer = self.inner.as_ref().ok_or_else(closed_exc)?;
        Ok(writer.file().get_ref().as_raw_fd())
    }

    fn close(&mut self) -> PyResult<()> {
        let writer = self.inner.take().ok_or_else(closed_exc)?;
        let mut file = writer.finish()?.into_inner()?;
        file.flush()?;

        if self.sync {
            file.sync_all()?;
        }

        Ok(())
    }
}

#[inline]
fn closed_exc() -> PyErr {
    ValueError::py_err("Writer is closed")
}

#[pyproto]
impl PyMappingProtocol for Writer {
    fn __setitem__(&mut self, key: &PyBytes, value: &PyBytes) -> PyResult<()> {
        let writer = self.inner.as_mut().ok_or_else(closed_exc)?;

        writer.write(key.as_bytes(), value.as_bytes())?;

        Ok(())
    }
}

/*#[pyproto]
impl<'p> PyContextProtocol<'p> for Writer {
    fn __enter__(&mut self) -> ??? {
        todo!("Figure out how to return writer here")
    }

    fn __exit__(&mut self,
                ty: Option<&'p PyType>,
                _value: Option<PyObject>,
                _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        if ty.is_none() {
            self.close()?;
        }
        Ok(false)
    }
}*/

impl core::convert::From<crate::ReadError> for pyo3::PyErr {
    fn from(error: crate::ReadError) -> Self {
        match error {
            crate::ReadError::OutOfBounds => {
                pyo3::exceptions::EOFError::py_err("Tried to read beyond end of file.")
            }
            crate::ReadError::InvalidFile => pyo3::exceptions::IOError::py_err("Invalid file data."),
        }
    }
}

#[pymodule]
fn cordoba(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<Writer>()?;
    m.add_class::<FileIter>()?;
    m.add_class::<LookupIter>()?;

    Ok(())
}
