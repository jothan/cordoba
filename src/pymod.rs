use memmap::Mmap;
use std::fs::File;

use pyo3::prelude::*;

use pyo3::class::PyMappingProtocol;
use pyo3::exc;
use pyo3::py::class as pyclass;
use pyo3::py::methods as pymethods;
use pyo3::py::modinit as pymodinit;
use pyo3::py::proto as pyproto;

use read::CDBReader;

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
        obj.init(|t| Reader { reader, token: t })
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

#[pymodinit(cordoba)]
fn init_mod(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;

    Ok(())
}
