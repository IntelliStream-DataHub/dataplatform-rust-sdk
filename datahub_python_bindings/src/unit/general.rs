use super::*;
#[pymethods]
impl PyUnit {
    #[getter]
    fn id(&self) -> u64 {
        self.inner.id
    }

    #[getter]
    fn external_id(&self) -> String {
        self.inner.external_id.clone()
    }
    
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }
    
    #[getter]
    fn symbol(&self) -> String {
        self.inner.symbol.clone()
    }
}