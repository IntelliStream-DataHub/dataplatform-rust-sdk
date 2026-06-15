use super::*;
#[pymethods]
impl PyUnit {
    #[getter]
    fn id(&self) -> u64 {
        self.inner.id
    }
    #[setter]
    fn set_id(&mut self, value: u64) {
        self.inner.id = value;
    }

    #[getter]
    fn external_id(&self) -> String {
        self.inner.external_id.clone()
    }
    #[setter]
    fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }
    #[setter]
    fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }

    #[getter]
    fn long_name(&self) -> String {
        self.inner.long_name.clone()
    }
    #[setter]
    fn set_long_name(&mut self, value: String) {
        self.inner.long_name = value;
    }

    #[getter]
    fn symbol(&self) -> String {
        self.inner.symbol.clone()
    }
    #[setter]
    fn set_symbol(&mut self, value: String) {
        self.inner.symbol = value;
    }

    #[getter]
    fn description(&self) -> String {
        self.inner.description.clone()
    }
    #[setter]
    fn set_description(&mut self, value: String) {
        self.inner.description = value;
    }

    #[getter]
    fn alias_names(&self) -> Vec<String> {
        self.inner.alias_names.clone()
    }
    #[setter]
    fn set_alias_names(&mut self, value: Vec<String>) {
        self.inner.alias_names = value;
    }

    #[getter]
    fn quantity(&self) -> String {
        self.inner.quantity.clone()
    }
    #[setter]
    fn set_quantity(&mut self, value: String) {
        self.inner.quantity = value;
    }

    #[getter]
    fn conversion(&self) -> HashMap<String, f64> {
        self.inner.conversion.clone()
    }
    #[setter]
    fn set_conversion(&mut self, value: HashMap<String, f64>) {
        self.inner.conversion = value;
    }

    #[getter]
    fn source(&self) -> String {
        self.inner.source.clone()
    }
    #[setter]
    fn set_source(&mut self, value: String) {
        self.inner.source = value;
    }

    #[getter]
    fn source_reference(&self) -> String {
        self.inner.source_reference.clone()
    }
    #[setter]
    fn set_source_reference(&mut self, value: String) {
        self.inner.source_reference = value;
    }
}
