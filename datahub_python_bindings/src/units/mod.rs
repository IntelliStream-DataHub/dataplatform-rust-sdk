use dataplatform_rust_sdk::{TimeSeries, Unit};
use pyo3::{pyclass, pymethods};
use std::collections::HashMap;

pub mod async_service;
pub mod general;
pub mod sync_service;

/// Represents a Unit in the Datahub unit system
///
/// Parameters
/// ---------
/// id: int
///     internal id of the unit
/// external_id: str
///     user provided external id of the unit
/// name: str
///     name of the unit ie Celcius, Newton,
/// long_name: str
///     long name of the unit ie Temperature_Celsius, Force_Newton,
/// symbol: str
///     symbol of the unit ie C, N,
/// description: str
///     description of the unit
/// alias_names: list[str]
///     alias names of the unit ie Pascal, Newton/Meter Squared,
/// quantity: str
///     The quantity dimension of the unit ie Temperature, Mass, Energy-seconds
/// conversion: dict[str,float]
///     dict of conversion factors from this unit to other units
/// source: str
///     source of the unit
/// source_reference:
///     url to the source of the unit
///
#[pyclass(module = "datahub_python_sdk", name = "Unit")]
#[derive(Clone)]
pub struct PyUnit {
    pub inner: Unit,
}

impl From<Unit> for PyUnit {
    fn from(ts: Unit) -> Self {
        Self { inner: ts }
    }
}

impl From<PyUnit> for Unit {
    fn from(ts: PyUnit) -> Self {
        ts.inner
    }
}

#[pymethods]
impl PyUnit {
    #[new]
    fn new(
        id: u64,
        external_id: String,
        name: String,
        long_name: String,
        symbol: String,
        description: String,
        alias_names: Vec<String>,
        quantity: String,
        conversion: HashMap<String, f64>,
        source: String,
        source_reference: String,
    ) -> Self {
        Self {
            inner: Unit {
                id,
                external_id,
                name,
                long_name,
                symbol,
                description,
                alias_names,
                quantity,
                conversion,
                source,
                source_reference,
            },
        }
    }
}
