use intellistream_datahub_sdk::{TimeSeries, Unit};
use pyo3::{pyclass, pymethods};
use std::collections::HashMap;

pub mod async_service;
pub mod general;
pub mod sync_service;

/// One entry of the DataHub unit catalogue — the shared vocabulary a timeseries points at
/// through `unit_external_id`.
///
/// Referencing a catalogue entry rather than typing free text into `TimeSeries.unit` is what
/// makes a series' unit comparable and convertible: `conversion` carries the factors to the
/// other units of the same `quantity`. Units are read-only here — the catalogue is seeded
/// server-side, so this class is what `units.list()` and `units.by_ids()` hand back.
///
/// Parameters
/// ----------
/// id: int
///     internal id of the unit
/// external_id: str
///     user provided external id of the unit, e.g. `temperature_celsius`
/// name: str
///     name of the unit, e.g. Celsius, Newton
/// long_name: str
///     long name of the unit, e.g. Temperature_Celsius, Force_Newton
/// symbol: str
///     symbol of the unit, e.g. C, N
/// description: str
///     description of the unit
/// alias_names: list[str]
///     alias names of the unit, e.g. Pascal, Newton/Meter Squared
/// quantity: str
///     the quantity dimension of the unit, e.g. Temperature, Mass, Energy-seconds
/// conversion: dict[str, float]
///     conversion factors from this unit to other units
/// source: str
///     source of the unit
/// source_reference: str
///     url to the source of the unit
#[pyclass(module = "intellistream_datahub_sdk", name = "Unit")]
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
