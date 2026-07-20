use chrono::{DateTime, Utc};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyTzInfo};

/// Convert any Python `datetime` into a chrono `DateTime<Utc>`.
///
/// Every timezone-aware `datetime.datetime` is accepted — including a `pandas.Timestamp`,
/// which subclasses `datetime` — in any zone: a fixed offset, a pytz zone, or a
/// `zoneinfo.ZoneInfo` named zone. Python resolves the offset via `astimezone` and the
/// value is normalized to UTC. (Extracting straight into `DateTime<Utc>`/`DateTime<FixedOffset>`
/// cannot do this: PyO3 only accepts a tzinfo equal to `timezone.utc`, and it cannot pull a
/// fixed offset out of a `ZoneInfo`.)
///
/// Rejected with a clear error:
/// - non-`datetime` objects such as `numpy.datetime64` (not a `datetime`, and it carries no
///   timezone at all) — the caller must convert first, e.g. `pd.Timestamp(x, tz="UTC")`;
/// - naive datetimes (no tzinfo) — without a zone the intended instant is ambiguous, and
///   silently assuming one would be a footgun.
pub fn py_datetime_to_utc(ob: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    let py = ob.py();
    if !ob.is_instance_of::<PyDateTime>() {
        let type_name = ob.get_type().name()?;
        return Err(PyTypeError::new_err(format!(
            "expected a timezone-aware datetime, got `{type_name}`"
        )));
    }
    if ob.getattr(intern!(py, "tzinfo"))?.is_none() {
        return Err(PyTypeError::new_err(
            "expected a timezone-aware datetime; naive datetimes are not accepted \
             (attach a tzinfo, e.g. datetime.timezone.utc or zoneinfo.ZoneInfo(\"Europe/Oslo\"))",
        ));
    }
    let utc = PyTzInfo::utc(py)?;
    // After astimezone(utc) the tzinfo is exactly `timezone.utc`, so the extract succeeds.
    let normalized = ob.call_method1(intern!(py, "astimezone"), (utc,))?;
    normalized.extract::<DateTime<Utc>>()
}

/// `Option` convenience for optional datetime parameters.
pub fn opt_py_datetime_to_utc(ob: Option<&Bound<'_, PyAny>>) -> PyResult<Option<DateTime<Utc>>> {
    ob.map(py_datetime_to_utc).transpose()
}
