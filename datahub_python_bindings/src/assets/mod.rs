//! Python bindings for the `/assets` service.
//!
//! There is no `PyAsset` here: the class lives in [`crate::nodes`] because `PyNode`'s dispatch,
//! `NodeInput` and `ResourceIdentifiable` all reach for it, and `nodes::register` already adds it
//! to the module. This module is the service pair and nothing else.

pub mod async_service;
pub mod sync_service;

use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<sync_service::PyAssetsServiceSync>()?;
    m.add_class::<async_service::PyAssetsServiceAsync>()?;
    Ok(())
}
