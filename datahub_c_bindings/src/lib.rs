// SPDX-License-Identifier: Apache-2.0
//! C ABI for the IntelliStream DataHub SDK.
//!
//! This crate is a thin FFI layer over `intellistream-datahub-sdk`, built as
//! `libintellistream_datahub` (shared and static) with a cbindgen-generated header in
//! `include/`. There is exactly one implementation of every call — the Rust core — which is why
//! this crate contains no HTTP, auth, buffering or WebSocket code of its own.
//!
//! Every exported function is `extern "C"`, `#[no_mangle]`, and runs inside [`error::guard`], so a
//! Rust panic never unwinds into the caller: it becomes `DATAHUB_PANIC` with the panic message
//! in `datahub_last_error()`.
//!
//! The runtime model copies the core's blocking client: a [`datahub_client`] owns a Tokio
//! runtime and every call is `block_on` — but it wraps the async `ApiService` directly rather
//! than the blocking client, because the blocking client deliberately omits the subscription
//! listener and that listener is one of the three things this ABI exists for.
#![allow(
    non_camel_case_types,
    clippy::missing_safety_doc,
    clippy::not_unsafe_ptr_arg_deref
)]

/// Unwrap a `Result<T, datahub_status>` or return the status from the enclosing FFI function.
macro_rules! ffi_try {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(status) => return status,
        }
    };
}

mod client;
mod config;
mod datapoints;
mod error;
mod events;
mod json;
mod listener;
mod timeseries;
mod util;

pub use client::*;
pub use config::*;
pub use datapoints::*;
pub use error::*;
pub use events::*;
pub use listener::*;
pub use timeseries::*;
pub use util::*;
