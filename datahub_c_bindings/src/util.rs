// SPDX-License-Identifier: Apache-2.0
//! Argument checking and string ownership at the boundary.

use std::ffi::{c_char, CStr, CString};

use crate::error::{datahub_status, fail};

/// A `CString` from arbitrary text: an interior NUL would make `CString::new` fail, so it is
/// replaced rather than losing the message.
pub(crate) fn to_cstring(text: &str) -> CString {
    CString::new(text.replace('\0', " ")).expect("NULs were just removed")
}

/// Borrow a required C string as `&str`.
pub(crate) unsafe fn str_arg<'a>(
    ptr: *const c_char,
    name: &str,
) -> Result<&'a str, datahub_status> {
    if ptr.is_null() {
        return Err(fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} must not be NULL"),
        ));
    }
    CStr::from_ptr(ptr).to_str().map_err(|e| {
        fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} is not valid UTF-8: {e}"),
        )
    })
}

/// Borrow an optional C string; NULL is `None`.
pub(crate) unsafe fn opt_str_arg<'a>(
    ptr: *const c_char,
    name: &str,
) -> Result<Option<&'a str>, datahub_status> {
    if ptr.is_null() {
        Ok(None)
    } else {
        str_arg(ptr, name).map(Some)
    }
}

/// A required string that must also carry something.
pub(crate) fn nonempty<'a>(value: &'a str, name: &str) -> Result<&'a str, datahub_status> {
    if value.trim().is_empty() {
        Err(fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} must not be empty"),
        ))
    } else {
        Ok(value)
    }
}

/// Borrow an array of `count` C strings.
pub(crate) unsafe fn str_array_arg(
    ptrs: *const *const c_char,
    count: usize,
    name: &str,
) -> Result<Vec<String>, datahub_status> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if ptrs.is_null() {
        return Err(fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} must not be NULL when its count is {count}"),
        ));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let item = str_arg(*ptrs.add(i), &format!("{name}[{i}]"))?;
        out.push(item.to_string());
    }
    Ok(out)
}

/// Borrow a required handle.
pub(crate) unsafe fn ref_arg<'a, T>(ptr: *const T, name: &str) -> Result<&'a T, datahub_status> {
    ptr.as_ref().ok_or_else(|| {
        fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} must not be NULL"),
        )
    })
}

/// Borrow a required handle mutably.
pub(crate) unsafe fn mut_arg<'a, T>(ptr: *mut T, name: &str) -> Result<&'a mut T, datahub_status> {
    ptr.as_mut().ok_or_else(|| {
        fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("{name} must not be NULL"),
        )
    })
}

/// Hand an owned string to the caller through a `char **` out-parameter.
pub(crate) unsafe fn out_str(out: *mut *mut c_char, text: String) -> Result<(), datahub_status> {
    let slot = mut_arg(out, "out")?;
    *slot = to_cstring(&text).into_raw();
    Ok(())
}

/// Release a string the library handed out through a `char **` out-parameter. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_string_free(text: *mut c_char) {
    if !text.is_null() {
        drop(CString::from_raw(text));
    }
}
