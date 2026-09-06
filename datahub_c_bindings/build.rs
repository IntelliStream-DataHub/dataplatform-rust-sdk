// SPDX-License-Identifier: Apache-2.0
//! Regenerates `include/intellistream_datahub.h` from the crate's sources on every build.
//!
//! The header is committed so a C user never needs cbindgen; CI regenerates it and fails on a
//! diff, the same way a stale `.so` is treated on the Python side. The version macros and the
//! `DATAHUB_TIME_UNSET` sentinel are added here rather than as Rust constants: cbindgen renders a
//! Rust `i64::MIN` path literally, which is not C, and a `-9223372036854775808` literal does not
//! fit a C `long long` before negation.

use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let version = env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION");
    let major = env::var("CARGO_PKG_VERSION_MAJOR").expect("CARGO_PKG_VERSION_MAJOR");
    let minor = env::var("CARGO_PKG_VERSION_MINOR").expect("CARGO_PKG_VERSION_MINOR");
    let patch = env::var("CARGO_PKG_VERSION_PATCH").expect("CARGO_PKG_VERSION_PATCH");

    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=build.rs");

    let mut config =
        cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).expect("cbindgen.toml");
    config.after_includes = Some(format!(
        "\n/* The library version this header was generated from. datahub_version() reports the\n \
         * version actually loaded; the two should agree. */\n\
         #define DATAHUB_VERSION \"{version}\"\n\
         #define DATAHUB_VERSION_MAJOR {major}\n\
         #define DATAHUB_VERSION_MINOR {minor}\n\
         #define DATAHUB_VERSION_PATCH {patch}\n\n\
         /* Pass as start_ms / end_ms to leave that end of a datapoint window open. */\n\
         #define DATAHUB_TIME_UNSET INT64_MIN\n"
    ));

    let header = crate_dir.join("include").join("intellistream_datahub.h");
    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file(&header);
        }
        // A syntax error in the sources is rustc's to report, with a far better message.
        Err(cbindgen::Error::ParseSyntaxError { .. }) => {}
        Err(e) => panic!("cbindgen failed to generate {}: {e}", header.display()),
    }
}
