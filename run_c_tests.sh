#!/usr/bin/env bash
#
# Build-and-test wrapper for the C bindings (datahub_c_bindings/).
#
# Runs, in order: `cargo build` (which also regenerates include/intellistream_datahub.h),
# `cargo test` (the boundary tests, the mock-api tests, and the live tests — which print SKIP
# and pass without a BASE_URL), then compiles tests/c/smoke.c with the system C compiler against
# the freshly built shared library and runs it. No backend is needed for anything but the live
# tests.
#
# Usage:
#   ./run_c_tests.sh                 # everything
#   ./run_c_tests.sh --release       # optimized build (what a release ships)
#   ./run_c_tests.sh --check-header  # additionally fail if the committed header is stale (CI)
#   ./run_c_tests.sh --smoke-only    # skip cargo test; just build and run the C smoke test
#   ./run_c_tests.sh -- -k buffering # everything after `--` goes to `cargo test`
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$REPO_ROOT/datahub_c_bindings"

RELEASE=0
CHECK_HEADER=0
SMOKE_ONLY=0
CARGO_TEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)      RELEASE=1; shift ;;
        --check-header) CHECK_HEADER=1; shift ;;
        --smoke-only)   SMOKE_ONLY=1; shift ;;
        -h|--help)
            sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        --)             shift; CARGO_TEST_ARGS+=("$@"); break ;;
        *)              CARGO_TEST_ARGS+=("$1"); shift ;;
    esac
done

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

command -v cargo >/dev/null 2>&1 || die "cargo not found on PATH"
command -v cc >/dev/null 2>&1 || die "no C compiler (cc) on PATH"

cd "$CRATE_DIR"

profile=debug
build_flags=()
if [[ $RELEASE -eq 1 ]]; then
    profile=release
    build_flags+=(--release)
fi
target_dir="${CARGO_TARGET_DIR:-$CRATE_DIR/target}"
lib_dir="$target_dir/$profile"

log "Building datahub_c_bindings ($profile) — this also regenerates include/intellistream_datahub.h"
cargo build "${build_flags[@]}"

if [[ $CHECK_HEADER -eq 1 ]]; then
    log "Checking that the committed header matches the sources"
    if ! git -C "$REPO_ROOT" diff --exit-code -- datahub_c_bindings/include; then
        die "include/intellistream_datahub.h is stale: commit the regenerated header"
    fi
fi

if [[ $SMOKE_ONLY -eq 0 ]]; then
    log "Running the crate's Rust tests (boundary, mock api, live-if-configured)"
    cargo test "${build_flags[@]}" -- ${CARGO_TEST_ARGS[@]+"${CARGO_TEST_ARGS[@]}"}
fi

log "Compiling tests/c/smoke.c against $lib_dir"
smoke_bin="$lib_dir/datahub_c_smoke"
extra_libs=(-lpthread -ldl -lm)
[[ "$(uname -s)" == "Darwin" ]] && extra_libs=(-lm)
cc -std=c11 -Wall -Wextra -Werror \
    -I include tests/c/smoke.c \
    -L "$lib_dir" -lintellistream_datahub "${extra_libs[@]}" \
    -o "$smoke_bin"

log "Running the smoke test"
LD_LIBRARY_PATH="$lib_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
DYLD_LIBRARY_PATH="$lib_dir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
    "$smoke_bin"
