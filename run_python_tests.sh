#!/usr/bin/env bash
#
# Setup-and-run wrapper for the Python (PyO3) test suite.
#
# The tests import the *compiled* `datahub_sdk` module, not the Rust sources, so a
# stale `.so` silently masquerades as a source bug (e.g. ids deserialized as the old
# numeric type after the wire format changed to strings). This script always rebuilds
# the bindings before running, so the tests reflect the current `src/` and
# `datahub_python_bindings/src/`.
#
# Usage:
#   ./run_python_tests.sh                         # build + run the whole suite
#   ./run_python_tests.sh -k timeseries           # extra args are forwarded to pytest
#   ./run_python_tests.sh python_tests/test_units.py::test_by_ids
#   ./run_python_tests.sh -- -s -v                # everything after `--` goes to pytest
#
# Flags (consumed by this script, not pytest):
#   --release          build the bindings optimized (slower build, faster tests)
#   --no-build         skip `maturin develop` (only safe if the .so is already current)
#   --no-deps          skip the pip dependency install/upgrade
#   --recreate-venv    delete and recreate the virtualenv from scratch
#   -h, --help         show this help and exit
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$REPO_ROOT/.venv"
BINDINGS_DIR="$REPO_ROOT/datahub_python_bindings"
REQ_FILE="$REPO_ROOT/python_tests/requirements.txt"

RELEASE=0
DO_BUILD=1
DO_DEPS=1
RECREATE_VENV=0
PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)       RELEASE=1; shift ;;
        --no-build)      DO_BUILD=0; shift ;;
        --no-deps)       DO_DEPS=0; shift ;;
        --recreate-venv) RECREATE_VENV=1; shift ;;
        -h|--help)
            sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        --)              shift; PYTEST_ARGS+=("$@"); break ;;
        *)               PYTEST_ARGS+=("$1"); shift ;;
    esac
done

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mwarning:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

cd "$REPO_ROOT"

# --- venv ---------------------------------------------------------------------
if [[ $RECREATE_VENV -eq 1 && -d "$VENV_DIR" ]]; then
    log "Removing existing virtualenv at $VENV_DIR"
    rm -rf "$VENV_DIR"
fi

if [[ ! -d "$VENV_DIR" ]]; then
    command -v python3 >/dev/null 2>&1 || die "python3 not found on PATH"
    log "Creating virtualenv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# --- dependencies -------------------------------------------------------------
if [[ $DO_DEPS -eq 1 ]]; then
    log "Installing test dependencies (maturin, pytest, ...)"
    python -m pip install --quiet --upgrade pip
    python -m pip install --quiet -r "$REQ_FILE"
else
    log "Skipping dependency install (--no-deps)"
fi

# --- build the bindings -------------------------------------------------------
if [[ $DO_BUILD -eq 1 ]]; then
    command -v maturin >/dev/null 2>&1 || die "maturin not found; drop --no-deps or 'pip install maturin'"
    build_flags=()
    [[ $RELEASE -eq 1 ]] && build_flags+=(--release)
    log "Building datahub_sdk bindings with 'maturin develop ${build_flags[*]}'"
    ( cd "$BINDINGS_DIR" && maturin develop "${build_flags[@]}" )
else
    warn "Skipping 'maturin develop' (--no-build): tests run against the existing .so, which may be stale"
fi

# --- env sanity ---------------------------------------------------------------
if [[ ! -f "$REPO_ROOT/.env" ]]; then
    warn "No .env at the repo root — the live-backend fixtures will fail to configure a client."
    warn "Create one with BASE_URL and TOKEN (or CLIENT_ID/CLIENT_SECRET/TOKEN_URI). See python_tests/README.md."
fi

# --- run pytest ---------------------------------------------------------------
# Default to the whole suite unless the caller already named a python_tests target.
target_given=0
for a in ${PYTEST_ARGS[@]+"${PYTEST_ARGS[@]}"}; do
    [[ "$a" == *python_tests* ]] && target_given=1
done
if [[ $target_given -eq 0 ]]; then
    PYTEST_ARGS=(python_tests ${PYTEST_ARGS[@]+"${PYTEST_ARGS[@]}"})
fi

log "Running: python -m pytest ${PYTEST_ARGS[*]}"
# Run from the repo root so both `from fixtures import` and
# `from python_tests.fixtures import` styles resolve (see python_tests/README.md).
exec python -m pytest "${PYTEST_ARGS[@]}"
