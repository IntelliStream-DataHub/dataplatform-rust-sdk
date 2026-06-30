# Running the Python tests

The tests in this directory exercise the **`datahub_sdk`** Python module, which is the
PyO3 binding layer compiled from the `datahub_python_bindings/` crate. Because the module
is native (Rust compiled to a `.so`), you must **build and install it into your Python
environment with `maturin` before the tests can import it** — there is no pure-Python
package to `pip install`.

Like the Rust integration tests, these tests call a **live backend** and read connection
config from the repo-root `.env` file.

---

## Quick start

From the repo root, run the wrapper script — it creates the venv, installs the test
dependencies, **rebuilds the bindings** (`maturin develop`), and runs pytest:

```bash
./run_python_tests.sh                              # set up + run the whole suite
./run_python_tests.sh -k timeseries                # extra args are forwarded to pytest
./run_python_tests.sh python_tests/test_units.py   # run a single file or ::test
./run_python_tests.sh --help                       # all flags (--release, --no-build, ...)
```

The script always runs `maturin develop` before the tests, so a stale `.so` can't
masquerade as a source bug — the tests always reflect your current Rust code. You still
need a reachable backend and a valid `.env` (see [Backend config](#backend-config)).

The rest of this document explains the same steps manually, for when you want to run them
piecemeal or debug the setup.

---

## Prerequisites

- **Rust toolchain** (`cargo`) — needed to compile the bindings.
- **Python 3.9+** (this machine has `python3` 3.12). Note: only `python3` exists here,
  there is no bare `python`.
- A reachable backend and a valid `.env` (see [Backend config](#backend-config)).

---

## 1. Create and activate a virtual environment

Create the venv once (at the repo root), then activate it for each session.

```bash
cd /home/olav/projects/git/dataplatform-rust-sdk

# Create the virtual environment
python3 -m venv .venv

# Activate it (bash/zsh)
source .venv/bin/activate
```

Your prompt should now be prefixed with `(.venv)`. To leave the venv later, run
`deactivate`.

> Tip: add `.venv/` to `.gitignore` if it isn't already, so the environment isn't committed.

---

## 2. Install the Python dependencies

With the venv **active**, install from `python_tests/requirements.txt`:

```bash
pip install --upgrade pip
pip install -r python_tests/requirements.txt
```

That file pins:

- `maturin` — builds and installs the Rust bindings.
- `pytest` — the test runner.
- `numpy`, `pandas` — used by the time-series fixtures and tests.

---

## 3. Build and install the `datahub_sdk` bindings

`maturin develop` compiles the Rust crate and installs the resulting `datahub_sdk` module
directly into the active venv's `site-packages`, so it becomes importable from anywhere.

```bash
# from the bindings crate directory, with the venv active
cd datahub_python_bindings
maturin develop          # add --release for a faster (optimized) build
cd ..
```

Re-run `maturin develop` whenever you change Rust code in `datahub_python_bindings/` or
the parent SDK — the tests import the **compiled** module, not the source.

Verify the install:

```bash
python3 -c "import datahub_sdk; print('ok')"
```

---

## 4. Backend config (`.env`)

The fixtures build a client from an env file containing at least:

- `BASE_URL` — backend root, e.g. `http://localhost:8081`
- **either** `TOKEN` (a bearer token used as-is) **or** the OAuth2 client-credentials set
  `CLIENT_ID` / `CLIENT_SECRET` / `TOKEN_URI` (optional `PROJECT_NAME`)

A gitignored `.env` already exists at the repo root. Make sure it points at a backend you
can reach and that the credentials are valid.

`fixtures.py` resolves this `.env` relative to its own location (`../.env`), so no path
editing is needed — just keep the env file at the project root.

---

## 5. Run the tests

The test files import shared fixtures two different ways — some with `from fixtures import ...`
and some with `from python_tests.fixtures import *`. To satisfy both, **run from the repo
root with `python -m pytest`**. The `-m` form puts the repo root on `sys.path` (for the
`python_tests.fixtures` imports), and pytest adds `python_tests/` automatically (for the
`fixtures` imports):

```bash
# from the repo root, with the venv active
python -m pytest python_tests                                    # run everything
python -m pytest python_tests/test_units.py                      # one file
python -m pytest python_tests/test_units.py::test_by_ids         # one test
python -m pytest python_tests -k timeseries                      # tests matching a substring
python -m pytest python_tests -s                                 # show stdout (the SDK prints response bodies)
python -m pytest python_tests -v                                 # verbose: one line per test
```

> Note: running bare `pytest` from inside `python_tests/` will fail to collect the files
> that do `from python_tests.fixtures import *`, because the repo root won't be on the path.
> Use `python -m pytest` from the repo root as shown above.

---

## Quick start (copy-paste)

The wrapper script does all of this for you:

```bash
cd /home/olav/projects/git/dataplatform-rust-sdk
./run_python_tests.sh
```

Or run the equivalent steps by hand:

```bash
cd /home/olav/projects/git/dataplatform-rust-sdk
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r python_tests/requirements.txt
(cd datahub_python_bindings && maturin develop)
python -m pytest python_tests
```

---

## Troubleshooting

- **`ModuleNotFoundError: No module named 'datahub_sdk'`** — the venv isn't active, or you
  haven't run `maturin develop` since creating it. Activate the venv and rebuild.
- **`ModuleNotFoundError: No module named 'fixtures'` / `'python_tests'`** — you're not
  running from the repo root, or you used bare `pytest` instead of `python -m pytest`. Run
  `python -m pytest python_tests` from the repo root.
- **Datapoint insert tests fail with `422` / `WRONGPASS ...`** — backend-side issue: the
  DataHub backend's Redis (time-series store) is rejecting its own credentials. Not a test
  or SDK problem; fix the backend's Redis auth. The dependent retrieve/delete tests then
  error because their datapoint fixtures couldn't insert.
- **`FileNotFoundError` / env errors at fixture setup** — the `.env` is missing from the
  project root or is incomplete (see [Backend config](#backend-config)).
- **Auth (401) or connection errors** — check `BASE_URL` and `TOKEN` (or the OAuth2
  variables) in `.env`, and confirm the backend is running and reachable.
- **Stale behavior after editing Rust code** — re-run `maturin develop`; the tests use the
  compiled module, not the `.rs` sources. `./run_python_tests.sh` does this automatically
  on every run, so prefer it over invoking `pytest` directly.
- **Tests that create/delete backend state are race-sensitive** — they `sleep` between
  operations and can be flaky under load; re-run or run a single file in isolation.
