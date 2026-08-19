#!/usr/bin/env bash
#
# Build portable Linux wheels for this SDK locally, for every architecture, without CI,
# without root and without a system cross-toolchain.
#
#   ./build-local-wheels.sh                 # x86_64 + aarch64 -> dist/
#   ./build-local-wheels.sh x86_64          # just one
#   TARGETS="x86_64 aarch64 musl-x86_64" ./build-local-wheels.sh
#
# Why this exists: the wheels are the only way to install this SDK today. `pip install
# intellistream-datahub-sdk` 404s because the publish job in .github/workflows/release.yml
# is gated on a vX.Y.Z tag and no tag has ever been pushed, so anything that needs the
# Python bindings — a container image, a colleague's laptop, a demo — has to be handed a
# built wheel. CI produces them on every push, but only as Actions artifacts, which needs
# `gh` and access to this private repo.
#
# It also fixes a portability trap in the obvious local build. A plain `maturin build`
# links against the build host's glibc and emits a `linux_x86_64` wheel, which then refuses
# to install on any older distribution — a wheel built on Fedora 43 (glibc 2.34) will not
# run on Debian 12 (2.36 is fine, but 2.31 is not) and cannot be uploaded to PyPI at all.
# Building through zig targets glibc 2.17 and produces a real `manylinux_2_17` wheel, which
# installs essentially anywhere, and cross-compiles to aarch64 from an x86_64 host as a
# side effect — so Apple Silicon is covered without an ARM machine.
#
# Once the SDK is published to PyPI this script is a convenience rather than the only route.
set -euo pipefail
cd "$(dirname "$0")"

TARGETS="${TARGETS:-${*:-x86_64 aarch64}}"
OUT="${OUT:-$PWD/dist}"
VENV="${VENV:-$PWD/.xbuild}"

rust_target() {
  case "$1" in
    x86_64)      echo x86_64-unknown-linux-gnu ;;
    aarch64)     echo aarch64-unknown-linux-gnu ;;
    musl-x86_64) echo x86_64-unknown-linux-musl ;;
    musl-aarch64) echo aarch64-unknown-linux-musl ;;
    *)           echo "$1" ;;   # allow a full rust triple
  esac
}

command -v rustup >/dev/null || { echo "rustup is required (https://rustup.rs)" >&2; exit 1; }

# maturin + ziglang both come from pip, so this needs no dnf/apt and no sudo.
if [ ! -x "$VENV/bin/maturin" ]; then
  echo "==> creating build venv at $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet maturin ziglang
fi

# maturin shells out to a `zig` BINARY, but the pip package only exposes the module, so it
# reports "Failed to find zig" unless we put a shim on PATH.
if [ ! -x "$VENV/bin/zig" ]; then
  printf '#!/bin/sh\nexec %s -m ziglang "$@"\n' "$VENV/bin/python" > "$VENV/bin/zig"
  chmod +x "$VENV/bin/zig"
fi
export PATH="$VENV/bin:$PATH"

mkdir -p "$OUT"
for t in $TARGETS; do
  triple=$(rust_target "$t")
  echo "==> $triple"
  rustup target add "$triple" >/dev/null
  (cd datahub_python_bindings && maturin build --release --zig --target "$triple" --out "$OUT")
done

echo
echo "wheels in $OUT:"
ls -1 "$OUT"/*.whl 2>/dev/null || echo "  (none — the build produced nothing)"
echo
echo "Sanity-check one before shipping it:"
echo "  python3 -c \"import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); print([n for n in z.namelist() if n.endswith('.so')])\" <wheel>"
