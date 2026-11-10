#!/bin/sh
# Build the wasm module (build/pqparser.wasm). Needs Rust 1.88 or later
# with the wasm32-unknown-unknown target:
#   rustup target add wasm32-unknown-unknown
# CARGO may point at a rustup toolchain's cargo when the default one has
# no wasm target.
#
# zstd is C libzstd, so the build also needs a clang and llvm-ar that
# target wasm32: Homebrew's llvm on macOS (Apple's clang does not), the
# distribution's clang and llvm on Linux (apt-get install clang llvm).
# CC_wasm32_unknown_unknown and AR_wasm32_unknown_unknown choose others.
set -e
cd "$(dirname "$0")/.."
if [ -z "$CC_wasm32_unknown_unknown" ]; then
  llvm=""
  if command -v brew >/dev/null 2>&1 && [ -x "$(brew --prefix llvm)/bin/clang" ]; then
    llvm="$(brew --prefix llvm)/bin/"
  fi
  CC_wasm32_unknown_unknown="${llvm}clang"
  export CC_wasm32_unknown_unknown
fi
if [ -z "$AR_wasm32_unknown_unknown" ]; then
  for ar in "$(dirname "$(command -v "$CC_wasm32_unknown_unknown")")/llvm-ar" llvm-ar llvm-ar-20 llvm-ar-19 llvm-ar-18; do
    if command -v "$ar" >/dev/null 2>&1; then
      AR_wasm32_unknown_unknown="$ar"
      break
    fi
  done
  export AR_wasm32_unknown_unknown
fi
if ! "$CC_wasm32_unknown_unknown" --print-targets 2>/dev/null | grep -q wasm32; then
  echo "build-wasm.sh: $CC_wasm32_unknown_unknown does not target wasm32; install LLVM's clang (see above)" >&2
  exit 1
fi
CFLAGS_wasm32_unknown_unknown="${CFLAGS_wasm32_unknown_unknown:--Os}"
# Panic messages name source files; keep the build machine's paths out of them.
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--remap-path-prefix=${CARGO_HOME:-$HOME/.cargo}=cargo --remap-path-prefix=$(pwd)=pqparser"
export RUSTFLAGS
export CFLAGS_wasm32_unknown_unknown
${CARGO:-cargo} build --release --target wasm32-unknown-unknown
mkdir -p build
cp target/wasm32-unknown-unknown/release/pqparser.wasm build/pqparser.wasm
