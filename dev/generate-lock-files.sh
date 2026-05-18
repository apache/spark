#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Generates pip lock files for:
#   1. PySpark user-facing dependencies (pyspark-requirements.lock, pyspark.pylock.toml)
#   2. Spark build/dev tools        (build-requirements.lock, build.pylock.toml)
#
# Output is written to dev/lock-files/.  Commit the results so every checkout
# can reproduce the exact environment used at release time.
#
# Usage:
#   dev/generate-lock-files.sh [--python-version X.Y] [--platform PLATFORM]
#
# Defaults: Python 3.11, linux (maps to manylinux_2_17_x86_64).
# Run from the root of the Spark checkout.

set -euo pipefail

PYTHON_VERSION="3.11"
PLATFORM="linux"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-version) PYTHON_VERSION="$2"; shift 2 ;;
    --platform)       PLATFORM="$2";       shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Map friendly platform names to uv's --python-platform values and PEP 508 marker expressions.
case "$PLATFORM" in
  linux)
    UV_PLATFORM="manylinux_2_17_x86_64"
    UV_ENV_MARKER="sys_platform == 'linux' and platform_machine == 'x86_64'"
    ;;
  linux-arm64)
    UV_PLATFORM="manylinux_2_17_aarch64"
    UV_ENV_MARKER="sys_platform == 'linux' and platform_machine == 'aarch64'"
    ;;
  macos-arm64)
    UV_PLATFORM="macosx_11_0_arm64"
    UV_ENV_MARKER="sys_platform == 'darwin' and platform_machine == 'arm64'"
    ;;
  macos-x86_64)
    UV_PLATFORM="macosx_10_12_x86_64"
    UV_ENV_MARKER="sys_platform == 'darwin' and platform_machine == 'x86_64'"
    ;;
  *)
    UV_PLATFORM="$PLATFORM"
    UV_ENV_MARKER=""  # pass through verbatim; caller must set manually if needed
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_DIR="$SPARK_HOME/dev/lock-files"

if ! command -v uv &>/dev/null; then
  echo "uv not found; installing via pip..."
  pip install uv
fi

UV_COMPILE_ARGS=(
  --python-version "$PYTHON_VERSION"
  --python-platform "$UV_PLATFORM"
  --generate-hashes
  --quiet
)

echo "Generating lock files (Python $PYTHON_VERSION, platform $UV_PLATFORM)"

# ── 1. requirements.txt-format lock files ────────────────────────────────────

echo "  pyspark-requirements.lock"
uv pip compile "${UV_COMPILE_ARGS[@]}" \
  "$LOCK_DIR/pyspark-deps.in" \
  -o "$LOCK_DIR/pyspark-requirements.lock"

echo "  build-requirements.lock"
uv pip compile "${UV_COMPILE_ARGS[@]}" \
  "$SPARK_HOME/dev/requirements.txt" \
  -o "$LOCK_DIR/build-requirements.lock"

# ── 2. PEP 751 pylock.toml files ─────────────────────────────────────────────
# uv export --format pylock.toml reads a uv.lock produced by `uv lock`, which
# in turn needs a pyproject.toml.  We create two minimal temp projects, one per
# dependency set, resolve them, export, then clean up.

_generate_pylock() {
  local label="$1"       # e.g. "pyspark" or "build"
  local deps_file="$2"   # path to the .in / requirements.txt input
  local out_file="$3"    # destination .pylock.toml

  echo "  ${label}.pylock.toml"

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap "rm -rf '$tmp_dir'" RETURN

  # Convert the deps file into a PEP 508 list for pyproject.toml.
  # Strip blank lines and comment lines; collapse continuation lines.
  local deps_toml
  deps_toml=$(grep -v '^\s*#' "$deps_file" | grep -v '^\s*$' | \
    sed 's/\(.*\)/  "\1",/')

  local env_section=""
  if [[ -n "$UV_ENV_MARKER" ]]; then
    env_section="
[tool.uv]
environments = [\"${UV_ENV_MARKER}\"]"
  fi

  cat > "$tmp_dir/pyproject.toml" <<TOML
[project]
name = "spark-${label}-lock"
version = "0.0.0"
requires-python = ">=${PYTHON_VERSION}"
dependencies = [
${deps_toml}
]
${env_section}
TOML

  (
    cd "$tmp_dir"
    uv lock \
      --python "${PYTHON_VERSION}" \
      --quiet
    uv export \
      --format pylock.toml \
      --no-dev \
      --quiet \
      -o "$out_file"
  )
}

_generate_pylock "pyspark" "$LOCK_DIR/pyspark-deps.in"          "$LOCK_DIR/pylock.pyspark.toml"
_generate_pylock "build"   "$SPARK_HOME/dev/requirements.txt"   "$LOCK_DIR/pylock.build.toml"

echo "Done.  Updated files in dev/lock-files/:"
ls -1 "$LOCK_DIR"/*.lock "$LOCK_DIR"/*.toml 2>/dev/null
