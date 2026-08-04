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
# Regenerates the DataFrame golden test files under results/ by re-running the
# test modules under inputs/ with SPARK_GENERATE_GOLDEN_FILES=1.
#
# Usage:
#   python/pyspark/sql/tests/df_golden/regenerate.sh [--verify]
#
#   --verify  re-run the tests against the regenerated golden files.

set -euo pipefail

VERIFY=false
for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY=true ;;
    *) echo "ERROR: unknown argument: $arg" >&2; exit 1 ;;
  esac
done

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$THIS_DIR" && git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

REL_DIR="python/pyspark/sql/tests/df_golden"

# Every module declaring golden cases, i.e. every test module under inputs/ with
# a top-level class mixing in DFGoldenTestMixin. Adding one needs no change here.
MODULES=()
for file in "$THIS_DIR"/inputs/test_*.py; do
  if grep -q "^class .*(DFGoldenTestMixin" "$file"; then
    MODULES+=("pyspark.sql.tests.df_golden.inputs.$(basename "$file" .py)")
  fi
done
if [[ ${#MODULES[@]} -eq 0 ]]; then
  echo "ERROR: no golden test modules found in $REL_DIR/inputs" >&2
  exit 1
fi
TESTNAMES="$(IFS=,; echo "${MODULES[*]}")"

echo ">>> Regenerating golden files for: $TESTNAMES"
SPARK_GENERATE_GOLDEN_FILES=1 python/run-tests --testnames "$TESTNAMES"

echo ">>> Regenerated. Local changes:"
git status --short -- "$REL_DIR"

if [[ "$VERIFY" == true ]]; then
  echo ">>> Verifying against the regenerated golden files..."
  python/run-tests --testnames "$TESTNAMES"
else
  echo ">>> Done. Review the diff, then optionally verify with: $0 --verify"
fi
