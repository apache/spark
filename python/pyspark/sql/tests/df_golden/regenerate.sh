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
# Regenerates the DataFrame golden test files (*.test) in this directory by
# re-running the test module with SPARK_GENERATE_GOLDEN_FILES=1.
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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

MODULE="pyspark.sql.tests.df_golden.test_df_golden"
REL_DIR="python/pyspark/sql/tests/df_golden"

echo ">>> Regenerating golden files..."
SPARK_GENERATE_GOLDEN_FILES=1 python/run-tests --testnames "$MODULE"

echo ">>> Regenerated. Local changes:"
git status --short -- "$REL_DIR"

if [[ "$VERIFY" == true ]]; then
  echo ">>> Verifying against the regenerated golden files..."
  python/run-tests --testnames "$MODULE"
else
  echo ">>> Done. Review the diff, then optionally verify with: $0 --verify"
fi
