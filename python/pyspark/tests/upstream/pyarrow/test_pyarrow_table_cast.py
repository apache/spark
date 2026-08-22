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

"""
Tests for PyArrow's pa.Table.cast() method using golden file comparison.

Unlike pa.Array.cast() (covered by test_pyarrow_array_cast.py), Table.cast() casts a
whole table to a target *schema*. Per-column type conversion matches the Array tests,
so this file pins the genuinely Table-level behavior instead: multi-column casts, strict
field-name/order matching, target-field nullability enforcement, temporal coercion, and
the empty-table edges. Spark calls Table.cast() at
python/pyspark/sql/pandas/conversion.py:499 and :1108 (classic toArrow / createDataFrame)
and, in Spark Connect, at python/pyspark/sql/connect/dataframe.py:1995 and
python/pyspark/sql/connect/session.py:647,663.

This suite covers both safe=True (default) and safe=False modes:
- safe=True: checks for overflow/truncation, raising on unsafe conversions.
- safe=False: allows unsafe conversions (overflow wrapping, truncation).
Each mode has its own golden file. Field-name/order and nullability errors raise in both
modes -- they are checked before the per-column value cast.

## Golden File Cell Format

Each cell uses the value@type format:
- pyarrow Table: "{col: [val1, val2, None], ...}@Table[name: type, ...]"
- Error: "ERR@ExceptionClassName"

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_table_cast.py

If package tabulate (https://pypi.org/project/tabulate/) is installed,
it will also regenerate the Markdown files.

## PyArrow Version Compatibility

The golden files capture behavior for a specific PyArrow version. Regenerate when
upgrading PyArrow, as cast support may change between versions. Table.cast() is
pandas-free (PyArrow in, PyArrow out), so no pandas-version differences apply.
"""

import unittest

from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    pyarrow_requirement_message,
    pandas_requirement_message,
)
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_pyarrow:
    import pyarrow as pa


class _PyArrowTableCastTestBase(GoldenFileTestMixin, unittest.TestCase):
    """Base class for pa.Table.cast() golden file tests. Defines no test_* of its own."""

    def _try_cast(self, table, target_schema, safe=True) -> str:
        """
        Cast ``table`` to ``target_schema`` and format the result as a golden cell,
        returning ``ERR@<ExceptionClass>`` if the cast raises. Only the cast is guarded:
        a formatting error is a test bug, not a cast signal, so it propagates.
        """
        try:
            result = table.cast(target_schema, safe=safe)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return self.repr_value(result, max_len=0)

    def _cast_scenarios(self):
        """
        Ordered {name: (source_table, target_schema)} pairs, each isolating one
        Table.cast contract. Shared by the safe and unsafe test methods.
        """
        scenarios = {}

        # =====================================================================
        # Multi-column type cast (whole-schema assembly)
        # =====================================================================
        scenarios["types:downcast"] = (
            pa.table(
                {
                    "a": pa.array([1, 2, 3], pa.int64()),
                    "b": pa.array([1.5, 2.5, 3.5], pa.float64()),
                }
            ),
            pa.schema([("a", pa.int32()), ("b", pa.float32())]),
        )
        scenarios["types:upcast"] = (
            pa.table(
                {
                    "a": pa.array([1, 2, 3], pa.int32()),
                    "b": pa.array([1.5, 2.5, 3.5], pa.float32()),
                }
            ),
            pa.schema([("a", pa.int64()), ("b", pa.float64())]),
        )

        # =====================================================================
        # safe axis: these flip between the safe and unsafe goldens
        # =====================================================================
        scenarios["overflow:int64->int32"] = (
            pa.table({"a": pa.array([2**40, 1], pa.int64())}),
            pa.schema([("a", pa.int32())]),
        )
        scenarios["truncate:float->int"] = (
            pa.table({"a": pa.array([1.9, -2.1], pa.float64())}),
            pa.schema([("a", pa.int64())]),
        )

        # =====================================================================
        # Columns are matched positionally + name-equal (no match/reorder by name), so
        # a name mismatch, a pure reorder, and a wrong field count all raise ValueError.
        # =====================================================================
        base_ab = pa.table(
            {"a": pa.array([1, 2], pa.int64()), "b": pa.array([1.5, 2.5], pa.float64())}
        )
        scenarios["names:mismatch"] = (
            base_ab,
            pa.schema([("x", pa.int32()), ("b", pa.float32())]),
        )
        scenarios["names:reordered"] = (
            base_ab,
            pa.schema([("b", pa.float64()), ("a", pa.int64())]),
        )
        scenarios["names:field-count"] = (
            base_ab,
            pa.schema([("a", pa.int32())]),
        )

        # =====================================================================
        # Target-field nullability enforcement
        # =====================================================================
        scenarios["nullable:false-with-nulls"] = (
            pa.table({"a": pa.array([1, None], pa.int64())}),
            pa.schema([pa.field("a", pa.int32(), nullable=False)]),
        )
        scenarios["nullable:false-no-nulls"] = (
            pa.table({"a": pa.array([1, 2], pa.int64())}),
            pa.schema([pa.field("a", pa.int32(), nullable=False)]),
        )

        # =====================================================================
        # Temporal unit / timezone coercion
        # =====================================================================
        ts_us = pa.table({"ts": pa.array([0, 1_000_000], pa.timestamp("us"))})
        scenarios["timestamp:us->ns"] = (
            ts_us,
            pa.schema([("ts", pa.timestamp("ns"))]),
        )
        scenarios["timestamp:attach-tz"] = (
            ts_us,
            pa.schema([("ts", pa.timestamp("us", "UTC"))]),
        )

        # =====================================================================
        # Variable-width widening
        # =====================================================================
        scenarios["string->large_string"] = (
            pa.table({"s": pa.array(["hello", "world", None], pa.string())}),
            pa.schema([("s", pa.large_string())]),
        )
        scenarios["binary->large_binary"] = (
            pa.table({"b": pa.array([b"x", b"yz", None], pa.binary())}),
            pa.schema([("b", pa.large_binary())]),
        )

        # =====================================================================
        # Nested column types: cast recurses into the container and casts each inner
        # element, carrying safe= down (see nested:list-overflow). Kept name- and
        # order-matched so these stay clean successes on every PyArrow version.
        # =====================================================================
        scenarios["nested:list"] = (
            pa.table({"lst": pa.array([[1, 2], [3], None], pa.list_(pa.int64()))}),
            pa.schema([("lst", pa.list_(pa.int32()))]),
        )
        scenarios["nested:list-overflow"] = (
            pa.table({"lst": pa.array([[2**40, 1]], pa.list_(pa.int64()))}),
            pa.schema([("lst", pa.list_(pa.int32()))]),
        )
        scenarios["nested:struct"] = (
            pa.table(
                {
                    "st": pa.array(
                        [{"x": 1, "y": "a"}, None],
                        pa.struct([("x", pa.int64()), ("y", pa.string())]),
                    )
                }
            ),
            pa.schema([("st", pa.struct([("x", pa.int32()), ("y", pa.large_string())]))]),
        )
        scenarios["nested:map"] = (
            pa.table(
                {"m": pa.array([[("k", 1), ("j", 2)], None], pa.map_(pa.string(), pa.int64()))}
            ),
            pa.schema([("m", pa.map_(pa.string(), pa.int32()))]),
        )

        # =====================================================================
        # Multi-chunk column: exercises pa.ChunkedArray.cast under Table.cast
        # =====================================================================
        scenarios["multi-chunk-column"] = (
            pa.table({"a": pa.chunked_array([[1, 2], [3, None]], pa.int64())}),
            pa.schema([("a", pa.int32())]),
        )

        # =====================================================================
        # Empty edges
        # =====================================================================
        scenarios["empty:0-columns"] = (pa.table({}), pa.schema([]))
        scenarios["empty:columns-no-rows"] = (
            pa.table({"i": pa.array([], pa.int64()), "s": pa.array([], pa.string())}),
            pa.schema([("i", pa.int32()), ("s", pa.large_string())]),
        )

        return scenarios


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message or pandas_requirement_message,
)
class PyArrowTableCastTests(_PyArrowTableCastTestBase):
    """
    Tests pa.Table.cast(target_schema) with safe=True and safe=False via golden files.

    Pins Table-level cast behavior distinct from pa.Array.cast: whole-schema casts,
    strict field-name/order matching, target-field nullability enforcement, temporal
    coercion, and the empty-table edges.
    """

    def _run(self, safe, golden_file_prefix, overrides):
        scenarios = self._cast_scenarios()
        row_names = list(scenarios.keys())
        col_names = ["pyarrow table", "cast result"]

        def compute_cell(row_name, col_name):
            source_table, target_schema = scenarios[row_name]
            if col_name == "pyarrow table":
                return self.repr_value(source_table, max_len=0)
            elif col_name == "cast result":
                return self._try_cast(source_table, target_schema, safe=safe)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix=golden_file_prefix,
            index_name="test case",
            overrides=overrides,
        )

    def test_table_cast_matrix(self):
        """Test pa.Table.cast(target_schema) with safe=True (default)."""
        # PyArrow-version-specific expected cells; empty at the pa24/pd2 baseline.
        overrides: dict[tuple[str, str], str] = {}
        self._run(
            safe=True,
            golden_file_prefix="golden_pyarrow_table_cast_safe",
            overrides=overrides,
        )

    def test_table_cast_matrix_unsafe(self):
        """Test pa.Table.cast(target_schema) with safe=False."""
        overrides: dict[tuple[str, str], str] = {}
        self._run(
            safe=False,
            golden_file_prefix="golden_pyarrow_table_cast_unsafe",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
