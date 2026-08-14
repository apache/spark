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
Tests for PyArrow Table.to_pandas() using golden file comparison.

Unlike Array/ChunkedArray.to_pandas() (which returns a Series and is covered by
test_pyarrow_arrow_to_pandas_{default,non_default}.py), Table.to_pandas() returns a
DataFrame. Its per-column conversion matches the Array tests, so this file pins the
genuinely Table-specific behavior instead: multi-column DataFrame assembly and the
empty-table edges (0 columns / 0 rows). Spark calls Table.to_pandas() at
python/pyspark/sql/pandas/conversion.py:255 (the 0-column path) and, in Spark Connect,
at python/pyspark/sql/connect/client/core.py:1423 (a bare whole-Table conversion).

## Golden File Cell Format

Each cell uses the value@type format:
- pyarrow Table: "{col: [val1, val2, None], ...}@Table[name: type, ...]"
- pandas DataFrame: "{col: [values], ...}@Dataframe[name dtype, ...]"
- Error: "ERR@ExceptionClassName"

Values are formatted via tolist() for stable, Python-native representation.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_table_to_pandas.py
"""

import datetime
import unittest

from pyspark.loose_version import LooseVersion
from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    pyarrow_requirement_message,
    pandas_requirement_message,
)
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


class _PyArrowTableToPandasTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for pa.Table.to_pandas() golden file tests.

    Holds the conversion helper and the source-table inventory, split into group
    methods that these and the (temporal-flag) tests reuse. Defines no ``test_*`` of
    its own.
    """

    def _to_pandas_cell(self, table, **to_pandas_kwargs) -> str:
        """
        Convert ``table`` via ``to_pandas(**to_pandas_kwargs)`` and format the result
        as a golden-file cell, returning ``ERR@<ExceptionClass>`` if the conversion
        raises. Only the conversion is guarded: a formatting error is a test bug, not
        a conversion signal, so it propagates instead of masquerading as ``ERR@``.
        """
        try:
            pdf = table.to_pandas(**to_pandas_kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return self.repr_value(pdf, max_len=0)

    def _structural_tables(self):
        """Assembly shapes and empty edges (no temporal coercion involved)."""
        sources = {}

        # =====================================================================
        # Empty edges (shapes an Array/ChunkedArray cannot be)
        # =====================================================================
        sources["empty:0-columns"] = pa.table({})
        sources["empty:columns-no-rows"] = pa.table(
            {"i": pa.array([], pa.int64()), "s": pa.array([], pa.string())}
        )

        # =====================================================================
        # Multi-column assembly
        # =====================================================================
        sources["single-column"] = pa.table({"i": pa.array([1, 2, None], pa.int64())})
        sources["single-column:string"] = pa.table(
            {"s": pa.array(["hello", "world", None], pa.string())}
        )
        sources["multi-column:mixed-scalar"] = pa.table(
            {
                "i": pa.array([1, 2, None], pa.int64()),
                "s": pa.array(["a", "b", None], pa.string()),
                "f": pa.array([1.5, 2.5, 3.5], pa.float64()),
                "b": pa.array([True, False, None], pa.bool_()),
            }
        )
        sources["multi-column:all-null"] = pa.table(
            {
                "i": pa.array([None, None], pa.int64()),
                "s": pa.array([None, None], pa.string()),
            }
        )
        sources["multi-column:nested"] = pa.table(
            {
                "lst": pa.array([[1, 2], [3], None], pa.list_(pa.int64())),
                "st": pa.array([{"x": 1}, None, {"x": 3}], pa.struct([("x", pa.int64())])),
            }
        )

        return sources

    def _temporal_tables(self):
        """
        Tables with temporal columns. Shared with the temporal-flag tests, where
        coerce_temporal_nanoseconds / date_as_object actually move cells.
        """
        sources = {}

        sources["temporal:timestamp"] = pa.table(
            {
                "ts": pa.array(
                    [
                        datetime.datetime(2020, 1, 1, 5, 30),
                        datetime.datetime(2021, 6, 15, 23, 59),
                    ],
                    pa.timestamp("us"),
                )
            }
        )
        sources["temporal:date32"] = pa.table(
            {"d": pa.array([datetime.date(2020, 1, 1), datetime.date(2021, 6, 15)], pa.date32())}
        )
        sources["temporal:date64"] = pa.table(
            {"d": pa.array([datetime.date(2020, 1, 1), datetime.date(2021, 6, 15)], pa.date64())}
        )
        sources["temporal:date32-far-future"] = pa.table(
            {"d": pa.array([datetime.date(9999, 12, 31)], pa.date32())}
        )
        sources["temporal:multi-column-mix"] = pa.table(
            {
                "ts": pa.array([datetime.datetime(2020, 1, 1, 5, 30)] * 2, pa.timestamp("us")),
                "d": pa.array(
                    [datetime.date(2020, 1, 1), datetime.date(9999, 12, 31)], pa.date32()
                ),
                "i": pa.array([1, 2], pa.int64()),
            }
        )

        return sources

    def _build_all_tables(self):
        """Build an ordered dict of named source PyArrow tables for testing."""
        sources = {}
        for group in [
            self._structural_tables(),
            self._temporal_tables(),
        ]:
            sources.update(group)
        return sources


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message or pandas_requirement_message,
)
class PyArrowTableToPandasDefaultTests(_PyArrowTableToPandasTestBase):
    """
    Tests pa.Table.to_pandas() with default arguments via golden file comparison.

    Pins multi-column DataFrame assembly (combined schema, column names, coexisting
    dtypes) and the empty-table edges.
    """

    def test_to_pandas_default(self):
        """Test pa.Table.to_pandas() with default arguments against golden file."""
        sources = self._build_all_tables()
        row_names = list(sources.keys())
        col_names = ["pyarrow table", "pandas dataframe"]

        overrides = {}
        pandas_3_plus = LooseVersion(pd.__version__) >= LooseVersion("3.0.0")
        pyarrow_19_plus = LooseVersion(pa.__version__) >= LooseVersion("19.0.0")

        # On pandas 3 with PyArrow 19+, Arrow string columns convert to the "str"
        # dtype (missing values become nan) rather than object dtype (None). This
        # boundary differs from the Array path (test_pyarrow_arrow_to_pandas_*),
        # whose empty string columns switch only at PyArrow 24.
        if pandas_3_plus and pyarrow_19_plus:
            overrides.update(
                {
                    ("single-column:string", "pandas dataframe"): (
                        "{'s': ['hello', 'world', nan]}@Dataframe[s str]"
                    )
                }
            )
            overrides.update(
                {
                    ("multi-column:mixed-scalar", "pandas dataframe"): (
                        "{'i': [1.0, 2.0, nan], 's': ['a', 'b', nan], "
                        "'f': [1.5, 2.5, 3.5], 'b': [True, False, None]}"
                        "@Dataframe[i float64, s str, f float64, b object]"
                    )
                }
            )
            overrides.update(
                {
                    ("multi-column:all-null", "pandas dataframe"): (
                        "{'i': [nan, nan], 's': [nan, nan]}@Dataframe[i float64, s str]"
                    )
                }
            )
            overrides.update(
                {
                    ("empty:columns-no-rows", "pandas dataframe"): (
                        "{'i': [], 's': []}@Dataframe[i int64, s str]"
                    )
                }
            )

        def compute_cell(row_name, col_name):
            table = sources[row_name]
            if col_name == "pyarrow table":
                return self.repr_value(table, max_len=0)
            else:
                return self._to_pandas_cell(table)

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_table_to_pandas",
            index_name="test case",
            overrides=overrides,
        )


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message or pandas_requirement_message,
)
class PyArrowTableToPandasCoerceTemporalTests(_PyArrowTableToPandasTestBase):
    """
    Tests pa.Table.to_pandas(coerce_temporal_nanoseconds=True) via golden file comparison.

    Reuses the shared temporal tables plus a coercion overflow row, recorded under two
    columns: the default date_as_object=True (dates stay object, unaffected by coercion)
    and date_as_object=False (the datetime64[ns] path where coercion applies, including
    the far-future overflow).
    """

    # to_pandas(coerce_temporal_nanoseconds=True); date_as_object at its default (True).
    COL_PANDAS = "pandas dataframe"

    # to_pandas(coerce_temporal_nanoseconds=True, date_as_object=False) -- the only path
    # on which coercion observably affects date columns.
    COL_PANDAS_DATE_AS_OBJECT_FALSE = "pandas dataframe (date_as_object=False)"

    def test_to_pandas_coerce_temporal_nanoseconds(self):
        """Test pa.Table.to_pandas(coerce_temporal_nanoseconds=True) against golden file."""
        sources = self._temporal_tables()
        # Coercion to nanoseconds has a valid range (~1677-2262); a far-future
        # second-resolution timestamp column cannot fit and raises.
        sources["temporal:timestamp-overflow"] = pa.table(
            {"ts": pa.array([datetime.datetime(2500, 1, 1)], pa.timestamp("s"))}
        )
        # A duration beyond ~292 years also exceeds the int64 nanosecond range, but
        # unlike timestamp overflow, coercion wraps it silently to a bogus value.
        sources["temporal:duration-overflow"] = pa.table(
            {"dur": pa.array([datetime.timedelta(days=300 * 365)], pa.duration("s"))}
        )
        row_names = list(sources.keys())
        col_names = [
            "pyarrow table",
            self.COL_PANDAS,
            self.COL_PANDAS_DATE_AS_OBJECT_FALSE,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a newer
        # pandas/PyArrow legitimately changes a cell. Add a LooseVersion-guarded block
        # for each known drift.
        overrides: dict[tuple[str, str], str] = {}

        def compute_cell(row_name, col_name):
            table = sources[row_name]
            if col_name == "pyarrow table":
                return self.repr_value(table, max_len=0)
            elif col_name == self.COL_PANDAS:
                return self._to_pandas_cell(table, coerce_temporal_nanoseconds=True)
            elif col_name == self.COL_PANDAS_DATE_AS_OBJECT_FALSE:
                return self._to_pandas_cell(
                    table, coerce_temporal_nanoseconds=True, date_as_object=False
                )
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_table_to_pandas_coerce_temporal",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
