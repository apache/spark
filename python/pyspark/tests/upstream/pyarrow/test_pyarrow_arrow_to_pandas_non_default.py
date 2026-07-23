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
Tests for PyArrow Array.to_pandas() with non-default arguments, using golden file
comparison.

PySpark relies on these arguments in production: when building pandas objects from
Arrow data (see ``python/pyspark/sql/pandas/conversion.py``), it calls
``to_pandas(coerce_temporal_nanoseconds=True)`` (and, when applicable,
``date_as_object=True``) so that the Arrow path produces the same
nanosecond-resolution ``datetime64[ns]`` / ``timedelta64[ns]`` values as the
non-Arrow path.  These tests record how each Arrow type behaves under those
arguments so CI fails loudly if the behavior drifts across pandas/PyArrow/NumPy
upgrades.

The shared ``_PyArrowToPandasTestBase`` holds the golden-file matrix driver and
the per-cell conversion helper.  Each concrete test class supplies its own source
arrays (the rows relevant to the argument under test) and one test method per
argument combination (each producing its own golden file).  New ``to_pandas``
arguments (e.g. ``zero_copy_only``) can be added as additional classes without
touching the existing ones.

## Golden File Cell Format

Each cell uses the value@type format:
- numpy ndarray: "python_list_repr@ndarray[dtype]"
- pandas Series: "python_list_repr@Series[dtype]"
- Error: "ERR@ExceptionClassName"

Values are formatted via tolist() for stable, Python-native representation.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_arrow_to_pandas_non_default.py

## PyArrow and pandas Version Compatibility

The golden files capture behavior for specific PyArrow and pandas versions.
Regenerate when upgrading either dependency, as to_pandas() behavior may change.
The committed golden files were generated with pandas 2.3.3, pyarrow 24.0.0, and
numpy 2.4.1.
"""

import datetime
import inspect
import os
import unittest
from typing import Callable, List, Optional

from pyspark.loose_version import LooseVersion
from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    have_numpy,
    pyarrow_requirement_message,
    pandas_requirement_message,
    numpy_requirement_message,
)
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


class _PyArrowToPandasTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for pa.Array.to_pandas() golden file tests.

    Concrete subclasses provide their own ``_build_source_arrays`` (the rows) and
    one or more ``test_*`` methods that call ``compare_or_generate_golden_matrix``.
    This base defines no ``test_*`` methods, so it contributes no tests itself.
    """

    def compare_or_generate_golden_matrix(
        self,
        row_names: List[str],
        col_names: List[str],
        compute_cell: Callable[[str, str], str],
        golden_file_prefix: str,
        index_name: str = "source \\ target",
        overrides: Optional[dict[tuple[str, str], str]] = None,
    ) -> None:
        """
        Run a matrix of computations and compare against (or generate) a golden file.

        1. If SPARK_GENERATE_GOLDEN_FILES=1, compute every cell, build a
           DataFrame, and save it as the new golden CSV / Markdown file.
        2. Otherwise, load the existing golden file and assert that every cell
           matches the freshly computed value.
        """
        generating = self.is_generating_golden()

        test_dir = os.path.dirname(inspect.getfile(type(self)))
        golden_csv = os.path.join(test_dir, f"{golden_file_prefix}.csv")
        golden_md = os.path.join(test_dir, f"{golden_file_prefix}.md")

        golden = None
        if not generating:
            golden = self.load_golden_csv(golden_csv)

        errors = []
        results = {}

        for row_name in row_names:
            for col_name in col_names:
                result = compute_cell(row_name, col_name)
                results[(row_name, col_name)] = result

                if not generating:
                    if overrides and (row_name, col_name) in overrides:
                        expected = overrides[(row_name, col_name)]
                    else:
                        expected = golden.loc[row_name, col_name]
                    if expected != result:
                        errors.append(
                            f"{row_name} -> {col_name}: expected '{expected}', got '{result}'"
                        )

        if generating:
            import pandas as pd

            index = pd.Index(row_names, name=index_name)
            df = pd.DataFrame(index=index)
            for col_name in col_names:
                df[col_name] = [results[(row, col_name)] for row in row_names]
            self.save_golden(df, golden_csv, golden_md)
        else:
            self.assertEqual(
                len(errors),
                0,
                f"\n{len(errors)} golden file mismatches:\n" + "\n".join(errors),
            )

    def _to_pandas_cell(self, arr, **to_pandas_kwargs) -> str:
        """
        Convert ``arr`` via ``to_pandas(**to_pandas_kwargs)`` and format the
        result as a golden-file cell, returning ``ERR@<ExceptionClass>`` if the
        conversion raises.
        """
        try:
            return self.repr_value(arr.to_pandas(**to_pandas_kwargs), max_len=0)
        except Exception as e:
            return f"ERR@{type(e).__name__}"


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayToPandasCoerceTemporalTests(_PyArrowToPandasTestBase):
    """
    Tests pa.Array.to_pandas(coerce_temporal_nanoseconds=True) via golden file comparison.

    Covers the temporal Arrow types the argument affects (timestamp and duration in
    units s/ms/us/ns, tz-aware timestamp, date, and time), plus overflow cases and
    a few non-temporal control rows.  Each type is tested without nulls, with a null,
    and empty.

    The date rows are recorded under two output columns: the default
    ``date_as_object=True`` (object-dtype ``datetime.date``, which coercion does not
    affect) and ``date_as_object=False`` (the numeric ``datetime64[ns]`` path where
    coercion takes effect).
    """

    def _build_source_arrays(self):
        """Build an ordered dict of named source PyArrow arrays for testing."""
        sources = {}

        # =====================================================================
        # Timestamp types (the primary target of coerce_temporal_nanoseconds)
        # =====================================================================
        dt1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
        dt2 = datetime.datetime(2024, 6, 15, 18, 30, 0)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"timestamp[{unit}]:standard"] = pa.array([dt1, dt2], pa.timestamp(unit))
            sources[f"timestamp[{unit}]:nullable"] = pa.array([dt1, None], pa.timestamp(unit))
            sources[f"timestamp[{unit}]:empty"] = pa.array([], pa.timestamp(unit))
        # Timestamp with timezone
        sources["timestamp[us,tz=UTC]:standard"] = pa.array(
            [dt1, dt2], pa.timestamp("us", tz="UTC")
        )
        sources["timestamp[us,tz=UTC]:nullable"] = pa.array(
            [dt1, None], pa.timestamp("us", tz="UTC")
        )
        sources["timestamp[us,tz=UTC]:empty"] = pa.array([], pa.timestamp("us", tz="UTC"))
        # Overflow: coercion to nanoseconds has a valid range (~1677-2262); a
        # far-future second-resolution timestamp cannot fit and should error.
        sources["timestamp[s]:overflow"] = pa.array(
            [datetime.datetime(2500, 1, 1)], pa.timestamp("s")
        )

        # =====================================================================
        # Duration types (also coerced to nanoseconds)
        # =====================================================================
        td1 = datetime.timedelta(days=1)
        td2 = datetime.timedelta(hours=2, minutes=30)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"duration[{unit}]:standard"] = pa.array([td1, td2], pa.duration(unit))
            sources[f"duration[{unit}]:nullable"] = pa.array([td1, None], pa.duration(unit))
            sources[f"duration[{unit}]:empty"] = pa.array([], pa.duration(unit))
        # Overflow: a duration beyond ~292 years exceeds the int64 nanosecond
        # range. Unlike timestamp overflow (which raises), coercing it silently
        # wraps around to a bogus value; this row pins that behavior.
        sources["duration[s]:overflow"] = pa.array(
            [datetime.timedelta(days=300 * 365)], pa.duration("s")
        )

        # =====================================================================
        # Date types. With the default date_as_object=True, pandas yields an
        # object-dtype Series of datetime.date, so coerce_temporal_nanoseconds
        # has nothing to coerce; the "date_as_object=False" column forces the
        # numeric datetime64[ns] path where the argument actually takes effect.
        # =====================================================================
        d1 = datetime.date(2024, 1, 1)
        d2 = datetime.date(2024, 6, 15)
        sources["date32:standard"] = pa.array([d1, d2], pa.date32())
        sources["date32:nullable"] = pa.array([d1, None], pa.date32())
        sources["date32:empty"] = pa.array([], pa.date32())
        sources["date64:standard"] = pa.array([d1, d2], pa.date64())
        sources["date64:nullable"] = pa.array([d1, None], pa.date64())
        sources["date64:empty"] = pa.array([], pa.date64())

        # =====================================================================
        # Time types (control: pandas yields object-dtype datetime.time, no
        # native time-of-day dtype, so coerce_temporal_nanoseconds is a no-op)
        # =====================================================================
        t1 = datetime.time(12, 30, 0)
        t2 = datetime.time(18, 45, 30)
        sources["time32[s]:standard"] = pa.array([t1, t2], pa.time32("s"))
        sources["time32[s]:nullable"] = pa.array([t1, None], pa.time32("s"))
        sources["time32[s]:empty"] = pa.array([], pa.time32("s"))
        sources["time32[ms]:standard"] = pa.array([t1, t2], pa.time32("ms"))
        sources["time32[ms]:nullable"] = pa.array([t1, None], pa.time32("ms"))
        sources["time32[ms]:empty"] = pa.array([], pa.time32("ms"))
        sources["time64[us]:standard"] = pa.array([t1, t2], pa.time64("us"))
        sources["time64[us]:nullable"] = pa.array([t1, None], pa.time64("us"))
        sources["time64[us]:empty"] = pa.array([], pa.time64("us"))
        sources["time64[ns]:standard"] = pa.array([t1, t2], pa.time64("ns"))
        sources["time64[ns]:nullable"] = pa.array([t1, None], pa.time64("ns"))
        sources["time64[ns]:empty"] = pa.array([], pa.time64("ns"))

        # =====================================================================
        # Non-temporal controls (unaffected by coerce_temporal_nanoseconds)
        # =====================================================================
        sources["int64:standard"] = pa.array([0, 1, -1], pa.int64())
        sources["int64:nullable"] = pa.array([0, 1, None], pa.int64())
        sources["float64:standard"] = pa.array([0.0, 1.5, -1.5], pa.float64())
        sources["string:standard"] = pa.array(["hello", "world", ""], pa.string())

        return sources

    # Output column recording to_pandas(coerce_temporal_nanoseconds=True).
    # date_as_object stays at its default (True).
    COL_PANDAS = "pandas series"

    # Output column recording to_pandas(coerce_temporal_nanoseconds=True,
    # date_as_object=False). date_as_object=False is the only path on which
    # coercion observably affects date types.
    COL_PANDAS_DATE_AS_OBJECT_FALSE = "pandas series (date_as_object=False)"

    def test_to_pandas_coerce_temporal_nanoseconds(self):
        """Test pa.Array.to_pandas(coerce_temporal_nanoseconds=True) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_PANDAS,
            self.COL_PANDAS_DATE_AS_OBJECT_FALSE,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a
        # newer pandas/PyArrow/NumPy legitimately changes a cell's output.
        # Add a LooseVersion-guarded block for each known drift.
        overrides: dict[tuple[str, str], str] = {}
        # Pandas 3 uses its dedicated string dtype for non-empty Arrow string arrays.
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            str_expected = "['hello', 'world', '']@Series[str]"
            overrides[("string:standard", self.COL_PANDAS)] = str_expected
            overrides[("string:standard", self.COL_PANDAS_DATE_AS_OBJECT_FALSE)] = str_expected

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_PANDAS:
                return self._to_pandas_cell(arr, coerce_temporal_nanoseconds=True)
            elif col_name == self.COL_PANDAS_DATE_AS_OBJECT_FALSE:
                return self._to_pandas_cell(
                    arr, coerce_temporal_nanoseconds=True, date_as_object=False
                )
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_coerce_temporal",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
