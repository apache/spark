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
Tests for PyArrow Array.from_pandas() with default arguments using golden file comparison.

This test monitors the pandas -> Arrow direction, which PySpark relies on for
``createDataFrame(pandas_df)`` and for every pandas UDF's return value.  PySpark calls
``pa.Array.from_pandas(series, mask=mask, type=arrow_type, safe=safecheck)`` in
``pyspark/sql/conversion.py`` and ``pyspark/sql/pandas/conversion.py``; the non-default
arguments are covered by ``test_pyarrow_array_from_pandas_non_default.py``, which reuses
the source Series built here.

Rows are grouped by how pandas stores the Series, because that decides which branch
PySpark takes when computing ``mask``:

- numpy-backed dtypes, where ``mask=series.isnull()`` is passed;
- dtypes implementing the ``__arrow_array__`` protocol, where PySpark passes ``mask=None``
  because supplying a mask raises.  The protocol means "can export Arrow", not "is stored
  as Arrow": ``Int64`` is numpy values plus a byte mask and ``string[python]`` is an object
  ndarray, yet both implement it alongside the genuinely Arrow-backed ``[pyarrow]`` dtypes;
- values chosen to be lossy or ambiguous when coerced, which the non-default type tests
  need and whose unconverted baseline is recorded here.

## Golden File Cell Format

Each cell uses the value@type format:
- pandas Series: "python_list_repr@Series[dtype]"
- PyArrow Array: "python_list_repr@arrow_type"
- PyArrow ChunkedArray: "python_list_repr@chunked<arrow_type>"
- Error: "ERR@ExceptionClassName"

``from_pandas`` returns a ChunkedArray rather than an Array when the input Series is backed
by a chunked Arrow array, and the two are otherwise indistinguishable because both report
the element type.  The distinction is a real contract: ``create_arrow_table_from_pandas``
builds a ``pa.Table`` (which accepts either) specifically because of it -- see SPARK-46776.
``chunked<...>`` follows Arrow's angle-bracket spelling for parameterized types, keeping it
distinct from the square-bracket ``Series[...]`` / ``ndarray[...]`` used for pandas and
numpy containers.  The chunk count is deliberately not recorded: it varies with batching,
whereas chunked-versus-not is the behavior under test.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_array_from_pandas_default.py

## PyArrow and pandas Version Compatibility

The golden files capture behavior for specific PyArrow and pandas versions.
Regenerate when upgrading either dependency, as from_pandas() behavior may change.
"""

import datetime
import decimal
import unittest

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
if have_numpy:
    import numpy as np


class _PyArrowFromPandasTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for pa.Array.from_pandas() golden file tests.

    Owns the source Series inventory: three disjoint group methods unioned by
    ``_build_source_arrays``, whose order fixes the row order.  The default test below and
    the non-default tests in ``test_pyarrow_array_from_pandas_non_default.py`` subclass this
    to reuse the whole inventory or one group, along with ``repr_from_pandas_result``.  This
    base defines no ``test_*`` methods, so it contributes no tests itself.
    """

    @staticmethod
    def repr_from_pandas_result(value):
        """
        Format a ``from_pandas`` return value, marking a ChunkedArray as ``chunked<type>``.

        ``repr_value`` reports the element type, which is the same for an Array and a
        ChunkedArray, so without this the two are indistinguishable in a cell.
        """
        rendered = GoldenFileTestMixin.repr_value(value, max_len=0)
        if isinstance(value, pa.ChunkedArray):
            values, _, arrow_type = rendered.rpartition("@")
            return f"{values}@chunked<{arrow_type}>"
        return rendered

    def _from_pandas_cell(self, series, **from_pandas_kwargs) -> str:
        """
        Convert ``series`` via ``from_pandas(**from_pandas_kwargs)`` and format the result
        as a golden-file cell, returning ``ERR@<ExceptionClass>`` if the conversion raises.
        """
        try:
            return self.repr_from_pandas_result(pa.Array.from_pandas(series, **from_pandas_kwargs))
        except Exception as e:
            return f"ERR@{type(e).__name__}"

    def _numpy_backed_sources(self):
        """
        Series whose storage is numpy, so ``hasattr(series.array, "__arrow_array__")`` is
        False and PySpark passes ``mask=series.isnull()``.
        """
        sources = {}

        # =====================================================================
        # Integer types
        # =====================================================================
        for dtype in ["int8", "int16", "int32", "int64"]:
            info = np.iinfo(dtype)
            sources[f"{dtype}:standard"] = pd.Series([0, 1, -1, info.max, info.min], dtype=dtype)
            sources[f"{dtype}:empty"] = pd.Series([], dtype=dtype)
        # A numpy integer Series cannot hold a null, so pandas widens it to float64.
        sources["int64:nullable"] = pd.Series([0, 1, None])

        for dtype in ["uint8", "uint16", "uint32", "uint64"]:
            sources[f"{dtype}:standard"] = pd.Series([0, 1, np.iinfo(dtype).max], dtype=dtype)

        # =====================================================================
        # Float and boolean types
        # =====================================================================
        for dtype in ["float32", "float64"]:
            sources[f"{dtype}:standard"] = pd.Series([0.0, 1.5, -1.5], dtype=dtype)
            sources[f"{dtype}:nullable"] = pd.Series([0.0, np.nan, 1.5], dtype=dtype)
            sources[f"{dtype}:empty"] = pd.Series([], dtype=dtype)

        sources["bool:standard"] = pd.Series([True, False, True])
        sources["bool:empty"] = pd.Series([], dtype="bool")

        # =====================================================================
        # Object types (string, binary, decimal)
        # =====================================================================
        # Only the string rows need an explicit dtype; pandas 3 would infer its ``str``
        # dtype and they would stop being object rows.
        sources["object:string"] = pd.Series(["hello", "world", ""], dtype=object)
        sources["object:string-nullable"] = pd.Series(["hello", None, "world"], dtype=object)
        # Unpinned, so this records the dtype pandas infers from Python strings: object on
        # pandas 2, its dedicated str dtype on pandas 3, which Arrow reads as large_string.
        sources["string:inferred"] = pd.Series(["hello", "world"])
        sources["object:bytes"] = pd.Series([b"hello", b"world"])
        sources["object:empty"] = pd.Series([], dtype=object)
        sources["object:all-null"] = pd.Series([None, None], dtype=object)
        sources["object:decimal"] = pd.Series([decimal.Decimal("1.50"), decimal.Decimal("-2.25")])

        # =====================================================================
        # Nested types
        # =====================================================================
        # Inference builds these from Python containers.  It cannot reach ``map`` (that
        # needs an explicit type), nor ``large_list`` / ``fixed_size_list``, which
        # ``to_arrow_type`` never requests.
        sources["list<int64>:standard"] = pd.Series([[1, 2], [3]])
        sources["list<int64>:nullable"] = pd.Series([[1, 2], None])
        sources["list<int64>:null-element"] = pd.Series([[1, None], [3]])
        sources["list<string>:standard"] = pd.Series([["a", "b"], ["c"]])
        sources["list<list<int64>>:standard"] = pd.Series([[[1, 2], [3]], [[4]]])
        sources["list<struct>:standard"] = pd.Series([[{"a": 1}], [{"a": 2}]])
        sources["struct:standard"] = pd.Series([{"a": 1, "b": "x"}])
        sources["struct:nullable"] = pd.Series([{"a": 1, "b": "x"}, None])
        sources["struct<struct>:standard"] = pd.Series([{"a": {"b": 1}}])
        sources["struct<list<int64>>:standard"] = pd.Series([{"a": [1, 2]}])

        # These feed the nested type tests (non_default): a child value that overflows a
        # narrower element type, and homogeneous-value dicts, which infer as ``struct`` but
        # convert to ``map`` when the requested type asks for one.
        sources["list<int64>:overflow"] = pd.Series([[300, 2], [3]])
        sources["struct:overflow"] = pd.Series([{"a": 300, "b": "x"}])
        sources["struct<int64>:standard"] = pd.Series([{"a": 1, "b": 2}])
        sources["struct<int64>:overflow"] = pd.Series([{"a": 300, "b": 2}])

        # =====================================================================
        # Temporal types
        # =====================================================================
        # Each row pins its resolution so the row name stays accurate on both pandas
        # majors: pandas 2 infers "ns" from a datetime, pandas 3 infers "us".
        dt = datetime.datetime(2024, 6, 15, 18, 30, 0)
        for unit in ["ns", "us"]:
            sources[f"datetime64[{unit}]:standard"] = pd.Series([dt], dtype=f"datetime64[{unit}]")
            sources[f"datetime64[{unit}]:nullable"] = pd.Series(
                [dt, None], dtype=f"datetime64[{unit}]"
            )
            sources[f"datetime64[{unit}]:empty"] = pd.Series([], dtype=f"datetime64[{unit}]")
            sources[f"datetime64[{unit},tz]:standard"] = pd.Series(
                [dt], dtype=f"datetime64[{unit}, UTC]"
            )
            sources[f"datetime64[{unit},tz]:nullable"] = pd.Series(
                [dt, None], dtype=f"datetime64[{unit}, UTC]"
            )
            sources[f"timedelta64[{unit}]:standard"] = pd.Series(
                pd.to_timedelta(["1 days", "2 hours"]), dtype=f"timedelta64[{unit}]"
            )
            sources[f"timedelta64[{unit}]:nullable"] = pd.Series(
                pd.to_timedelta(["1 days", None]), dtype=f"timedelta64[{unit}]"
            )

        # Unpinned, so the cell always duplicates one of the pinned rows above.  Which one
        # it matches is the behavior recorded: "ns" on pandas 2, "us" on pandas 3.
        sources["datetime64:inferred"] = pd.Series([dt])
        sources["timedelta64:inferred"] = pd.Series(pd.to_timedelta(["1 days", "2 hours"]))

        # datetime64[ns] spans only 1677-2262, so a far-past value needs microseconds.
        sources["datetime64[us]:out-of-ns-range"] = pd.Series(
            [datetime.datetime(1500, 1, 1)], dtype="datetime64[us]"
        )

        # pandas has no native date or time dtype, so these stay object on their own.
        sources["date:standard"] = pd.Series([datetime.date(2024, 6, 15)])
        sources["time:standard"] = pd.Series([datetime.time(18, 30, 45)])

        # In object dtype pyarrow takes the unit from datetime's own microsecond
        # resolution, not from a numpy dtype, so these give timestamp[us] where the
        # datetime64[ns] rows above give timestamp[ns] -- and sub-microsecond digits are
        # dropped silently, while an explicit type=timestamp("us") raises ArrowInvalid.
        sources["object:datetime"] = pd.Series([dt], dtype=object)
        sources["object:datetime-sub-us"] = pd.Series(
            [pd.Timestamp("2024-01-01 00:00:00.000000123")], dtype=object
        )
        sources["object:timedelta"] = pd.Series([datetime.timedelta(days=1, hours=2)], dtype=object)

        # =====================================================================
        # Categorical type
        # =====================================================================
        # PySpark casts a categorical to its categories' dtype first, but the raw
        # categorical is what a user hands to createDataFrame.
        sources["category:standard"] = pd.Series(pd.Categorical(["a", "b", "a"]))
        sources["category:nullable"] = pd.Series(pd.Categorical(["a", None, "b"]))

        return sources

    def _protocol_sources(self):
        """
        Series implementing ``__arrow_array__``, so PySpark passes ``mask=None`` --
        supplying one raises, because the protocol returns an array whose validity bitmap
        is already built.

        The protocol means "can export Arrow", not "is stored as Arrow", and the rows
        below deliberately cover both: ``Int64`` is numpy values plus a byte mask and
        ``string[python]`` is an object ndarray, while the ``[pyarrow]`` dtypes hold Arrow
        buffers.  PySpark does not distinguish them -- ``conversion.py:435`` branches only
        on the protocol -- so they share one group.
        """
        sources = {}

        # =====================================================================
        # Nullable extension dtypes (numpy values plus a byte mask)
        # =====================================================================
        for dtype in ["Int8", "Int16", "Int32", "Int64"]:
            info = np.iinfo(dtype.lower())
            sources[f"{dtype}:standard"] = pd.Series([0, 1, info.max, info.min], dtype=dtype)
            sources[f"{dtype}:nullable"] = pd.Series([0, 1, None], dtype=dtype)
        sources["UInt64:standard"] = pd.Series([0, 1, np.iinfo("uint64").max], dtype="UInt64")
        sources["Int64:empty"] = pd.Series([], dtype="Int64")
        sources["Int64:all-null"] = pd.Series([None, None], dtype="Int64")

        sources["Float64:standard"] = pd.Series([0.0, 1.5], dtype="Float64")
        sources["Float64:nullable"] = pd.Series([0.0, None], dtype="Float64")

        sources["boolean:standard"] = pd.Series([True, False], dtype="boolean")
        sources["boolean:nullable"] = pd.Series([True, None], dtype="boolean")

        # StringArray: an object ndarray of Python str, yet it exports Arrow.
        sources["string[python]:standard"] = pd.Series(["hello", "world"], dtype="string[python]")
        sources["string[python]:nullable"] = pd.Series(["hello", None], dtype="string[python]")
        sources["string[python]:empty"] = pd.Series([], dtype="string[python]")

        # =====================================================================
        # PyArrow-backed dtypes (Arrow buffers, so the handoff is zero-copy)
        # =====================================================================
        # Since pandas 2.2 a pyarrow-backed string stores large_string, and the protocol
        # hands that back rather than the 32-bit string the name suggests.  PySpark works
        # around pyarrow < 19 ignoring a narrower request (SPARK-46776).
        sources["int64[pyarrow]:standard"] = pd.Series([0, 1, -1], dtype="int64[pyarrow]")
        sources["int64[pyarrow]:nullable"] = pd.Series([0, 1, None], dtype="int64[pyarrow]")
        sources["int64[pyarrow]:empty"] = pd.Series([], dtype="int64[pyarrow]")
        sources["double[pyarrow]:nullable"] = pd.Series([0.0, None], dtype="double[pyarrow]")
        sources["bool[pyarrow]:nullable"] = pd.Series([True, None], dtype="bool[pyarrow]")
        sources["string[pyarrow]:standard"] = pd.Series(["hello", "world"], dtype="string[pyarrow]")
        sources["string[pyarrow]:nullable"] = pd.Series(["hello", None], dtype="string[pyarrow]")
        sources["string[pyarrow]:empty"] = pd.Series([], dtype="string[pyarrow]")
        # Binary is not auto-promoted to large_binary the way string is, so large_binary
        # has to be requested explicitly.  This is the binary counterpart of the string
        # rows, and lets the type tests exercise the binary half of SPARK-46776.
        sources["large_binary[pyarrow]:standard"] = pd.Series(
            [b"hello", b"world"], dtype="large_binary[pyarrow]"
        )
        sources["timestamp[us][pyarrow]:standard"] = pd.Series(
            [datetime.datetime(2024, 1, 1, 12, 0, 0)], dtype="timestamp[us][pyarrow]"
        )

        # A chunked backing array makes from_pandas return a ChunkedArray, which
        # pa.RecordBatch.from_arrays rejects -- hence PySpark builds a pa.Table.
        sources["int64[pyarrow]:single-chunk"] = pd.Series(
            pd.arrays.ArrowExtensionArray(pa.chunked_array([pa.array([1, 2], pa.int64())]))
        )
        sources["int64[pyarrow]:multi-chunk"] = pd.Series(
            pd.arrays.ArrowExtensionArray(
                pa.chunked_array([pa.array([1, 2], pa.int64()), pa.array([3], pa.int64())])
            )
        )

        return sources

    def _coercion_sources(self):
        """
        Series whose values are lossy or ambiguous once a target type is requested.

        These exist for the non-default type tests, where ``safe=`` decides whether the
        conversion raises or silently changes the value.  Recording their unconverted
        results here gives those cells a baseline to be read against.
        """
        sources = {}

        # =====================================================================
        # Values that narrow lossily
        # =====================================================================
        sources["int64:overflow"] = pd.Series([300, 1])
        sources["float64:fractional"] = pd.Series([1.5, 2.5])
        sources["float64:infinity"] = pd.Series([np.inf, 1.0])
        sources["float64:precision"] = pd.Series([1.1234567890123])

        # The only temporal case where safe= flips.
        sources["datetime64[ns]:sub-us"] = pd.Series(
            pd.to_datetime(["2024-01-01 00:00:00.000000123"])
        )

        # =====================================================================
        # Values whose inferred type depends on element order
        # =====================================================================
        # Inference commits to the first element's type, so these same values either lose
        # the time component or raise, depending on their order.
        sources["object:date-then-datetime"] = pd.Series(
            [datetime.date(2024, 1, 1), datetime.datetime(2024, 1, 1, 5, 30)], dtype=object
        )
        sources["object:datetime-then-date"] = pd.Series(
            [datetime.datetime(2024, 1, 1, 5, 30), datetime.date(2024, 1, 1)], dtype=object
        )

        return sources

    def _build_source_arrays(self):
        """Build an ordered dict of named source pandas Series for testing."""
        sources = {}
        for group in [
            self._numpy_backed_sources(),
            self._protocol_sources(),
            self._coercion_sources(),
        ]:
            sources.update(group)
        return sources


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayFromPandasDefaultTests(_PyArrowFromPandasTestBase):
    """Tests pa.Array.from_pandas() with default arguments via golden file comparison."""

    def test_from_pandas_default(self):
        """Test pa.Array.from_pandas() with default arguments against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = ["pandas series", "arrow array"]

        overrides = {}
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            # Only the deliberately unpinned rows move: pandas 3 infers microseconds where
            # pandas 2 inferred nanoseconds, and its dedicated str dtype is backed by
            # large_string -- which a categorical's values inherit too.
            overrides.update(
                {
                    ("string:inferred", "pandas series"): "['hello', 'world']@Series[str]",
                    ("string:inferred", "arrow array"): "[hello, world]@large_string",
                    ("datetime64:inferred", "pandas series"): (
                        "[Timestamp('2024-06-15 18:30:00')]@Series[datetime64[us]]"
                    ),
                    ("datetime64:inferred", "arrow array"): "[2024-06-15 18:30:00]@timestamp[us]",
                    ("timedelta64:inferred", "pandas series"): (
                        "[Timedelta('1 days 00:00:00'), "
                        "Timedelta('0 days 02:00:00')]@Series[timedelta64[us]]"
                    ),
                    ("timedelta64:inferred", "arrow array"): (
                        "[1 day, 0:00:00, 2:00:00]@duration[us]"
                    ),
                    ("category:standard", "arrow array"): (
                        "[a, b, a]@dictionary<values=large_string, indices=int8, ordered=0>"
                    ),
                    ("category:nullable", "arrow array"): (
                        "[a, None, b]@dictionary<values=large_string, indices=int8, ordered=0>"
                    ),
                }
            )

        def compute_cell(row_name, col_name):
            series = sources[row_name]
            if col_name == "pandas series":
                return self.repr_value(series, max_len=0)
            else:
                return self._from_pandas_cell(series)

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_array_from_pandas_default",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
