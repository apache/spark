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
Tests for PyArrow Array.to_pandas() with default arguments using golden file comparison.

This test monitors the behavior of PyArrow's to_pandas() conversion to ensure
PySpark's assumptions about PyArrow behavior remain valid across versions.

The test covers conversion of all major Arrow types to pandas/numpy with default
arguments (no types_mapper, no self_destruct, etc.), tracking:
- Which numpy/pandas dtype each Arrow type maps to
- How null values are handled (NaN, None, NaT, etc.)
- Whether values are preserved correctly after conversion

The shared ``_PyArrowToPandasTestBase`` holds the conversion helper and the source-array
inventory, split into type-family group methods that these and the non-default tests
reuse whole or by group.

## Golden File Cell Format

Each cell uses the value@type format:
- numpy ndarray: "python_list_repr@ndarray[dtype]"
- pandas Series: "python_list_repr@Series[dtype]"
- pandas Categorical: "python_list_repr@Categorical[dtype]"
- Error: "ERR@ExceptionClassName"

Values are formatted via tolist() for stable, Python-native representation.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_arrow_to_pandas_default.py

## PyArrow and pandas Version Compatibility

The golden files capture behavior for specific PyArrow and pandas versions.
Regenerate when upgrading either dependency, as to_pandas() behavior may change.
"""

import datetime
import unittest
from decimal import Decimal

from pyspark.loose_version import LooseVersion
from pyspark.testing.goldenutils import GoldenFileTestMixin
from pyspark.testing.utils import (
    have_numpy,
    have_pandas,
    have_pyarrow,
    numpy_requirement_message,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


class _PyArrowToPandasTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for pa.Array.to_pandas() golden file tests.

    Holds the conversion helper and the source-array inventory, split into type-family
    group methods reusable whole (via ``_build_source_arrays``) or one at a time, plus a
    ``_chunked_sources`` layout group of ChunkedArrays (chunk count is orthogonal to
    type).  Defines no ``test_*`` of its own.
    """

    def _to_pandas_cell(self, arr, **to_pandas_kwargs) -> str:
        """
        Convert ``arr`` via ``to_pandas(**to_pandas_kwargs)`` and format the
        result as a golden-file cell, returning ``ERR@<ExceptionClass>`` if the
        conversion raises.
        """
        try:
            result = arr.to_pandas(**to_pandas_kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return self.repr_value(result, max_len=0)

    def _numeric_sources(self):
        """Integer, float, and boolean arrays."""
        sources = {}

        # =====================================================================
        # Integer types
        # =====================================================================
        for bits, pa_type in [
            (8, pa.int8()),
            (16, pa.int16()),
            (32, pa.int32()),
            (64, pa.int64()),
        ]:
            max_val = 2 ** (bits - 1) - 1
            min_val = -(2 ** (bits - 1))
            sources[f"int{bits}:standard"] = pa.array([0, 1, -1, max_val, min_val], pa_type)
            sources[f"int{bits}:nullable"] = pa.array([0, 1, None], pa_type)
            sources[f"int{bits}:empty"] = pa.array([], pa_type)

        for bits, pa_type in [
            (8, pa.uint8()),
            (16, pa.uint16()),
            (32, pa.uint32()),
            (64, pa.uint64()),
        ]:
            max_val = 2**bits - 1
            sources[f"uint{bits}:standard"] = pa.array([0, 1, max_val], pa_type)
            sources[f"uint{bits}:nullable"] = pa.array([0, 1, None], pa_type)
            sources[f"uint{bits}:empty"] = pa.array([], pa_type)

        # =====================================================================
        # Float types
        # =====================================================================
        sources["float32:standard"] = pa.array([0.0, 1.5, -1.5], pa.float32())
        sources["float32:nullable"] = pa.array([0.0, 1.5, None], pa.float32())
        sources["float32:empty"] = pa.array([], pa.float32())
        sources["float64:standard"] = pa.array([0.0, 1.5, -1.5], pa.float64())
        sources["float64:nullable"] = pa.array([0.0, 1.5, None], pa.float64())
        sources["float64:special"] = pa.array(
            [float("nan"), float("inf"), float("-inf")], pa.float64()
        )
        sources["float64:empty"] = pa.array([], pa.float64())

        # =====================================================================
        # Boolean
        # =====================================================================
        sources["bool:standard"] = pa.array([True, False, True], pa.bool_())
        sources["bool:nullable"] = pa.array([True, False, None], pa.bool_())
        sources["bool:empty"] = pa.array([], pa.bool_())

        return sources

    def _string_binary_sources(self):
        """String, binary, and decimal arrays."""
        sources = {}

        # =====================================================================
        # String types
        # =====================================================================
        sources["string:standard"] = pa.array(["hello", "world", ""], pa.string())
        sources["string:nullable"] = pa.array(["hello", None, "world"], pa.string())
        sources["string:empty"] = pa.array([], pa.string())
        sources["large_string:standard"] = pa.array(["hello", "world"], pa.large_string())
        sources["large_string:nullable"] = pa.array(["hello", None], pa.large_string())
        sources["large_string:empty"] = pa.array([], pa.large_string())

        # =====================================================================
        # Binary types
        # =====================================================================
        sources["binary:standard"] = pa.array([b"hello", b"world"], pa.binary())
        sources["binary:nullable"] = pa.array([b"hello", None], pa.binary())
        sources["binary:empty"] = pa.array([], pa.binary())
        sources["large_binary:standard"] = pa.array([b"hello", b"world"], pa.large_binary())
        sources["large_binary:nullable"] = pa.array([b"hello", None], pa.large_binary())
        sources["large_binary:empty"] = pa.array([], pa.large_binary())

        # =====================================================================
        # Decimal
        # =====================================================================
        sources["decimal128:standard"] = pa.array(
            [Decimal("1.23"), Decimal("4.56"), Decimal("-7.89")],
            pa.decimal128(5, 2),
        )
        sources["decimal128:nullable"] = pa.array(
            [Decimal("1.23"), None, Decimal("4.56")], pa.decimal128(5, 2)
        )
        sources["decimal128:empty"] = pa.array([], pa.decimal128(5, 2))

        return sources

    def _temporal_sources(self):
        """Date, timestamp, duration, and time arrays."""
        sources = {}

        # =====================================================================
        # Date types
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
        # Timestamp types
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

        # =====================================================================
        # Duration types
        # =====================================================================
        td1 = datetime.timedelta(days=1)
        td2 = datetime.timedelta(hours=2, minutes=30)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"duration[{unit}]:standard"] = pa.array([td1, td2], pa.duration(unit))
            sources[f"duration[{unit}]:nullable"] = pa.array([td1, None], pa.duration(unit))
            sources[f"duration[{unit}]:empty"] = pa.array([], pa.duration(unit))

        # =====================================================================
        # Time types
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

        return sources

    def _nested_sources(self):
        """Null, list, struct, map, and their nested combinations."""
        sources = {}

        # =====================================================================
        # Null type
        # =====================================================================
        sources["null:standard"] = pa.array([None, None, None], pa.null())
        sources["null:empty"] = pa.array([], pa.null())

        # =====================================================================
        # Nested types
        # =====================================================================
        sources["list<int64>:standard"] = pa.array([[1, 2], [3, 4, 5]], pa.list_(pa.int64()))
        sources["list<int64>:nullable"] = pa.array([[1, 2], None, [3]], pa.list_(pa.int64()))
        sources["list<int64>:empty"] = pa.array([], pa.list_(pa.int64()))
        sources["list<string>:standard"] = pa.array([["a", "b"], ["c"]], pa.list_(pa.string()))
        sources["large_list<int64>:standard"] = pa.array(
            [[1, 2], [3, 4]], pa.large_list(pa.int64())
        )
        sources["large_list<int64>:empty"] = pa.array([], pa.large_list(pa.int64()))
        sources["fixed_size_list<int64>[3]:standard"] = pa.array(
            [[1, 2, 3], [4, 5, 6]], pa.list_(pa.int64(), 3)
        )
        sources["fixed_size_list<int64>[3]:empty"] = pa.array([], pa.list_(pa.int64(), 3))
        sources["struct:standard"] = pa.array(
            [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
            pa.struct([("x", pa.int64()), ("y", pa.string())]),
        )
        sources["struct:nullable"] = pa.array(
            [{"x": 1, "y": "a"}, None],
            pa.struct([("x", pa.int64()), ("y", pa.string())]),
        )
        sources["struct:empty"] = pa.array([], pa.struct([("x", pa.int64()), ("y", pa.string())]))
        sources["map<string,int64>:standard"] = pa.array(
            [[("a", 1), ("b", 2)], [("c", 3)]],
            pa.map_(pa.string(), pa.int64()),
        )
        sources["map<string,int64>:empty"] = pa.array([], pa.map_(pa.string(), pa.int64()))
        # list of list (nested list)
        sources["list<list<int64>>:standard"] = pa.array(
            [[[1, 2], [3]], [[4, 5, 6]]],
            pa.list_(pa.list_(pa.int64())),
        )
        # list of struct
        sources["list<struct>:standard"] = pa.array(
            [[{"x": 1}, {"x": 2}], [{"x": 3}]],
            pa.list_(pa.struct([("x", pa.int64())])),
        )
        # list of map
        sources["list<map<string,int64>>:standard"] = pa.array(
            [[[("a", 1)], [("b", 2)]], [[("c", 3)]]],
            pa.list_(pa.map_(pa.string(), pa.int64())),
        )
        # struct of struct
        sources["struct<struct>:standard"] = pa.array(
            [{"outer": {"inner": 1}}, {"outer": {"inner": 2}}],
            pa.struct([("outer", pa.struct([("inner", pa.int64())]))]),
        )
        # struct of list
        sources["struct<list<int64>>:standard"] = pa.array(
            [{"items": [1, 2, 3]}, {"items": [4, 5]}],
            pa.struct([("items", pa.list_(pa.int64()))]),
        )
        # struct of map
        sources["struct<map<string,int64>>:standard"] = pa.array(
            [{"mapping": [("a", 1)]}, {"mapping": [("b", 2)]}],
            pa.struct([("mapping", pa.map_(pa.string(), pa.int64()))]),
        )
        # map with list values
        sources["map<string,list<int64>>:standard"] = pa.array(
            [[("a", [1, 2]), ("b", [3])], [("c", [4, 5, 6])]],
            pa.map_(pa.string(), pa.list_(pa.int64())),
        )
        # map with struct values
        sources["map<string,struct>:standard"] = pa.array(
            [[("a", {"v": 1}), ("b", {"v": 2})], [("c", {"v": 3})]],
            pa.map_(pa.string(), pa.struct([("v", pa.int64())])),
        )
        # map of map (map with map values)
        sources["map<string,map<string,int64>>:standard"] = pa.array(
            [[("a", [("x", 1)]), ("b", [("y", 2)])], [("c", [("z", 3)])]],
            pa.map_(pa.string(), pa.map_(pa.string(), pa.int64())),
        )

        return sources

    def _dictionary_sources(self):
        """Dictionary-encoded arrays."""
        sources = {}

        # =====================================================================
        # Dictionary type
        # =====================================================================
        sources["dictionary<int32,string>:standard"] = pa.DictionaryArray.from_arrays(
            pa.array([0, 1, 0, 1], pa.int32()),
            pa.array(["a", "b"], pa.string()),
        )
        sources["dictionary<int32,string>:nullable"] = pa.DictionaryArray.from_arrays(
            pa.array([0, 1, None, 0], pa.int32()),
            pa.array(["a", "b"], pa.string()),
        )
        sources["dictionary<int32,string>:empty"] = pa.DictionaryArray.from_arrays(
            pa.array([], pa.int32()),
            pa.array([], pa.string()),
        )

        return sources

    def _chunked_sources(self):
        """
        ChunkedArray inputs covering the chunk-count axis.

        pa.Array and pa.ChunkedArray share one ``to_pandas``, and Spark feeds a
        ChunkedArray into it on the Table->columns path
        (``python/pyspark/sql/pandas/conversion.py``): ``df.toPandas()`` builds one per
        partition (no ``combine_chunks``), the empty-dataset branch uses
        ``empty_table()`` (a single empty chunk), and the UDF paths ``combine_chunks()``
        first (one chunk).  Chunk count is orthogonal to type, so these rows reuse a few
        representative types and vary only how the values are split.
        """
        sources = {}

        # =====================================================================
        # Numeric chunk layouts (int64: the type most Spark columns reduce to)
        # =====================================================================
        sources["int64:zero-chunk"] = pa.chunked_array([], type=pa.int64())
        sources["int64:empty-chunk"] = pa.chunked_array([pa.array([], pa.int64())])
        sources["int64:single-chunk"] = pa.chunked_array([pa.array([1, 2, 3], pa.int64())])
        sources["int64:multi-chunk"] = pa.chunked_array(
            [pa.array([1, 2], pa.int64()), pa.array([3, 4], pa.int64())]
        )
        # Null in one chunk, not the other. Reachable via cogrouped applyInPandas, the
        # one UDF path that does not combine_chunks() first.
        sources["int64:multi-chunk-nullable"] = pa.chunked_array(
            [pa.array([1, None], pa.int64()), pa.array([2], pa.int64())]
        )
        # Empty chunk between two non-empty ones: concatenation must skip it cleanly.
        sources["int64:multi-chunk-with-empty"] = pa.chunked_array(
            [pa.array([1, 2], pa.int64()), pa.array([], pa.int64()), pa.array([3], pa.int64())]
        )
        sources["float64:multi-chunk"] = pa.chunked_array(
            [pa.array([1.5, 2.5], pa.float64()), pa.array([3.5], pa.float64())]
        )

        # =====================================================================
        # Variable-width chunk layouts (offset buffers, not just values)
        # =====================================================================
        sources["string:single-chunk"] = pa.chunked_array([pa.array(["a", "b"], pa.string())])
        sources["string:multi-chunk"] = pa.chunked_array(
            [pa.array(["a", "b"], pa.string()), pa.array(["c"], pa.string())]
        )
        sources["string:multi-chunk-nullable"] = pa.chunked_array(
            [pa.array(["a", None], pa.string()), pa.array(["c"], pa.string())]
        )

        # =====================================================================
        # Nested chunk layouts (child arrays split across chunks)
        # =====================================================================
        sources["list<int64>:multi-chunk"] = pa.chunked_array(
            [pa.array([[1, 2], [3]], pa.list_(pa.int64())), pa.array([[4]], pa.list_(pa.int64()))]
        )
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        sources["struct:multi-chunk"] = pa.chunked_array(
            [
                pa.array([{"x": 1, "y": "a"}], struct_type),
                pa.array([{"x": 2, "y": "b"}], struct_type),
            ]
        )

        return sources

    def _build_source_arrays(self):
        """Build an ordered dict of named source PyArrow arrays for testing."""
        sources = {}
        for group in [
            self._numeric_sources(),
            self._string_binary_sources(),
            self._temporal_sources(),
            self._nested_sources(),
            self._dictionary_sources(),
            self._chunked_sources(),
        ]:
            sources.update(group)
        return sources


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayToPandasDefaultTests(_PyArrowToPandasTestBase):
    """
    Tests pa.Array.to_pandas() with default arguments via golden file comparison.

    Covers all major Arrow types: integers, floats, bool, string, binary,
    decimal, date, timestamp, duration, time, null, and nested types.
    Each type is tested both without and with null values.
    """

    def test_to_pandas_default(self):
        """Test pa.Array.to_pandas() with default arguments against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = ["pyarrow array", "pandas series"]

        overrides = {}
        # Pandas 3 uses its dedicated string dtype for non-empty Arrow string arrays.
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            overrides.update(
                {
                    ("string:standard", "pandas series"): ("['hello', 'world', '']@Series[str]"),
                    ("string:nullable", "pandas series"): ("['hello', nan, 'world']@Series[str]"),
                    ("large_string:standard", "pandas series"): ("['hello', 'world']@Series[str]"),
                    ("large_string:nullable", "pandas series"): "['hello', nan]@Series[str]",
                    ("string:single-chunk", "pandas series"): "['a', 'b']@Series[str]",
                    ("string:multi-chunk", "pandas series"): "['a', 'b', 'c']@Series[str]",
                    ("string:multi-chunk-nullable", "pandas series"): "['a', nan, 'c']@Series[str]",
                }
            )
            # PyArrow 24 extends the pandas string dtype conversion to empty arrays.
            if LooseVersion(pa.__version__) >= LooseVersion("24.0.0"):
                overrides.update(
                    {
                        ("string:empty", "pandas series"): "[]@Series[str]",
                        ("large_string:empty", "pandas series"): "[]@Series[str]",
                    }
                )

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            else:
                return self._to_pandas_cell(arr)

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_default",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
