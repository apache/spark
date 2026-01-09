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
Test pa.array type inference for various input types.

This test monitors the behavior of PyArrow's type inference to ensure
PySpark's assumptions about PyArrow behavior remain valid across versions.

Test cases are organized by input category:
1. Nullable data - None value handling
2. Python list - Plain Python lists
3. Python tuple - Plain Python tuples
4. Python dict - Plain Python dicts (inferred as struct)
5. Pandas Series (numpy-backed)
6. Pandas nullable extension types
7. Pandas ArrowDtype
8. NumPy array
9. Nested list types
10. Explicit type specification
"""

import datetime
import unittest
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowTypeInferenceTests(unittest.TestCase):
    """Test PyArrow's type inference behavior for pa.array."""

    # =========================================================================
    # Helper method
    # =========================================================================
    def _run_inference_tests(self, cases, use_expected_as_explicit=False):
        """Run type inference tests from a list of (name, data, expected_type) tuples.

        If use_expected_as_explicit is True, pass expected_type as explicit type to pa.array().
        """
        import pyarrow as pa

        for name, data, expected_type in cases:
            with self.subTest(name=name):
                if use_expected_as_explicit:
                    arr = pa.array(data, type=expected_type)
                else:
                    arr = pa.array(data)
                self.assertEqual(arr.type, expected_type, f"Failed for {name}")

    # =========================================================================
    # 1. NULLABLE DATA - Test None value handling
    # =========================================================================
    def test_nullable_data(self):
        """Test type inference with nullable data (None values)."""
        import pyarrow as pa

        cases = [
            # fmt: off
            ("null_only",         [None],                                              pa.null()),
            ("int64",             [1, 2, 3],                                           pa.int64()),
            ("int64_with_none",   [1, 2, 3, None],                                     pa.int64()),
            ("float64",           [1.0, 2.0, 3.0],                                     pa.float64()),
            ("float64_with_none", [1.0, 2.0, None],                                    pa.float64()),
            ("string",            ["a", "b", "c"],                                     pa.string()),
            ("string_with_none",  ["a", "b", None],                                    pa.string()),
            ("bool",              [True, False, None],                                 pa.bool_()),
            ("date32",            [datetime.date(2024, 1, 1), None],                   pa.date32()),
            ("timestamp_us",      [datetime.datetime(2024, 1, 1, 12, 0, 0), None],     pa.timestamp("us")),
            ("time64_us",         [datetime.time(12, 30, 0), None],                    pa.time64("us")),
            ("duration_us",       [datetime.timedelta(days=1), None],                  pa.duration("us")),
            ("binary",            [b"hello", None],                                    pa.binary()),
            ("decimal128",        [Decimal("1.23"), None],                             pa.decimal128(3, 2)),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 2. PYTHON LIST - Type inference from Python lists
    # =========================================================================
    def test_plain_python_list(self):
        """Test type inference from Python lists."""
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = ZoneInfo("America/Los_Angeles")
        cases = [
            # fmt: off
            # ---- Primitive types ----
            ("int64",           [1, 2, 3],                                              pa.int64()),
            ("float64",         [1.0, 2.0, 3.0],                                        pa.float64()),
            ("mixed_int_float", [1, 2.0, 3],                                            pa.float64()),
            ("string",          ["a", "b", "c"],                                        pa.string()),
            ("bool",            [True, False, True],                                    pa.bool_()),

            # ---- Nested list ----
            ("list_int64",      [[1, 2], [3, 4]],                                       pa.list_(pa.int64())),

            # ---- Temporal types ----
            ("date32",       [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],    pa.date32()),
            ("timestamp_us", [datetime.datetime(2024, 1, 1, 12), 
                              datetime.datetime(2024, 1, 2, 12)],                       pa.timestamp("us")),
            ("time64_us",    [datetime.time(12, 30, 0), datetime.time(13, 45, 0)],      pa.time64("us")),
            ("duration_us",  [datetime.timedelta(days=1), datetime.timedelta(hours=2)], pa.duration("us")),

            # ---- Timezone-aware datetime ----
            ("timestamp_tz_sg",
             [datetime.datetime(2024, 1, 1, 12, tzinfo=sg), datetime.datetime(2024, 1, 2, 12, tzinfo=sg)],
                                                                                        pa.timestamp("us", tz=sg)),
            ("timestamp_tz_la",
             [datetime.datetime(2024, 1, 1, 12, tzinfo=la), datetime.datetime(2024, 1, 2, 12, tzinfo=la)],
                                                                                        pa.timestamp("us", tz=la)),

            # ---- Binary and decimal ----
            ("binary",         [b"hello", b"world"],                                    pa.binary()),
            ("decimal128_3_2", [Decimal("1.23"), Decimal("4.56")],                      pa.decimal128(3, 2)),
            ("decimal128_9_3", [Decimal("123456.789"), Decimal("987654.321")],          pa.decimal128(9, 3)),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 3. PYTHON TUPLE - Type inference from Python tuples
    # =========================================================================
    def test_plain_python_tuple(self):
        """Test type inference from Python tuples."""
        import pyarrow as pa

        cases = [
            # fmt: off
            ("int64",        (1, 2, 3),                                                pa.int64()),
            ("float64",      (1.0, 2.0, 3.0),                                          pa.float64()),
            ("string",       ("a", "b", "c"),                                          pa.string()),
            ("bool",         (True, False, True),                                      pa.bool_()),
            ("date32",       (datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),   pa.date32()),
            ("timestamp_us", (datetime.datetime(2024, 1, 1, 12), datetime.datetime(2024, 1, 2, 12)),
                                                                                       pa.timestamp("us")),
            ("time64_us",    (datetime.time(12, 30, 0), datetime.time(13, 45, 0)),     pa.time64("us")),
            ("duration_us",  (datetime.timedelta(days=1), datetime.timedelta(hours=2)),
                                                                                       pa.duration("us")),
            ("binary",       (b"hello", b"world"),                                     pa.binary()),
            ("decimal128",   (Decimal("1.23"), Decimal("4.56")),                       pa.decimal128(3, 2)),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 4. PYTHON DICT - Type inference from Python dicts (struct)
    # =========================================================================
    def test_plain_python_dict(self):
        """Test type inference from Python dicts (inferred as struct)."""
        import pyarrow as pa

        cases = [
            # fmt: off
            # ---- Basic dict ----
            ("struct_empty",      [{}],                                                 pa.struct([])),
            ("struct_single_int", [{"x": 1}],                                           pa.struct([("x", pa.int64())])),
            ("struct_single_str", [{"x": "a"}],                                         pa.struct([("x", pa.string())])),
            ("struct_int_str",    [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],             pa.struct([("x", pa.int64()), ("y", pa.string())])),

            # ---- Dict with None values ----
            ("struct_with_null_field", [{"a": 3, "b": None, "c": "s"}],                 pa.struct([("a", pa.int64()), ("b", pa.null()), ("c", pa.string())])),
            ("struct_with_none", [{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}],        pa.struct([("x", pa.int64()), ("y", pa.string())])),

            # ---- Dict with mixed/different keys ----
            ("struct_mixed_keys", [{"a": 3, "b": "s"}, {"x": 5, "a": 1.0}],             pa.struct([("a", pa.float64()), ("b", pa.string()), ("x", pa.int64())])),

            # ---- Dict with nested values ----
            ("struct_nested_list", [{"x": 1, "y": [1, 2]}, {"x": 2, "y": [3, 4, 5]}],   pa.struct([("x", pa.int64()), ("y", pa.list_(pa.int64()))])),
            ("struct_nested_dict", [{"outer": {"inner": 1}}],                           pa.struct([("outer", pa.struct([("inner", pa.int64())]))])),

            # ---- Dict with various value types ----
            ("struct_bool",     [{"flag": True}, {"flag": False}],                      pa.struct([("flag", pa.bool_())])),
            ("struct_float",    [{"val": 1.5}, {"val": 2.5}],                           pa.struct([("val", pa.float64())])),
            ("struct_date",     [{"d": datetime.date(2024, 1, 1)}],                     pa.struct([("d", pa.date32())])),
            ("struct_datetime", [{"ts": datetime.datetime(2024, 1, 1, 12, 0, 0)}],      pa.struct([("ts", pa.timestamp("us"))])),
            ("struct_binary",   [{"b": b"hello"}],                                      pa.struct([("b", pa.binary())])),
            ("struct_decimal",  [{"d": Decimal("1.23")}],                               pa.struct([("d", pa.decimal128(3, 2))])),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 5. PANDAS SERIES (numpy-backed) - Default pandas Series
    # =========================================================================
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_numpy_backed(self):
        """Test type inference from numpy-backed pandas Series."""
        import pandas as pd
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = "America/Los_Angeles"
        cases = [
            # fmt: off
            # ---- Basic types ----
            ("int64",         pd.Series([1, 2, 3]),                                    pa.int64()),
            ("float64",       pd.Series([1.0, 2.0, 3.0]),                              pa.float64()),
            ("string",        pd.Series(["a", "b", "c"]),                              pa.string()),
            ("bool",          pd.Series([True, False, True]),                          pa.bool_()),
            ("int_with_none", pd.Series([1, 2, None]),                                 pa.float64()),

            # ---- Temporal types ----
            ("date32",       pd.Series([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]),
                                                                                       pa.date32()),
            ("timestamp_ns", pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])),  pa.timestamp("ns")),
            ("duration_ns",  pd.Series(pd.to_timedelta(["1 day", "2 hours"])),         pa.duration("ns")),
            ("binary",       pd.Series([b"hello", b"world"]),                          pa.binary()),

            # ---- Timezone-aware ----
            ("timestamp_tz_sg",
             pd.Series([datetime.datetime(2024, 1, 1, 12, tzinfo=sg),
                        datetime.datetime(2024, 1, 2, 12, tzinfo=sg)]),
             pa.timestamp("ns", tz="Asia/Singapore")),
            ("timestamp_tz_la",
             pd.Series([pd.Timestamp("2024-01-01 12:00:00", tz=la),
                        pd.Timestamp("2024-01-02 12:00:00", tz=la)]),
             pa.timestamp("ns", tz=la)),

            # ---- Nested pandas Series ----
            ("list_int64",     pd.Series([[1, 2], [3, 4, 5]]),                         pa.list_(pa.int64())),
            ("list_float64",   pd.Series([[1.0, 2.0], [3.0]]),                         pa.list_(pa.float64())),
            ("list_string",    pd.Series([["a", "b"], ["c"]]),                         pa.list_(pa.string())),
            ("list_with_none", pd.Series([[1, 2], None, [3]]),                         pa.list_(pa.int64())),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 6. PANDAS NULLABLE EXTENSION TYPES
    # =========================================================================
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_nullable_extension(self):
        """Test type inference from pandas nullable extension types."""
        import pandas as pd
        import pyarrow as pa

        cases = [
            # fmt: off
            # ---- Nullable Integer ----
            ("Int8",              pd.Series([1, 2, 3], dtype=pd.Int8Dtype()),          pa.int8()),
            ("Int16",             pd.Series([1, 2, 3], dtype=pd.Int16Dtype()),         pa.int16()),
            ("Int32",             pd.Series([1, 2, 3], dtype=pd.Int32Dtype()),         pa.int32()),
            ("Int64",             pd.Series([1, 2, 3], dtype=pd.Int64Dtype()),         pa.int64()),
            ("Int64_with_none",   pd.Series([1, 2, None], dtype=pd.Int64Dtype()),      pa.int64()),

            # ---- Nullable Unsigned Integer ----
            ("UInt8",             pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()),         pa.uint8()),
            ("UInt16",            pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()),        pa.uint16()),
            ("UInt32",            pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()),        pa.uint32()),
            ("UInt64",            pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()),        pa.uint64()),

            # ---- Nullable Float ----
            ("Float32",           pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype()), pa.float32()),
            ("Float64",           pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype()), pa.float64()),
            ("Float64_with_none", pd.Series([1.0, 2.0, None], dtype=pd.Float64Dtype()),
                                                                                       pa.float64()),

            # ---- Nullable Boolean ----
            ("Boolean",           pd.Series([True, False, True], dtype=pd.BooleanDtype()),
                                                                                       pa.bool_()),
            ("Boolean_with_none", pd.Series([True, False, None], dtype=pd.BooleanDtype()),
                                                                                       pa.bool_()),

            # ---- Nullable String ----
            ("String",            pd.Series(["a", "b", "c"], dtype=pd.StringDtype()),  pa.string()),
            ("String_with_none",  pd.Series(["a", "b", None], dtype=pd.StringDtype()), pa.string()),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 7. PANDAS ARROWDTYPE
    # =========================================================================
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_arrow_dtype(self):
        """Test type inference from PyArrow-backed pandas Series (ArrowDtype)."""
        import pandas as pd
        import pyarrow as pa

        cases = [
            # fmt: off
            # ---- Integers ----
            ("arrow_int8",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8())),      pa.int8()),
            ("arrow_int64", pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),     pa.int64()),

            # ---- Floats ----
            ("arrow_float32", pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())),
                                                                                       pa.float32()),
            ("arrow_float64", pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())),
                                                                                       pa.float64()),

            # ---- Large string/binary ----
            ("arrow_large_string", pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.large_string())),
                                                                                       pa.large_string()),
            ("arrow_large_binary", pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.large_binary())),
                                                                                       pa.large_binary()),

            # ---- Date ----
            ("arrow_date32",
             pd.Series([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)], dtype=pd.ArrowDtype(pa.date32())),
                                                                                       pa.date32()),

            # ---- Timestamp with timezone ----
            ("arrow_ts_utc",
             pd.Series([datetime.datetime(2024, 1, 1, 12), datetime.datetime(2024, 1, 2, 12)],
                       dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC"))),             pa.timestamp("us", tz="UTC")),
            ("arrow_ts_sg",
             pd.Series([datetime.datetime(2024, 1, 1, 12), datetime.datetime(2024, 1, 2, 12)],
                       dtype=pd.ArrowDtype(pa.timestamp("us", tz="Asia/Singapore"))),  pa.timestamp("us", tz="Asia/Singapore")),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 8. NUMPY ARRAY
    # =========================================================================
    def test_numpy_array(self):
        """Test type inference from numpy arrays."""
        import numpy as np
        import pyarrow as pa

        cases = [
            # fmt: off
            # ---- Signed integers ----
            ("int8",   np.array([1, 2, 3], dtype=np.int8),                             pa.int8()),
            ("int16",  np.array([1, 2, 3], dtype=np.int16),                            pa.int16()),
            ("int32",  np.array([1, 2, 3], dtype=np.int32),                            pa.int32()),
            ("int64",  np.array([1, 2, 3], dtype=np.int64),                            pa.int64()),

            # ---- Unsigned integers ----
            ("uint8",  np.array([1, 2, 3], dtype=np.uint8),                            pa.uint8()),
            ("uint16", np.array([1, 2, 3], dtype=np.uint16),                           pa.uint16()),
            ("uint32", np.array([1, 2, 3], dtype=np.uint32),                           pa.uint32()),
            ("uint64", np.array([1, 2, 3], dtype=np.uint64),                           pa.uint64()),

            # ---- Floats ----
            ("float16", np.array([1.0, 2.0, 3.0], dtype=np.float16),                   pa.float16()),
            ("float32", np.array([1.0, 2.0, 3.0], dtype=np.float32),                   pa.float32()),
            ("float64", np.array([1.0, 2.0, 3.0], dtype=np.float64),                   pa.float64()),

            # ---- Boolean ----
            ("bool",   np.array([True, False, True], dtype=np.bool_),                  pa.bool_()),

            # ---- String ----
            ("string", np.array(["a", "b", "c"]),                                      pa.string()),

            # ---- Datetime64 ----
            ("datetime64_D",  np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[D]"),
                                                                                       pa.date32()),
            ("datetime64_s",  np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[s]"),
                                                                                       pa.timestamp("s")),
            ("datetime64_ms", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[ms]"),
                                                                                       pa.timestamp("ms")),
            ("datetime64_us", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[us]"),
                                                                                       pa.timestamp("us")),
            ("datetime64_ns", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[ns]"),
                                                                                       pa.timestamp("ns")),

            # ---- Timedelta64 ----
            ("timedelta64_s",  np.array([1, 2, 3], dtype="timedelta64[s]"),            pa.duration("s")),
            ("timedelta64_ms", np.array([1, 2, 3], dtype="timedelta64[ms]"),           pa.duration("ms")),
            ("timedelta64_us", np.array([1, 2, 3], dtype="timedelta64[us]"),           pa.duration("us")),
            ("timedelta64_ns", np.array([1, 2, 3], dtype="timedelta64[ns]"),           pa.duration("ns")),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 9. NESTED LIST TYPES
    # =========================================================================
    def test_nested_list_types(self):
        """Test type inference for nested list types."""
        import pyarrow as pa

        cases = [
            # fmt: off
            ("list_int64",           [[1, 2], [3, 4, 5]],                              pa.list_(pa.int64())),
            ("list_float64",         [[1.0, 2.0], [3.0]],                              pa.list_(pa.float64())),
            ("list_string",          [["a", "b"], ["c"]],                              pa.list_(pa.string())),
            ("list_bool",            [[True, False], [True]],                          pa.list_(pa.bool_())),
            ("list_with_none",       [[1, 2], None, [3]],                              pa.list_(pa.int64())),
            ("nested_list_2d",       [[[1, 2], [3]], [[4, 5]]],                        pa.list_(pa.list_(pa.int64()))),
            ("nested_list_3d",       [[[[1]]]],                                        pa.list_(pa.list_(pa.list_(pa.int64())))),
            ("list_mixed_int_float", [[1, 2.0], [3, 4.0]],                             pa.list_(pa.float64())),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 10. EXPLICIT TYPE SPECIFICATION
    # =========================================================================
    def test_explicit_type_specification(self):
        """Test that pa.array uses explicit type when specified."""
        import pyarrow as pa

        cases = [
            # fmt: off
            ("large_list",      [[1, 2], [3, 4]],                                      pa.large_list(pa.int64())),
            ("fixed_size_list", [[1, 2, 3], [4, 5, 6]],                                pa.list_(pa.int64(), 3)),
            ("map_str_int",     [[("a", 1), ("b", 2)]],                                pa.map_(pa.string(), pa.int64())),
            ("large_string",    ["hello", "world"],                                    pa.large_string()),
            ("large_binary",    [b"hello", b"world"],                                  pa.large_binary()),
            # fmt: on
        ]
        self._run_inference_tests(cases, use_expected_as_explicit=True)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
