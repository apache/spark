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
1. Nullable data - with None values
2. Plain Python instances - list, tuple, dict (struct)
3. Pandas instances - numpy-backed Series, nullable extension types, ArrowDtype
4. NumPy array - all numeric dtypes, datetime64, timedelta64
5. Nested types - list of list, struct, map
6. Explicit type specification - large_list, fixed_size_list, map_, large_string, large_binary
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

        # (name, data, expected_type)
        # fmt: off
        cases = [
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
        ]
        # fmt: on
        self._run_inference_tests(cases)

    # =========================================================================
    # 2. PLAIN PYTHON INSTANCES - list, tuple, dict
    # =========================================================================
    # 2a. Python list
    # -------------------------------------------------------------------------
    def test_plain_python_list(self):
        """Test type inference from Python lists."""
        import math
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = ZoneInfo("America/Los_Angeles")
        int64_max = 2**63 - 1
        int64_min = -(2**63)
        epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)
        epoch_date = datetime.date(1970, 1, 1)

        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- Integer ----
            ("int64",           [1, 2, 3],                                              pa.int64()),
            ("int64_max",       [int64_max],                                            pa.int64()),
            ("int64_min",       [int64_min],                                            pa.int64()),
            ("int64_range",     [int64_min, 0, int64_max],                              pa.int64()),
            ("zero",            [0],                                                    pa.int64()),
            ("negative",        [-1, -2, -3],                                           pa.int64()),

            # ---- Float ----
            ("float64",         [1.0, 2.0, 3.0],                                        pa.float64()),
            ("mixed_int_float", [1, 2.0, 3],                                            pa.float64()),
            ("nan",             [float("nan")],                                         pa.float64()),
            ("inf",             [float("inf")],                                         pa.float64()),
            ("neg_inf",         [float("-inf")],                                        pa.float64()),
            ("all_special",     [float("nan"), float("inf"), float("-inf")],            pa.float64()),
            ("mixed_nan",       [1.0, float("nan"), 3.0],                               pa.float64()),
            ("float64_max",     [1.7976931348623157e+308],                              pa.float64()),
            ("float64_tiny",    [2.2250738585072014e-308],                              pa.float64()),
            ("both_zeros",      [0.0, -0.0],                                            pa.float64()),

            # ---- String ----
            ("string",          ["a", "b", "c"],                                        pa.string()),
            ("chinese",         ["你好", "世界", "中文"],                                 pa.string()),
            ("japanese",        ["こんにちは", "世界"],                                   pa.string()),
            ("korean",          ["안녕하세요", "세계"],                                     pa.string()),
            ("emoji",           ["😀", "🎉", "🚀", "❤️"],                               pa.string()),
            ("emoji_mixed",     ["Hello 👋", "World 🌍"],                               pa.string()),
            ("arabic",          ["مرحبا", "العالم"],                                    pa.string()),
            ("cyrillic",        ["Привет", "Мир"],                                      pa.string()),
            ("french_accent",   ["Café", "Résumé"],                                     pa.string()),
            ("german_umlaut",   ["Grüß Gott", "Größe"],                                 pa.string()),
            ("empty_string",    ["", "a", ""],                                          pa.string()),
            ("whitespace",      ["  ", "\t", "\n"],                                     pa.string()),
            ("zero_width",      ["foo\u200bbar"],                                       pa.string()),

            # ---- Boolean ----
            ("bool",            [True, False, True],                                    pa.bool_()),

            # ---- Nested list ----
            ("list_int64",      [[1, 2], [3, 4]],                                       pa.list_(pa.int64())),

            # ---- Temporal ----
            ("date32",          [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)], pa.date32()),
            ("date_min",        [datetime.date(1, 1, 1)],                               pa.date32()),
            ("date_max",        [datetime.date(9999, 12, 31)],                          pa.date32()),
            ("timestamp_us",    [datetime.datetime(2024, 1, 1, 12),
                                 datetime.datetime(2024, 1, 2, 12)],                    pa.timestamp("us")),
            ("epoch_datetime",  [epoch],                                                pa.timestamp("us")),
            ("epoch_date",      [epoch_date],                                           pa.date32()),
            ("time64_us",       [datetime.time(12, 30, 0), datetime.time(13, 45, 0)],   pa.time64("us")),
            ("time_min",        [datetime.time(0, 0, 0, 0)],                            pa.time64("us")),
            ("time_max",        [datetime.time(23, 59, 59, 999999)],                    pa.time64("us")),
            ("duration_us",     [datetime.timedelta(days=1), datetime.timedelta(hours=2)],
                                                                                        pa.duration("us")),
            ("duration_zero",   [datetime.timedelta(0)],                                pa.duration("us")),
            ("duration_neg",    [datetime.timedelta(days=-1)],                          pa.duration("us")),

            # ---- Timezone-aware ----
            ("timestamp_tz_sg",
             [datetime.datetime(2024, 1, 1, 12, tzinfo=sg), datetime.datetime(2024, 1, 2, 12, tzinfo=sg)],
                                                                                        pa.timestamp("us", tz=sg)),
            ("timestamp_tz_la",
             [datetime.datetime(2024, 1, 1, 12, tzinfo=la), datetime.datetime(2024, 1, 2, 12, tzinfo=la)],
                                                                                        pa.timestamp("us", tz=la)),

            # ---- Binary and decimal ----
            ("binary",         [b"hello", b"world"],                                    pa.binary()),
            ("bytearray",      [bytearray(b"hello"), bytearray(b"world")],              pa.binary()),
            ("memoryview",     [memoryview(b"foo"), memoryview(b"bar")],      pa.binary()),
            ("binary_mixed",   [b"a", bytearray(b"b"), memoryview(b"c")],          pa.binary()),
            ("decimal128_3_2", [Decimal("1.23"), Decimal("4.56")],                      pa.decimal128(3, 2)),
            ("decimal128_9_3", [Decimal("123456.789"), Decimal("987654.321")],          pa.decimal128(9, 3)),
        ]
        # fmt: on
        self._run_inference_tests(cases)

        # Verify NaN/Inf behavior
        arr = pa.array([float("nan")])
        self.assertTrue(math.isnan(arr.to_pylist()[0]))
        self.assertEqual(pa.array([float("inf")]).to_pylist()[0], float("inf"))

        # Verify overflow behavior (values > int64 max)
        with self.assertRaises(OverflowError):
            pa.array([2**63])  # Just over int64 max

    # -------------------------------------------------------------------------
    # 2b. Python tuple
    # -------------------------------------------------------------------------
    def test_plain_python_tuple(self):
        """Test type inference from Python tuples."""
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
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

    # -------------------------------------------------------------------------
    # 2c. Python dict (struct)
    # -------------------------------------------------------------------------
    def test_plain_python_dict(self):
        """Test type inference from Python dicts (inferred as struct)."""
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
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
            ("struct_datetime", [{"ts": datetime.datetime(2024, 1, 1, 12)}],            pa.struct([("ts", pa.timestamp("us"))])),
            ("struct_binary",   [{"b": b"hello"}],                                      pa.struct([("b", pa.binary())])),
            ("struct_decimal",  [{"d": Decimal("1.23")}],                               pa.struct([("d", pa.decimal128(3, 2))])),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 3. PANDAS INSTANCES - Series with various dtypes
    # =========================================================================
    # 3a. Pandas Series (numpy-backed)
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_numpy_backed(self):
        """Test type inference from numpy-backed pandas Series."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = "America/Los_Angeles"
        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- Integer ----
            ("int64",         pd.Series([1, 2, 3]),                                    pa.int64()),
            ("int_with_none", pd.Series([1, 2, None]),                                 pa.float64()),

            # ---- Float ----
            ("float64",       pd.Series([1.0, 2.0, 3.0]),                              pa.float64()),
            ("pd_nan",        pd.Series([np.nan, 1.0, 2.0]),                           pa.float64()),
            ("pd_inf",        pd.Series([np.inf, 1.0, 2.0]),                           pa.float64()),
            ("pd_neg_inf",    pd.Series([-np.inf, 1.0, 2.0]),                          pa.float64()),

            # ---- String ----
            ("string",        pd.Series(["a", "b", "c"]),                              pa.string()),

            # ---- Boolean ----
            ("bool",          pd.Series([True, False, True]),                          pa.bool_()),

            # ---- Temporal ----
            ("date32",        pd.Series([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]),
                                                                                       pa.date32()),
            ("timestamp_ns",  pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])), pa.timestamp("ns")),
            ("pd_ts_epoch",   pd.Series([pd.Timestamp("1970-01-01")]),                 pa.timestamp("ns")),
            ("pd_ts_min",     pd.Series([pd.Timestamp.min]),                           pa.timestamp("ns")),
            ("pd_ts_max",     pd.Series([pd.Timestamp.max]),                           pa.timestamp("ns")),
            ("duration_ns",   pd.Series(pd.to_timedelta(["1 day", "2 hours"])),        pa.duration("ns")),
            ("pd_td_zero",    pd.Series([pd.Timedelta(0)]),                            pa.duration("ns")),
            ("pd_td_min",     pd.Series([pd.Timedelta.min]),                           pa.duration("ns")),
            ("pd_td_max",     pd.Series([pd.Timedelta.max]),                           pa.duration("ns")),

            # ---- Timezone-aware ----
            ("timestamp_tz_sg",
             pd.Series([datetime.datetime(2024, 1, 1, 12, tzinfo=sg),
                        datetime.datetime(2024, 1, 2, 12, tzinfo=sg)]),
             pa.timestamp("ns", tz="Asia/Singapore")),
            ("timestamp_tz_la",
             pd.Series([pd.Timestamp("2024-01-01 12:00:00", tz=la),
                        pd.Timestamp("2024-01-02 12:00:00", tz=la)]),
             pa.timestamp("ns", tz=la)),

            # ---- Binary ----
            ("binary",       pd.Series([b"hello", b"world"]),                          pa.binary()),

            # ---- Nested ----
            ("list_int64",     pd.Series([[1, 2], [3, 4, 5]]),                         pa.list_(pa.int64())),
            ("list_float64",   pd.Series([[1.0, 2.0], [3.0]]),                         pa.list_(pa.float64())),
            ("list_string",    pd.Series([["a", "b"], ["c"]]),                         pa.list_(pa.string())),
            ("list_with_none", pd.Series([[1, 2], None, [3]]),                         pa.list_(pa.int64())),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # -------------------------------------------------------------------------
    # 3b. Pandas nullable extension types
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_nullable_extension(self):
        """Test type inference from pandas nullable extension types."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- Integer ----
            ("Int8",              pd.Series([1, 2, 3], dtype=pd.Int8Dtype()),          pa.int8()),
            ("Int16",             pd.Series([1, 2, 3], dtype=pd.Int16Dtype()),         pa.int16()),
            ("Int32",             pd.Series([1, 2, 3], dtype=pd.Int32Dtype()),         pa.int32()),
            ("Int64",             pd.Series([1, 2, 3], dtype=pd.Int64Dtype()),         pa.int64()),
            ("Int64_with_none",   pd.Series([1, 2, None], dtype=pd.Int64Dtype()),      pa.int64()),

            # ---- Unsigned Integer ----
            ("UInt8",             pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()),         pa.uint8()),
            ("UInt16",            pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()),        pa.uint16()),
            ("UInt32",            pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()),        pa.uint32()),
            ("UInt64",            pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()),        pa.uint64()),

            # ---- Float ----
            ("Float32",           pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype()), pa.float32()),
            ("Float64",           pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype()), pa.float64()),
            ("Float64_with_none", pd.Series([1.0, 2.0, None], dtype=pd.Float64Dtype()),
                                                                                       pa.float64()),
            ("Float64_nan",       pd.Series([np.nan, 1.0], dtype=pd.Float64Dtype()),   pa.float64()),
            ("Float64_inf",       pd.Series([np.inf, 1.0], dtype=pd.Float64Dtype()),   pa.float64()),

            # ---- Boolean ----
            ("Boolean",           pd.Series([True, False, True], dtype=pd.BooleanDtype()),
                                                                                       pa.bool_()),
            ("Boolean_with_none", pd.Series([True, False, None], dtype=pd.BooleanDtype()),
                                                                                       pa.bool_()),

            # ---- String ----
            ("String",            pd.Series(["a", "b", "c"], dtype=pd.StringDtype()),  pa.string()),
            ("String_with_none",  pd.Series(["a", "b", None], dtype=pd.StringDtype()), pa.string()),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # -------------------------------------------------------------------------
    # 3c. Pandas ArrowDtype
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_arrow_dtype(self):
        """Test type inference from PyArrow-backed pandas Series (ArrowDtype)."""
        import pandas as pd
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- Integers ----
            ("arrow_int8",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8())),       pa.int8()),
            ("arrow_int64", pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),      pa.int64()),

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
             pd.Series([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
                       dtype=pd.ArrowDtype(pa.date32())),                               pa.date32()),

            # ---- Timestamp with timezone ----
            ("arrow_ts_utc",
             pd.Series([datetime.datetime(2024, 1, 1, 12), datetime.datetime(2024, 1, 2, 12)],
                       dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC"))),              pa.timestamp("us", tz="UTC")),
            ("arrow_ts_sg",
             pd.Series([datetime.datetime(2024, 1, 1, 12), datetime.datetime(2024, 1, 2, 12)],
                       dtype=pd.ArrowDtype(pa.timestamp("us", tz="Asia/Singapore"))),   pa.timestamp("us", tz="Asia/Singapore")),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 4. NUMPY ARRAY - all numeric dtypes, datetime64, timedelta64
    # =========================================================================
    def test_numpy_array(self):
        """Test type inference from numpy arrays."""
        import numpy as np
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- Signed integers ----
            ("int8",       np.array([1, 2, 3], dtype=np.int8),                          pa.int8()),
            ("int8_max",   np.array([127], dtype=np.int8),                              pa.int8()),
            ("int8_min",   np.array([-128], dtype=np.int8),                             pa.int8()),
            ("int8_range", np.array([-128, 0, 127], dtype=np.int8),                     pa.int8()),
            ("int16",      np.array([1, 2, 3], dtype=np.int16),                         pa.int16()),
            ("int16_max",  np.array([32767], dtype=np.int16),                           pa.int16()),
            ("int16_min",  np.array([-32768], dtype=np.int16),                          pa.int16()),
            ("int32",      np.array([1, 2, 3], dtype=np.int32),                         pa.int32()),
            ("int32_max",  np.array([2147483647], dtype=np.int32),                      pa.int32()),
            ("int32_min",  np.array([-2147483648], dtype=np.int32),                     pa.int32()),
            ("int64",      np.array([1, 2, 3], dtype=np.int64),                         pa.int64()),

            # ---- Unsigned integers ----
            ("uint8",      np.array([1, 2, 3], dtype=np.uint8),                         pa.uint8()),
            ("uint8_max",  np.array([255], dtype=np.uint8),                             pa.uint8()),
            ("uint16",     np.array([1, 2, 3], dtype=np.uint16),                        pa.uint16()),
            ("uint16_max", np.array([65535], dtype=np.uint16),                          pa.uint16()),
            ("uint32",     np.array([1, 2, 3], dtype=np.uint32),                        pa.uint32()),
            ("uint32_max", np.array([4294967295], dtype=np.uint32),                     pa.uint32()),
            ("uint64",     np.array([1, 2, 3], dtype=np.uint64),                        pa.uint64()),
            ("uint64_max", np.array([2**64 - 1], dtype=np.uint64),                      pa.uint64()),

            # ---- Floats ----
            ("float16",        np.array([1.0, 2.0, 3.0], dtype=np.float16),             pa.float16()),
            ("float32",        np.array([1.0, 2.0, 3.0], dtype=np.float32),             pa.float32()),
            ("float64",        np.array([1.0, 2.0, 3.0], dtype=np.float64),             pa.float64()),
            ("float64_w_none", np.array([1.0, 2.0, None], dtype=np.float64),            pa.float64()),
            ("np_nan",         np.array([np.nan, 1.0, 2.0]),                            pa.float64()),
            ("np_nan_w_none",  np.array([np.nan, 1.0, None], dtype=np.float64),         pa.float64()),
            ("np_inf",         np.array([np.inf, 1.0, 2.0]),                            pa.float64()),
            ("np_inf_w_none",  np.array([np.inf, 1.0, None], dtype=np.float64),         pa.float64()),
            ("np_neg_inf",     np.array([-np.inf, 1.0, 2.0]),                           pa.float64()),
            ("np_special",     np.array([np.nan, np.inf, -np.inf]),                     pa.float64()),

            # ---- Boolean ----
            ("bool",   np.array([True, False, True], dtype=np.bool_),                   pa.bool_()),

            # ---- String ----
            ("string", np.array(["a", "b", "c"]),                                       pa.string()),

            # ---- Datetime64 ----
            ("datetime64_D",  np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[D]"),
                                                                                        pa.date32()),
            ("np_epoch",      np.array(["1970-01-01"], dtype="datetime64[D]"),          pa.date32()),
            ("np_date_min",   np.array(["0001-01-01"], dtype="datetime64[D]"),          pa.date32()),
            ("np_date_max",   np.array(["9999-12-31"], dtype="datetime64[D]"),          pa.date32()),
            ("datetime64_s",  np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[s]"),
                                                                                        pa.timestamp("s")),
            ("datetime64_ms", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[ms]"),
                                                                                        pa.timestamp("ms")),
            ("datetime64_us", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[us]"),
                                                                                        pa.timestamp("us")),
            ("datetime64_ns", np.array(["2024-01-01T12", "2024-01-02T12"], dtype="datetime64[ns]"),
                                                                                        pa.timestamp("ns")),

            # ---- Timedelta64 ----
            ("timedelta64_s",  np.array([1, 2, 3], dtype="timedelta64[s]"),             pa.duration("s")),
            ("timedelta64_ms", np.array([1, 2, 3], dtype="timedelta64[ms]"),            pa.duration("ms")),
            ("timedelta64_us", np.array([1, 2, 3], dtype="timedelta64[us]"),            pa.duration("us")),
            ("timedelta64_ns", np.array([1, 2, 3], dtype="timedelta64[ns]"),            pa.duration("ns")),

            # ---- NaT with explicit unit ----
            ("datetime64_nat_ns", [np.datetime64("NaT", "ns")],                         pa.timestamp("ns")),
            ("datetime64_nat_us", [np.datetime64("NaT", "us")],                         pa.timestamp("us")),
            ("timedelta64_nat",   [np.timedelta64("NaT", "ns")],                        pa.duration("ns")),
            ("datetime64_mixed",
             [np.datetime64("2024-01-01T12:00", "us"), np.datetime64("NaT", "us")],     pa.timestamp("us")),
            # fmt: on
        ]
        self._run_inference_tests(cases)

        # Verify NaT values produce null_count=1
        self.assertEqual(pa.array([np.datetime64("NaT", "ns")]).null_count, 1)
        self.assertEqual(pa.array([np.timedelta64("NaT", "ns")]).null_count, 1)

    # =========================================================================
    # 5. NESTED TYPES - list of list, struct, map
    # =========================================================================
    def test_nested_list_types(self):
        """Test type inference for nested list types."""
        import pyarrow as pa

        # (name, data, expected_type)
        # fmt: off
        cases = [
            # ---- List types ----
            ("list_int64",           [[1, 2], [3, 4, 5]],                               pa.list_(pa.int64())),
            ("list_float64",         [[1.0, 2.0], [3.0]],                               pa.list_(pa.float64())),
            ("list_string",          [["a", "b"], ["c"]],                               pa.list_(pa.string())),
            ("list_bool",            [[True, False], [True]],                           pa.list_(pa.bool_())),
            ("list_with_none",       [[1, 2], None, [3]],                               pa.list_(pa.int64())),
            ("nested_list_2d",       [[[1, 2], [3]], [[4, 5]]],                         pa.list_(pa.list_(pa.int64()))),
            ("nested_list_3d",       [[[[1]]]],                                         pa.list_(pa.list_(pa.list_(pa.int64())))),
            ("list_mixed_int_float", [[1, 2.0], [3, 4.0]],                              pa.list_(pa.float64())),

            # ---- Struct types ----
            ("struct_of_list",       [{"items": [1, 2, 3]}],                            pa.struct([("items", pa.list_(pa.int64()))])),
            ("struct_of_struct",     [{"outer": {"inner": 1}}],                         pa.struct([("outer", pa.struct([("inner", pa.int64())]))])),
            ("complex_struct",       [{"items": [1, 2], "nested": {"val": "a"}}],       pa.struct([("items", pa.list_(pa.int64())), ("nested", pa.struct([("val", pa.string())]))])),

            # ---- List of struct ----
            ("list_of_struct",       [[{"x": 1}, {"x": 2}]],                            pa.list_(pa.struct([("x", pa.int64())]))),
            ("list_list_struct",     [[[{"x": 1}]]],                                    pa.list_(pa.list_(pa.struct([("x", pa.int64())])))),
            # fmt: on
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 6. EXPLICIT TYPE SPECIFICATION - large_list, fixed_size_list, map_, etc.
    # =========================================================================
    def test_explicit_type_specification(self):
        """Test types that require explicit specification.

        - Large types (large_list, large_string, large_binary) are NEVER auto-inferred.
        - Map types cannot be inferred without explicit type.
        """
        import pyarrow as pa

        # (name, data, explicit_type & expected_type)
        # fmt: off
        cases = [
            # ---- Large types ----
            ("large_list",          [[1, 2], [3, 4]],                                   pa.large_list(pa.int64())),
            ("large_string",        ["hello", "world"],                                 pa.large_string()),
            ("large_binary",        [b"hello", b"world"],                               pa.large_binary()),

            # ---- Fixed size types ----
            ("fixed_size_list",     [[1, 2, 3], [4, 5, 6]],                             pa.list_(pa.int64(), 3)),

            # ---- Map types (cannot be inferred, require explicit type) ----
            ("map_str_int",         [[("a", 1), ("b", 2)]],                             pa.map_(pa.string(), pa.int64())),
            ("map_int_str",         [[(1, "a"), (2, "b")]],                             pa.map_(pa.int64(), pa.string())),
            ("map_of_list",         [[("k", [1, 2, 3])]],                               pa.map_(pa.string(), pa.list_(pa.int64()))),
            ("map_of_struct",       [[("k", {"x": 1})]],                                pa.map_(pa.string(), pa.struct([("x", pa.int64())]))),
            ("list_of_map",         [[[("a", 1)], [("b", 2)]]],                         pa.list_(pa.map_(pa.string(), pa.int64()))),
            # fmt: on
        ]
        self._run_inference_tests(cases, use_expected_as_explicit=True)

        # Map types cannot be inferred without explicit type - raises error
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("a", 1), ("b", 2)]])  # map_str_int
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([[(1, "a"), (2, "b")]])  # map_int_str
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("k", [1, 2, 3])]])  # map_of_list
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("k", {"x": 1})]])  # map_of_struct
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[[("a", 1)], [("b", 2)]]])  # list_of_map

if __name__ == "__main__":
    from pyspark.testing import main

    main()
