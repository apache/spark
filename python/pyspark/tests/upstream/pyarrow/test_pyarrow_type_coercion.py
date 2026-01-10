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
Test pa.array type coercion behavior when creating arrays with explicit type parameter.

This test monitors the behavior of PyArrow's type coercion to ensure PySpark's assumptions
about PyArrow behavior remain valid across versions.

Test categories (each tests multiple types: numeric, temporal, string, etc.):
1. Nullable data - with None values, NaN handling for various types
2. Plain Python instances - list, tuple, generator with all data types
3. Pandas instances - numpy-backed Series, nullable extension types, ArrowDtype
4. NumPy array - all numeric dtypes, datetime64, timedelta64
5. Nested types - list element coercion, struct field coercion, map
"""

import datetime
from decimal import Decimal
import math
import unittest
from typing import Any, List, Tuple

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowTypeCoercionTests(unittest.TestCase):
    """Test PyArrow's type coercion behavior for pa.array with explicit type parameter."""

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _run_coercion_tests(self, cases: List[Tuple[str, Any, Any]]) -> None:
        """Run coercion tests: (name, data, target_type)."""
        import pyarrow as pa

        for name, data, target_type in cases:
            with self.subTest(name=name):
                arr = pa.array(data, type=target_type)
                self.assertEqual(arr.type, target_type)

    def _run_coercion_tests_with_values(self, cases: List[Tuple[str, Any, Any, Any]]) -> None:
        """Run coercion tests with value verification: (name, data, target_type, expected)."""
        import pyarrow as pa

        for name, data, target_type, expected in cases:
            with self.subTest(name=name):
                arr = pa.array(data, type=target_type)
                self.assertEqual(arr.type, target_type)
                self.assertEqual(arr.to_pylist(), expected)

    def _run_error_tests(self, cases: List[Tuple[str, Any, Any]], error_type) -> None:
        """Run tests expecting errors: (name, data, target_type)."""
        import pyarrow as pa

        for name, data, target_type in cases:
            with self.subTest(name=name):
                with self.assertRaises(error_type):
                    pa.array(data, type=target_type)

    # =========================================================================
    # SECTION 1: Nullable Data Coercion
    # =========================================================================

    def test_nullable_data_coercion(self):
        """Test type coercion with None, NaN for numeric and temporal types."""
        import pyarrow as pa

        nan, inf, neg_inf = float("nan"), float("inf"), float("-inf")

        # ---- None values ----
        # (name, data, target_type, expected_values)
        # fmt: off
        none_cases = [
            # Numeric
            ("int_none_to_float64", [1, None, 3],       pa.float64(),         [1.0, None, 3.0]),
            ("int_none_to_decimal", [1, None, 3],       pa.decimal128(10, 0), [Decimal("1"), None, Decimal("3")]),
            ("int_none_to_int32",   [1, None, 3],       pa.int32(),           [1, None, 3]),
            ("float_none_to_int64", [1.0, None, 3.0],   pa.int64(),           [1, None, 3]),
            ("all_none_to_float64", [None, None, None], pa.float64(),         [None, None, None]),
            ("all_none_to_int64",   [None, None, None], pa.int64(),           [None, None, None]),
            # String
            ("all_none_to_string",  [None, None],       pa.string(),          [None, None]),
            # Temporal
            ("date_none", [datetime.date(2024, 1, 1), None], pa.date32(),     [datetime.date(2024, 1, 1), None]),
            ("time_none", [datetime.time(12, 0), None],      pa.time64("us"), [datetime.time(12, 0), None]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(none_cases)

        # Verify null counts
        self.assertEqual(pa.array([1, None, 3], type=pa.float64()).null_count, 1)
        self.assertEqual(pa.array([None, None, None], type=pa.float64()).null_count, 3)

        # ---- NaN/Inf in float ----
        # (name, data, target_type)
        # fmt: off
        nan_cases = [
            ("nan_to_float64",     [nan],               pa.float64()),
            ("inf_to_float64",     [inf],               pa.float64()),
            ("neg_inf_to_float64", [neg_inf],           pa.float64()),
            ("all_special",        [nan, inf, neg_inf], pa.float64()),
            ("mixed_nan",          [1.0, nan, 3.0],     pa.float64()),
            ("nan_to_float32",     [nan],               pa.float32()),
            ("inf_to_float32",     [inf],               pa.float32()),
        ]
        # fmt: on
        self._run_coercion_tests(nan_cases)

        # Verify NaN/Inf values
        self.assertTrue(math.isnan(pa.array([nan], type=pa.float64()).to_pylist()[0]))
        self.assertEqual(pa.array([inf], type=pa.float64()).to_pylist()[0], inf)

        # ---- NaN/Inf to non-float fails ----
        # (name, data, target_type)
        # fmt: off
        nan_error_cases = [
            ("nan_to_decimal", [nan], pa.decimal128(10, 2)),
            ("nan_to_int64",   [nan], pa.int64()),
            ("inf_to_int64",   [inf], pa.int64()),
        ]
        # fmt: on
        self._run_error_tests(nan_error_cases, (pa.ArrowInvalid, pa.ArrowTypeError))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_na_coercion(self):
        """Test pd.NA handling during type coercion."""
        import pandas as pd
        import pyarrow as pa

        # (name, data, target_type, expected_values)
        # fmt: off
        cases = [
            ("Int64_na_to_float",  pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()),     pa.float64(), [1.0, None, 3.0]),
            ("Float64_na_to_int",  pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float64Dtype()), pa.int64(), [1, None, 3]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(cases)

    # =========================================================================
    # SECTION 2: Plain Python Instances Coercion
    # =========================================================================

    def test_python_instances_coercion(self):
        """Test type coercion from Python list, tuple, generator with all data types."""
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        # ==== 2.1 Numeric Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        numeric_cases = [
            # Int <-> Float
            ("int_to_float64",  [1, 2, 3],       pa.float64(), [1.0, 2.0, 3.0]),
            ("int_to_float32",  [1, 2, 3],       pa.float32(), [1.0, 2.0, 3.0]),
            ("float_to_int64",  [1.0, 2.0, 3.0], pa.int64(),   [1, 2, 3]),
            ("tuple_to_float",  (1, 2, 3),       pa.float64(), [1.0, 2.0, 3.0]),
            # Int narrowing/widening
            ("int_to_int8",     [1, 2, 3], pa.int8(),   [1, 2, 3]),
            ("int_to_int16",    [1, 2, 3], pa.int16(),  [1, 2, 3]),
            ("int_to_int32",    [1, 2, 3], pa.int32(),  [1, 2, 3]),
            ("int_to_uint8",    [1, 2, 3], pa.uint8(),  [1, 2, 3]),
            ("int_to_uint16",   [1, 2, 3], pa.uint16(), [1, 2, 3]),
            ("int_to_uint32",   [1, 2, 3], pa.uint32(), [1, 2, 3]),
            ("int_to_uint64",   [1, 2, 3], pa.uint64(), [1, 2, 3]),
            # Decimal
            ("int_to_decimal",  [1, 2, 3], pa.decimal128(10, 2), [Decimal("1.00"), Decimal("2.00"), Decimal("3.00")]),
            # Empty
            ("empty_to_int64",  [], pa.int64(),   []),
            ("empty_to_float",  [], pa.float64(), []),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(numeric_cases)

        # Generator
        self.assertEqual(
            pa.array((x for x in [1, 2, 3]), type=pa.float64()).to_pylist(),
            [1.0, 2.0, 3.0],
        )

        # ==== 2.2 Int Boundary Values ====

        int8_min, int8_max = -128, 127
        int16_min, int16_max = -32768, 32767
        int32_min, int32_max = -(2**31), 2**31 - 1
        int64_min, int64_max = -(2**63), 2**63 - 1

        # (name, data, target_type, expected_values)
        # fmt: off
        boundary_cases = [
            ("int8_range",  [int8_min, 0, int8_max],   pa.int8(),  [int8_min, 0, int8_max]),
            ("int16_range", [int16_min, int16_max],    pa.int16(), [int16_min, int16_max]),
            ("int32_range", [int32_min, int32_max],    pa.int32(), [int32_min, int32_max]),
            ("int64_range", [int64_min, 0, int64_max], pa.int64(), [int64_min, 0, int64_max]),
            # Widening
            ("int8_to_int64",  [int8_min, int8_max],   pa.int64(), [int8_min, int8_max]),
            ("int16_to_int32", [int16_min, int16_max], pa.int32(), [int16_min, int16_max]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(boundary_cases)

        # ==== 2.3 Int Overflow (Error) ====

        # (name, data, target_type)
        # fmt: off
        overflow_cases = [
            ("int8_overflow",   [128],          pa.int8()),
            ("int8_underflow",  [-129],         pa.int8()),
            ("int16_overflow",  [32768],        pa.int16()),
            ("int32_overflow",  [2**31],        pa.int32()),
            ("uint8_overflow",  [256],          pa.uint8()),
            ("uint16_overflow", [65536],        pa.uint16()),
            ("uint32_overflow", [2**32],        pa.uint32()),
        ]
        # fmt: on
        self._run_error_tests(overflow_cases, pa.ArrowInvalid)

        # Negative to unsigned
        with self.assertRaises(OverflowError):
            pa.array([-1], type=pa.uint8())

        # ==== 2.4 String Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        string_cases = [
            # Basic
            ("ascii",            ["hello", "world"],     pa.string(),       ["hello", "world"]),
            ("empty_str",        ["", "a", ""],          pa.string(),       ["", "a", ""]),
            ("whitespace",       ["  ", "\t", "\n"],     pa.string(),       ["  ", "\t", "\n"]),
            ("string_with_none", ["a", None, "b"],       pa.string(),       ["a", None, "b"]),
            # Non-English
            ("chinese",          ["你好", "世界"],        pa.string(),       ["你好", "世界"]),
            ("japanese",         ["こんにちは"],          pa.string(),       ["こんにちは"]),
            ("korean",           ["안녕하세요"],            pa.string(),       ["안녕하세요"]),
            ("emoji",            ["😀", "🎉", "🚀"],    pa.string(),       ["😀", "🎉", "🚀"]),
            ("arabic",           ["مرحبا"],             pa.string(),       ["مرحبا"]),
            ("cyrillic",         ["Привет"],            pa.string(),       ["Привет"]),
            # Large string
            ("to_large_string",  ["hello", "world"],     pa.large_string(), ["hello", "world"]),
            ("large_str_none",   ["a", None, "b"],       pa.large_string(), ["a", None, "b"]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(string_cases)

        # ==== 2.5 Binary Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        binary_cases = [
            ("bytes_to_binary",     [b"hello", b"world"], pa.binary(),       [b"hello", b"world"]),
            ("bytes_to_large_bin",  [b"hello", b"world"], pa.large_binary(), [b"hello", b"world"]),
            ("binary_with_none",    [b"a", None, b"b"],   pa.binary(),       [b"a", None, b"b"]),
            ("bytearray_to_bin",    [bytearray(b"hi")],   pa.binary(),       [b"hi"]),
            # Fixed size binary
            ("fixed_bin_4",         [b"abcd", b"efgh"],   pa.binary(4),      [b"abcd", b"efgh"]),
            ("fixed_bin_1",         [b"a", b"b", b"c"],   pa.binary(1),      [b"a", b"b", b"c"]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(binary_cases)

        # ==== 2.6 Temporal Types ====

        ts_data = [datetime.datetime(2024, 1, 1, 12, 0), datetime.datetime(2024, 1, 2, 12, 0)]
        date_data = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]
        time_data = [datetime.time(12, 30), datetime.time(13, 45)]
        dur_data = [datetime.timedelta(days=1), datetime.timedelta(hours=2)]

        # (name, data, target_type, expected_values)
        # fmt: off
        temporal_cases = [
            # Timestamp resolutions (microseconds truncated for lower resolutions)
            ("timestamp_s",  ts_data, pa.timestamp("s"),  ts_data),
            ("timestamp_ms", ts_data, pa.timestamp("ms"), ts_data),
            ("timestamp_us", ts_data, pa.timestamp("us"), ts_data),
            ("timestamp_ns", ts_data, pa.timestamp("ns"), ts_data),
            # Date
            ("date32", date_data, pa.date32(), date_data),
            ("date64", date_data, pa.date64(), date_data),
            # Time (seconds truncated for lower resolutions)
            ("time32_s",  time_data, pa.time32("s"),  [datetime.time(12, 30), datetime.time(13, 45)]),
            ("time32_ms", time_data, pa.time32("ms"), time_data),
            ("time64_us", time_data, pa.time64("us"), time_data),
            ("time64_ns", time_data, pa.time64("ns"), time_data),
            # Duration
            ("duration_s",  dur_data, pa.duration("s"),  dur_data),
            ("duration_ms", dur_data, pa.duration("ms"), dur_data),
            ("duration_us", dur_data, pa.duration("us"), dur_data),
            ("duration_ns", dur_data, pa.duration("ns"), dur_data),
            # Boundary values
            ("epoch_date",   [datetime.date(1970, 1, 1)],         pa.date32(),      [datetime.date(1970, 1, 1)]),
            ("date_min",     [datetime.date(1, 1, 1)],            pa.date32(),      [datetime.date(1, 1, 1)]),
            ("date_max",     [datetime.date(9999, 12, 31)],       pa.date32(),      [datetime.date(9999, 12, 31)]),
            ("time_min",     [datetime.time(0, 0, 0, 0)],         pa.time64("us"),  [datetime.time(0, 0, 0, 0)]),
            ("time_max",     [datetime.time(23, 59, 59, 999999)], pa.time64("us"),  [datetime.time(23, 59, 59, 999999)]),
            ("duration_neg", [datetime.timedelta(days=-1)],       pa.duration("us"), [datetime.timedelta(days=-1)]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(temporal_cases)

        # ==== 2.7 Timezone Coercion ====

        utc_tz = ZoneInfo("UTC")
        sg_tz = ZoneInfo("Asia/Singapore")
        la_tz = ZoneInfo("America/Los_Angeles")
        tokyo_tz = ZoneInfo("Asia/Tokyo")
        ny_tz = ZoneInfo("America/New_York")

        # (name, data, target_type, expected_values)
        # Note: PyArrow returns tz-aware datetime with the target timezone
        # fmt: off
        tz_cases = [
            # Same timezone
            ("utc_to_utc", [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)], pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            ("sg_to_sg",   [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)],  pa.timestamp("us", tz="Asia/Singapore"), [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)]),
            # Cross timezone conversion (SG +8 → UTC, so 20:00 SG = 12:00 UTC)
            ("sg_to_utc",  [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)],  pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            # LA -8 → UTC, so 4:00 LA = 12:00 UTC
            ("la_to_utc",  [datetime.datetime(2024, 1, 1, 4, 0, tzinfo=la_tz)],   pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            # Naive to tz-aware (treated as UTC)
            ("naive_to_utc", [datetime.datetime(2024, 1, 1, 12, 0)], pa.timestamp("us", tz="UTC"), [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            # Tz-aware to tz-naive (returns naive datetime)
            ("aware_to_naive", [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)], pa.timestamp("us"), [datetime.datetime(2024, 1, 1, 12, 0)]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(tz_cases)

        # Mixed timezones → same instant
        ts_mixed = [
            datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=utc_tz),
            datetime.datetime(2024, 1, 1, 20, 0, 0, tzinfo=sg_tz),
            datetime.datetime(2024, 1, 1, 4, 0, 0, tzinfo=la_tz),
        ]
        values = pa.array(ts_mixed, type=pa.timestamp("us", tz="UTC")).to_pylist()
        self.assertEqual(values[0], values[1])
        self.assertEqual(values[1], values[2])

        # Positive/negative UTC offsets → same instant
        ts_tokyo = pa.array(
            [datetime.datetime(2024, 1, 1, 21, 0, tzinfo=tokyo_tz)],
            type=pa.timestamp("us", tz="UTC"),
        )
        ts_ny = pa.array(
            [datetime.datetime(2024, 1, 1, 7, 0, tzinfo=ny_tz)],
            type=pa.timestamp("us", tz="UTC"),
        )
        self.assertEqual(ts_tokyo.to_pylist()[0], ts_ny.to_pylist()[0])

        # ==== 2.8 Cross-Type Coercion (Success) ====

        # (name, data, target_type, expected_values)
        # fmt: off
        cross_type_ok_with_values = [
            # int → date (epoch days)
            ("int_to_date32",    [19723],              pa.date32(), [datetime.date(2024, 1, 1)]),
            # binary ↔ string (UTF-8)
            ("binary_to_string", [b"hello", b"world"], pa.string(), ["hello", "world"]),
            ("string_to_binary", ["hello", "world"],   pa.binary(), [b"hello", b"world"]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(cross_type_ok_with_values)

        # (name, data, target_type, expected_values)
        # fmt: off
        cross_type_ok = [
            # int → timestamp (epoch seconds: 1704067200 = 2024-01-01 00:00:00 UTC)
            ("int_to_timestamp_s", [1704067200], pa.timestamp("s"), [datetime.datetime(2024, 1, 1, 0, 0, 0)]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(cross_type_ok)

        # ==== 2.9 Cross-Type Coercion (Fails - Requires Explicit Cast) ====

        # (name, data, target_type)
        # fmt: off
        cross_type_fail = [
            # numeric → string
            ("int_to_string",      [1, 2, 3],       pa.string()),
            ("float_to_string",    [1.5, 2.5],      pa.string()),
            ("bool_to_string",     [True, False],   pa.string()),
            # string → numeric
            ("string_to_int",      ["1", "2"],      pa.int64()),
            ("string_to_float",    ["1.5", "2.5"],  pa.float64()),
            ("string_to_bool",     ["true"],        pa.bool_()),
            # bool ↔ int
            ("bool_to_int",        [True, False],   pa.int64()),
            ("int_to_bool",        [1, 0, 1],       pa.bool_()),
            # temporal → numeric
            ("date_to_int",        [datetime.date(2024, 1, 1)],     pa.int64()),
            ("datetime_to_int",    [datetime.datetime(2024, 1, 1)], pa.int64()),
            ("time_to_int",        [datetime.time(12, 0)],          pa.int64()),
            ("duration_to_int",    [datetime.timedelta(days=1)],    pa.int64()),
            # date → timestamp
            ("date_to_timestamp",  [datetime.date(2024, 1, 1)], pa.timestamp("us")),
            # binary → numeric
            ("binary_to_int",      [b"hello"], pa.int64()),
            # nested → scalar
            ("list_to_scalar",     [[1, 2, 3]], pa.int64()),
            ("dict_to_scalar",     [{"a": 1}],  pa.int64()),
        ]
        # fmt: on
        self._run_error_tests(
            cross_type_fail, (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)
        )

        # ==== 2.10 Precision Loss (Silent) ====

        # (name, data, target_type, expected_values)
        # fmt: off
        precision_loss = [
            # float → int (truncation, not rounding)
            ("float_truncate",     [1.9, 2.1, 3.7], pa.int64(), [1, 2, 3]),
            ("float_neg_truncate", [-1.9, -2.1],    pa.int64(), [-1, -2]),
            # decimal → int (truncation)
            ("decimal_truncate",   [Decimal("1.9"), Decimal("2.1")], pa.int64(), [1, 2]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(precision_loss)

        # Timestamp resolution loss
        ts = datetime.datetime(2024, 1, 1, 12, 30, 45, 123456)
        self.assertEqual(pa.array([ts], type=pa.timestamp("us")).to_pylist()[0].microsecond, 123456)
        self.assertEqual(pa.array([ts], type=pa.timestamp("ms")).to_pylist()[0].microsecond, 123000)
        self.assertEqual(pa.array([ts], type=pa.timestamp("s")).to_pylist()[0].microsecond, 0)

        # Time resolution loss
        t = datetime.time(12, 30, 45, 123456)
        self.assertEqual(pa.array([t], type=pa.time32("ms")).to_pylist()[0].microsecond, 123000)
        self.assertEqual(pa.array([t], type=pa.time32("s")).to_pylist()[0].microsecond, 0)

        # float64 → float32 precision loss
        large_float = 1.23456789012345678
        result = pa.array([large_float], type=pa.float32()).to_pylist()[0]
        self.assertNotEqual(result, large_float)

    # =========================================================================
    # SECTION 3: Pandas Instances Coercion
    # =========================================================================

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_instances_coercion(self):
        """Test type coercion from pandas Series with various backend types."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        # Constants
        int8_min, int8_max = np.iinfo(np.int8).min, np.iinfo(np.int8).max
        int16_min, int16_max = np.iinfo(np.int16).min, np.iinfo(np.int16).max
        int32_min, int32_max = np.iinfo(np.int32).min, np.iinfo(np.int32).max
        int64_min, int64_max = np.iinfo(np.int64).min, np.iinfo(np.int64).max
        nan, inf, neg_inf = float("nan"), float("inf"), float("-inf")
        utc_tz, sg_tz = ZoneInfo("UTC"), ZoneInfo("Asia/Singapore")

        # ==== 3.1 Numpy-backed Series ====
        # (name, data, target_type, expected_values)
        # fmt: off
        numpy_cases = [
            # Int types → float
            ("np_int8_to_float",   pd.Series([1, 2, 3], dtype="int8"),   pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int16_to_float",  pd.Series([1, 2, 3], dtype="int16"),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int32_to_float",  pd.Series([1, 2, 3], dtype="int32"),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int64_to_float",  pd.Series([1, 2, 3], dtype="int64"),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint8_to_float",  pd.Series([1, 2, 3], dtype="uint8"),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint16_to_float", pd.Series([1, 2, 3], dtype="uint16"), pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint32_to_float", pd.Series([1, 2, 3], dtype="uint32"), pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint64_to_float", pd.Series([1, 2, 3], dtype="uint64"), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types → int
            ("np_float32_to_int",  pd.Series([1.0, 2.0, 3.0], dtype="float32"), pa.int64(), [1, 2, 3]),
            ("np_float64_to_int",  pd.Series([1.0, 2.0, 3.0], dtype="float64"), pa.int64(), [1, 2, 3]),
            # Float ↔ float
            ("np_float32_to_f64",  pd.Series([1.0, 2.0], dtype="float32"), pa.float64(), [1.0, 2.0]),
            ("np_float64_to_f32",  pd.Series([1.0, 2.0], dtype="float64"), pa.float32(), [1.0, 2.0]),
            # Narrowing
            ("np_int64_to_int8",   pd.Series([1, 2, 3], dtype="int64"), pa.int8(),  [1, 2, 3]),
            ("np_int64_to_int16",  pd.Series([1, 2, 3], dtype="int64"), pa.int16(), [1, 2, 3]),
            ("np_int64_to_int32",  pd.Series([1, 2, 3], dtype="int64"), pa.int32(), [1, 2, 3]),
            # Bool, string
            ("np_bool",            pd.Series([True, False], dtype="bool"),   pa.bool_(),       [True, False]),
            ("np_object_str",      pd.Series(["a", "b"], dtype="object"),    pa.string(),      ["a", "b"]),
            ("np_object_large",    pd.Series(["a", "b"], dtype="object"),    pa.large_string(), ["a", "b"]),
            # Empty
            ("np_empty_int",       pd.Series([], dtype="int64"),   pa.float64(), []),
            ("np_empty_float",     pd.Series([], dtype="float64"), pa.int64(),   []),
            # Boundary values
            ("np_int8_minmax",     pd.Series([int8_min, int8_max], dtype="int8"),     pa.float64(), [float(int8_min), float(int8_max)]),
            ("np_int16_minmax",    pd.Series([int16_min, int16_max], dtype="int16"),  pa.float64(), [float(int16_min), float(int16_max)]),
            ("np_int32_minmax",    pd.Series([int32_min, int32_max], dtype="int32"),  pa.float64(), [float(int32_min), float(int32_max)]),
            ("np_int64_minmax",    pd.Series([int64_min, int64_max], dtype="int64"),  pa.int64(),   [int64_min, int64_max]),
            ("np_int8_widen",      pd.Series([int8_min, 0, int8_max], dtype="int8"),  pa.int64(),   [int8_min, 0, int8_max]),
            # NaN to int → None (pandas-specific behavior)
            ("np_nan_to_int",      pd.Series([nan, 1.0], dtype="float64"), pa.int64(), [None, 1]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(numpy_cases)

        # Special float values (NaN/Inf) - type only
        for name, data, target in [
            ("np_nan",     pd.Series([nan, 1.0], dtype="float64"), pa.float64()),
            ("np_inf",     pd.Series([inf, neg_inf], dtype="float64"), pa.float64()),
            ("np_nan_f32", pd.Series([nan], dtype="float64"), pa.float32()),
        ]:
            with self.subTest(name=name):
                self.assertEqual(pa.array(data, type=target).type, target)

        # numpy int → decimal128 does NOT work
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(pd.Series([1, 2, 3], dtype="int64"), type=pa.decimal128(10, 0))

        # ==== 3.2 Nullable Extension Types ====
        # (name, data, target_type, expected_values)
        # fmt: off
        nullable_cases = [
            # Int types → float
            ("Int8_to_float",      pd.Series([1, 2, 3], dtype=pd.Int8Dtype()),   pa.float64(), [1.0, 2.0, 3.0]),
            ("Int16_to_float",     pd.Series([1, 2, 3], dtype=pd.Int16Dtype()),  pa.float64(), [1.0, 2.0, 3.0]),
            ("Int32_to_float",     pd.Series([1, 2, 3], dtype=pd.Int32Dtype()),  pa.float64(), [1.0, 2.0, 3.0]),
            ("Int64_to_float",     pd.Series([1, 2, 3], dtype=pd.Int64Dtype()),  pa.float64(), [1.0, 2.0, 3.0]),
            ("UInt8_to_float",     pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()),  pa.float64(), [1.0, 2.0, 3.0]),
            ("UInt16_to_float",    pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            ("UInt32_to_float",    pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            ("UInt64_to_float",    pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types
            ("Float32_to_int",     pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype()), pa.int64(),   [1, 2, 3]),
            ("Float64_to_int",     pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype()), pa.int64(),   [1, 2, 3]),
            ("Float64_to_f32",     pd.Series([1.0, 2.0], dtype=pd.Float64Dtype()),      pa.float32(), [1.0, 2.0]),
            # String, bool
            ("StringDtype",        pd.Series(["a", "b"], dtype=pd.StringDtype()),       pa.string(),       ["a", "b"]),
            ("StringDtype_large",  pd.Series(["a", "b"], dtype=pd.StringDtype()),       pa.large_string(), ["a", "b"]),
            ("BooleanDtype",       pd.Series([True, False], dtype=pd.BooleanDtype()),   pa.bool_(),        [True, False]),
            # With NA
            ("Int64_na",           pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()),     pa.float64(), [1.0, None, 3.0]),
            ("Int64_na_to_int32",  pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()),     pa.int32(),   [1, None, 3]),
            ("Float64_na",         pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float64Dtype()), pa.int64(), [1, None, 3]),
            ("StringDtype_na",     pd.Series(["a", pd.NA, "c"], dtype=pd.StringDtype()), pa.string(), ["a", None, "c"]),
            ("BooleanDtype_na",    pd.Series([True, pd.NA, False], dtype=pd.BooleanDtype()), pa.bool_(), [True, None, False]),
            # Boundary with NA
            ("Int8_minmax_na",     pd.Series([int8_min, pd.NA, int8_max], dtype=pd.Int8Dtype()),   pa.int8(),  [int8_min, None, int8_max]),
            ("Int16_minmax_na",    pd.Series([int16_min, pd.NA, int16_max], dtype=pd.Int16Dtype()), pa.int16(), [int16_min, None, int16_max]),
            ("Int32_minmax_na",    pd.Series([int32_min, pd.NA, int32_max], dtype=pd.Int32Dtype()), pa.int32(), [int32_min, None, int32_max]),
            ("Int64_minmax_na",    pd.Series([int64_min, pd.NA, int64_max], dtype=pd.Int64Dtype()), pa.int64(), [int64_min, None, int64_max]),
            ("Int8_widen_na",      pd.Series([int8_min, pd.NA, int8_max], dtype=pd.Int8Dtype()),   pa.int64(), [int8_min, None, int8_max]),
            ("Int64_all_na",       pd.Series([pd.NA, pd.NA, pd.NA], dtype=pd.Int64Dtype()),        pa.int64(), [None, None, None]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(nullable_cases)

        # ==== 3.3 ArrowDtype-backed Series ====
        # (name, data, target_type, expected_values)
        # fmt: off
        arrow_cases = [
            # Int types → float
            ("arrow_int8_to_float",   pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8())),   pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_int16_to_float",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int16())),  pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_int32_to_float",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int32())),  pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_int64_to_float",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),  pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_uint8_to_float",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint8())),  pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_uint16_to_float", pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint16())), pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_uint32_to_float", pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint32())), pa.float64(), [1.0, 2.0, 3.0]),
            ("arrow_uint64_to_float", pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint64())), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types
            ("arrow_float32_to_int",  pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())), pa.int64(),   [1, 2]),
            ("arrow_float64_to_int",  pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())), pa.int64(),   [1, 2]),
            ("arrow_float32_to_f64",  pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())), pa.float64(), [1.0, 2.0]),
            ("arrow_float64_to_f32",  pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())), pa.float32(), [1.0, 2.0]),
            # Narrowing
            ("arrow_int64_to_int8",   pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.int8(),  [1, 2, 3]),
            ("arrow_int64_to_int16",  pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.int16(), [1, 2, 3]),
            # String, binary, bool
            ("arrow_str",             pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())),  pa.string(),       ["a", "b"]),
            ("arrow_str_to_large",    pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())),  pa.large_string(), ["a", "b"]),
            ("arrow_str_to_binary",   pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())),  pa.binary(),       [b"a", b"b"]),
            ("arrow_bin_to_str",      pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.binary())), pa.string(),      ["a", "b"]),
            ("arrow_bin_to_large",    pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.binary())), pa.large_binary(), [b"a", b"b"]),
            ("arrow_bool",            pd.Series([True, False], dtype=pd.ArrowDtype(pa.bool_())), pa.bool_(),       [True, False]),
            # Boundary values
            ("arrow_int8_minmax",     pd.Series([int8_min, int8_max], dtype=pd.ArrowDtype(pa.int8())),   pa.int8(),  [int8_min, int8_max]),
            ("arrow_int16_minmax",    pd.Series([int16_min, int16_max], dtype=pd.ArrowDtype(pa.int16())), pa.int16(), [int16_min, int16_max]),
            ("arrow_int32_minmax",    pd.Series([int32_min, int32_max], dtype=pd.ArrowDtype(pa.int32())), pa.int32(), [int32_min, int32_max]),
            ("arrow_int64_minmax",    pd.Series([int64_min, int64_max], dtype=pd.ArrowDtype(pa.int64())), pa.int64(), [int64_min, int64_max]),
            ("arrow_int8_widen",      pd.Series([int8_min, int8_max], dtype=pd.ArrowDtype(pa.int8())),   pa.int64(), [int8_min, int8_max]),
            ("arrow_int64_null",      pd.Series([int64_min, None, int64_max], dtype=pd.ArrowDtype(pa.int64())), pa.int64(), [int64_min, None, int64_max]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(arrow_cases)

        # Special float values (NaN/Inf) - type only
        for name, data, target in [
            ("arrow_nan",     pd.Series([nan, 1.0], dtype=pd.ArrowDtype(pa.float64())), pa.float64()),
            ("arrow_inf",     pd.Series([inf, neg_inf], dtype=pd.ArrowDtype(pa.float64())), pa.float64()),
            ("arrow_nan_f32", pd.Series([nan], dtype=pd.ArrowDtype(pa.float32())), pa.float32()),
        ]:
            with self.subTest(name=name):
                self.assertEqual(pa.array(data, type=target).type, target)

        # ArrowDtype int64 → decimal128 requires sufficient precision (19 digits)
        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
        self.assertEqual(
            pa.array(s, type=pa.decimal128(19, 0)).to_pylist(),
            [Decimal("1"), Decimal("2"), Decimal("3")],
        )
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(s, type=pa.decimal128(10, 0))

        # ==== 3.4 Datetime Types ====
        # (name, data, target_type, expected_values)
        # fmt: off
        datetime_cases = [
            # datetime64[ns] (numpy-backed) → various resolutions
            ("datetime64_us", pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])), pa.timestamp("us"), [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)]),
            ("datetime64_ns", pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])), pa.timestamp("ns"), [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)]),
            ("datetime64_ms", pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])), pa.timestamp("ms"), [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)]),
            # datetime64[ns, tz] (timezone-aware)
            ("datetime64_utc",       pd.Series(pd.to_datetime(["2024-01-01 12:00"]).tz_localize("UTC")),            pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            ("datetime64_sg",        pd.Series(pd.to_datetime(["2024-01-01 20:00"]).tz_localize("Asia/Singapore")), pa.timestamp("us", tz="Asia/Singapore"), [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)]),
            ("datetime64_sg_to_utc", pd.Series(pd.to_datetime(["2024-01-01 20:00"]).tz_localize("Asia/Singapore")), pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            # pd.Timestamp list
            ("pd_ts_utc",     [pd.Timestamp("2024-01-01 12:00:00", tz="UTC")],            pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            ("pd_ts_sg",      [pd.Timestamp("2024-01-01 20:00:00", tz="Asia/Singapore")], pa.timestamp("us", tz="Asia/Singapore"), [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)]),
            ("pd_ts_sg_utc",  [pd.Timestamp("2024-01-01 20:00:00", tz="Asia/Singapore")], pa.timestamp("us", tz="UTC"),            [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)]),
            # ArrowDtype temporal
            ("arrow_ts_us_ns",    pd.Series([datetime.datetime(2024, 1, 1)], dtype=pd.ArrowDtype(pa.timestamp("us"))), pa.timestamp("ns"),    [datetime.datetime(2024, 1, 1)]),
            ("arrow_ts_ns_us",    pd.Series([datetime.datetime(2024, 1, 1)], dtype=pd.ArrowDtype(pa.timestamp("ns"))), pa.timestamp("us"),    [datetime.datetime(2024, 1, 1)]),
            ("arrow_date32",      pd.Series([datetime.date(2024, 1, 1)], dtype=pd.ArrowDtype(pa.date32())),            pa.date32(),           [datetime.date(2024, 1, 1)]),
            ("arrow_date64",      pd.Series([datetime.date(2024, 1, 1)], dtype=pd.ArrowDtype(pa.date64())),            pa.date64(),           [datetime.date(2024, 1, 1)]),
            ("arrow_time64",      pd.Series([datetime.time(12, 30)], dtype=pd.ArrowDtype(pa.time64("us"))),            pa.time64("us"),       [datetime.time(12, 30)]),
            ("arrow_time32",      pd.Series([datetime.time(12, 30)], dtype=pd.ArrowDtype(pa.time32("ms"))),            pa.time32("ms"),       [datetime.time(12, 30)]),
            ("arrow_duration",    pd.Series([datetime.timedelta(days=1)], dtype=pd.ArrowDtype(pa.duration("us"))),    pa.duration("us"),     [datetime.timedelta(days=1)]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(datetime_cases)

    # =========================================================================
    # SECTION 4: NumPy Array Coercion
    # =========================================================================

    def test_numpy_array_coercion(self):
        """Test type coercion from numpy arrays with numeric and temporal types."""
        import numpy as np
        import pyarrow as pa

        # Constants
        int8_min, int8_max = np.iinfo(np.int8).min, np.iinfo(np.int8).max
        int16_min, int16_max = np.iinfo(np.int16).min, np.iinfo(np.int16).max
        int32_min, int32_max = np.iinfo(np.int32).min, np.iinfo(np.int32).max
        int64_min, int64_max = np.iinfo(np.int64).min, np.iinfo(np.int64).max
        nan, inf, neg_inf = float("nan"), float("inf"), float("-inf")

        # ==== 4.1 All Int/Float Types ====
        # (name, data, target_type, expected_values)
        # fmt: off
        numeric_cases = [
            # Int types → float64
            ("np_int8_to_float",   np.array([1, 2, 3], dtype=np.int8),   pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int16_to_float",  np.array([1, 2, 3], dtype=np.int16),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int32_to_float",  np.array([1, 2, 3], dtype=np.int32),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_int64_to_float",  np.array([1, 2, 3], dtype=np.int64),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint8_to_float",  np.array([1, 2, 3], dtype=np.uint8),  pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint16_to_float", np.array([1, 2, 3], dtype=np.uint16), pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint32_to_float", np.array([1, 2, 3], dtype=np.uint32), pa.float64(), [1.0, 2.0, 3.0]),
            ("np_uint64_to_float", np.array([1, 2, 3], dtype=np.uint64), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types → int64
            ("np_float16_to_int",  np.array([1.0, 2.0, 3.0], dtype=np.float16), pa.int64(), [1, 2, 3]),
            ("np_float32_to_int",  np.array([1.0, 2.0, 3.0], dtype=np.float32), pa.int64(), [1, 2, 3]),
            ("np_float64_to_int",  np.array([1.0, 2.0, 3.0], dtype=np.float64), pa.int64(), [1, 2, 3]),
            # Float ↔ float
            ("np_float32_to_f64",  np.array([1.0, 2.0], dtype=np.float32), pa.float64(), [1.0, 2.0]),
            ("np_float64_to_f32",  np.array([1.0, 2.0], dtype=np.float64), pa.float32(), [1.0, 2.0]),
            # Widening
            ("np_int8_to_int64",   np.array([1, 2, 3], dtype=np.int8),  pa.int64(), [1, 2, 3]),
            ("np_int16_to_int64",  np.array([1, 2, 3], dtype=np.int16), pa.int64(), [1, 2, 3]),
            ("np_int32_to_int64",  np.array([1, 2, 3], dtype=np.int32), pa.int64(), [1, 2, 3]),
            # Narrowing
            ("np_int64_to_int8",   np.array([1, 2, 3], dtype=np.int64), pa.int8(),  [1, 2, 3]),
            ("np_int64_to_int16",  np.array([1, 2, 3], dtype=np.int64), pa.int16(), [1, 2, 3]),
            ("np_int64_to_int32",  np.array([1, 2, 3], dtype=np.int64), pa.int32(), [1, 2, 3]),
            # Bool
            ("np_bool",            np.array([True, False, True], dtype=np.bool_), pa.bool_(), [True, False, True]),
            # Empty
            ("np_empty_int",       np.array([], dtype=np.int64),   pa.float64(), []),
            ("np_empty_float",     np.array([], dtype=np.float64), pa.int64(),   []),
            ("np_empty_bool",      np.array([], dtype=np.bool_),   pa.bool_(),   []),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(numeric_cases)

        # numpy int64 → decimal128 does NOT work
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.decimal128(10, 0))

        # ==== 4.2 Boundary Values ====
        # (name, data, target_type, expected_values)
        # fmt: off
        boundary_cases = [
            # Int min/max → same type
            ("np_int8_minmax",     np.array([int8_min, int8_max], dtype=np.int8),     pa.int8(),    [int8_min, int8_max]),
            ("np_int16_minmax",    np.array([int16_min, int16_max], dtype=np.int16),  pa.int16(),   [int16_min, int16_max]),
            ("np_int32_minmax",    np.array([int32_min, int32_max], dtype=np.int32),  pa.int32(),   [int32_min, int32_max]),
            ("np_int64_minmax",    np.array([int64_min, int64_max], dtype=np.int64),  pa.int64(),   [int64_min, int64_max]),
            # Int min/max → float64 (exact for smaller types)
            ("np_int8_to_f64",     np.array([int8_min, int8_max], dtype=np.int8),     pa.float64(), [float(int8_min), float(int8_max)]),
            ("np_int16_to_f64",    np.array([int16_min, int16_max], dtype=np.int16),  pa.float64(), [float(int16_min), float(int16_max)]),
            ("np_int32_to_f64",    np.array([int32_min, int32_max], dtype=np.int32),  pa.float64(), [float(int32_min), float(int32_max)]),
            # Widening with boundary
            ("np_int8_widen",      np.array([int8_min, 0, int8_max], dtype=np.int8),  pa.int64(),   [int8_min, 0, int8_max]),
            ("np_int16_widen",     np.array([int16_min, int16_max], dtype=np.int16),  pa.int32(),   [int16_min, int16_max]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(boundary_cases)

        # ==== 4.3 Special Float Values ====
        # NaN/Inf → float (type check only, NaN equality is tricky)
        for name, data, target in [
            ("np_nan",         np.array([nan, 1.0, nan], dtype=np.float64), pa.float64()),
            ("np_inf",         np.array([inf, neg_inf], dtype=np.float64), pa.float64()),
            ("np_nan_to_f32",  np.array([nan, 1.0], dtype=np.float64), pa.float32()),
            ("np_inf_to_f32",  np.array([inf, neg_inf], dtype=np.float64), pa.float32()),
        ]:
            with self.subTest(name=name):
                self.assertEqual(pa.array(data, type=target).type, target)

        # NaN to int fails for numpy array (unlike pandas Series)
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(np.array([nan, 1.0], dtype=np.float64), type=pa.int64())

        # ==== 4.4 Datetime Arrays ====
        arr_d = np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[D]")
        arr_ns = np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[ns]")
        arr_us = np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[us]")
        arr_ms = np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[ms]")
        expected_dates = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]
        expected_ts = [datetime.datetime(2024, 1, 1, 12, 0), datetime.datetime(2024, 1, 2, 12, 0)]

        # (name, data, target_type, expected_values)
        # fmt: off
        datetime_cases = [
            # Date arrays
            ("np_date_to_date32",  arr_d,  pa.date32(), expected_dates),
            ("np_date_to_date64",  arr_d,  pa.date64(), expected_dates),
            # Timestamp arrays - various source resolutions
            ("np_ts_ns_to_s",      arr_ns, pa.timestamp("s"),  expected_ts),
            ("np_ts_ns_to_ms",     arr_ns, pa.timestamp("ms"), expected_ts),
            ("np_ts_ns_to_us",     arr_ns, pa.timestamp("us"), expected_ts),
            ("np_ts_ns_to_ns",     arr_ns, pa.timestamp("ns"), expected_ts),
            ("np_ts_us_to_us",     arr_us, pa.timestamp("us"), expected_ts),
            ("np_ts_ms_to_ms",     arr_ms, pa.timestamp("ms"), expected_ts),
            # Empty datetime
            ("np_empty_date",      np.array([], dtype="datetime64[D]"),  pa.date32(),       []),
            ("np_empty_ts",        np.array([], dtype="datetime64[ns]"), pa.timestamp("us"), []),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(datetime_cases)

        # ==== 4.5 Timedelta Arrays ====
        arr_td_ns = np.array([86400000000000, 7200000000000], dtype="timedelta64[ns]")  # 1 day, 2 hours
        arr_td_us = np.array([86400000000, 7200000000], dtype="timedelta64[us]")
        expected_td = [datetime.timedelta(days=1), datetime.timedelta(hours=2)]

        # (name, data, target_type, expected_values)
        # fmt: off
        timedelta_cases = [
            ("np_td_ns_to_us", arr_td_ns, pa.duration("us"), expected_td),
            ("np_td_ns_to_ns", arr_td_ns, pa.duration("ns"), expected_td),
            ("np_td_us_to_us", arr_td_us, pa.duration("us"), expected_td),
            ("np_empty_td",    np.array([], dtype="timedelta64[ns]"), pa.duration("us"), []),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(timedelta_cases)

    # =========================================================================
    # SECTION 5: Nested Types Coercion
    # =========================================================================

    def test_nested_types_coercion(self):
        """Test type coercion for nested types: list, struct, map."""
        import pyarrow as pa

        # ==== 5.1 List Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        list_cases = [
            # Element type coercion
            ("list_int_to_float32", [[1, 2], [3, 4]],    pa.list_(pa.float32()),    [[1.0, 2.0], [3.0, 4.0]]),
            ("list_int_to_float64", [[1, 2], [3, 4]],    pa.list_(pa.float64()),    [[1.0, 2.0], [3.0, 4.0]]),
            ("list_int_to_int8",    [[1, 2], [3, 4]],    pa.list_(pa.int8()),       [[1, 2], [3, 4]]),
            ("list_float_to_int64", [[1.0, 2.0], [3.0]], pa.list_(pa.int64()),      [[1, 2], [3]]),
            # Empty
            ("list_empty_outer",    [],                  pa.list_(pa.int64()),      []),
            ("list_empty_inner",    [[]],                pa.list_(pa.int64()),      [[]]),
            ("list_mixed_empty",    [[1, 2], []],        pa.list_(pa.float64()),    [[1.0, 2.0], []]),
            # None
            ("list_none_elem",      [[1, None, 3]],      pa.list_(pa.int64()),      [[1, None, 3]]),
            ("list_none_outer",     [None, [1, 2]],      pa.list_(pa.int64()),      [None, [1, 2]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(list_cases)

        # ==== 5.2 Large List ====

        # (name, data, target_type, expected_values)
        # fmt: off
        large_list_cases = [
            ("large_list_int",        [[1, 2], [3, 4]],    pa.large_list(pa.int64()),   [[1, 2], [3, 4]]),
            ("large_list_int_to_flt", [[1, 2], [3, 4]],    pa.large_list(pa.float64()), [[1.0, 2.0], [3.0, 4.0]]),
            ("large_list_str",        [["a", "b"], ["c"]], pa.large_list(pa.string()),  [["a", "b"], ["c"]]),
            ("large_list_none",       [None, [1, 2]],      pa.large_list(pa.int64()),   [None, [1, 2]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(large_list_cases)

        # ==== 5.3 Fixed Size List ====

        # (name, data, target_type, expected_values)
        # fmt: off
        fixed_list_cases = [
            ("fixed_list_int",        [[1, 2], [3, 4]], pa.list_(pa.int64(), 2),   [[1, 2], [3, 4]]),
            ("fixed_list_int_to_flt", [[1, 2], [3, 4]], pa.list_(pa.float64(), 2), [[1.0, 2.0], [3.0, 4.0]]),
            ("fixed_list_str",        [["a", "b"]],    pa.list_(pa.string(), 2),  [["a", "b"]]),
            ("fixed_list_none",       [None, [1, 2]],  pa.list_(pa.int64(), 2),   [None, [1, 2]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(fixed_list_cases)

        # ==== 5.4 List with Temporal Elements ====

        # (name, data, target_type, expected_values)
        # fmt: off
        list_temporal_cases = [
            ("list_date32",    [[datetime.date(2024, 1, 1)]],            pa.list_(pa.date32()),       [[datetime.date(2024, 1, 1)]]),
            ("list_timestamp", [[datetime.datetime(2024, 1, 1, 12, 0)]], pa.list_(pa.timestamp("us")), [[datetime.datetime(2024, 1, 1, 12, 0)]]),
            ("list_time",      [[datetime.time(12, 30)]],                pa.list_(pa.time64("us")),   [[datetime.time(12, 30)]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(list_temporal_cases)

        # ==== 5.5 Struct Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        struct_cases = [
            # Field type coercion (int → float)
            ("struct_int_to_float", [{"x": 1, "y": 2}, {"x": 3, "y": 4}], pa.struct([("x", pa.float64()), ("y", pa.float64())]), [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]),
            # Struct with None
            ("struct_with_none",    [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}], pa.struct([("x", pa.int64()), ("y", pa.string())]), [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}]),
            # Empty struct
            ("struct_empty",        [], pa.struct([("x", pa.int64()), ("y", pa.string())]), []),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(struct_cases)

        # (name, data, target_type, expected_values)
        # fmt: off
        struct_type_only = [
            # Mixed field types
            ("struct_mixed",     [{"id": 1, "name": "Alice", "date": datetime.date(2024, 1, 1)}], pa.struct([("id", pa.int64()), ("name", pa.string()), ("date", pa.date32())]), [{"id": 1, "name": "Alice", "date": datetime.date(2024, 1, 1)}]),
            # Struct with large_string
            ("struct_large_str", [{"id": 1, "name": "hi"}], pa.struct([("id", pa.int64()), ("name", pa.large_string())]), [{"id": 1, "name": "hi"}]),
            # Struct with binary
            ("struct_binary",    [{"id": 1, "data": b"hi"}], pa.struct([("id", pa.int64()), ("data", pa.binary())]), [{"id": 1, "data": b"hi"}]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(struct_type_only)

        # ==== 5.6 Map Types ====

        # (name, data, target_type, expected_values)
        # Note: map to_pylist() returns list of list of tuples
        # fmt: off
        map_cases = [
            # Value coercion
            ("map_str_int_to_flt", [[("a", 1), ("b", 2)]],     pa.map_(pa.string(), pa.float64()),     [[("a", 1.0), ("b", 2.0)]]),
            ("map_str_int8",       [[("a", 1), ("b", 2)]],     pa.map_(pa.string(), pa.int8()),        [[("a", 1), ("b", 2)]]),
            ("map_str_large_str",  [[("a", "x"), ("b", "y")]], pa.map_(pa.string(), pa.large_string()), [[("a", "x"), ("b", "y")]]),
            # Key types
            ("map_int_str",        [[(1, "a"), (2, "b")]],     pa.map_(pa.int64(), pa.string()),       [[(1, "a"), (2, "b")]]),
            ("map_int32_str",      [[(1, "a"), (2, "b")]],     pa.map_(pa.int32(), pa.string()),       [[(1, "a"), (2, "b")]]),
            ("map_int_float",      [[(1, 10), (2, 20)]],       pa.map_(pa.int64(), pa.float64()),      [[(1, 10.0), (2, 20.0)]]),
            # Empty
            ("map_empty_outer",    [],                         pa.map_(pa.string(), pa.int64()),       []),
            ("map_empty_inner",    [[]],                       pa.map_(pa.string(), pa.int64()),       [[]]),
            # None
            ("map_none_outer",     [None, [("a", 1)]],         pa.map_(pa.string(), pa.int64()),       [None, [("a", 1)]]),
            ("map_none_value",     [[("a", 1), ("b", None)]],  pa.map_(pa.string(), pa.int64()),       [[("a", 1), ("b", None)]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(map_cases)

        # ==== 5.7 Deeply Nested Types ====

        # (name, data, target_type, expected_values)
        # fmt: off
        deeply_nested_cases = [
            # List of lists
            ("list_of_list",       [[[1, 2], [3]], [[4, 5, 6]]], pa.list_(pa.list_(pa.float64())), [[[1.0, 2.0], [3.0]], [[4.0, 5.0, 6.0]]]),
            # List of structs
            ("list_of_struct",     [[{"x": 1}, {"x": 2}]],       pa.list_(pa.struct([("x", pa.float64())])), [[{"x": 1.0}, {"x": 2.0}]]),
            # Struct containing list
            ("struct_with_list",   [{"id": 1, "values": [1, 2, 3]}], pa.struct([("id", pa.int64()), ("values", pa.list_(pa.float64()))]), [{"id": 1, "values": [1.0, 2.0, 3.0]}]),
            # Triple nested
            ("triple_nested",      [[[[1, 2]], [[3]]]],          pa.list_(pa.list_(pa.list_(pa.int64()))), [[[[1, 2]], [[3]]]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(deeply_nested_cases)

        # (name, data, target_type, expected_values)
        # fmt: off
        deeply_nested_type_only = [
            # Struct containing map
            ("struct_with_map",   [{"id": 1, "meta": [("a", 1)]}],      pa.struct([("id", pa.int64()), ("meta", pa.map_(pa.string(), pa.int64()))]), [{"id": 1, "meta": [("a", 1)]}]),
            # Map with list values
            ("map_with_list",     [[("a", [1, 2]), ("b", [3])]],        pa.map_(pa.string(), pa.list_(pa.int64())),                                  [[("a", [1, 2]), ("b", [3])]]),
            # Map with struct values
            ("map_with_struct",   [[("a", {"x": 1}), ("b", {"x": 2})]], pa.map_(pa.string(), pa.struct([("x", pa.int64())])),                        [[("a", {"x": 1}), ("b", {"x": 2})]]),
            # List of maps
            ("list_of_map",       [[[("a", 1)], [("b", 2)]]],           pa.list_(pa.map_(pa.string(), pa.int64())),                                  [[[("a", 1)], [("b", 2)]]]),
            # Large nested types
            ("large_list_struct", [[{"x": 1}, {"x": 2}]],               pa.large_list(pa.struct([("x", pa.int64())])),                               [[{"x": 1}, {"x": 2}]]),
        ]
        # fmt: on
        self._run_coercion_tests_with_values(deeply_nested_type_only)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
