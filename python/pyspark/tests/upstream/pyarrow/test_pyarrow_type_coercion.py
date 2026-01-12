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

    def _run_coercion_tests(self, cases: List[Tuple[Any, Any]]) -> None:
        """Run coercion tests: (data, target_type)."""
        import pyarrow as pa

        for data, target_type in cases:
            arr = pa.array(data, type=target_type)
            self.assertEqual(arr.type, target_type)

    def _run_coercion_tests_with_values(self, cases: List[Tuple[Any, Any, Any]]) -> None:
        """Run coercion tests with value verification: (data, target_type, expected)."""
        import pyarrow as pa

        for data, target_type, expected in cases:
            arr = pa.array(data, type=target_type)
            self.assertEqual(arr.type, target_type)
            self.assertEqual(arr.to_pylist(), expected)

    def _run_error_tests(self, cases: List[Tuple[Any, Any]], error_type) -> None:
        """Run tests expecting errors: (data, target_type)."""
        import pyarrow as pa

        for data, target_type in cases:
            with self.assertRaises(error_type):
                pa.array(data, type=target_type)

    # =========================================================================
    # SECTION 1: Nullable Data Coercion
    # =========================================================================

    def test_nullable_data_coercion(self):
        """Test type coercion with None, NaN for numeric and temporal types."""
        import pyarrow as pa

        nan, inf, neg_inf = float("nan"), float("inf"), float("-inf")
        date1 = datetime.date(2024, 1, 1)
        time1 = datetime.time(12, 0)

        none_cases = [
            # Numeric
            ([1, None, 3], pa.float64(), [1.0, None, 3.0]),
            ([1, None, 3], pa.decimal128(10, 0), [Decimal("1"), None, Decimal("3")]),
            ([1, None, 3], pa.int32(), [1, None, 3]),
            ([1.0, None, 3.0], pa.int64(), [1, None, 3]),
            ([None, None, None], pa.float64(), [None, None, None]),
            ([None, None, None], pa.int64(), [None, None, None]),
            # String
            ([None, None], pa.string(), [None, None]),
            # Temporal
            ([date1, None], pa.date32(), [date1, None]),
            ([time1, None], pa.time64("us"), [time1, None]),
        ]
        self._run_coercion_tests_with_values(none_cases)

        # Verify null counts
        self.assertEqual(pa.array([1, None, 3], type=pa.float64()).null_count, 1)
        self.assertEqual(pa.array([None, None, None], type=pa.float64()).null_count, 3)

        # ---- NaN/Inf in float ----
        # (data, target_type)
        nan_cases = [
            ([nan], pa.float64()),
            ([inf], pa.float64()),
            ([neg_inf], pa.float64()),
            ([nan, inf, neg_inf], pa.float64()),
            ([1.0, nan, 3.0], pa.float64()),
            ([nan], pa.float32()),
            ([inf], pa.float32()),
        ]
        self._run_coercion_tests(nan_cases)

        # Verify NaN/Inf values
        self.assertTrue(math.isnan(pa.array([nan], type=pa.float64()).to_pylist()[0]))
        self.assertEqual(pa.array([inf], type=pa.float64()).to_pylist()[0], inf)

        # ---- NaN/Inf to non-float fails ----
        # (data, target_type)
        nan_error_cases = [
            ([nan], pa.decimal128(10, 2)),
            ([nan], pa.int64()),
            ([inf], pa.int64()),
        ]
        self._run_error_tests(nan_error_cases, (pa.ArrowInvalid, pa.ArrowTypeError))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_na_coercion(self):
        """Test pd.NA handling during type coercion."""
        import pandas as pd
        import pyarrow as pa

        # (data, target_type, expected_values)
        cases = [
            (pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()), pa.float64(), [1.0, None, 3.0]),
            (pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float64Dtype()), pa.int64(), [1, None, 3]),
        ]
        self._run_coercion_tests_with_values(cases)

    # =========================================================================
    # SECTION 2: Plain Python Instances Coercion
    # =========================================================================

    def test_python_instances_coercion(self):
        """Test type coercion from Python list, tuple, generator with all data types."""
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        # ==== 2.1 Numeric Types ====

        # (data, target_type, expected_values)
        numeric_cases = [
            # Int <-> Float
            ([1, 2, 3], pa.float64(), [1.0, 2.0, 3.0]),
            ([1, 2, 3], pa.float32(), [1.0, 2.0, 3.0]),
            ([1.0, 2.0, 3.0], pa.int64(), [1, 2, 3]),
            ((1, 2, 3), pa.float64(), [1.0, 2.0, 3.0]),
            # Int narrowing/widening
            ([1, 2, 3], pa.int8(), [1, 2, 3]),
            ([1, 2, 3], pa.int16(), [1, 2, 3]),
            ([1, 2, 3], pa.int32(), [1, 2, 3]),
            ([1, 2, 3], pa.uint8(), [1, 2, 3]),
            ([1, 2, 3], pa.uint16(), [1, 2, 3]),
            ([1, 2, 3], pa.uint32(), [1, 2, 3]),
            ([1, 2, 3], pa.uint64(), [1, 2, 3]),
            # Decimal
            ([1, 2, 3], pa.decimal128(10, 2), [Decimal("1.00"), Decimal("2.00"), Decimal("3.00")]),
            # Empty
            ([], pa.int64(), []),
            ([], pa.float64(), []),
        ]
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

        # (data, target_type, expected_values)
        boundary_cases = [
            ([int8_min, 0, int8_max], pa.int8(), [int8_min, 0, int8_max]),
            ([int16_min, int16_max], pa.int16(), [int16_min, int16_max]),
            ([int32_min, int32_max], pa.int32(), [int32_min, int32_max]),
            ([int64_min, 0, int64_max], pa.int64(), [int64_min, 0, int64_max]),
            # Widening
            ([int8_min, int8_max], pa.int64(), [int8_min, int8_max]),
            ([int16_min, int16_max], pa.int32(), [int16_min, int16_max]),
        ]
        self._run_coercion_tests_with_values(boundary_cases)

        # ==== 2.3 Int Overflow (Error) ====

        # (data, target_type)
        overflow_cases = [
            ([128], pa.int8()),
            ([-129], pa.int8()),
            ([32768], pa.int16()),
            ([2**31], pa.int32()),
            ([256], pa.uint8()),
            ([65536], pa.uint16()),
            ([2**32], pa.uint32()),
        ]
        self._run_error_tests(overflow_cases, pa.ArrowInvalid)

        # Negative to unsigned
        with self.assertRaises(OverflowError):
            pa.array([-1], type=pa.uint8())

        # ==== 2.4 String Types ====

        # (data, target_type, expected_values)
        string_cases = [
            # Basic
            (["hello", "world"], pa.string(), ["hello", "world"]),
            (["", "a", ""], pa.string(), ["", "a", ""]),
            (["  ", "\t", "\n"], pa.string(), ["  ", "\t", "\n"]),
            (["a", None, "b"], pa.string(), ["a", None, "b"]),
            # Non-English
            (["你好", "世界"], pa.string(), ["你好", "世界"]),
            (["こんにちは"], pa.string(), ["こんにちは"]),
            (["안녕하세요"], pa.string(), ["안녕하세요"]),
            (["😀", "🎉", "🚀"], pa.string(), ["😀", "🎉", "🚀"]),
            (["مرحبا"], pa.string(), ["مرحبا"]),
            (["Привет"], pa.string(), ["Привет"]),
            # Large string
            (["hello", "world"], pa.large_string(), ["hello", "world"]),
            (["a", None, "b"], pa.large_string(), ["a", None, "b"]),
        ]
        self._run_coercion_tests_with_values(string_cases)

        # ==== 2.5 Binary Types ====

        # (data, target_type, expected_values)
        binary_cases = [
            ([b"hello", b"world"], pa.binary(), [b"hello", b"world"]),
            ([b"hello", b"world"], pa.large_binary(), [b"hello", b"world"]),
            ([b"a", None, b"b"], pa.binary(), [b"a", None, b"b"]),
            ([bytearray(b"hi")], pa.binary(), [b"hi"]),
            # Fixed size binary
            ([b"abcd", b"efgh"], pa.binary(4), [b"abcd", b"efgh"]),
            ([b"a", b"b", b"c"], pa.binary(1), [b"a", b"b", b"c"]),
        ]
        self._run_coercion_tests_with_values(binary_cases)

        # ==== 2.6 Temporal Types ====

        ts_data = [datetime.datetime(2024, 1, 1, 12, 0), datetime.datetime(2024, 1, 2, 12, 0)]
        date_data = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]
        time_data = [datetime.time(12, 30), datetime.time(13, 45)]
        dur_data = [datetime.timedelta(days=1), datetime.timedelta(hours=2)]
        epoch_date = datetime.date(1970, 1, 1)
        date_min = datetime.date(1, 1, 1)
        date_max = datetime.date(9999, 12, 31)
        time_min = datetime.time(0, 0, 0, 0)
        time_max = datetime.time(23, 59, 59, 999999)
        td_neg = datetime.timedelta(days=-1)

        temporal_cases = [
            # Timestamp resolutions (microseconds truncated for lower resolutions)
            (ts_data, pa.timestamp("s"), ts_data),
            (ts_data, pa.timestamp("ms"), ts_data),
            (ts_data, pa.timestamp("us"), ts_data),
            (ts_data, pa.timestamp("ns"), ts_data),
            # Date
            (date_data, pa.date32(), date_data),
            (date_data, pa.date64(), date_data),
            # Time (seconds truncated for lower resolutions)
            (time_data, pa.time32("s"), [datetime.time(12, 30), datetime.time(13, 45)]),
            (time_data, pa.time32("ms"), time_data),
            (time_data, pa.time64("us"), time_data),
            (time_data, pa.time64("ns"), time_data),
            # Duration
            (dur_data, pa.duration("s"), dur_data),
            (dur_data, pa.duration("ms"), dur_data),
            (dur_data, pa.duration("us"), dur_data),
            (dur_data, pa.duration("ns"), dur_data),
            # Boundary values
            ([epoch_date], pa.date32(), [epoch_date]),
            ([date_min], pa.date32(), [date_min]),
            ([date_max], pa.date32(), [date_max]),
            ([time_min], pa.time64("us"), [time_min]),
            ([time_max], pa.time64("us"), [time_max]),
            ([td_neg], pa.duration("us"), [td_neg]),
        ]
        self._run_coercion_tests_with_values(temporal_cases)

        # ==== 2.7 Timezone Coercion ====

        utc_tz = ZoneInfo("UTC")
        sg_tz = ZoneInfo("Asia/Singapore")
        la_tz = ZoneInfo("America/Los_Angeles")
        tokyo_tz = ZoneInfo("Asia/Tokyo")
        ny_tz = ZoneInfo("America/New_York")
        dt_utc = datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)
        dt_sg = datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)
        dt_la = datetime.datetime(2024, 1, 1, 4, 0, tzinfo=la_tz)
        dt_naive = datetime.datetime(2024, 1, 1, 12, 0)

        # Note: PyArrow returns tz-aware datetime with the target timezone
        tz_cases = [
            # Same timezone
            ([dt_utc], pa.timestamp("us", tz="UTC"), [dt_utc]),
            ([dt_sg], pa.timestamp("us", tz="Asia/Singapore"), [dt_sg]),
            # Cross timezone conversion (SG +8 → UTC, so 20:00 SG = 12:00 UTC)
            ([dt_sg], pa.timestamp("us", tz="UTC"), [dt_utc]),
            # LA -8 → UTC, so 4:00 LA = 12:00 UTC
            ([dt_la], pa.timestamp("us", tz="UTC"), [dt_utc]),
            # Naive to tz-aware (treated as UTC)
            ([dt_naive], pa.timestamp("us", tz="UTC"), [dt_utc]),
            # Tz-aware to tz-naive (returns naive datetime)
            ([dt_utc], pa.timestamp("us"), [dt_naive]),
        ]
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

        # (data, target_type, expected_values)
        cross_type_ok_with_values = [
            # int → date (epoch days)
            ([19723], pa.date32(), [datetime.date(2024, 1, 1)]),
            # binary ↔ string (UTF-8)
            ([b"hello", b"world"], pa.string(), ["hello", "world"]),
            (["hello", "world"], pa.binary(), [b"hello", b"world"]),
        ]
        self._run_coercion_tests_with_values(cross_type_ok_with_values)

        # (data, target_type, expected_values)
        cross_type_ok = [
            # int → timestamp (epoch seconds: 1704067200 = 2024-01-01 00:00:00 UTC)
            ([1704067200], pa.timestamp("s"), [datetime.datetime(2024, 1, 1, 0, 0, 0)]),
        ]
        self._run_coercion_tests_with_values(cross_type_ok)

        # ==== 2.9 Cross-Type Coercion (Fails - Requires Explicit Cast) ====

        # (data, target_type)
        cross_type_fail = [
            # numeric → string
            ([1, 2, 3], pa.string()),
            ([1.5, 2.5], pa.string()),
            ([True, False], pa.string()),
            # string → numeric
            (["1", "2"], pa.int64()),
            (["1.5", "2.5"], pa.float64()),
            (["true"], pa.bool_()),
            # bool ↔ int
            ([True, False], pa.int64()),
            ([1, 0, 1], pa.bool_()),
            # temporal → numeric
            ([datetime.date(2024, 1, 1)], pa.int64()),
            ([datetime.datetime(2024, 1, 1)], pa.int64()),
            ([datetime.time(12, 0)], pa.int64()),
            ([datetime.timedelta(days=1)], pa.int64()),
            # date → timestamp
            ([datetime.date(2024, 1, 1)], pa.timestamp("us")),
            # binary → numeric
            ([b"hello"], pa.int64()),
            # nested → scalar
            ([[1, 2, 3]], pa.int64()),
            ([{"a": 1}], pa.int64()),
        ]
        self._run_error_tests(
            cross_type_fail, (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)
        )

        # ==== 2.10 Precision Loss (Silent) ====

        # (data, target_type, expected_values)
        precision_loss = [
            # float → int (truncation, not rounding)
            ([1.9, 2.1, 3.7], pa.int64(), [1, 2, 3]),
            ([-1.9, -2.1], pa.int64(), [-1, -2]),
            # decimal → int (truncation)
            ([Decimal("1.9"), Decimal("2.1")], pa.int64(), [1, 2]),
        ]
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
        # (data, target_type, expected_values)
        numpy_cases = [
            # Int types → float
            (pd.Series([1, 2, 3], dtype="int8"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="int16"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="int32"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="int64"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="uint8"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="uint16"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="uint32"), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype="uint64"), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types → int
            (pd.Series([1.0, 2.0, 3.0], dtype="float32"), pa.int64(), [1, 2, 3]),
            (pd.Series([1.0, 2.0, 3.0], dtype="float64"), pa.int64(), [1, 2, 3]),
            # Float ↔ float
            (pd.Series([1.0, 2.0], dtype="float32"), pa.float64(), [1.0, 2.0]),
            (pd.Series([1.0, 2.0], dtype="float64"), pa.float32(), [1.0, 2.0]),
            # Narrowing
            (pd.Series([1, 2, 3], dtype="int64"), pa.int8(), [1, 2, 3]),
            (pd.Series([1, 2, 3], dtype="int64"), pa.int16(), [1, 2, 3]),
            (pd.Series([1, 2, 3], dtype="int64"), pa.int32(), [1, 2, 3]),
            # Bool, string
            (pd.Series([True, False], dtype="bool"), pa.bool_(), [True, False]),
            (pd.Series(["a", "b"], dtype="object"), pa.string(), ["a", "b"]),
            (pd.Series(["a", "b"], dtype="object"), pa.large_string(), ["a", "b"]),
            # Empty
            (pd.Series([], dtype="int64"), pa.float64(), []),
            (pd.Series([], dtype="float64"), pa.int64(), []),
            # Boundary values
            (
                pd.Series([int8_min, int8_max], dtype="int8"),
                pa.float64(),
                [float(int8_min), float(int8_max)],
            ),
            (
                pd.Series([int16_min, int16_max], dtype="int16"),
                pa.float64(),
                [float(int16_min), float(int16_max)],
            ),
            (
                pd.Series([int32_min, int32_max], dtype="int32"),
                pa.float64(),
                [float(int32_min), float(int32_max)],
            ),
            (pd.Series([int64_min, int64_max], dtype="int64"), pa.int64(), [int64_min, int64_max]),
            (pd.Series([int8_min, 0, int8_max], dtype="int8"), pa.int64(), [int8_min, 0, int8_max]),
            # NaN to int → None (pandas-specific behavior)
            (pd.Series([nan, 1.0], dtype="float64"), pa.int64(), [None, 1]),
        ]
        self._run_coercion_tests_with_values(numpy_cases)

        # Special float values (NaN/Inf) - type only
        for data, target in [
            (pd.Series([nan, 1.0], dtype="float64"), pa.float64()),
            (pd.Series([inf, neg_inf], dtype="float64"), pa.float64()),
            (pd.Series([nan], dtype="float64"), pa.float32()),
        ]:
            self.assertEqual(pa.array(data, type=target).type, target)

        # numpy int → decimal128 does NOT work
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(pd.Series([1, 2, 3], dtype="int64"), type=pa.decimal128(10, 0))

        # ==== 3.2 Nullable Extension Types ====
        # (data, target_type, expected_values)
        nullable_cases = [
            # Int types → float
            (pd.Series([1, 2, 3], dtype=pd.Int8Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.Int16Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.Int32Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.Int64Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types
            (pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype()), pa.int64(), [1, 2, 3]),
            (pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype()), pa.int64(), [1, 2, 3]),
            (pd.Series([1.0, 2.0], dtype=pd.Float64Dtype()), pa.float32(), [1.0, 2.0]),
            # String, bool
            (pd.Series(["a", "b"], dtype=pd.StringDtype()), pa.string(), ["a", "b"]),
            (pd.Series(["a", "b"], dtype=pd.StringDtype()), pa.large_string(), ["a", "b"]),
            (pd.Series([True, False], dtype=pd.BooleanDtype()), pa.bool_(), [True, False]),
            # With NA
            (pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()), pa.float64(), [1.0, None, 3.0]),
            (pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype()), pa.int32(), [1, None, 3]),
            (pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float64Dtype()), pa.int64(), [1, None, 3]),
            (pd.Series(["a", pd.NA, "c"], dtype=pd.StringDtype()), pa.string(), ["a", None, "c"]),
            (
                pd.Series([True, pd.NA, False], dtype=pd.BooleanDtype()),
                pa.bool_(),
                [True, None, False],
            ),
            # Boundary with NA
            (
                pd.Series([int8_min, pd.NA, int8_max], dtype=pd.Int8Dtype()),
                pa.int8(),
                [int8_min, None, int8_max],
            ),
            (
                pd.Series([int16_min, pd.NA, int16_max], dtype=pd.Int16Dtype()),
                pa.int16(),
                [int16_min, None, int16_max],
            ),
            (
                pd.Series([int32_min, pd.NA, int32_max], dtype=pd.Int32Dtype()),
                pa.int32(),
                [int32_min, None, int32_max],
            ),
            (
                pd.Series([int64_min, pd.NA, int64_max], dtype=pd.Int64Dtype()),
                pa.int64(),
                [int64_min, None, int64_max],
            ),
            (
                pd.Series([int8_min, pd.NA, int8_max], dtype=pd.Int8Dtype()),
                pa.int64(),
                [int8_min, None, int8_max],
            ),
            (
                pd.Series([pd.NA, pd.NA, pd.NA], dtype=pd.Int64Dtype()),
                pa.int64(),
                [None, None, None],
            ),
        ]
        self._run_coercion_tests_with_values(nullable_cases)

        # ==== 3.3 ArrowDtype-backed Series ====
        # (data, target_type, expected_values)
        arrow_cases = [
            # Int types → float
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int16())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int32())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint8())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint16())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint32())), pa.float64(), [1.0, 2.0, 3.0]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.uint64())), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())), pa.int64(), [1, 2]),
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())), pa.int64(), [1, 2]),
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())), pa.float64(), [1.0, 2.0]),
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())), pa.float32(), [1.0, 2.0]),
            # Narrowing
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.int8(), [1, 2, 3]),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.int16(), [1, 2, 3]),
            # String, binary, bool
            (pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())), pa.string(), ["a", "b"]),
            (
                pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())),
                pa.large_string(),
                ["a", "b"],
            ),
            (pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.string())), pa.binary(), [b"a", b"b"]),
            (pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.binary())), pa.string(), ["a", "b"]),
            (
                pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.binary())),
                pa.large_binary(),
                [b"a", b"b"],
            ),
            (pd.Series([True, False], dtype=pd.ArrowDtype(pa.bool_())), pa.bool_(), [True, False]),
            # Boundary values
            (
                pd.Series([int8_min, int8_max], dtype=pd.ArrowDtype(pa.int8())),
                pa.int8(),
                [int8_min, int8_max],
            ),
            (
                pd.Series([int16_min, int16_max], dtype=pd.ArrowDtype(pa.int16())),
                pa.int16(),
                [int16_min, int16_max],
            ),
            (
                pd.Series([int32_min, int32_max], dtype=pd.ArrowDtype(pa.int32())),
                pa.int32(),
                [int32_min, int32_max],
            ),
            (
                pd.Series([int64_min, int64_max], dtype=pd.ArrowDtype(pa.int64())),
                pa.int64(),
                [int64_min, int64_max],
            ),
            (
                pd.Series([int8_min, int8_max], dtype=pd.ArrowDtype(pa.int8())),
                pa.int64(),
                [int8_min, int8_max],
            ),
            (
                pd.Series([int64_min, None, int64_max], dtype=pd.ArrowDtype(pa.int64())),
                pa.int64(),
                [int64_min, None, int64_max],
            ),
        ]
        self._run_coercion_tests_with_values(arrow_cases)

        # Special float values (NaN/Inf) - type only
        for data, target in [
            (pd.Series([nan, 1.0], dtype=pd.ArrowDtype(pa.float64())), pa.float64()),
            (pd.Series([inf, neg_inf], dtype=pd.ArrowDtype(pa.float64())), pa.float64()),
            (pd.Series([nan], dtype=pd.ArrowDtype(pa.float32())), pa.float32()),
        ]:
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
        # (data, target_type, expected_values)
        datetime_cases = [
            # datetime64[ns] (numpy-backed) → various resolutions
            (
                pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])),
                pa.timestamp("us"),
                [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
            ),
            (
                pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])),
                pa.timestamp("ns"),
                [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
            ),
            (
                pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])),
                pa.timestamp("ms"),
                [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
            ),
            # datetime64[ns, tz] (timezone-aware)
            (
                pd.Series(pd.to_datetime(["2024-01-01 12:00"]).tz_localize("UTC")),
                pa.timestamp("us", tz="UTC"),
                [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)],
            ),
            (
                pd.Series(pd.to_datetime(["2024-01-01 20:00"]).tz_localize("Asia/Singapore")),
                pa.timestamp("us", tz="Asia/Singapore"),
                [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)],
            ),
            (
                pd.Series(pd.to_datetime(["2024-01-01 20:00"]).tz_localize("Asia/Singapore")),
                pa.timestamp("us", tz="UTC"),
                [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)],
            ),
            # pd.Timestamp list
            (
                [pd.Timestamp("2024-01-01 12:00:00", tz="UTC")],
                pa.timestamp("us", tz="UTC"),
                [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)],
            ),
            (
                [pd.Timestamp("2024-01-01 20:00:00", tz="Asia/Singapore")],
                pa.timestamp("us", tz="Asia/Singapore"),
                [datetime.datetime(2024, 1, 1, 20, 0, tzinfo=sg_tz)],
            ),
            (
                [pd.Timestamp("2024-01-01 20:00:00", tz="Asia/Singapore")],
                pa.timestamp("us", tz="UTC"),
                [datetime.datetime(2024, 1, 1, 12, 0, tzinfo=utc_tz)],
            ),
            # ArrowDtype temporal
            (
                pd.Series([datetime.datetime(2024, 1, 1)], dtype=pd.ArrowDtype(pa.timestamp("us"))),
                pa.timestamp("ns"),
                [datetime.datetime(2024, 1, 1)],
            ),
            (
                pd.Series([datetime.datetime(2024, 1, 1)], dtype=pd.ArrowDtype(pa.timestamp("ns"))),
                pa.timestamp("us"),
                [datetime.datetime(2024, 1, 1)],
            ),
            (
                pd.Series([datetime.date(2024, 1, 1)], dtype=pd.ArrowDtype(pa.date32())),
                pa.date32(),
                [datetime.date(2024, 1, 1)],
            ),
            (
                pd.Series([datetime.date(2024, 1, 1)], dtype=pd.ArrowDtype(pa.date64())),
                pa.date64(),
                [datetime.date(2024, 1, 1)],
            ),
            (
                pd.Series([datetime.time(12, 30)], dtype=pd.ArrowDtype(pa.time64("us"))),
                pa.time64("us"),
                [datetime.time(12, 30)],
            ),
            (
                pd.Series([datetime.time(12, 30)], dtype=pd.ArrowDtype(pa.time32("ms"))),
                pa.time32("ms"),
                [datetime.time(12, 30)],
            ),
            (
                pd.Series([datetime.timedelta(days=1)], dtype=pd.ArrowDtype(pa.duration("us"))),
                pa.duration("us"),
                [datetime.timedelta(days=1)],
            ),
        ]
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
        # (data, target_type, expected_values)
        numeric_cases = [
            # Int types → float64
            (np.array([1, 2, 3], dtype=np.int8), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.int16), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.int32), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.int64), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.uint8), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.uint16), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.uint32), pa.float64(), [1.0, 2.0, 3.0]),
            (np.array([1, 2, 3], dtype=np.uint64), pa.float64(), [1.0, 2.0, 3.0]),
            # Float types → int64
            (np.array([1.0, 2.0, 3.0], dtype=np.float16), pa.int64(), [1, 2, 3]),
            (np.array([1.0, 2.0, 3.0], dtype=np.float32), pa.int64(), [1, 2, 3]),
            (np.array([1.0, 2.0, 3.0], dtype=np.float64), pa.int64(), [1, 2, 3]),
            # Float ↔ float
            (np.array([1.0, 2.0], dtype=np.float32), pa.float64(), [1.0, 2.0]),
            (np.array([1.0, 2.0], dtype=np.float64), pa.float32(), [1.0, 2.0]),
            # Widening
            (np.array([1, 2, 3], dtype=np.int8), pa.int64(), [1, 2, 3]),
            (np.array([1, 2, 3], dtype=np.int16), pa.int64(), [1, 2, 3]),
            (np.array([1, 2, 3], dtype=np.int32), pa.int64(), [1, 2, 3]),
            # Narrowing
            (np.array([1, 2, 3], dtype=np.int64), pa.int8(), [1, 2, 3]),
            (np.array([1, 2, 3], dtype=np.int64), pa.int16(), [1, 2, 3]),
            (np.array([1, 2, 3], dtype=np.int64), pa.int32(), [1, 2, 3]),
            # Bool
            (np.array([True, False, True], dtype=np.bool_), pa.bool_(), [True, False, True]),
            # Empty
            (np.array([], dtype=np.int64), pa.float64(), []),
            (np.array([], dtype=np.float64), pa.int64(), []),
            (np.array([], dtype=np.bool_), pa.bool_(), []),
        ]
        self._run_coercion_tests_with_values(numeric_cases)

        # numpy int64 → decimal128 does NOT work
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.decimal128(10, 0))

        # ==== 4.2 Boundary Values ====
        # (data, target_type, expected_values)
        boundary_cases = [
            # Int min/max → same type
            (np.array([int8_min, int8_max], dtype=np.int8), pa.int8(), [int8_min, int8_max]),
            (np.array([int16_min, int16_max], dtype=np.int16), pa.int16(), [int16_min, int16_max]),
            (np.array([int32_min, int32_max], dtype=np.int32), pa.int32(), [int32_min, int32_max]),
            (np.array([int64_min, int64_max], dtype=np.int64), pa.int64(), [int64_min, int64_max]),
            # Int min/max → float64 (exact for smaller types)
            (
                np.array([int8_min, int8_max], dtype=np.int8),
                pa.float64(),
                [float(int8_min), float(int8_max)],
            ),
            (
                np.array([int16_min, int16_max], dtype=np.int16),
                pa.float64(),
                [float(int16_min), float(int16_max)],
            ),
            (
                np.array([int32_min, int32_max], dtype=np.int32),
                pa.float64(),
                [float(int32_min), float(int32_max)],
            ),
            # Widening with boundary
            (np.array([int8_min, 0, int8_max], dtype=np.int8), pa.int64(), [int8_min, 0, int8_max]),
            (np.array([int16_min, int16_max], dtype=np.int16), pa.int32(), [int16_min, int16_max]),
        ]
        self._run_coercion_tests_with_values(boundary_cases)

        # ==== 4.3 Special Float Values ====
        # NaN/Inf → float (type check only, NaN equality is tricky)
        for data, target in [
            (np.array([nan, 1.0, nan], dtype=np.float64), pa.float64()),
            (np.array([inf, neg_inf], dtype=np.float64), pa.float64()),
            (np.array([nan, 1.0], dtype=np.float64), pa.float32()),
            (np.array([inf, neg_inf], dtype=np.float64), pa.float32()),
        ]:
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

        # (data, target_type, expected_values)
        datetime_cases = [
            # Date arrays
            (arr_d, pa.date32(), expected_dates),
            (arr_d, pa.date64(), expected_dates),
            # Timestamp arrays - various source resolutions
            (arr_ns, pa.timestamp("s"), expected_ts),
            (arr_ns, pa.timestamp("ms"), expected_ts),
            (arr_ns, pa.timestamp("us"), expected_ts),
            (arr_ns, pa.timestamp("ns"), expected_ts),
            (arr_us, pa.timestamp("us"), expected_ts),
            (arr_ms, pa.timestamp("ms"), expected_ts),
            # Empty datetime
            (np.array([], dtype="datetime64[D]"), pa.date32(), []),
            (np.array([], dtype="datetime64[ns]"), pa.timestamp("us"), []),
        ]
        self._run_coercion_tests_with_values(datetime_cases)

        # ==== 4.5 Timedelta Arrays ====
        arr_td_ns = np.array(
            [86400000000000, 7200000000000], dtype="timedelta64[ns]"
        )  # 1 day, 2 hours
        arr_td_us = np.array([86400000000, 7200000000], dtype="timedelta64[us]")
        expected_td = [datetime.timedelta(days=1), datetime.timedelta(hours=2)]

        # (data, target_type, expected_values)
        timedelta_cases = [
            (arr_td_ns, pa.duration("us"), expected_td),
            (arr_td_ns, pa.duration("ns"), expected_td),
            (arr_td_us, pa.duration("us"), expected_td),
            (np.array([], dtype="timedelta64[ns]"), pa.duration("us"), []),
        ]
        self._run_coercion_tests_with_values(timedelta_cases)

    # =========================================================================
    # SECTION 5: Nested Types Coercion
    # =========================================================================

    def test_nested_types_coercion(self):
        """Test type coercion for nested types: list, struct, map."""
        import pyarrow as pa

        # ==== 5.1 List Types ====

        # (data, target_type, expected_values)
        list_cases = [
            # Element type coercion
            ([[1, 2], [3, 4]], pa.list_(pa.float32()), [[1.0, 2.0], [3.0, 4.0]]),
            ([[1, 2], [3, 4]], pa.list_(pa.float64()), [[1.0, 2.0], [3.0, 4.0]]),
            ([[1, 2], [3, 4]], pa.list_(pa.int8()), [[1, 2], [3, 4]]),
            ([[1.0, 2.0], [3.0]], pa.list_(pa.int64()), [[1, 2], [3]]),
            # Empty
            ([], pa.list_(pa.int64()), []),
            ([[]], pa.list_(pa.int64()), [[]]),
            ([[1, 2], []], pa.list_(pa.float64()), [[1.0, 2.0], []]),
            # None
            ([[1, None, 3]], pa.list_(pa.int64()), [[1, None, 3]]),
            ([None, [1, 2]], pa.list_(pa.int64()), [None, [1, 2]]),
        ]
        self._run_coercion_tests_with_values(list_cases)

        # ==== 5.2 Large List ====

        # (data, target_type, expected_values)
        large_list_cases = [
            ([[1, 2], [3, 4]], pa.large_list(pa.int64()), [[1, 2], [3, 4]]),
            ([[1, 2], [3, 4]], pa.large_list(pa.float64()), [[1.0, 2.0], [3.0, 4.0]]),
            ([["a", "b"], ["c"]], pa.large_list(pa.string()), [["a", "b"], ["c"]]),
            ([None, [1, 2]], pa.large_list(pa.int64()), [None, [1, 2]]),
        ]
        self._run_coercion_tests_with_values(large_list_cases)

        # ==== 5.3 Fixed Size List ====

        # (data, target_type, expected_values)
        fixed_list_cases = [
            ([[1, 2], [3, 4]], pa.list_(pa.int64(), 2), [[1, 2], [3, 4]]),
            ([[1, 2], [3, 4]], pa.list_(pa.float64(), 2), [[1.0, 2.0], [3.0, 4.0]]),
            ([["a", "b"]], pa.list_(pa.string(), 2), [["a", "b"]]),
            ([None, [1, 2]], pa.list_(pa.int64(), 2), [None, [1, 2]]),
        ]
        self._run_coercion_tests_with_values(fixed_list_cases)

        # ==== 5.4 List with Temporal Elements ====

        # (data, target_type, expected_values)
        list_temporal_cases = [
            ([[datetime.date(2024, 1, 1)]], pa.list_(pa.date32()), [[datetime.date(2024, 1, 1)]]),
            (
                [[datetime.datetime(2024, 1, 1, 12, 0)]],
                pa.list_(pa.timestamp("us")),
                [[datetime.datetime(2024, 1, 1, 12, 0)]],
            ),
            ([[datetime.time(12, 30)]], pa.list_(pa.time64("us")), [[datetime.time(12, 30)]]),
        ]
        self._run_coercion_tests_with_values(list_temporal_cases)

        # ==== 5.5 Struct Types ====

        # (data, target_type, expected_values)
        struct_cases = [
            # Field type coercion (int → float)
            (
                [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
                pa.struct([("x", pa.float64()), ("y", pa.float64())]),
                [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}],
            ),
            # Struct with None
            (
                [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
                [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}],
            ),
            # Empty struct
            ([], pa.struct([("x", pa.int64()), ("y", pa.string())]), []),
        ]
        self._run_coercion_tests_with_values(struct_cases)

        # (data, target_type, expected_values)
        struct_type_only = [
            # Mixed field types
            (
                [{"id": 1, "name": "Alice", "date": datetime.date(2024, 1, 1)}],
                pa.struct([("id", pa.int64()), ("name", pa.string()), ("date", pa.date32())]),
                [{"id": 1, "name": "Alice", "date": datetime.date(2024, 1, 1)}],
            ),
            # Struct with large_string
            (
                [{"id": 1, "name": "hi"}],
                pa.struct([("id", pa.int64()), ("name", pa.large_string())]),
                [{"id": 1, "name": "hi"}],
            ),
            # Struct with binary
            (
                [{"id": 1, "data": b"hi"}],
                pa.struct([("id", pa.int64()), ("data", pa.binary())]),
                [{"id": 1, "data": b"hi"}],
            ),
        ]
        self._run_coercion_tests_with_values(struct_type_only)

        # ==== 5.6 Map Types ====

        # (data, target_type, expected_values)
        # Note: map to_pylist() returns list of list of tuples
        map_cases = [
            # Value coercion
            (
                [[("a", 1), ("b", 2)]],
                pa.map_(pa.string(), pa.float64()),
                [[("a", 1.0), ("b", 2.0)]],
            ),
            ([[("a", 1), ("b", 2)]], pa.map_(pa.string(), pa.int8()), [[("a", 1), ("b", 2)]]),
            (
                [[("a", "x"), ("b", "y")]],
                pa.map_(pa.string(), pa.large_string()),
                [[("a", "x"), ("b", "y")]],
            ),
            # Key types
            ([[(1, "a"), (2, "b")]], pa.map_(pa.int64(), pa.string()), [[(1, "a"), (2, "b")]]),
            ([[(1, "a"), (2, "b")]], pa.map_(pa.int32(), pa.string()), [[(1, "a"), (2, "b")]]),
            ([[(1, 10), (2, 20)]], pa.map_(pa.int64(), pa.float64()), [[(1, 10.0), (2, 20.0)]]),
            # Empty
            ([], pa.map_(pa.string(), pa.int64()), []),
            ([[]], pa.map_(pa.string(), pa.int64()), [[]]),
            # None
            ([None, [("a", 1)]], pa.map_(pa.string(), pa.int64()), [None, [("a", 1)]]),
            (
                [[("a", 1), ("b", None)]],
                pa.map_(pa.string(), pa.int64()),
                [[("a", 1), ("b", None)]],
            ),
        ]
        self._run_coercion_tests_with_values(map_cases)

        # ==== 5.7 Deeply Nested Types ====

        # (data, target_type, expected_values)
        deeply_nested_cases = [
            # List of lists
            (
                [[[1, 2], [3]], [[4, 5, 6]]],
                pa.list_(pa.list_(pa.float64())),
                [[[1.0, 2.0], [3.0]], [[4.0, 5.0, 6.0]]],
            ),
            # List of structs
            (
                [[{"x": 1}, {"x": 2}]],
                pa.list_(pa.struct([("x", pa.float64())])),
                [[{"x": 1.0}, {"x": 2.0}]],
            ),
            # Struct containing list
            (
                [{"id": 1, "values": [1, 2, 3]}],
                pa.struct([("id", pa.int64()), ("values", pa.list_(pa.float64()))]),
                [{"id": 1, "values": [1.0, 2.0, 3.0]}],
            ),
            # Triple nested
            ([[[[1, 2]], [[3]]]], pa.list_(pa.list_(pa.list_(pa.int64()))), [[[[1, 2]], [[3]]]]),
        ]
        self._run_coercion_tests_with_values(deeply_nested_cases)

        # (data, target_type, expected_values)
        deeply_nested_type_only = [
            # Struct containing map
            (
                [{"id": 1, "meta": [("a", 1)]}],
                pa.struct([("id", pa.int64()), ("meta", pa.map_(pa.string(), pa.int64()))]),
                [{"id": 1, "meta": [("a", 1)]}],
            ),
            # Map with list values
            (
                [[("a", [1, 2]), ("b", [3])]],
                pa.map_(pa.string(), pa.list_(pa.int64())),
                [[("a", [1, 2]), ("b", [3])]],
            ),
            # Map with struct values
            (
                [[("a", {"x": 1}), ("b", {"x": 2})]],
                pa.map_(pa.string(), pa.struct([("x", pa.int64())])),
                [[("a", {"x": 1}), ("b", {"x": 2})]],
            ),
            # List of maps
            (
                [[[("a", 1)], [("b", 2)]]],
                pa.list_(pa.map_(pa.string(), pa.int64())),
                [[[("a", 1)], [("b", 2)]]],
            ),
            # Large nested types
            (
                [[{"x": 1}, {"x": 2}]],
                pa.large_list(pa.struct([("x", pa.int64())])),
                [[{"x": 1}, {"x": 2}]],
            ),
        ]
        self._run_coercion_tests_with_values(deeply_nested_type_only)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
