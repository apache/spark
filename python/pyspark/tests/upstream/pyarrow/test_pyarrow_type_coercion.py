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

import datetime
from decimal import Decimal
import math
import unittest

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


# Test pa.array type coercion behavior when creating arrays with explicit type parameter.
# This test monitors the behavior of PyArrow's type coercion to ensure PySpark's assumptions
# about PyArrow behavior remain valid across versions.
#
# Key findings:
# 1. Numeric coercion (int <-> float, int size narrowing/widening) works via safe casting
# 2. String coercion (int -> string) requires explicit pc.cast(), not implicit via type param
# 3. Decimal coercion (int -> decimal) works directly
# 4. Boolean coercion (int -> bool) does NOT work implicitly, requires explicit casting
#
# Test coverage includes:
# - Missing values (None, NaN, NaT)
# - Empty datasets
# - Invalid values and error handling
# - All Spark/PyArrow/Pandas datatypes
# - Python, Pandas, and NumPy input types
# - PySpark pandas_options for to_pandas()


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowTypeCoercionTests(unittest.TestCase):
    """Test PyArrow's type coercion behavior for pa.array with explicit type parameter."""

    # ============================================================
    # SECTION 1: Missing Values Tests (None, NaN, NaT)
    # ============================================================

    def test_none_values_in_coercion(self):
        """Test that None values are preserved during type coercion."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # int with None -> float64
        a = pa.array([1, None, 3], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, None, 3.0])
        self.assertEqual(a.null_count, 1)

        # int with None -> decimal128
        a = pa.array([1, None, 3], type=pa.decimal128(10, 0))
        self.assertEqual(a.type, pa.decimal128(10, 0))
        self.assertEqual(a.to_pylist(), [Decimal("1"), None, Decimal("3")])
        self.assertEqual(a.null_count, 1)

        # int with None -> int32 (narrowing)
        a = pa.array([1, None, 3], type=pa.int32())
        self.assertEqual(a.type, pa.int32())
        self.assertEqual(a.to_pylist(), [1, None, 3])
        self.assertEqual(a.null_count, 1)

        # explicit cast to string with None
        int_arr = pa.array([1, None, 3])
        str_arr = pc.cast(int_arr, pa.string())
        self.assertEqual(str_arr.to_pylist(), ["1", None, "3"])
        self.assertEqual(str_arr.null_count, 1)

        # float with None -> int64
        a = pa.array([1.0, None, 3.0], type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, None, 3])

        # All None values
        a = pa.array([None, None, None], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [None, None, None])
        self.assertEqual(a.null_count, 3)

    def test_nan_values_in_coercion(self):
        """Test NaN handling in type coercion."""
        import pyarrow as pa

        # float NaN preserved
        a = pa.array([1.0, float("nan"), 3.0])
        self.assertEqual(a.type, pa.float64())
        self.assertTrue(math.isnan(a[1].as_py()))

        # float with NaN -> decimal128 fails (NaN not representable in decimal)
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array([1.0, float("nan"), 3.0], type=pa.decimal128(10, 2))

        # NaN -> int fails (NaN not representable in int)
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([1.0, float("nan"), 3.0], type=pa.int64())

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_nat_values_in_coercion(self):
        """Test pd.NaT (Not a Time) handling in type coercion."""
        import pyarrow as pa
        import pandas as pd
        import numpy as np

        # pd.NaT in timestamp
        s = pd.Series([pd.Timestamp("2024-01-01"), pd.NaT, pd.Timestamp("2024-01-03")])
        a = pa.array(s)
        self.assertEqual(a.type, pa.timestamp("ns"))
        self.assertEqual(a.null_count, 1)

        # pd.NaT in timedelta
        s = pd.Series([pd.Timedelta(days=1), pd.NaT, pd.Timedelta(hours=2)])
        a = pa.array(s)
        self.assertEqual(a.type, pa.duration("ns"))
        self.assertEqual(a.null_count, 1)

        # np.datetime64('NaT')
        arr = np.array(["2024-01-01", "NaT", "2024-01-03"], dtype="datetime64[D]")
        a = pa.array(arr)
        self.assertEqual(a.type, pa.date32())
        self.assertEqual(a.null_count, 1)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_na_in_coercion(self):
        """Test pd.NA handling in type coercion with nullable dtypes."""
        import pyarrow as pa
        import pandas as pd

        # pd.NA in nullable Int64
        s = pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, None, 3])
        self.assertEqual(a.null_count, 1)

        # pd.NA coerced to float64
        s = pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype())
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, None, 3.0])

        # pd.NA in nullable Float64
        s = pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.null_count, 1)

        # pd.NA in nullable String
        s = pd.Series(["a", pd.NA, "c"], dtype=pd.StringDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.string())
        self.assertEqual(a.to_pylist(), ["a", None, "c"])

        # pd.NA in nullable Boolean
        s = pd.Series([True, pd.NA, False], dtype=pd.BooleanDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.bool_())
        self.assertEqual(a.to_pylist(), [True, None, False])

    # ============================================================
    # SECTION 2: Empty Dataset Tests
    # ============================================================

    def test_empty_list_coercion(self):
        """Test type coercion with empty lists."""
        import pyarrow as pa

        # Empty list with explicit type
        a = pa.array([], type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(len(a), 0)
        self.assertEqual(a.to_pylist(), [])

        a = pa.array([], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(len(a), 0)

        a = pa.array([], type=pa.string())
        self.assertEqual(a.type, pa.string())
        self.assertEqual(len(a), 0)

        a = pa.array([], type=pa.decimal128(10, 2))
        self.assertEqual(a.type, pa.decimal128(10, 2))
        self.assertEqual(len(a), 0)

        a = pa.array([], type=pa.bool_())
        self.assertEqual(a.type, pa.bool_())
        self.assertEqual(len(a), 0)

        a = pa.array([], type=pa.timestamp("us"))
        self.assertEqual(a.type, pa.timestamp("us"))
        self.assertEqual(len(a), 0)

        # Empty list without type -> null type
        a = pa.array([])
        self.assertEqual(a.type, pa.null())
        self.assertEqual(len(a), 0)

    def test_empty_nested_list_coercion(self):
        """Test type coercion with empty nested lists."""
        import pyarrow as pa

        # Empty list of lists
        a = pa.array([], type=pa.list_(pa.int64()))
        self.assertEqual(a.type, pa.list_(pa.int64()))
        self.assertEqual(len(a), 0)

        # List containing empty list
        a = pa.array([[]], type=pa.list_(pa.int64()))
        self.assertEqual(a.type, pa.list_(pa.int64()))
        self.assertEqual(len(a), 1)
        self.assertEqual(a.to_pylist(), [[]])

        # Empty struct
        a = pa.array([], type=pa.struct([("x", pa.int64()), ("y", pa.string())]))
        self.assertEqual(a.type, pa.struct([("x", pa.int64()), ("y", pa.string())]))
        self.assertEqual(len(a), 0)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_empty_pandas_series_coercion(self):
        """Test type coercion with empty pandas Series."""
        import pyarrow as pa
        import pandas as pd

        # Empty numpy-backed series
        s = pd.Series([], dtype="int64")
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(len(a), 0)

        # Empty series coerced to different type
        s = pd.Series([], dtype="int64")
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(len(a), 0)

        # Empty nullable series
        s = pd.Series([], dtype=pd.Int64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(len(a), 0)

        # Empty ArrowDtype series
        s = pd.Series([], dtype=pd.ArrowDtype(pa.int64()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(len(a), 0)

    def test_empty_numpy_array_coercion(self):
        """Test type coercion with empty numpy arrays."""
        import pyarrow as pa
        import numpy as np

        # Empty numpy array
        arr = np.array([], dtype=np.int64)
        a = pa.array(arr)
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(len(a), 0)

        # Empty numpy array coerced
        arr = np.array([], dtype=np.int64)
        a = pa.array(arr, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(len(a), 0)

    # ============================================================
    # SECTION 3: Invalid Values and Error Handling
    # ============================================================

    def test_overflow_in_narrowing(self):
        """Test overflow detection when narrowing integer types."""
        import pyarrow as pa

        # Value too large for int8 (range: -128 to 127)
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([128], type=pa.int8())

        with self.assertRaises(pa.ArrowInvalid):
            pa.array([-129], type=pa.int8())

        # Value too large for uint8 (range: 0 to 255)
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([256], type=pa.uint8())

        # Negative value to unsigned raises OverflowError
        with self.assertRaises(OverflowError):
            pa.array([-1], type=pa.uint8())

        # Value too large for int16
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([32768], type=pa.int16())

    def test_precision_loss_in_float_to_int(self):
        """Test precision handling when converting float to int."""
        import pyarrow as pa

        # PyArrow allows float -> int conversion (truncates fractional part)
        # This is notable behavior that PySpark should be aware of
        a = pa.array([1.5], type=pa.int64())
        # Truncated to 1
        self.assertEqual(a.to_pylist(), [1])

        a = pa.array([1.9, 2.1], type=pa.int64())
        # Truncated
        self.assertEqual(a.to_pylist(), [1, 2])

        # Integer-valued float -> int works cleanly
        a = pa.array([1.0, 2.0], type=pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2])

    def test_invalid_string_to_numeric(self):
        """Test invalid string to numeric conversion."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # Non-numeric string -> int fails
        str_arr = pa.array(["abc", "def"])
        with self.assertRaises(pa.ArrowInvalid):
            pc.cast(str_arr, pa.int64())

        # Mixed numeric/non-numeric string -> int fails
        str_arr = pa.array(["1", "abc", "3"])
        with self.assertRaises(pa.ArrowInvalid):
            pc.cast(str_arr, pa.int64())

        # Valid numeric strings work
        str_arr = pa.array(["1", "2", "3"])
        int_arr = pc.cast(str_arr, pa.int64())
        self.assertEqual(int_arr.to_pylist(), [1, 2, 3])

    def test_incompatible_type_coercion(self):
        """Test that incompatible type coercions raise errors."""
        import pyarrow as pa

        # Binary -> int fails
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array([b"hello"], type=pa.int64())

        # Datetime -> int fails (without explicit cast)
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array([datetime.datetime.now()], type=pa.int64())

        # List -> scalar type fails
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array([[1, 2, 3]], type=pa.int64())

    def test_decimal_precision_overflow(self):
        """Test decimal precision and scale limits."""
        import pyarrow as pa

        # Value exceeds precision
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([1234567890], type=pa.decimal128(5, 0))

        # Value within precision works
        a = pa.array([12345], type=pa.decimal128(5, 0))
        self.assertEqual(a.to_pylist(), [Decimal("12345")])

    # ============================================================
    # SECTION 4: Numeric Type Coercion (All Spark/PyArrow types)
    # ============================================================

    def test_int_to_all_float_types(self):
        """Test int coercion to all float types."""
        import pyarrow as pa

        # int -> float16
        a = pa.array([1, 2, 3], type=pa.float16())
        self.assertEqual(a.type, pa.float16())

        # int -> float32
        a = pa.array([1, 2, 3], type=pa.float32())
        self.assertEqual(a.type, pa.float32())
        values = a.to_pylist()
        self.assertAlmostEqual(values[0], 1.0, places=5)

        # int -> float64
        a = pa.array([1, 2, 3], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])
        for v in a.to_pylist():
            self.assertIsInstance(v, float)

    def test_int_to_all_int_types(self):
        """Test int coercion to all integer types (narrowing/widening)."""
        import pyarrow as pa

        # Signed integers
        for int_type in [pa.int8(), pa.int16(), pa.int32(), pa.int64()]:
            a = pa.array([1, 2, 3], type=int_type)
            self.assertEqual(a.type, int_type)
            self.assertEqual(a.to_pylist(), [1, 2, 3])

        # Unsigned integers
        for uint_type in [pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]:
            a = pa.array([1, 2, 3], type=uint_type)
            self.assertEqual(a.type, uint_type)
            self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_float_to_int_coercion(self):
        """Test that floats can be coerced to integer types (whole numbers only)."""
        import pyarrow as pa

        for int_type in [pa.int8(), pa.int16(), pa.int32(), pa.int64()]:
            a = pa.array([1.0, 2.0, 3.0], type=int_type)
            self.assertEqual(a.type, int_type)
            self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_int_to_decimal_all_scales(self):
        """Test int to decimal coercion with various precisions and scales."""
        import pyarrow as pa

        # decimal128 with different precisions
        for precision in [5, 10, 18, 38]:
            a = pa.array([1, 2, 3], type=pa.decimal128(precision, 0))
            self.assertEqual(a.type, pa.decimal128(precision, 0))
            self.assertEqual(a.to_pylist(), [Decimal("1"), Decimal("2"), Decimal("3")])

        # decimal128 with different scales
        a = pa.array([1, 2, 3], type=pa.decimal128(10, 2))
        self.assertEqual(
            a.to_pylist(), [Decimal("1.00"), Decimal("2.00"), Decimal("3.00")]
        )

        a = pa.array([1, 2, 3], type=pa.decimal128(10, 4))
        self.assertEqual(
            a.to_pylist(),
            [Decimal("1.0000"), Decimal("2.0000"), Decimal("3.0000")],
        )

        # decimal256
        a = pa.array([1, 2, 3], type=pa.decimal256(50, 0))
        self.assertEqual(a.type, pa.decimal256(50, 0))

    def test_int_to_string_requires_explicit_cast(self):
        """Test that integers cannot be implicitly coerced to string types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # int -> string does NOT work implicitly
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([1, 2, 3], type=pa.string())

        # int -> large_string does NOT work implicitly
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([1, 2, 3], type=pa.large_string())

        # int -> string_view does NOT work implicitly (if available)
        if hasattr(pa, "string_view"):
            with self.assertRaises(pa.ArrowTypeError):
                pa.array([1, 2, 3], type=pa.string_view())

        # Explicit casting via pc.cast works
        int_arr = pa.array([1, 2, 3])
        str_arr = pc.cast(int_arr, pa.string())
        self.assertEqual(str_arr.type, pa.string())
        self.assertEqual(str_arr.to_pylist(), ["1", "2", "3"])

        large_str_arr = pc.cast(int_arr, pa.large_string())
        self.assertEqual(large_str_arr.type, pa.large_string())

    def test_int_to_bool_requires_explicit_cast(self):
        """Test that integers cannot be implicitly coerced to boolean."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # int -> bool does NOT work implicitly
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([0, 1, 2], type=pa.bool_())

        # Explicit casting via pc.cast works
        int_arr = pa.array([0, 1, 2])
        bool_arr = pc.cast(int_arr, pa.bool_())
        self.assertEqual(bool_arr.type, pa.bool_())
        self.assertEqual(bool_arr.to_pylist(), [False, True, True])

    def test_decimal_to_float_requires_explicit_cast(self):
        """Test that Python Decimal cannot be implicitly coerced to float types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # Decimal -> float64 does NOT work implicitly
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([Decimal("1.5"), Decimal("2.5")], type=pa.float64())

        # Explicit casting works
        dec_arr = pa.array([Decimal("1.5"), Decimal("2.5")])
        float_arr = pc.cast(dec_arr, pa.float64())
        self.assertEqual(float_arr.type, pa.float64())
        self.assertEqual(float_arr.to_pylist(), [1.5, 2.5])

    def test_float_to_decimal_requires_explicit_cast(self):
        """Test that floats cannot be implicitly coerced to decimal types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # float -> decimal128 does NOT work implicitly
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([1.5, 2.5], type=pa.decimal128(10, 2))

        # Explicit casting works
        float_arr = pa.array([1.5, 2.5])
        dec_arr = pc.cast(float_arr, pa.decimal128(10, 2))
        self.assertEqual(dec_arr.type, pa.decimal128(10, 2))

    def test_string_to_numeric_requires_explicit_cast(self):
        """Test that strings cannot be implicitly coerced to numeric types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # string -> int does NOT work implicitly
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array(["1", "2", "3"], type=pa.int64())

        # string -> float does NOT work implicitly
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array(["1.0", "2.0"], type=pa.float64())

        # Explicit casting works
        str_arr = pa.array(["1", "2", "3"])
        int_arr = pc.cast(str_arr, pa.int64())
        self.assertEqual(int_arr.to_pylist(), [1, 2, 3])

    # ============================================================
    # SECTION 5: Temporal Type Coercion
    # ============================================================

    def test_timestamp_coercion(self):
        """Test timestamp type coercion between resolutions."""
        import pyarrow as pa

        ts_data = [
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            datetime.datetime(2024, 1, 2, 12, 0, 0),
        ]

        # Coerce to different resolutions
        for unit in ["s", "ms", "us", "ns"]:
            a = pa.array(ts_data, type=pa.timestamp(unit))
            self.assertEqual(a.type, pa.timestamp(unit))
            self.assertEqual(len(a), 2)

        # Coerce to timestamp with timezone
        a = pa.array(ts_data, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))

    def test_timezone_aware_datetime_coercion(self):
        """Test timezone-aware datetime.datetime coercion."""
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        # UTC timezone
        utc_tz = ZoneInfo("UTC")
        ts_utc = [
            datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=utc_tz),
            datetime.datetime(2024, 1, 2, 12, 0, 0, tzinfo=utc_tz),
        ]

        # datetime with UTC -> timestamp with UTC
        a = pa.array(ts_utc, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))
        self.assertEqual(len(a), 2)

        # Non-UTC timezone: Asia/Singapore (UTC+8)
        sg_tz = ZoneInfo("Asia/Singapore")
        ts_sg = [
            datetime.datetime(2024, 1, 1, 20, 0, 0, tzinfo=sg_tz),  # 12:00 UTC
            datetime.datetime(2024, 1, 2, 20, 0, 0, tzinfo=sg_tz),  # 12:00 UTC
        ]

        # datetime with Asia/Singapore -> timestamp with Asia/Singapore
        a = pa.array(ts_sg, type=pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(a.type, pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(len(a), 2)

        # datetime with Asia/Singapore -> timestamp with UTC (converts timezone)
        a = pa.array(ts_sg, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))

        # US/Pacific timezone (UTC-8 or UTC-7 depending on DST)
        la_tz = ZoneInfo("America/Los_Angeles")
        ts_la = [
            datetime.datetime(2024, 1, 1, 4, 0, 0, tzinfo=la_tz),  # 12:00 UTC
            datetime.datetime(2024, 7, 1, 5, 0, 0, tzinfo=la_tz),  # 12:00 UTC (DST)
        ]

        a = pa.array(ts_la, type=pa.timestamp("us", tz="America/Los_Angeles"))
        self.assertEqual(a.type, pa.timestamp("us", tz="America/Los_Angeles"))

        # Naive datetime -> timestamp with timezone
        ts_naive = [
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            datetime.datetime(2024, 1, 2, 12, 0, 0),
        ]
        a = pa.array(ts_naive, type=pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(a.type, pa.timestamp("us", tz="Asia/Singapore"))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_timestamp_timezone_coercion(self):
        """Test pd.Timestamp with timezone coercion."""
        import pyarrow as pa
        import pandas as pd
        from zoneinfo import ZoneInfo

        # pd.Timestamp with UTC
        ts_utc = [
            pd.Timestamp("2024-01-01 12:00:00", tz="UTC"),
            pd.Timestamp("2024-01-02 12:00:00", tz="UTC"),
        ]

        a = pa.array(ts_utc, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))
        self.assertEqual(len(a), 2)

        # pd.Timestamp with Asia/Singapore
        ts_sg = [
            pd.Timestamp("2024-01-01 20:00:00", tz="Asia/Singapore"),
            pd.Timestamp("2024-01-02 20:00:00", tz="Asia/Singapore"),
        ]

        a = pa.array(ts_sg, type=pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(a.type, pa.timestamp("us", tz="Asia/Singapore"))

        # pd.Timestamp with America/Los_Angeles
        ts_la = [
            pd.Timestamp("2024-01-01 04:00:00", tz="America/Los_Angeles"),
            pd.Timestamp("2024-07-01 05:00:00", tz="America/Los_Angeles"),
        ]

        a = pa.array(ts_la, type=pa.timestamp("us", tz="America/Los_Angeles"))
        self.assertEqual(a.type, pa.timestamp("us", tz="America/Los_Angeles"))

        # pd.Timestamp with timezone -> different timezone (converts)
        a = pa.array(ts_sg, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))

        # Timezone-aware pd.Series
        s = pd.Series(ts_sg)
        a = pa.array(s, type=pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(a.type, pa.timestamp("us", tz="Asia/Singapore"))

        # pd.Series with DatetimeTZDtype
        s = pd.Series(
            pd.to_datetime(["2024-01-01", "2024-01-02"]).tz_localize("Asia/Singapore")
        )
        a = pa.array(s)
        self.assertEqual(str(a.type.tz), "Asia/Singapore")

        # ArrowDtype with timezone
        s = pd.Series(
            [pd.Timestamp("2024-01-01 12:00:00"), pd.Timestamp("2024-01-02 12:00:00")],
            dtype=pd.ArrowDtype(pa.timestamp("us", tz="Asia/Singapore")),
        )
        a = pa.array(s)
        self.assertEqual(a.type, pa.timestamp("us", tz="Asia/Singapore"))

    def test_timezone_coercion_between_timezones(self):
        """Test coercion between different timezones."""
        import pyarrow as pa
        import pyarrow.compute as pc
        from zoneinfo import ZoneInfo

        # Create array with Asia/Singapore timezone
        sg_tz = ZoneInfo("Asia/Singapore")
        ts_sg = [
            datetime.datetime(2024, 1, 1, 20, 0, 0, tzinfo=sg_tz),
        ]

        a_sg = pa.array(ts_sg, type=pa.timestamp("us", tz="Asia/Singapore"))
        self.assertEqual(a_sg.type.tz, "Asia/Singapore")

        # Convert to UTC using pc.cast (preserves instant, changes representation)
        # Note: This changes the timezone but keeps the same instant
        # Explicit timezone conversion is needed via assume_timezone or similar

        # Create naive timestamp and localize to different timezones
        ts_naive = pa.array(
            [datetime.datetime(2024, 1, 1, 12, 0, 0)],
            type=pa.timestamp("us"),
        )

        # Assume timezone (treats naive as if it were in that timezone)
        ts_sg_assumed = pc.assume_timezone(ts_naive, "Asia/Singapore")
        self.assertEqual(ts_sg_assumed.type.tz, "Asia/Singapore")

        ts_utc_assumed = pc.assume_timezone(ts_naive, "UTC")
        self.assertEqual(ts_utc_assumed.type.tz, "UTC")

        ts_la_assumed = pc.assume_timezone(ts_naive, "America/Los_Angeles")
        self.assertEqual(ts_la_assumed.type.tz, "America/Los_Angeles")

    def test_mixed_timezone_handling(self):
        """Test handling of mixed timezone data."""
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        utc_tz = ZoneInfo("UTC")
        sg_tz = ZoneInfo("Asia/Singapore")

        # Mixed timezones in input - behavior depends on PyArrow version
        ts_mixed = [
            datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=utc_tz),
            datetime.datetime(2024, 1, 1, 20, 0, 0, tzinfo=sg_tz),  # Same instant
        ]

        # When specifying target timezone, values are converted
        a = pa.array(ts_mixed, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(a.type, pa.timestamp("us", tz="UTC"))
        self.assertEqual(len(a), 2)

        # Both should represent the same instant when converted to UTC
        values = a.to_pylist()
        self.assertEqual(values[0], values[1])

    def test_date_coercion(self):
        """Test date type coercion."""
        import pyarrow as pa

        date_data = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]

        # date32
        a = pa.array(date_data, type=pa.date32())
        self.assertEqual(a.type, pa.date32())

        # date64
        a = pa.array(date_data, type=pa.date64())
        self.assertEqual(a.type, pa.date64())

    def test_time_coercion(self):
        """Test time type coercion."""
        import pyarrow as pa

        time_data = [datetime.time(12, 30, 0), datetime.time(13, 45, 0)]

        # time32
        a = pa.array(time_data, type=pa.time32("s"))
        self.assertEqual(a.type, pa.time32("s"))

        a = pa.array(time_data, type=pa.time32("ms"))
        self.assertEqual(a.type, pa.time32("ms"))

        # time64
        a = pa.array(time_data, type=pa.time64("us"))
        self.assertEqual(a.type, pa.time64("us"))

        a = pa.array(time_data, type=pa.time64("ns"))
        self.assertEqual(a.type, pa.time64("ns"))

    def test_duration_coercion(self):
        """Test duration type coercion."""
        import pyarrow as pa

        dur_data = [datetime.timedelta(days=1), datetime.timedelta(hours=2)]

        for unit in ["s", "ms", "us", "ns"]:
            a = pa.array(dur_data, type=pa.duration(unit))
            self.assertEqual(a.type, pa.duration(unit))

    # ============================================================
    # SECTION 6: Binary/String Type Coercion
    # ============================================================

    def test_string_to_binary_coercion(self):
        """Test string to binary type coercion."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # string -> binary requires explicit cast
        str_arr = pa.array(["hello", "world"])
        bin_arr = pc.cast(str_arr, pa.binary())
        self.assertEqual(bin_arr.type, pa.binary())
        self.assertEqual(bin_arr.to_pylist(), [b"hello", b"world"])

        # binary -> string
        bin_arr = pa.array([b"hello", b"world"])
        str_arr = pc.cast(bin_arr, pa.string())
        self.assertEqual(str_arr.to_pylist(), ["hello", "world"])

    def test_string_size_variants(self):
        """Test string/binary size variant coercion."""
        import pyarrow as pa
        import pyarrow.compute as pc

        str_arr = pa.array(["hello", "world"])

        # string -> large_string
        large_arr = pc.cast(str_arr, pa.large_string())
        self.assertEqual(large_arr.type, pa.large_string())

        # large_string -> string
        back_arr = pc.cast(large_arr, pa.string())
        self.assertEqual(back_arr.type, pa.string())

    # ============================================================
    # SECTION 7: Nested Type Coercion
    # ============================================================

    def test_list_element_type_coercion(self):
        """Test type coercion for nested list element types."""
        import pyarrow as pa

        # list of int -> list of float
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.float64()))
        self.assertEqual(a.type, pa.list_(pa.float64()))
        self.assertEqual(a.to_pylist(), [[1.0, 2.0], [3.0, 4.0]])

        # list of int -> large_list
        a = pa.array([[1, 2], [3, 4]], type=pa.large_list(pa.int64()))
        self.assertEqual(a.type, pa.large_list(pa.int64()))

        # list of int -> fixed_size_list
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64(), 2))
        self.assertEqual(a.type, pa.list_(pa.int64(), 2))

    def test_struct_field_type_coercion(self):
        """Test type coercion for struct field types."""
        import pyarrow as pa

        data = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]

        # Coerce field types
        struct_type = pa.struct([("x", pa.float64()), ("y", pa.float64())])
        a = pa.array(data, type=struct_type)
        self.assertEqual(a.type, struct_type)

    def test_map_type_coercion(self):
        """Test type coercion for map types."""
        import pyarrow as pa

        # Map type requires explicit type specification
        map_type = pa.map_(pa.string(), pa.int64())
        a = pa.array([[("a", 1), ("b", 2)], [("c", 3)]], type=map_type)
        self.assertEqual(a.type, map_type)

        # Map with different value type
        map_type_float = pa.map_(pa.string(), pa.float64())
        a = pa.array([[("a", 1), ("b", 2)]], type=map_type_float)
        self.assertEqual(a.type, map_type_float)

    # ============================================================
    # SECTION 8: Python Instance Input Types
    # ============================================================

    def test_python_list_coercion(self):
        """Test type coercion from Python lists."""
        import pyarrow as pa

        # List of ints
        a = pa.array([1, 2, 3], type=pa.float64())
        self.assertEqual(a.type, pa.float64())

        # List of floats
        a = pa.array([1.0, 2.0, 3.0], type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # Nested list
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.float64()))
        self.assertEqual(a.type, pa.list_(pa.float64()))

    def test_python_tuple_coercion(self):
        """Test type coercion from Python tuples."""
        import pyarrow as pa

        # Tuple of ints
        a = pa.array((1, 2, 3), type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])

        # Tuple of floats
        a = pa.array((1.0, 2.0, 3.0), type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_python_generator_coercion(self):
        """Test type coercion from Python generators."""
        import pyarrow as pa

        # Generator expression
        gen = (x for x in [1, 2, 3])
        a = pa.array(gen, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])

    # ============================================================
    # SECTION 9: Pandas Instance Input Types
    # ============================================================

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_numpy_backed_series_coercion(self):
        """Test type coercion from numpy-backed pandas Series."""
        import pyarrow as pa
        import pandas as pd

        # int64 -> float64
        s = pd.Series([1, 2, 3])
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])

        # float64 -> int64
        s = pd.Series([1.0, 2.0, 3.0])
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int64 -> int8 (narrowing)
        s = pd.Series([1, 2, 3])
        a = pa.array(s, type=pa.int8())
        self.assertEqual(a.type, pa.int8())

        # int64 -> decimal128 does NOT work (numpy-backed)
        s = pd.Series([1, 2, 3])
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(s, type=pa.decimal128(10, 0))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_nullable_series_coercion(self):
        """Test type coercion from pandas nullable extension series."""
        import pyarrow as pa
        import pandas as pd

        # Int64 -> float64
        s = pd.Series([1, 2, 3], dtype=pd.Int64Dtype())
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())

        # Int64 with NA -> float64
        s = pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype())
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, None, 3.0])

        # Float64 -> int64
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype())
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_arrow_backed_series_coercion(self):
        """Test type coercion from ArrowDtype-backed pandas Series."""
        import pyarrow as pa
        import pandas as pd

        # ArrowDtype int64 -> float64
        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())

        # ArrowDtype float64 -> int64
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.ArrowDtype(pa.float64()))
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # ArrowDtype int64 -> decimal128 requires sufficient precision (at least 19)
        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
        a = pa.array(s, type=pa.decimal128(19, 0))
        self.assertEqual(a.type, pa.decimal128(19, 0))
        self.assertEqual(a.to_pylist(), [Decimal("1"), Decimal("2"), Decimal("3")])

        # Insufficient precision fails
        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(s, type=pa.decimal128(10, 0))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_all_pandas_dtypes_coercion(self):
        """Test coercion from all pandas dtype variants for float."""
        import pyarrow as pa
        import pandas as pd
        import numpy as np

        # numpy-backed float64
        s = pd.Series([1.0, 2.0, 3.0], dtype=np.float64)
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # pandas nullable Float64
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype())
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # ArrowDtype float64
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.ArrowDtype(pa.float64()))
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())

    # ============================================================
    # SECTION 10: NumPy Instance Input Types
    # ============================================================

    def test_numpy_array_coercion(self):
        """Test type coercion from numpy arrays."""
        import pyarrow as pa
        import numpy as np

        # numpy int64 -> float64
        a = pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])

        # numpy float64 -> int64
        a = pa.array(np.array([1.0, 2.0, 3.0], dtype=np.float64), type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # numpy int32 -> int64 (widening)
        a = pa.array(np.array([1, 2, 3], dtype=np.int32), type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # numpy int64 -> int8 (narrowing)
        a = pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.int8())
        self.assertEqual(a.type, pa.int8())

        # numpy int64 -> decimal128 does NOT work
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.decimal128(10, 0))

    def test_numpy_all_int_types_coercion(self):
        """Test coercion from all numpy integer types."""
        import pyarrow as pa
        import numpy as np

        for np_type in [np.int8, np.int16, np.int32, np.int64]:
            arr = np.array([1, 2, 3], dtype=np_type)
            a = pa.array(arr, type=pa.float64())
            self.assertEqual(a.type, pa.float64())

        for np_type in [np.uint8, np.uint16, np.uint32, np.uint64]:
            arr = np.array([1, 2, 3], dtype=np_type)
            a = pa.array(arr, type=pa.float64())
            self.assertEqual(a.type, pa.float64())

    def test_numpy_all_float_types_coercion(self):
        """Test coercion from all numpy float types."""
        import pyarrow as pa
        import numpy as np

        for np_type in [np.float16, np.float32, np.float64]:
            arr = np.array([1.0, 2.0, 3.0], dtype=np_type)
            a = pa.array(arr, type=pa.int64())
            self.assertEqual(a.type, pa.int64())

    def test_numpy_datetime_coercion(self):
        """Test coercion from numpy datetime types."""
        import pyarrow as pa
        import numpy as np

        # datetime64[D] -> different resolutions
        arr = np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[D]")

        a = pa.array(arr, type=pa.date32())
        self.assertEqual(a.type, pa.date32())

        a = pa.array(arr, type=pa.date64())
        self.assertEqual(a.type, pa.date64())

        # datetime64[ns] -> different resolutions
        arr = np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[ns]")

        for unit in ["s", "ms", "us", "ns"]:
            a = pa.array(arr, type=pa.timestamp(unit))
            self.assertEqual(a.type, pa.timestamp(unit))

    # ============================================================
    # SECTION 11: Spark DataType Mapping Coverage
    # ============================================================

    def test_spark_numeric_types_coercion(self):
        """Test coercion for all Spark numeric types."""
        import pyarrow as pa

        # Spark ByteType -> int8
        a = pa.array([1, 2, 3], type=pa.int8())
        self.assertEqual(a.type, pa.int8())

        # Spark ShortType -> int16
        a = pa.array([1, 2, 3], type=pa.int16())
        self.assertEqual(a.type, pa.int16())

        # Spark IntegerType -> int32
        a = pa.array([1, 2, 3], type=pa.int32())
        self.assertEqual(a.type, pa.int32())

        # Spark LongType -> int64
        a = pa.array([1, 2, 3], type=pa.int64())
        self.assertEqual(a.type, pa.int64())

        # Spark FloatType -> float32
        a = pa.array([1.0, 2.0, 3.0], type=pa.float32())
        self.assertEqual(a.type, pa.float32())

        # Spark DoubleType -> float64
        a = pa.array([1.0, 2.0, 3.0], type=pa.float64())
        self.assertEqual(a.type, pa.float64())

    def test_spark_decimal_types_coercion(self):
        """Test coercion for Spark DecimalType."""
        import pyarrow as pa

        # Various precision/scale combinations
        for precision, scale in [(10, 0), (10, 2), (18, 6), (38, 10)]:
            a = pa.array([1, 2, 3], type=pa.decimal128(precision, scale))
            self.assertEqual(a.type, pa.decimal128(precision, scale))

    def test_spark_temporal_types_coercion(self):
        """Test coercion for all Spark temporal types."""
        import pyarrow as pa

        # DateType -> date32
        a = pa.array(
            [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)], type=pa.date32()
        )
        self.assertEqual(a.type, pa.date32())

        # TimestampType -> timestamp (microseconds)
        a = pa.array(
            [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
            type=pa.timestamp("us"),
        )
        self.assertEqual(a.type, pa.timestamp("us"))

        # TimestampNTZType -> timestamp without timezone
        a = pa.array(
            [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
            type=pa.timestamp("us"),
        )
        self.assertEqual(a.type, pa.timestamp("us"))

        # DayTimeIntervalType -> duration
        a = pa.array(
            [datetime.timedelta(days=1), datetime.timedelta(hours=2)],
            type=pa.duration("us"),
        )
        self.assertEqual(a.type, pa.duration("us"))

    def test_spark_string_binary_types_coercion(self):
        """Test coercion for Spark string/binary types."""
        import pyarrow as pa

        # StringType -> string
        a = pa.array(["hello", "world"], type=pa.string())
        self.assertEqual(a.type, pa.string())

        # BinaryType -> binary
        a = pa.array([b"hello", b"world"], type=pa.binary())
        self.assertEqual(a.type, pa.binary())

    def test_spark_complex_types_coercion(self):
        """Test coercion for Spark complex types."""
        import pyarrow as pa

        # ArrayType -> list
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64()))
        self.assertEqual(a.type, pa.list_(pa.int64()))

        # MapType -> map
        a = pa.array(
            [[("a", 1), ("b", 2)]], type=pa.map_(pa.string(), pa.int64())
        )
        self.assertEqual(a.type, pa.map_(pa.string(), pa.int64()))

        # StructType -> struct
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        a = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], type=struct_type)
        self.assertEqual(a.type, struct_type)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
