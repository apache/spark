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
Tests for PyArrow pa.scalar type inference behavior.

This module tests how PyArrow infers types when creating scalars from various
Python objects. This helps ensure PySpark's assumptions about PyArrow behavior
remain valid across versions.

Key behaviors tested:
1. Type inference from Python primitives (int, float, str, bool, bytes)
2. Type inference from None values
3. Type inference from datetime/date/time/timedelta objects
4. Type inference from Decimal objects
5. Type inference from pandas Timestamp/Timedelta
6. Type inference from numpy scalars
7. Explicit type specification overrides inference
"""

import datetime
import math
import unittest
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.testing.utils import (
    have_numpy,
    have_pandas,
    have_pyarrow,
    numpy_requirement_message,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowScalarTypeInferenceTests(unittest.TestCase):
    """
    Test pa.scalar type inference from various Python types.

    These tests document PyArrow's type inference behavior for scalar values,
    which is important for PySpark's type conversion logic.
    """

    # ==================== Section 1: Python Primitives ====================

    def test_none_value(self):
        """Test that None creates a null scalar."""
        import pyarrow as pa

        scalar = pa.scalar(None)
        self.assertEqual(scalar.type, pa.null())
        self.assertFalse(scalar.is_valid)
        self.assertIsNone(scalar.as_py())

    def test_boolean_values(self):
        """Test boolean type inference."""
        import pyarrow as pa

        for value in [True, False]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.bool_())
                self.assertEqual(scalar.as_py(), value)

    def test_integer_values(self):
        """Test integer type inference - PyArrow infers int64 for Python ints."""
        import pyarrow as pa

        # Small integers still infer as int64
        for value in [0, 1, -1, 42, -42]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.int64())
                self.assertEqual(scalar.as_py(), value)

        # Large integers within int64 range
        scalar = pa.scalar(2**62)
        self.assertEqual(scalar.type, pa.int64())
        self.assertEqual(scalar.as_py(), 2**62)

        # Negative large integers
        scalar = pa.scalar(-(2**62))
        self.assertEqual(scalar.type, pa.int64())
        self.assertEqual(scalar.as_py(), -(2**62))

    def test_float_values(self):
        """Test float type inference - PyArrow infers float64 for Python floats."""
        import pyarrow as pa

        for value in [0.0, 1.5, -1.5, 3.14159, float("inf"), float("-inf")]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.float64())
                self.assertEqual(scalar.as_py(), value)

        # NaN requires special handling
        scalar = pa.scalar(float("nan"))
        self.assertEqual(scalar.type, pa.float64())
        self.assertTrue(math.isnan(scalar.as_py()))

    def test_string_values(self):
        """Test string type inference - PyArrow infers string (utf8) for Python str."""
        import pyarrow as pa

        for value in ["", "hello", "Hello, World!", "日本語", "emoji: 🎉"]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.string())
                self.assertEqual(scalar.as_py(), value)

    def test_bytes_values(self):
        """Test bytes type inference - PyArrow infers binary for Python bytes."""
        import pyarrow as pa

        for value in [b"", b"hello", b"\x00\x01\x02"]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.binary())
                self.assertEqual(scalar.as_py(), value)

    def test_bytearray_and_memoryview(self):
        """Test bytearray and memoryview type inference - both infer as binary."""
        import pyarrow as pa

        for value in [bytearray(b"hello"), memoryview(b"hello")]:
            with self.subTest(value_type=type(value).__name__):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.binary())
                self.assertEqual(scalar.as_py(), b"hello")

    # ==================== Section 2: Decimal Values ====================

    def test_decimal_values(self):
        """Test Decimal type inference - PyArrow infers decimal128 with appropriate precision/scale."""
        import pyarrow as pa

        # Simple decimal - verify precision and scale are inferred correctly
        scalar = pa.scalar(Decimal("123.45"))
        self.assertTrue(pa.types.is_decimal(scalar.type))
        self.assertEqual(scalar.type.scale, 2)
        self.assertEqual(scalar.type.precision, 5)  # 3 integer digits + 2 decimal digits
        # Use string comparison to validate exact scale representation
        self.assertEqual(str(scalar.as_py()), "123.45")

        # High precision decimal
        scalar = pa.scalar(Decimal("12345678901234567890.123456789"))
        self.assertTrue(pa.types.is_decimal(scalar.type))
        self.assertEqual(scalar.type.scale, 9)
        self.assertEqual(scalar.type.precision, 29)  # 20 integer digits + 9 decimal digits

        # Negative decimal
        scalar = pa.scalar(Decimal("-999.999"))
        self.assertTrue(pa.types.is_decimal(scalar.type))
        self.assertEqual(scalar.type.scale, 3)
        self.assertEqual(str(scalar.as_py()), "-999.999")

        # Zero with no decimal places
        scalar = pa.scalar(Decimal("0"))
        self.assertTrue(pa.types.is_decimal(scalar.type))
        self.assertEqual(scalar.type.scale, 0)

        # Zero with decimal places - scale should be preserved
        scalar = pa.scalar(Decimal("0.00"))
        self.assertTrue(pa.types.is_decimal(scalar.type))
        self.assertEqual(scalar.type.scale, 2)
        self.assertEqual(str(scalar.as_py()), "0.00")

    # ==================== Section 3: Date and Time Types ====================

    def test_date_values(self):
        """Test date type inference - PyArrow infers date32 for Python date."""
        import pyarrow as pa

        date_val = datetime.date(2024, 1, 15)
        scalar = pa.scalar(date_val)
        self.assertEqual(scalar.type, pa.date32())
        self.assertEqual(scalar.as_py(), date_val)

        # Edge cases
        for date_val in [
            datetime.date(1970, 1, 1),  # Unix epoch
            datetime.date(2000, 12, 31),  # Y2K
            datetime.date(1, 1, 1),  # Minimum date
            datetime.date(9999, 12, 31),  # Maximum date
        ]:
            with self.subTest(date_val=date_val):
                scalar = pa.scalar(date_val)
                self.assertEqual(scalar.type, pa.date32())
                self.assertEqual(scalar.as_py(), date_val)

    def test_time_values(self):
        """Test time type inference - PyArrow infers time64[us] for Python time."""
        import pyarrow as pa

        for time_val in [
            datetime.time(12, 30, 45, 123456),  # With microseconds
            datetime.time(12, 30, 45),  # Without microseconds
            datetime.time(0, 0, 0),  # Midnight
        ]:
            with self.subTest(time_val=time_val):
                scalar = pa.scalar(time_val)
                self.assertEqual(scalar.type, pa.time64("us"))
                self.assertEqual(scalar.as_py(), time_val)

    def test_datetime_values(self):
        """Test datetime type inference - timezone-naive infers timestamp[us], timezone-aware includes tz."""
        import pyarrow as pa

        # Timezone-naive datetime -> timestamp[us] (no timezone)
        dt_naive = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)
        scalar = pa.scalar(dt_naive)
        self.assertEqual(scalar.type, pa.timestamp("us"))
        self.assertEqual(scalar.as_py(), dt_naive)

        # Timezone-aware datetime -> timestamp[us, tz=...]
        dt_aware = datetime.datetime(
            2024, 1, 15, 12, 30, 45, 123456, tzinfo=ZoneInfo("America/New_York")
        )
        scalar = pa.scalar(dt_aware)
        self.assertEqual(scalar.type.unit, "us")
        self.assertEqual(scalar.type.tz, "America/New_York")
        # Compare timestamps to avoid potential timezone object equality issues
        self.assertEqual(scalar.as_py().timestamp(), dt_aware.timestamp())

        # UTC timezone
        dt_utc = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=ZoneInfo("UTC"))
        scalar = pa.scalar(dt_utc)
        self.assertEqual(scalar.type.unit, "us")
        self.assertEqual(scalar.type.tz, "UTC")
        self.assertEqual(scalar.as_py().timestamp(), dt_utc.timestamp())

    def test_timedelta_values(self):
        """Test timedelta type inference - PyArrow infers duration[us] for Python timedelta."""
        import pyarrow as pa

        for td in [
            datetime.timedelta(days=5, hours=3, minutes=30, seconds=15, microseconds=123456),
            datetime.timedelta(0),  # Zero
            datetime.timedelta(days=-1),  # Negative
        ]:
            with self.subTest(td=td):
                scalar = pa.scalar(td)
                self.assertEqual(scalar.type, pa.duration("us"))
                self.assertEqual(scalar.as_py(), td)

    # ==================== Section 4: Pandas Types ====================

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_timestamp(self):
        """Test Pandas Timestamp type inference."""
        import pandas as pd
        import pyarrow as pa

        # Timezone-naive Timestamp
        ts = pd.Timestamp("2024-01-15 12:30:45.123456")
        scalar = pa.scalar(ts)
        self.assertEqual(scalar.type.unit, "us")
        self.assertIsNone(scalar.type.tz)
        self.assertEqual(scalar.as_py(), ts.to_pydatetime())

        # Timezone-aware Timestamp
        ts_tz = pd.Timestamp("2024-01-15 12:30:45.123456", tz="America/New_York")
        scalar = pa.scalar(ts_tz)
        self.assertEqual(scalar.type.unit, "us")
        self.assertEqual(scalar.type.tz, "America/New_York")
        # Compare timestamps to avoid potential timezone object equality issues
        self.assertEqual(scalar.as_py().timestamp(), ts_tz.timestamp())

        # NaT (Not a Time) - pa.scalar raises ValueError for pd.NaT
        # This is because NaTType doesn't support utcoffset attribute
        with self.assertRaises(ValueError):
            pa.scalar(pd.NaT)

        # Even with explicit type, pd.NaT still raises ValueError
        with self.assertRaises(ValueError):
            pa.scalar(pd.NaT, type=pa.timestamp("us"))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_timedelta(self):
        """Test Pandas Timedelta type inference."""
        import pandas as pd
        import pyarrow as pa

        td = pd.Timedelta(days=5, hours=3, minutes=30, seconds=15, microseconds=123456)
        scalar = pa.scalar(td)
        self.assertEqual(scalar.type, pa.duration("us"))
        self.assertEqual(scalar.as_py(), td.to_pytimedelta())

        # Zero timedelta
        td = pd.Timedelta(0)
        scalar = pa.scalar(td)
        self.assertEqual(scalar.type, pa.duration("us"))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_na(self):
        """Test Pandas NA (nullable missing value) - pa.scalar does not support pd.NA."""
        import pandas as pd
        import pyarrow as pa

        # pd.NA is not recognized by pa.scalar without explicit type
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(pd.NA)

        # Even with explicit type, pd.NA still raises ArrowInvalid
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(pd.NA, type=pa.int64())

    # ==================== Section 5: NumPy Scalars ====================

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_integer_scalars(self):
        """Test NumPy integer scalar type inference."""
        import numpy as np
        import pyarrow as pa

        test_cases = [
            (np.int8(42), pa.int8()),
            (np.int16(42), pa.int16()),
            (np.int32(42), pa.int32()),
            (np.int64(42), pa.int64()),
            (np.uint8(42), pa.uint8()),
            (np.uint16(42), pa.uint16()),
            (np.uint32(42), pa.uint32()),
            (np.uint64(42), pa.uint64()),
        ]

        for np_val, expected_type in test_cases:
            with self.subTest(np_type=type(np_val).__name__):
                scalar = pa.scalar(np_val)
                self.assertEqual(scalar.type, expected_type)
                self.assertEqual(scalar.as_py(), int(np_val))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_float_scalars(self):
        """Test NumPy float scalar type inference."""
        import numpy as np
        import pyarrow as pa

        test_cases = [
            (np.float16(1.5), pa.float16()),
            (np.float32(1.5), pa.float32()),
            (np.float64(1.5), pa.float64()),
        ]

        for np_val, expected_type in test_cases:
            with self.subTest(np_type=type(np_val).__name__):
                scalar = pa.scalar(np_val)
                self.assertEqual(scalar.type, expected_type)

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_bool_scalar(self):
        """Test NumPy bool scalar type inference."""
        import numpy as np
        import pyarrow as pa

        for val in [np.bool_(True), np.bool_(False)]:
            with self.subTest(value=bool(val)):
                scalar = pa.scalar(val)
                self.assertEqual(scalar.type, pa.bool_())
                self.assertEqual(scalar.as_py(), bool(val))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_datetime64_scalars(self):
        """Test NumPy datetime64 scalar type inference with different units."""
        import numpy as np
        import pyarrow as pa

        # datetime64 with time-based units (s, ms, us, ns) work directly
        test_cases = [
            (np.datetime64("2024-01-15T12:30", "s"), "s"),  # Second resolution
            (np.datetime64("2024-01-15T12:30:45", "ms"), "ms"),  # Millisecond resolution
            (np.datetime64("2024-01-15T12:30:45.123456", "us"), "us"),  # Microsecond resolution
            (np.datetime64("2024-01-15T12:30:45.123456789", "ns"), "ns"),  # Nanosecond resolution
        ]

        for np_val, expected_unit in test_cases:
            with self.subTest(unit=expected_unit):
                scalar = pa.scalar(np_val)
                self.assertTrue(pa.types.is_timestamp(scalar.type))
                self.assertEqual(scalar.type.unit, expected_unit)

        # datetime64 with date-based units (D, M, Y) raises TypeError in pa.scalar
        # because they convert to datetime.date which pa.scalar doesn't handle well
        with self.assertRaises(TypeError):
            pa.scalar(np.datetime64("2024-01-15", "D"))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_timedelta64_scalars(self):
        """Test NumPy timedelta64 scalar type inference with different units."""
        import numpy as np
        import pyarrow as pa

        # timedelta64 with time-based units (s, ms, us, ns) work directly
        test_cases = [
            (np.timedelta64(3600, "s"), "s"),  # Second resolution
            (np.timedelta64(1000, "ms"), "ms"),  # Millisecond resolution
            (np.timedelta64(1000000, "us"), "us"),  # Microsecond resolution
            (np.timedelta64(1000000000, "ns"), "ns"),  # Nanosecond resolution
        ]

        for np_val, expected_unit in test_cases:
            with self.subTest(unit=expected_unit):
                scalar = pa.scalar(np_val)
                self.assertTrue(pa.types.is_duration(scalar.type))
                self.assertEqual(scalar.type.unit, expected_unit)

        # timedelta64 with day unit (D) raises ArrowNotImplementedError
        with self.assertRaises(pa.ArrowNotImplementedError):
            pa.scalar(np.timedelta64(5, "D"))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_nat_values(self):
        """Test NumPy NaT (Not a Time) scalar handling - requires explicit unit."""
        import numpy as np
        import pyarrow as pa

        # Generic NaT without unit raises ArrowNotImplementedError
        with self.assertRaises(pa.ArrowNotImplementedError):
            pa.scalar(np.datetime64("NaT"))

        with self.assertRaises(pa.ArrowNotImplementedError):
            pa.scalar(np.timedelta64("NaT"))

        # NaT with explicit unit creates null scalar
        scalar = pa.scalar(np.datetime64("NaT", "ns"))
        self.assertTrue(pa.types.is_timestamp(scalar.type))
        self.assertFalse(scalar.is_valid)

        scalar = pa.scalar(np.timedelta64("NaT", "ns"))
        self.assertTrue(pa.types.is_duration(scalar.type))
        self.assertFalse(scalar.is_valid)

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_uint64_max(self):
        """Test NumPy uint64 max value round-trips correctly."""
        import numpy as np
        import pyarrow as pa

        # Max uint64 value
        max_uint64 = np.uint64(2**64 - 1)
        scalar = pa.scalar(max_uint64)
        self.assertEqual(scalar.type, pa.uint64())
        self.assertEqual(scalar.as_py(), 2**64 - 1)

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_float_nan(self):
        """Test NumPy float NaN survives in different float types."""
        import numpy as np
        import pyarrow as pa

        for np_type, pa_type in [(np.float32, pa.float32()), (np.float64, pa.float64())]:
            with self.subTest(np_type=np_type.__name__):
                scalar = pa.scalar(np_type("nan"))
                self.assertEqual(scalar.type, pa_type)
                self.assertTrue(math.isnan(scalar.as_py()))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_string_and_bytes_scalars(self):
        """Test NumPy string and bytes scalar type inference."""
        import numpy as np
        import pyarrow as pa

        test_cases = [
            (np.str_("hello"), pa.string(), "hello"),
            (np.bytes_(b"hello"), pa.binary(), b"hello"),
        ]
        for np_val, expected_type, expected_py in test_cases:
            with self.subTest(np_type=type(np_val).__name__):
                scalar = pa.scalar(np_val)
                self.assertEqual(scalar.type, expected_type)
                self.assertEqual(scalar.as_py(), expected_py)

    # ==================== Section 6: Explicit Type Specification ====================

    def test_explicit_type_overrides_inference(self):
        """Test that explicit type parameter overrides type inference."""
        import pyarrow as pa

        # Integer with different target types
        for target_type in [pa.int8(), pa.int16(), pa.int32()]:
            with self.subTest(target_type=str(target_type)):
                scalar = pa.scalar(42, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertEqual(scalar.as_py(), 42)

        # Integer to float
        scalar = pa.scalar(42, type=pa.float32())
        self.assertEqual(scalar.type, pa.float32())
        self.assertEqual(scalar.as_py(), 42.0)

        # Float to integer (truncation)
        scalar = pa.scalar(42.9, type=pa.int64())
        self.assertEqual(scalar.type, pa.int64())
        self.assertEqual(scalar.as_py(), 42)

        # String with large_string type
        scalar = pa.scalar("hello", type=pa.large_string())
        self.assertEqual(scalar.type, pa.large_string())
        self.assertEqual(scalar.as_py(), "hello")

        # Bytes with large_binary type
        scalar = pa.scalar(b"hello", type=pa.large_binary())
        self.assertEqual(scalar.type, pa.large_binary())
        self.assertEqual(scalar.as_py(), b"hello")

    def test_explicit_timestamp_units(self):
        """Test explicit timestamp unit specification."""
        import pyarrow as pa

        dt = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)

        for unit in ["s", "ms", "us", "ns"]:
            with self.subTest(unit=unit):
                scalar = pa.scalar(dt, type=pa.timestamp(unit))
                self.assertEqual(scalar.type, pa.timestamp(unit))

    def test_explicit_decimal_precision_scale(self):
        """Test explicit decimal precision and scale specification."""
        import pyarrow as pa

        decimal_val = Decimal("123.45")

        # Same scale as input
        scalar = pa.scalar(decimal_val, type=pa.decimal128(10, 2))
        self.assertEqual(scalar.type, pa.decimal128(10, 2))
        self.assertEqual(str(scalar.as_py()), "123.45")

        # Larger scale - trailing zeros should be added
        scalar = pa.scalar(decimal_val, type=pa.decimal128(20, 5))
        self.assertEqual(scalar.type, pa.decimal128(20, 5))
        # Use string comparison to verify scale is actually 5 (with trailing zeros)
        self.assertEqual(str(scalar.as_py()), "123.45000")

    def test_none_with_explicit_type(self):
        """Test None value with explicit type creates typed null scalar."""
        import pyarrow as pa

        for target_type in [
            pa.int64(),
            pa.float64(),
            pa.string(),
            pa.bool_(),
            pa.timestamp("us"),
        ]:
            with self.subTest(target_type=str(target_type)):
                scalar = pa.scalar(None, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertFalse(scalar.is_valid)
                self.assertIsNone(scalar.as_py())

    def test_incompatible_explicit_type_errors(self):
        """Test that incompatible value/type combinations raise ArrowInvalid."""
        import pyarrow as pa

        # All these incompatible conversions raise ArrowInvalid
        for value in ["abc", {"a": 1}, [1, 2, 3]]:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(pa.ArrowInvalid):
                    pa.scalar(value, type=pa.int64())

    # ==================== Section 7: List and Nested Types ====================

    def test_list_values(self):
        """Test list type inference for scalar values."""
        import pyarrow as pa

        # List of integers
        scalar = pa.scalar([1, 2, 3])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.int64())
        self.assertEqual(scalar.as_py(), [1, 2, 3])

        # List of floats
        scalar = pa.scalar([1.0, 2.0, 3.0])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.float64())

        # List of strings
        scalar = pa.scalar(["a", "b", "c"])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.string())

        # Empty list infers as null list
        scalar = pa.scalar([])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.null())

        # Nested list
        scalar = pa.scalar([[1, 2], [3, 4]])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertTrue(pa.types.is_list(scalar.type.value_type))

    def test_list_with_none_elements(self):
        """Test lists containing None elements - value type remains nullable."""
        import pyarrow as pa

        # List with None creates nullable list elements
        scalar = pa.scalar([1, None, 3])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.int64())
        self.assertEqual(scalar.as_py(), [1, None, 3])

        # List of only None becomes list<null>
        scalar = pa.scalar([None])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.null())
        self.assertEqual(scalar.as_py(), [None])

    def test_mixed_type_list(self):
        """Test lists with mixed types - int and float promotes to float."""
        import pyarrow as pa

        # Mixed int and float promotes to float64
        scalar = pa.scalar([1, 2.0])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.float64())
        self.assertEqual(scalar.as_py(), [1.0, 2.0])

        # Mixed int and string raises error - no common type
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar([1, "a"])

    def test_nested_containers(self):
        """Test nested list and dict structures."""
        import pyarrow as pa

        # List of dicts
        scalar = pa.scalar([{"a": 1}, {"a": 2}])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertTrue(pa.types.is_struct(scalar.type.value_type))

        # Dict with list values
        scalar = pa.scalar({"a": [1, 2]})
        self.assertTrue(pa.types.is_struct(scalar.type))

        # Nested tuples (become nested lists)
        scalar = pa.scalar(((1, 2), (3, 4)))
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertTrue(pa.types.is_list(scalar.type.value_type))

    def test_dict_with_none_values(self):
        """Test dict/struct with None values."""
        import pyarrow as pa

        # Dict with None value
        scalar = pa.scalar({"a": None, "b": 1})
        self.assertTrue(pa.types.is_struct(scalar.type))
        result = scalar.as_py()
        self.assertIsNone(result["a"])
        self.assertEqual(result["b"], 1)

    def test_dict_values(self):
        """Test dict/struct type inference for scalar values."""
        import pyarrow as pa

        # Dict creates a struct scalar
        scalar = pa.scalar({"a": 1, "b": 2})
        self.assertTrue(pa.types.is_struct(scalar.type))
        result = scalar.as_py()
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)

        # Mixed types in dict
        scalar = pa.scalar({"int_val": 42, "str_val": "hello", "float_val": 3.14})
        self.assertTrue(pa.types.is_struct(scalar.type))

    def test_tuple_values(self):
        """Test tuple type inference - tuples are treated as lists."""
        import pyarrow as pa

        # Tuple creates a list scalar (same as list)
        scalar = pa.scalar((1, 2, 3))
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.int64())
        self.assertEqual(scalar.as_py(), [1, 2, 3])

    # ==================== Section 8: Edge Cases and Special Values ====================

    def test_large_integers(self):
        """Test behavior with integers at type boundaries."""
        import pyarrow as pa

        # Max and min int64 values
        for value in [2**63 - 1, -(2**63)]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.int64())
                self.assertEqual(scalar.as_py(), value)

    def test_integers_outside_int64_range(self):
        """Test behavior with integers outside int64 range - raises OverflowError."""
        import pyarrow as pa

        # Values outside int64 range raise OverflowError
        for value in [2**63, -(2**63) - 1]:
            with self.subTest(value=value):
                with self.assertRaises(OverflowError):
                    pa.scalar(value)

    def test_special_float_values(self):
        """Test special float values (inf, -inf, nan)."""
        import pyarrow as pa

        # Infinity values
        for value in [float("inf"), float("-inf")]:
            with self.subTest(value=value):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, pa.float64())
                self.assertEqual(scalar.as_py(), value)

        # NaN requires special handling
        scalar = pa.scalar(float("nan"))
        self.assertEqual(scalar.type, pa.float64())
        self.assertTrue(math.isnan(scalar.as_py()))

    def test_empty_string_and_bytes(self):
        """Test empty string and bytes values."""
        import pyarrow as pa

        test_cases = [
            ("", pa.string()),
            (b"", pa.binary()),
        ]
        for value, expected_type in test_cases:
            with self.subTest(value_type=type(value).__name__):
                scalar = pa.scalar(value)
                self.assertEqual(scalar.type, expected_type)
                self.assertEqual(scalar.as_py(), value)

    # ==================== Section 9: Round-trip Consistency ====================

    def test_scalar_roundtrip(self):
        """Test that scalar creation and retrieval is consistent for safe primitives."""
        import pyarrow as pa

        # Only include types that round-trip cleanly without conversion quirks:
        # - Avoid datetime.datetime (timezone handling varies)
        # - Avoid Decimal (scale may change)
        # - Avoid mixed-type lists (Arrow may promote types)
        # - Avoid dicts (field ordering, type promotion)
        test_values = [
            42,
            3.14,
            "hello",
            b"bytes",
            True,
            False,
            datetime.date(2024, 1, 15),
            datetime.time(12, 30, 45),
            datetime.timedelta(days=5),
            [1, 2, 3],  # homogeneous list
        ]

        for value in test_values:
            with self.subTest(value_type=type(value).__name__):
                scalar = pa.scalar(value)
                retrieved = scalar.as_py()
                self.assertEqual(retrieved, value)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
