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
    """Test pa.scalar type inference from various Python types."""

    def test_simple_types(self):
        """Test type inference for simple Python types."""
        import pyarrow as pa

        # (value, expected_type, check_roundtrip)
        # check_roundtrip=False for values that need special comparison (e.g., NaN)
        test_cases = [
            # None
            (None, pa.null(), False),
            # Boolean
            (True, pa.bool_(), True),
            (False, pa.bool_(), True),
            # Integer - PyArrow infers int64 for Python ints
            (0, pa.int64(), True),
            (1, pa.int64(), True),
            (-1, pa.int64(), True),
            (42, pa.int64(), True),
            (2**62, pa.int64(), True),
            (-(2**62), pa.int64(), True),
            (2**63 - 1, pa.int64(), True),  # max int64
            (-(2**63), pa.int64(), True),  # min int64
            # Float - PyArrow infers float64 for Python floats
            (0.0, pa.float64(), True),
            (1.5, pa.float64(), True),
            (-1.5, pa.float64(), True),
            (3.14159, pa.float64(), True),
            (float("inf"), pa.float64(), True),
            (float("-inf"), pa.float64(), True),
            (float("nan"), pa.float64(), False),  # NaN needs special comparison
            # String - PyArrow infers string (utf8) for Python str
            ("", pa.string(), True),
            ("hello", pa.string(), True),
            ("日本語", pa.string(), True),
            ("emoji: 🎉", pa.string(), True),
            # Bytes - PyArrow infers binary for Python bytes
            (b"", pa.binary(), True),
            (b"hello", pa.binary(), True),
            (b"\x00\x01\x02", pa.binary(), True),
            # Bytearray and memoryview infer as binary
            (bytearray(b"hello"), pa.binary(), False),  # as_py returns bytes
            (memoryview(b"hello"), pa.binary(), False),  # as_py returns bytes
        ]

        for value, expected_type, check_roundtrip in test_cases:
            scalar = pa.scalar(value)
            self.assertEqual(
                scalar.type,
                expected_type,
                f"Type mismatch for {type(value).__name__}({value!r}): "
                f"expected {expected_type}, got {scalar.type}",
            )
            if check_roundtrip:
                self.assertEqual(
                    scalar.as_py(),
                    value,
                    f"Roundtrip failed for {type(value).__name__}({value!r})",
                )

        # Special case: NaN comparison
        scalar = pa.scalar(float("nan"))
        self.assertTrue(math.isnan(scalar.as_py()), "NaN roundtrip failed")

        # Special case: None is_valid check
        scalar = pa.scalar(None)
        self.assertFalse(scalar.is_valid, "None should create invalid scalar")

    def test_integer_overflow(self):
        """Test that integers outside int64 range raise OverflowError."""
        import pyarrow as pa

        for value in [2**63, -(2**63) - 1]:
            with self.assertRaises(OverflowError):
                pa.scalar(value)

    def test_decimal_types(self):
        """Test Decimal type inference with precision and scale."""
        import pyarrow as pa

        # (value, expected_scale, expected_precision)
        test_cases = [
            (Decimal("123.45"), 2, 5),
            (Decimal("12345678901234567890.123456789"), 9, 29),
            (Decimal("-999.999"), 3, 6),
            (Decimal("0"), 0, 1),
            (Decimal("0.00"), 2, 2),
        ]

        for value, expected_scale, expected_precision in test_cases:
            scalar = pa.scalar(value)
            self.assertTrue(
                pa.types.is_decimal(scalar.type),
                f"Expected decimal type for {value}, got {scalar.type}",
            )
            self.assertEqual(
                scalar.type.scale,
                expected_scale,
                f"Scale mismatch for {value}: expected {expected_scale}, got {scalar.type.scale}",
            )
            self.assertEqual(
                scalar.type.precision,
                expected_precision,
                f"Precision mismatch for {value}: "
                f"expected {expected_precision}, got {scalar.type.precision}",
            )
            self.assertEqual(
                str(scalar.as_py()),
                str(value),
                f"Roundtrip failed for Decimal {value}",
            )

    def test_date_time_types(self):
        """Test date, time, datetime, and timedelta type inference."""
        import pyarrow as pa

        # Date - infers date32
        date_cases = [
            datetime.date(2024, 1, 15),
            datetime.date(1970, 1, 1),  # Unix epoch
            datetime.date(1, 1, 1),  # Minimum
            datetime.date(9999, 12, 31),  # Maximum
        ]
        for value in date_cases:
            scalar = pa.scalar(value)
            self.assertEqual(
                scalar.type,
                pa.date32(),
                f"Type mismatch for date {value}: expected date32, got {scalar.type}",
            )
            self.assertEqual(scalar.as_py(), value, f"Roundtrip failed for date {value}")

        # Time - infers time64[us]
        time_cases = [
            datetime.time(12, 30, 45, 123456),
            datetime.time(12, 30, 45),
            datetime.time(0, 0, 0),
        ]
        for value in time_cases:
            scalar = pa.scalar(value)
            self.assertEqual(
                scalar.type,
                pa.time64("us"),
                f"Type mismatch for time {value}: expected time64[us], got {scalar.type}",
            )
            self.assertEqual(scalar.as_py(), value, f"Roundtrip failed for time {value}")

        # Timedelta - infers duration[us]
        timedelta_cases = [
            datetime.timedelta(days=5, hours=3, minutes=30, seconds=15, microseconds=123456),
            datetime.timedelta(0),
            datetime.timedelta(days=-1),
        ]
        for value in timedelta_cases:
            scalar = pa.scalar(value)
            self.assertEqual(
                scalar.type,
                pa.duration("us"),
                f"Type mismatch for timedelta {value}: expected duration[us], got {scalar.type}",
            )
            self.assertEqual(scalar.as_py(), value, f"Roundtrip failed for timedelta {value}")

    def test_datetime_timezone(self):
        """Test datetime type inference with and without timezone."""
        import pyarrow as pa

        # Timezone-naive datetime -> timestamp[us]
        dt_naive = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)
        scalar = pa.scalar(dt_naive)
        self.assertEqual(scalar.type, pa.timestamp("us"))
        self.assertIsNone(scalar.type.tz)
        self.assertEqual(scalar.as_py(), dt_naive)

        # Timezone-aware datetime -> timestamp[us, tz=...]
        tz_cases = [
            ("America/New_York", ZoneInfo("America/New_York")),
            ("UTC", ZoneInfo("UTC")),
        ]
        for expected_tz, tzinfo in tz_cases:
            dt_aware = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=tzinfo)
            scalar = pa.scalar(dt_aware)
            self.assertEqual(
                scalar.type.unit,
                "us",
                f"Unit mismatch for tz={expected_tz}",
            )
            self.assertEqual(
                scalar.type.tz,
                expected_tz,
                f"Timezone mismatch: expected {expected_tz}, got {scalar.type.tz}",
            )
            self.assertEqual(
                scalar.as_py().timestamp(),
                dt_aware.timestamp(),
                f"Timestamp mismatch for tz={expected_tz}",
            )

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_types(self):
        """Test Pandas Timestamp and Timedelta type inference."""
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
        self.assertEqual(scalar.as_py().timestamp(), ts_tz.timestamp())

        # Timedelta
        td = pd.Timedelta(days=5, hours=3, minutes=30, seconds=15, microseconds=123456)
        scalar = pa.scalar(td)
        self.assertEqual(scalar.type, pa.duration("us"))
        self.assertEqual(scalar.as_py(), td.to_pytimedelta())

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_nat_and_na(self):
        """Test that pd.NaT and pd.NA raise errors in pa.scalar."""
        import pandas as pd
        import pyarrow as pa

        # pd.NaT raises ValueError
        with self.assertRaises(ValueError):
            pa.scalar(pd.NaT)

        # pd.NA raises ArrowInvalid
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(pd.NA)

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_scalar_types(self):
        """Test NumPy scalar type inference."""
        import numpy as np
        import pyarrow as pa

        # (numpy_value, expected_pyarrow_type)
        test_cases = [
            # Integer types
            (np.int8(42), pa.int8()),
            (np.int16(42), pa.int16()),
            (np.int32(42), pa.int32()),
            (np.int64(42), pa.int64()),
            (np.uint8(42), pa.uint8()),
            (np.uint16(42), pa.uint16()),
            (np.uint32(42), pa.uint32()),
            (np.uint64(42), pa.uint64()),
            (np.uint64(2**64 - 1), pa.uint64()),  # max uint64
            # Float types
            (np.float16(1.5), pa.float16()),
            (np.float32(1.5), pa.float32()),
            (np.float64(1.5), pa.float64()),
            # Boolean
            (np.bool_(True), pa.bool_()),
            (np.bool_(False), pa.bool_()),
            # String and bytes
            (np.str_("hello"), pa.string()),
            (np.bytes_(b"hello"), pa.binary()),
        ]

        for np_val, expected_type in test_cases:
            scalar = pa.scalar(np_val)
            self.assertEqual(
                scalar.type,
                expected_type,
                f"Type mismatch for {type(np_val).__name__}: "
                f"expected {expected_type}, got {scalar.type}",
            )

        # Float NaN - needs special comparison
        nan_cases = [
            (np.float32("nan"), pa.float32()),
            (np.float64("nan"), pa.float64()),
        ]
        for np_val, expected_type in nan_cases:
            scalar = pa.scalar(np_val)
            self.assertEqual(
                scalar.type,
                expected_type,
                f"Type mismatch for {type(np_val).__name__} NaN: "
                f"expected {expected_type}, got {scalar.type}",
            )
            self.assertTrue(
                math.isnan(scalar.as_py()),
                f"NaN roundtrip failed for {type(np_val).__name__}",
            )

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_datetime64(self):
        """Test NumPy datetime64 type inference with different units."""
        import numpy as np
        import pyarrow as pa

        # Time-based units work directly
        unit_cases = [
            (np.datetime64("2024-01-15T12:30", "s"), "s"),
            (np.datetime64("2024-01-15T12:30:45", "ms"), "ms"),
            (np.datetime64("2024-01-15T12:30:45.123456", "us"), "us"),
            (np.datetime64("2024-01-15T12:30:45.123456789", "ns"), "ns"),
        ]
        for np_val, expected_unit in unit_cases:
            scalar = pa.scalar(np_val)
            self.assertTrue(pa.types.is_timestamp(scalar.type))
            self.assertEqual(
                scalar.type.unit,
                expected_unit,
                f"Unit mismatch: expected {expected_unit}, got {scalar.type.unit}",
            )

        # Date-based units (D) raise TypeError
        with self.assertRaises(TypeError):
            pa.scalar(np.datetime64("2024-01-15", "D"))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_timedelta64(self):
        """Test NumPy timedelta64 type inference with different units."""
        import numpy as np
        import pyarrow as pa

        # Time-based units work directly
        unit_cases = [
            (np.timedelta64(3600, "s"), "s"),
            (np.timedelta64(1000, "ms"), "ms"),
            (np.timedelta64(1000000, "us"), "us"),
            (np.timedelta64(1000000000, "ns"), "ns"),
        ]
        for np_val, expected_unit in unit_cases:
            scalar = pa.scalar(np_val)
            self.assertTrue(pa.types.is_duration(scalar.type))
            self.assertEqual(
                scalar.type.unit,
                expected_unit,
                f"Unit mismatch: expected {expected_unit}, got {scalar.type.unit}",
            )

        # Day unit (D) raises ArrowNotImplementedError
        with self.assertRaises(pa.ArrowNotImplementedError):
            pa.scalar(np.timedelta64(5, "D"))

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_numpy_nat(self):
        """Test NumPy NaT handling - generic NaT raises, NaT with unit works."""
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

    def test_list_types(self):
        """Test list type inference."""
        import pyarrow as pa

        # Homogeneous lists
        list_cases = [
            ([1, 2, 3], pa.int64()),
            ([1.0, 2.0, 3.0], pa.float64()),
            (["a", "b", "c"], pa.string()),
        ]
        for value, expected_element_type in list_cases:
            scalar = pa.scalar(value)
            self.assertTrue(pa.types.is_list(scalar.type))
            self.assertEqual(
                scalar.type.value_type,
                expected_element_type,
                f"Element type mismatch for {value}",
            )
            self.assertEqual(scalar.as_py(), value)

        # Empty list infers as list<null>
        scalar = pa.scalar([])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.null())

        # Nested list
        scalar = pa.scalar([[1, 2], [3, 4]])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertTrue(pa.types.is_list(scalar.type.value_type))

        # List with None elements
        scalar = pa.scalar([1, None, 3])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.int64())
        self.assertEqual(scalar.as_py(), [1, None, 3])

        # Mixed int and float promotes to float64
        scalar = pa.scalar([1, 2.0])
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.float64())

        # Mixed incompatible types raise error
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar([1, "a"])

    def test_dict_types(self):
        """Test dict/struct type inference."""
        import pyarrow as pa

        # Simple dict creates struct
        scalar = pa.scalar({"a": 1, "b": 2})
        self.assertTrue(pa.types.is_struct(scalar.type))
        result = scalar.as_py()
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)

        # Dict with mixed value types
        scalar = pa.scalar({"int_val": 42, "str_val": "hello", "float_val": 3.14})
        self.assertTrue(pa.types.is_struct(scalar.type))

        # Dict with None value
        scalar = pa.scalar({"a": None, "b": 1})
        self.assertTrue(pa.types.is_struct(scalar.type))
        result = scalar.as_py()
        self.assertIsNone(result["a"])
        self.assertEqual(result["b"], 1)

    def test_tuple_types(self):
        """Test tuple type inference - tuples are treated as lists."""
        import pyarrow as pa

        scalar = pa.scalar((1, 2, 3))
        self.assertTrue(pa.types.is_list(scalar.type))
        self.assertEqual(scalar.type.value_type, pa.int64())
        self.assertEqual(scalar.as_py(), [1, 2, 3])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
