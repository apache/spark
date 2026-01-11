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
Tests for PyArrow pa.scalar type coercion behavior.

This module tests how PyArrow coerces values when an explicit type is provided
to pa.scalar(value, type=target_type). This helps ensure PySpark's assumptions
about PyArrow behavior remain valid across versions.
"""

import datetime
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
class PyArrowScalarTypeCoercionTests(unittest.TestCase):
    """Test pa.scalar type coercion with explicit type parameter."""

    def test_integer_narrowing(self):
        """Test coercion of integers to smaller integer types."""
        import pyarrow as pa

        # Successful narrowing (value fits in target type)
        success_cases = [
            (42, pa.int8()),
            (42, pa.int16()),
            (42, pa.int32()),
            (127, pa.int8()),  # max int8
            (-128, pa.int8()),  # min int8
        ]
        for value, target_type in success_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                value,
                f"Value mismatch for {value} -> {target_type}",
            )

        # Overflow raises ArrowInvalid
        overflow_cases = [
            (128, pa.int8()),  # exceeds max int8 (127)
            (-129, pa.int8()),  # exceeds min int8 (-128)
            (32768, pa.int16()),  # exceeds max int16 (32767)
        ]
        for value, target_type in overflow_cases:
            with self.assertRaises(pa.ArrowInvalid):
                pa.scalar(value, type=target_type)

    def test_integer_to_unsigned(self):
        """Test coercion of integers to unsigned integer types."""
        import pyarrow as pa

        # Successful coercion
        success_cases = [
            (42, pa.uint8()),
            (255, pa.uint8()),  # max uint8
            (0, pa.uint8()),
            (42, pa.uint16()),
            (42, pa.uint32()),
            (42, pa.uint64()),
        ]
        for value, target_type in success_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                value,
                f"Value mismatch for {value} -> {target_type}",
            )

        # Overflow raises ArrowInvalid
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(256, type=pa.uint8())  # exceeds max uint8 (255)

        # Negative to unsigned raises OverflowError
        with self.assertRaises(OverflowError):
            pa.scalar(-1, type=pa.uint8())

    def test_integer_to_float(self):
        """Test coercion of integers to float types."""
        import pyarrow as pa

        success_cases = [
            (42, pa.float32(), 42.0),
            (42, pa.float64(), 42.0),
            (0, pa.float32(), 0.0),
            (-100, pa.float64(), -100.0),
        ]
        for value, target_type, expected_value in success_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                expected_value,
                f"Value mismatch for {value} -> {target_type}",
            )

        # Large integers that can't be exactly represented in float32 raise error
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(16777217, type=pa.float32())  # 2^24 + 1, beyond float32 exact representation

    def test_float_to_integer(self):
        """Test coercion of floats to integer types - truncates toward zero."""
        import pyarrow as pa

        # Truncation (rounds toward zero)
        truncation_cases = [
            (42.9, pa.int64(), 42),
            (-42.9, pa.int64(), -42),
            (42.0, pa.int64(), 42),  # exact conversion
            (0.5, pa.int64(), 0),
            (-0.5, pa.int64(), 0),
        ]
        for value, target_type, expected_value in truncation_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                expected_value,
                f"Value mismatch for {value} -> {target_type}: "
                f"expected {expected_value}, got {scalar.as_py()}",
            )

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_float_precision_coercion(self):
        """Test coercion between float types."""
        import numpy as np
        import pyarrow as pa

        # float64 to float32 (may lose precision)
        scalar = pa.scalar(3.14159265358979, type=pa.float32())
        self.assertEqual(scalar.type, pa.float32())

        # float32 to float64 (no precision loss)
        scalar = pa.scalar(np.float32(3.14), type=pa.float64())
        self.assertEqual(scalar.type, pa.float64())

    def test_integer_to_decimal(self):
        """Test coercion of integers to decimal types."""
        import pyarrow as pa

        test_cases = [
            (42, pa.decimal128(10, 2), Decimal("42.00")),
            (-999, pa.decimal128(10, 2), Decimal("-999.00")),
            (0, pa.decimal128(10, 2), Decimal("0.00")),
        ]
        for value, target_type, expected_value in test_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                expected_value,
                f"Value mismatch for {value} -> {target_type}",
            )

    def test_float_to_decimal(self):
        """Test that float to decimal coercion requires Decimal input."""
        import pyarrow as pa

        # Float to decimal raises ArrowTypeError - must use Decimal directly
        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(42.5, type=pa.decimal128(10, 2))

        # Use Decimal for decimal types
        scalar = pa.scalar(Decimal("42.5"), type=pa.decimal128(10, 2))
        self.assertEqual(scalar.type, pa.decimal128(10, 2))
        self.assertEqual(scalar.as_py(), Decimal("42.50"))

    def test_decimal_precision_scale_change(self):
        """Test coercion of decimals to different precision/scale."""
        import pyarrow as pa

        # Increase scale (add trailing zeros)
        scalar = pa.scalar(Decimal("123.45"), type=pa.decimal128(20, 5))
        self.assertEqual(scalar.type, pa.decimal128(20, 5))
        self.assertEqual(scalar.as_py(), Decimal("123.45000"))

        # Decrease scale raises ArrowInvalid - PyArrow doesn't allow precision loss
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(Decimal("123.456"), type=pa.decimal128(10, 2))

    def test_string_type_variants(self):
        """Test coercion between string type variants."""
        import pyarrow as pa

        # (value, target_type, expected_type) - utf8 normalizes to string
        test_cases = [
            ("hello", pa.large_string(), pa.large_string()),
            ("hello", pa.utf8(), pa.string()),  # utf8 is same as string
        ]
        for value, target_type, expected_type in test_cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(
                scalar.type,
                expected_type,
                f"Type mismatch for {value} -> {target_type}",
            )
            self.assertEqual(
                scalar.as_py(),
                value,
                f"Value mismatch for {value} -> {target_type}",
            )

    def test_binary_type_variants(self):
        """Test coercion between binary type variants."""
        import pyarrow as pa

        scalar = pa.scalar(b"hello", type=pa.large_binary())
        self.assertEqual(scalar.type, pa.large_binary())
        self.assertEqual(scalar.as_py(), b"hello")

    def test_string_to_binary(self):
        """Test that string can be coerced to binary (encodes as UTF-8)."""
        import pyarrow as pa

        scalar = pa.scalar("hello", type=pa.binary())
        self.assertEqual(scalar.type, pa.binary())
        self.assertEqual(scalar.as_py(), b"hello")

    def test_incompatible_type_coercion(self):
        """Test that incompatible type coercions raise errors."""
        import pyarrow as pa

        # Integer to string raises ArrowTypeError
        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(42, type=pa.string())

        # Integer to boolean raises ArrowInvalid
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(1, type=pa.bool_())

        # Boolean to integer raises ArrowTypeError
        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(True, type=pa.int64())

    def test_timestamp_unit_coercion(self):
        """Test coercion of datetime to different timestamp units."""
        import pyarrow as pa

        dt = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)

        for unit in ["s", "ms", "us", "ns"]:
            target_type = pa.timestamp(unit)
            scalar = pa.scalar(dt, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for datetime -> timestamp[{unit}]",
            )

    def test_timestamp_timezone_coercion(self):
        """Test coercion of datetime with timezone specification."""
        import pyarrow as pa

        # Naive datetime to timezone-aware timestamp
        dt_naive = datetime.datetime(2024, 1, 15, 12, 30, 45)
        scalar = pa.scalar(dt_naive, type=pa.timestamp("us", tz="UTC"))
        self.assertEqual(scalar.type, pa.timestamp("us", tz="UTC"))

        # Timezone-aware datetime
        dt_aware = datetime.datetime(
            2024, 1, 15, 12, 30, 45, tzinfo=ZoneInfo("America/New_York")
        )
        scalar = pa.scalar(dt_aware, type=pa.timestamp("us", tz="America/New_York"))
        self.assertEqual(scalar.type.tz, "America/New_York")

    def test_date_type_coercion(self):
        """Test coercion between date types."""
        import pyarrow as pa

        date_val = datetime.date(2024, 1, 15)

        for target_type in [pa.date32(), pa.date64()]:
            scalar = pa.scalar(date_val, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for date -> {target_type}",
            )
            self.assertEqual(scalar.as_py(), date_val)

    def test_time_unit_coercion(self):
        """Test coercion of time to different time units."""
        import pyarrow as pa

        time_val = datetime.time(12, 30, 45, 123456)

        time_types = [
            pa.time32("s"),
            pa.time32("ms"),
            pa.time64("us"),
            pa.time64("ns"),
        ]
        for target_type in time_types:
            scalar = pa.scalar(time_val, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for time -> {target_type}",
            )

    def test_duration_unit_coercion(self):
        """Test coercion of timedelta to different duration units."""
        import pyarrow as pa

        td = datetime.timedelta(days=1, hours=2, minutes=30, seconds=45, microseconds=123456)

        for unit in ["s", "ms", "us", "ns"]:
            target_type = pa.duration(unit)
            scalar = pa.scalar(td, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for timedelta -> duration[{unit}]",
            )

    def test_none_to_typed_null(self):
        """Test coercion of None to typed null scalars."""
        import pyarrow as pa

        target_types = [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint32(),
            pa.uint64(),
            pa.float32(),
            pa.float64(),
            pa.string(),
            pa.binary(),
            pa.bool_(),
            pa.date32(),
            pa.timestamp("us"),
            pa.decimal128(10, 2),
        ]

        for target_type in target_types:
            scalar = pa.scalar(None, type=target_type)
            self.assertEqual(
                scalar.type,
                target_type,
                f"Type mismatch for None -> {target_type}",
            )
            self.assertFalse(
                scalar.is_valid,
                f"None should create invalid scalar for {target_type}",
            )
            self.assertIsNone(scalar.as_py())

    def test_list_element_type_coercion(self):
        """Test coercion of list elements to specified type."""
        import pyarrow as pa

        # List of ints coerced to list of int8
        scalar = pa.scalar([1, 2, 3], type=pa.list_(pa.int8()))
        self.assertEqual(scalar.type, pa.list_(pa.int8()))
        self.assertEqual(scalar.as_py(), [1, 2, 3])

        # List of ints coerced to list of float64
        scalar = pa.scalar([1, 2, 3], type=pa.list_(pa.float64()))
        self.assertEqual(scalar.type, pa.list_(pa.float64()))
        self.assertEqual(scalar.as_py(), [1.0, 2.0, 3.0])

        # Empty list with explicit element type
        scalar = pa.scalar([], type=pa.list_(pa.int64()))
        self.assertEqual(scalar.type, pa.list_(pa.int64()))
        self.assertEqual(scalar.as_py(), [])

        # List with None elements
        scalar = pa.scalar([1, None, 3], type=pa.list_(pa.int64()))
        self.assertEqual(scalar.type, pa.list_(pa.int64()))
        self.assertEqual(scalar.as_py(), [1, None, 3])

    def test_struct_field_type_coercion(self):
        """Test coercion of struct field values to specified types."""
        import pyarrow as pa

        struct_type = pa.struct([("a", pa.int8()), ("b", pa.float32())])

        scalar = pa.scalar({"a": 42, "b": 3.14}, type=struct_type)
        self.assertEqual(scalar.type, struct_type)
        result = scalar.as_py()
        self.assertEqual(result["a"], 42)
        self.assertAlmostEqual(result["b"], 3.14, places=5)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_nat_coercion(self):
        """Test that pd.NaT with explicit type still raises ValueError."""
        import pandas as pd
        import pyarrow as pa

        # pd.NaT raises ValueError even with explicit type
        with self.assertRaises(ValueError):
            pa.scalar(pd.NaT, type=pa.timestamp("us"))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_na_coercion(self):
        """Test that pd.NA with explicit type still raises ArrowInvalid."""
        import pandas as pd
        import pyarrow as pa

        # pd.NA raises ArrowInvalid even with explicit type
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(pd.NA, type=pa.int64())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
