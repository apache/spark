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
to pa.scalar(value, type=target_type). Tests are organized by source type,
with separate tests for successful coercions and error cases.

This helps ensure PySpark's assumptions about PyArrow behavior remain valid
across versions.
"""

import datetime
import math
import unittest
from decimal import Decimal

from pyspark.testing.utils import (
    have_pyarrow,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowScalarTypeCoercionTests(unittest.TestCase):
    """Test pa.scalar type coercion with explicit type parameter."""

    # =========================================================================
    # None coercion tests
    # =========================================================================

    def test_none_to_all_types(self):
        """Test that None can be coerced to any type as a null scalar."""
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
            pa.decimal128(20, 2),
            pa.bool_(),
            pa.string(),
            pa.large_string(),
            pa.binary(),
            pa.large_binary(),
            pa.date32(),
            pa.date64(),
            pa.time32("s"),
            pa.time32("ms"),
            pa.time64("us"),
            pa.time64("ns"),
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            pa.timestamp("us", tz="UTC"),
            pa.duration("s"),
            pa.duration("us"),
            pa.list_(pa.int64()),
            pa.struct([("a", pa.int64()), ("b", pa.string())]),
        ]

        for target_type in target_types:
            with self.subTest(target_type=target_type):
                scalar = pa.scalar(None, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertFalse(scalar.is_valid)
                self.assertIsNone(scalar.as_py())

    # =========================================================================
    # Boolean coercion tests
    # =========================================================================

    def test_boolean_to_boolean(self):
        """Test boolean to boolean coercion."""
        import pyarrow as pa

        for bool_val in [True, False]:
            scalar = pa.scalar(bool_val, type=pa.bool_())
            self.assertEqual(scalar.type, pa.bool_())
            self.assertEqual(scalar.as_py(), bool_val)

    def test_boolean_to_float(self):
        """Test boolean to float coercion - True->1.0, False->0.0."""
        import pyarrow as pa

        for bool_val, expected in [(True, 1.0), (False, 0.0)]:
            scalar = pa.scalar(bool_val, type=pa.float64())
            self.assertEqual(scalar.type, pa.float64())
            self.assertEqual(scalar.as_py(), expected)

    def test_boolean_to_int_fails(self):
        """Test that boolean to integer coercion fails."""
        import pyarrow as pa

        for bool_val in [True, False]:
            with self.assertRaises(pa.ArrowTypeError):
                pa.scalar(bool_val, type=pa.int64())

    def test_boolean_to_string_fails(self):
        """Test that boolean to string coercion fails."""
        import pyarrow as pa

        for bool_val in [True, False]:
            with self.assertRaises(pa.ArrowTypeError):
                pa.scalar(bool_val, type=pa.string())

    def test_boolean_to_decimal_fails(self):
        """Test that boolean to decimal coercion fails."""
        import pyarrow as pa

        for bool_val in [True, False]:
            with self.assertRaises(pa.ArrowInvalid):
                pa.scalar(bool_val, type=pa.decimal128(10, 2))

    # =========================================================================
    # Integer coercion tests
    # =========================================================================

    def test_integer_to_integer_success(self):
        """Test successful integer to integer coercion within range."""
        import pyarrow as pa

        test_cases = [
            (42, pa.int8(), 42),
            (42, pa.int16(), 42),
            (42, pa.int32(), 42),
            (42, pa.int64(), 42),
            (127, pa.int8(), 127),  # max int8
            (-128, pa.int8(), -128),  # min int8
            (0, pa.uint8(), 0),
            (255, pa.uint8(), 255),  # max uint8
            (2**62, pa.int64(), 2**62),
        ]

        for value, target_type, expected in test_cases:
            with self.subTest(value=value, target_type=target_type):
                scalar = pa.scalar(value, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertEqual(scalar.as_py(), expected)

    def test_integer_overflow_fails(self):
        """Test that integer overflow raises ArrowInvalid."""
        import pyarrow as pa

        test_cases = [
            (128, pa.int8()),  # exceeds max int8 (127)
            (-129, pa.int8()),  # exceeds min int8 (-128)
            (256, pa.uint8()),  # exceeds max uint8 (255)
            (32768, pa.int16()),  # exceeds max int16
            (2**62, pa.int32()),  # exceeds max int32
        ]

        for value, target_type in test_cases:
            with self.subTest(value=value, target_type=target_type):
                with self.assertRaises(pa.ArrowInvalid):
                    pa.scalar(value, type=target_type)

    def test_negative_to_unsigned_fails(self):
        """Test that negative to unsigned integer raises OverflowError."""
        import pyarrow as pa

        for target_type in [pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]:
            with self.subTest(target_type=target_type):
                with self.assertRaises(OverflowError):
                    pa.scalar(-1, type=target_type)

    def test_integer_to_float(self):
        """Test integer to float coercion."""
        import pyarrow as pa

        test_cases = [
            (42, pa.float32(), 42.0),
            (42, pa.float64(), 42.0),
            (0, pa.float32(), 0.0),
            (-100, pa.float64(), -100.0),
        ]

        for value, target_type, expected in test_cases:
            with self.subTest(value=value, target_type=target_type):
                scalar = pa.scalar(value, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertEqual(scalar.as_py(), expected)

    def test_integer_to_float_precision_loss_fails(self):
        """Test that large integers that lose precision in float32 fail."""
        import pyarrow as pa

        # 2^24 + 1 cannot be exactly represented in float32
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(16777217, type=pa.float32())

    def test_integer_to_decimal(self):
        """Test integer to decimal coercion."""
        import pyarrow as pa

        test_cases = [
            (42, pa.decimal128(10, 2), Decimal("42.00")),
            (-999, pa.decimal128(10, 2), Decimal("-999.00")),
            (0, pa.decimal128(10, 2), Decimal("0.00")),
        ]

        for value, target_type, expected in test_cases:
            with self.subTest(value=value):
                scalar = pa.scalar(value, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertEqual(scalar.as_py(), expected)

    def test_integer_to_string_fails(self):
        """Test that integer to string coercion fails."""
        import pyarrow as pa

        for value in [0, 42, -42]:
            with self.subTest(value=value):
                with self.assertRaises(pa.ArrowTypeError):
                    pa.scalar(value, type=pa.string())

    def test_integer_to_bool_fails(self):
        """Test that integer to boolean coercion fails."""
        import pyarrow as pa

        for value in [0, 1, 42]:
            with self.subTest(value=value):
                with self.assertRaises(pa.ArrowInvalid):
                    pa.scalar(value, type=pa.bool_())

    # =========================================================================
    # Float coercion tests
    # =========================================================================

    def test_float_to_float(self):
        """Test float to float coercion."""
        import pyarrow as pa

        float_values = [0.0, 3.14, -3.14, float("inf"), float("-inf")]

        for value in float_values:
            for target_type in [pa.float32(), pa.float64()]:
                with self.subTest(value=value, target_type=target_type):
                    scalar = pa.scalar(value, type=target_type)
                    self.assertEqual(scalar.type, target_type)
                    if target_type == pa.float32():
                        self.assertAlmostEqual(scalar.as_py(), value, places=5)
                    else:
                        self.assertEqual(scalar.as_py(), value)

    def test_float_nan_to_float(self):
        """Test float NaN to float coercion."""
        import pyarrow as pa

        for target_type in [pa.float32(), pa.float64()]:
            scalar = pa.scalar(float("nan"), type=target_type)
            self.assertEqual(scalar.type, target_type)
            self.assertTrue(math.isnan(scalar.as_py()))

    def test_float_to_integer_truncation(self):
        """Test float to integer coercion truncates toward zero."""
        import pyarrow as pa

        test_cases = [
            (42.9, 42),
            (-42.9, -42),
            (42.0, 42),
            (0.5, 0),
            (-0.5, 0),
        ]

        for value, expected in test_cases:
            with self.subTest(value=value):
                scalar = pa.scalar(value, type=pa.int64())
                self.assertEqual(scalar.type, pa.int64())
                self.assertEqual(scalar.as_py(), expected)

    def test_float_special_to_integer_fails(self):
        """Test that NaN and Inf to integer fails."""
        import pyarrow as pa

        for value in [float("nan"), float("inf"), float("-inf")]:
            with self.subTest(value=value):
                with self.assertRaises(pa.ArrowInvalid):
                    pa.scalar(value, type=pa.int64())

    def test_float_to_decimal_fails(self):
        """Test that float to decimal coercion fails."""
        import pyarrow as pa

        for value in [42.5, 0.0, 3.14, float("nan")]:
            with self.subTest(value=value):
                with self.assertRaises(pa.ArrowTypeError):
                    pa.scalar(value, type=pa.decimal128(10, 2))

    # =========================================================================
    # Decimal coercion tests
    # =========================================================================

    def test_decimal_to_decimal(self):
        """Test Decimal to decimal coercion."""
        import pyarrow as pa

        scalar = pa.scalar(Decimal("123.45"), type=pa.decimal128(20, 5))
        self.assertEqual(scalar.type, pa.decimal128(20, 5))
        self.assertEqual(scalar.as_py(), Decimal("123.45000"))

    def test_decimal_precision_loss_fails(self):
        """Test that decimal precision loss fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(Decimal("123.456"), type=pa.decimal128(10, 2))

    def test_decimal_to_integer(self):
        """Test Decimal to integer coercion truncates fractional part."""
        import pyarrow as pa

        scalar = pa.scalar(Decimal("123.45"), type=pa.int64())
        self.assertEqual(scalar.type, pa.int64())
        self.assertEqual(scalar.as_py(), 123)

    def test_decimal_to_float_fails(self):
        """Test that Decimal to float coercion fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(Decimal("123.45"), type=pa.float64())

    # =========================================================================
    # String coercion tests
    # =========================================================================

    def test_string_to_string(self):
        """Test string to string type coercion."""
        import pyarrow as pa

        for value in ["hello", "", "42", "2024-01-15"]:
            for target_type in [pa.string(), pa.large_string()]:
                with self.subTest(value=value, target_type=target_type):
                    scalar = pa.scalar(value, type=target_type)
                    self.assertEqual(scalar.type, target_type)
                    self.assertEqual(scalar.as_py(), value)

    def test_string_to_binary(self):
        """Test string to binary coercion encodes as UTF-8."""
        import pyarrow as pa

        for value in ["hello", "", "42"]:
            for target_type in [pa.binary(), pa.large_binary()]:
                with self.subTest(value=value, target_type=target_type):
                    scalar = pa.scalar(value, type=target_type)
                    self.assertEqual(scalar.type, target_type)
                    self.assertEqual(scalar.as_py(), value.encode("utf-8"))

    def test_string_to_numeric_fails(self):
        """Test that string to numeric coercion fails."""
        import pyarrow as pa

        for value in ["hello", "", "42", "2024-01-15"]:
            for target_type in [pa.int64(), pa.float64()]:
                with self.subTest(value=value, target_type=target_type):
                    with self.assertRaises(pa.ArrowInvalid):
                        pa.scalar(value, type=target_type)

    def test_string_to_date_fails(self):
        """Test that string to date coercion fails (no automatic parsing)."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar("2024-01-15", type=pa.date32())

    # =========================================================================
    # Bytes coercion tests
    # =========================================================================

    def test_bytes_to_binary(self):
        """Test bytes to binary coercion."""
        import pyarrow as pa

        for value in [b"hello", b""]:
            for target_type in [pa.binary(), pa.large_binary()]:
                with self.subTest(value=value, target_type=target_type):
                    scalar = pa.scalar(value, type=target_type)
                    self.assertEqual(scalar.type, target_type)
                    self.assertEqual(scalar.as_py(), value)

    def test_bytes_to_string(self):
        """Test bytes to string coercion decodes as UTF-8."""
        import pyarrow as pa

        for value in [b"hello", b""]:
            for target_type in [pa.string(), pa.large_string()]:
                with self.subTest(value=value, target_type=target_type):
                    scalar = pa.scalar(value, type=target_type)
                    self.assertEqual(scalar.type, target_type)
                    self.assertEqual(scalar.as_py(), value.decode("utf-8"))

    # =========================================================================
    # Date coercion tests
    # =========================================================================

    def test_date_to_date(self):
        """Test date to date type coercion."""
        import pyarrow as pa

        date_val = datetime.date(2024, 1, 15)

        for target_type in [pa.date32(), pa.date64()]:
            with self.subTest(target_type=target_type):
                scalar = pa.scalar(date_val, type=target_type)
                self.assertEqual(scalar.type, target_type)
                self.assertEqual(scalar.as_py(), date_val)

    def test_date_to_string_fails(self):
        """Test that date to string coercion fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(datetime.date(2024, 1, 15), type=pa.string())

    def test_date_to_timestamp_fails(self):
        """Test that date to timestamp coercion fails (need datetime)."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(datetime.date(2024, 1, 15), type=pa.timestamp("us"))

    # =========================================================================
    # Time coercion tests
    # =========================================================================

    def test_time_to_time(self):
        """Test time to time type coercion."""
        import pyarrow as pa

        time_val = datetime.time(12, 30, 45, 123456)

        for target_type in [pa.time32("s"), pa.time32("ms"), pa.time64("us"), pa.time64("ns")]:
            with self.subTest(target_type=target_type):
                scalar = pa.scalar(time_val, type=target_type)
                self.assertEqual(scalar.type, target_type)

    def test_time_to_string_fails(self):
        """Test that time to string coercion fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(datetime.time(12, 30, 45), type=pa.string())

    # =========================================================================
    # Datetime coercion tests
    # =========================================================================

    def test_datetime_to_timestamp(self):
        """Test datetime to timestamp coercion."""
        import pyarrow as pa

        dt = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)

        timestamp_types = [
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            pa.timestamp("us", tz="UTC"),
            pa.timestamp("us", tz="America/New_York"),
        ]

        for target_type in timestamp_types:
            with self.subTest(target_type=target_type):
                scalar = pa.scalar(dt, type=target_type)
                self.assertEqual(scalar.type, target_type)

    def test_datetime_to_date(self):
        """Test datetime to date coercion extracts date part."""
        import pyarrow as pa

        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        scalar = pa.scalar(dt, type=pa.date32())
        self.assertEqual(scalar.type, pa.date32())
        self.assertEqual(scalar.as_py(), dt.date())

    def test_datetime_to_string_fails(self):
        """Test that datetime to string coercion fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowTypeError):
            pa.scalar(datetime.datetime(2024, 1, 15, 12, 30, 45), type=pa.string())

    # =========================================================================
    # Timedelta coercion tests
    # =========================================================================

    def test_timedelta_to_duration(self):
        """Test timedelta to duration coercion."""
        import pyarrow as pa

        td = datetime.timedelta(days=1, hours=2, minutes=30, seconds=45, microseconds=123456)

        for unit in ["s", "ms", "us", "ns"]:
            target_type = pa.duration(unit)
            with self.subTest(target_type=target_type):
                scalar = pa.scalar(td, type=target_type)
                self.assertEqual(scalar.type, target_type)

    def test_timedelta_to_integer_fails(self):
        """Test that timedelta to integer coercion fails."""
        import pyarrow as pa

        td = datetime.timedelta(days=1)
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(td, type=pa.int64())

    # =========================================================================
    # List coercion tests
    # =========================================================================

    def test_list_element_coercion(self):
        """Test list with element type coercion."""
        import pyarrow as pa

        # List of ints to list of int8
        scalar = pa.scalar([1, 2, 3], type=pa.list_(pa.int8()))
        self.assertEqual(scalar.type, pa.list_(pa.int8()))
        self.assertEqual(scalar.as_py(), [1, 2, 3])

        # List of ints to list of float64
        scalar = pa.scalar([1, 2, 3], type=pa.list_(pa.float64()))
        self.assertEqual(scalar.type, pa.list_(pa.float64()))
        self.assertEqual(scalar.as_py(), [1.0, 2.0, 3.0])

    def test_list_empty(self):
        """Test empty list with explicit type."""
        import pyarrow as pa

        scalar = pa.scalar([], type=pa.list_(pa.int64()))
        self.assertEqual(scalar.type, pa.list_(pa.int64()))
        self.assertEqual(scalar.as_py(), [])

    def test_list_with_nulls(self):
        """Test list with None elements."""
        import pyarrow as pa

        scalar = pa.scalar([1, None, 3], type=pa.list_(pa.int64()))
        self.assertEqual(scalar.type, pa.list_(pa.int64()))
        self.assertEqual(scalar.as_py(), [1, None, 3])

    def test_list_element_type_mismatch_fails(self):
        """Test that list element type mismatch fails."""
        import pyarrow as pa

        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(["a", "b"], type=pa.list_(pa.int64()))

    # =========================================================================
    # Struct coercion tests
    # =========================================================================

    def test_struct_coercion(self):
        """Test struct value coercion."""
        import pyarrow as pa

        struct_type = pa.struct([("a", pa.int8()), ("b", pa.float32())])

        scalar = pa.scalar({"a": 42, "b": 3.14}, type=struct_type)
        self.assertEqual(scalar.type, struct_type)
        result = scalar.as_py()
        self.assertEqual(result["a"], 42)
        self.assertAlmostEqual(result["b"], 3.14, places=5)

    def test_struct_missing_field_fills_null(self):
        """Test that missing struct field is filled with null."""
        import pyarrow as pa

        struct_type = pa.struct([("a", pa.int8()), ("b", pa.float32())])
        scalar = pa.scalar({"a": 42}, type=struct_type)
        self.assertEqual(scalar.type, struct_type)
        result = scalar.as_py()
        self.assertEqual(result["a"], 42)
        self.assertIsNone(result["b"])

    def test_struct_extra_field_ignored(self):
        """Test that extra struct field is ignored."""
        import pyarrow as pa

        struct_type = pa.struct([("a", pa.int8()), ("b", pa.float32())])
        scalar = pa.scalar({"a": 42, "b": 3.14, "c": "extra"}, type=struct_type)
        self.assertEqual(scalar.type, struct_type)
        result = scalar.as_py()
        self.assertEqual(result["a"], 42)
        self.assertAlmostEqual(result["b"], 3.14, places=5)
        self.assertNotIn("c", result)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
