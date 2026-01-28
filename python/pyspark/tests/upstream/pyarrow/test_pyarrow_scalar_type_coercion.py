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
Test pa.scalar type coercion behavior when creating scalars with explicit type parameter.

This test monitors the behavior of PyArrow's type coercion to ensure PySpark's assumptions
about PyArrow behavior remain valid across versions.

Test categories:
1. Null coercion - None to all types
2. Numeric types - int, float, decimal coercion and boundaries
3. Boolean coercion - bool to other types
4. String/Binary coercion - string, bytes, large_string, large_binary
5. Temporal types - date, time, datetime, timedelta

The helper method pattern is adapted from PR #53721 (pa.array type coercion tests).
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
    """Test PyArrow's type coercion behavior for pa.scalar with explicit type parameter."""

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _run_coercion_tests(self, cases):
        """Run coercion tests: (value, target_type)."""
        import pyarrow as pa

        for value, target_type in cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(scalar.type, target_type)

    def _run_coercion_tests_with_values(self, cases):
        """Run coercion tests with value verification: (value, target_type, expected)."""
        import pyarrow as pa

        for value, target_type, expected in cases:
            scalar = pa.scalar(value, type=target_type)
            self.assertEqual(scalar.type, target_type)
            self.assertEqual(scalar.as_py(), expected)

    def _run_error_tests(self, cases):
        """Run tests expecting errors: (value, target_type, error_type)."""
        import pyarrow as pa

        for value, target_type, error_type in cases:
            with self.assertRaises(error_type):
                pa.scalar(value, type=target_type)

    # =========================================================================
    # SECTION 1: Null Coercion
    # =========================================================================

    def test_null_coercion(self):
        """Test that None can be coerced to any type as a null scalar."""
        import pyarrow as pa

        target_types = [
            # Numeric
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
            # Boolean
            pa.bool_(),
            # String/Binary
            pa.string(),
            pa.large_string(),
            pa.binary(),
            pa.large_binary(),
            # Temporal
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
        ]

        for target_type in target_types:
            scalar = pa.scalar(None, type=target_type)
            self.assertEqual(scalar.type, target_type)
            self.assertFalse(scalar.is_valid)
            self.assertIsNone(scalar.as_py())

    # =========================================================================
    # SECTION 2: Numeric Type Coercion
    # =========================================================================

    def test_numeric_coercion(self):
        """Test numeric type coercion: int, float, decimal."""
        import pyarrow as pa

        # ---- Integer to Integer ----
        int_to_int_cases = [
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
        self._run_coercion_tests_with_values(int_to_int_cases)

        # ---- Integer to Float ----
        int_to_float_cases = [
            (42, pa.float32(), 42.0),
            (42, pa.float64(), 42.0),
            (0, pa.float32(), 0.0),
            (-100, pa.float64(), -100.0),
        ]
        self._run_coercion_tests_with_values(int_to_float_cases)

        # ---- Integer to Decimal ----
        int_to_decimal_cases = [
            (42, pa.decimal128(10, 2), Decimal("42.00")),
            (-999, pa.decimal128(10, 2), Decimal("-999.00")),
            (0, pa.decimal128(10, 2), Decimal("0.00")),
        ]
        self._run_coercion_tests_with_values(int_to_decimal_cases)

        # ---- Float to Float ----
        float_cases = [
            (0.0, pa.float32()),
            (0.0, pa.float64()),
            (3.14, pa.float32()),
            (3.14, pa.float64()),
            (-3.14, pa.float32()),
            (-3.14, pa.float64()),
            (float("inf"), pa.float32()),
            (float("inf"), pa.float64()),
            (float("-inf"), pa.float32()),
            (float("-inf"), pa.float64()),
        ]
        self._run_coercion_tests(float_cases)

        # NaN special case
        for target_type in [pa.float32(), pa.float64()]:
            scalar = pa.scalar(float("nan"), type=target_type)
            self.assertEqual(scalar.type, target_type)
            self.assertTrue(math.isnan(scalar.as_py()))

        # ---- Float to Integer (Truncation) ----
        float_to_int_cases = [
            (42.9, pa.int64(), 42),
            (-42.9, pa.int64(), -42),
            (42.0, pa.int64(), 42),
            (0.5, pa.int64(), 0),
            (-0.5, pa.int64(), 0),
        ]
        self._run_coercion_tests_with_values(float_to_int_cases)

        # ---- Decimal to Integer (Truncation) ----
        scalar = pa.scalar(Decimal("123.45"), type=pa.int64())
        self.assertEqual(scalar.type, pa.int64())
        self.assertEqual(scalar.as_py(), 123)

        # ---- Decimal to Decimal ----
        scalar = pa.scalar(Decimal("123.45"), type=pa.decimal128(20, 5))
        self.assertEqual(scalar.type, pa.decimal128(20, 5))
        self.assertEqual(scalar.as_py(), Decimal("123.45000"))

    def test_numeric_coercion_errors(self):
        """Test numeric coercion error cases."""
        import pyarrow as pa

        error_cases = [
            # Integer overflow
            (128, pa.int8(), pa.ArrowInvalid),
            (-129, pa.int8(), pa.ArrowInvalid),
            (256, pa.uint8(), pa.ArrowInvalid),
            (32768, pa.int16(), pa.ArrowInvalid),
            (2**62, pa.int32(), pa.ArrowInvalid),
            # Negative to unsigned
            (-1, pa.uint8(), OverflowError),
            (-1, pa.uint16(), OverflowError),
            (-1, pa.uint32(), OverflowError),
            (-1, pa.uint64(), OverflowError),
            # Integer precision loss in float32 (2^24 + 1)
            (16777217, pa.float32(), pa.ArrowInvalid),
            # NaN/Inf to integer
            (float("nan"), pa.int64(), pa.ArrowInvalid),
            (float("inf"), pa.int64(), pa.ArrowInvalid),
            (float("-inf"), pa.int64(), pa.ArrowInvalid),
            # Float to decimal
            (42.5, pa.decimal128(10, 2), pa.ArrowTypeError),
            (0.0, pa.decimal128(10, 2), pa.ArrowTypeError),
            (3.14, pa.decimal128(10, 2), pa.ArrowTypeError),
            (float("nan"), pa.decimal128(10, 2), pa.ArrowTypeError),
            # Decimal precision loss
            (Decimal("123.456"), pa.decimal128(10, 2), pa.ArrowInvalid),
            # Decimal to float
            (Decimal("123.45"), pa.float64(), pa.ArrowInvalid),
        ]
        self._run_error_tests(error_cases)

    # =========================================================================
    # SECTION 3: Boolean Coercion
    # =========================================================================

    def test_boolean_coercion(self):
        """Test boolean type coercion."""
        import pyarrow as pa

        cases = [
            # Boolean to Boolean
            (True, pa.bool_(), True),
            (False, pa.bool_(), False),
            # Boolean to Float
            (True, pa.float64(), 1.0),
            (False, pa.float64(), 0.0),
            (True, pa.float32(), 1.0),
            (False, pa.float32(), 0.0),
        ]
        self._run_coercion_tests_with_values(cases)

    def test_boolean_coercion_errors(self):
        """Test boolean coercion error cases."""
        import pyarrow as pa

        error_cases = [
            # Boolean to integer
            (True, pa.int64(), pa.ArrowTypeError),
            (False, pa.int64(), pa.ArrowTypeError),
            # Boolean to string
            (True, pa.string(), pa.ArrowTypeError),
            (False, pa.string(), pa.ArrowTypeError),
            # Boolean to decimal
            (True, pa.decimal128(10, 2), pa.ArrowInvalid),
            (False, pa.decimal128(10, 2), pa.ArrowInvalid),
        ]
        self._run_error_tests(error_cases)

    # =========================================================================
    # SECTION 4: String/Binary Coercion
    # =========================================================================

    def test_string_binary_coercion(self):
        """Test string and binary type coercion."""
        import pyarrow as pa

        cases = [
            # String to String
            ("hello", pa.string(), "hello"),
            ("", pa.string(), ""),
            ("42", pa.string(), "42"),
            ("hello", pa.large_string(), "hello"),
            ("", pa.large_string(), ""),
            # String to Binary (UTF-8 encode)
            ("hello", pa.binary(), b"hello"),
            ("", pa.binary(), b""),
            ("42", pa.binary(), b"42"),
            ("hello", pa.large_binary(), b"hello"),
            # Binary to Binary
            (b"hello", pa.binary(), b"hello"),
            (b"", pa.binary(), b""),
            (b"hello", pa.large_binary(), b"hello"),
            # Binary to String (UTF-8 decode)
            (b"hello", pa.string(), "hello"),
            (b"", pa.string(), ""),
            (b"hello", pa.large_string(), "hello"),
        ]
        self._run_coercion_tests_with_values(cases)

    def test_string_binary_coercion_errors(self):
        """Test string/binary coercion error cases."""
        import pyarrow as pa

        error_cases = [
            # String to numeric
            ("hello", pa.int64(), pa.ArrowInvalid),
            ("", pa.int64(), pa.ArrowInvalid),
            ("42", pa.int64(), pa.ArrowInvalid),
            ("hello", pa.float64(), pa.ArrowInvalid),
            ("3.14", pa.float64(), pa.ArrowInvalid),
            # String to date
            ("2024-01-15", pa.date32(), pa.ArrowTypeError),
        ]
        self._run_error_tests(error_cases)

    # =========================================================================
    # SECTION 5: Temporal Type Coercion
    # =========================================================================

    def test_temporal_coercion(self):
        """Test temporal type coercion: date, time, datetime, timedelta."""
        import pyarrow as pa

        date_val = datetime.date(2024, 1, 15)
        time_val = datetime.time(12, 30, 45, 123456)
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)
        td = datetime.timedelta(days=1, hours=2, minutes=30, seconds=45, microseconds=123456)

        cases_with_values = [
            # Date to Date
            (date_val, pa.date32(), date_val),
            (date_val, pa.date64(), date_val),
            # Datetime to Date (extracts date part)
            (dt, pa.date32(), dt.date()),
        ]
        self._run_coercion_tests_with_values(cases_with_values)

        cases = [
            # Time to Time
            (time_val, pa.time32("s")),
            (time_val, pa.time32("ms")),
            (time_val, pa.time64("us")),
            (time_val, pa.time64("ns")),
            # Datetime to Timestamp
            (dt, pa.timestamp("s")),
            (dt, pa.timestamp("ms")),
            (dt, pa.timestamp("us")),
            (dt, pa.timestamp("ns")),
            (dt, pa.timestamp("us", tz="UTC")),
            (dt, pa.timestamp("us", tz="America/New_York")),
            # Timedelta to Duration
            (td, pa.duration("s")),
            (td, pa.duration("ms")),
            (td, pa.duration("us")),
            (td, pa.duration("ns")),
        ]
        self._run_coercion_tests(cases)

    def test_temporal_coercion_errors(self):
        """Test temporal coercion error cases."""
        import pyarrow as pa

        error_cases = [
            # Date to string
            (datetime.date(2024, 1, 15), pa.string(), pa.ArrowTypeError),
            # Date to timestamp
            (datetime.date(2024, 1, 15), pa.timestamp("us"), pa.ArrowTypeError),
            # Time to string
            (datetime.time(12, 30, 45), pa.string(), pa.ArrowTypeError),
            # Datetime to string
            (datetime.datetime(2024, 1, 15, 12, 30, 45), pa.string(), pa.ArrowTypeError),
            # Timedelta to integer
            (datetime.timedelta(days=1), pa.int64(), pa.ArrowInvalid),
        ]
        self._run_error_tests(error_cases)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
