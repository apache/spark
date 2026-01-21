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
1. Null coercion - None to numeric types
2. Numeric types - int, float, decimal coercion and boundaries

The helper method pattern is adapted from PR #53721 (pa.array type coercion tests).
"""

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

    def _run_error_tests(self, cases, error_type):
        """Run tests expecting errors: (value, target_type)."""
        import pyarrow as pa

        for value, target_type in cases:
            with self.assertRaises(error_type):
                pa.scalar(value, type=target_type)

    # =========================================================================
    # SECTION 1: Null Coercion
    # =========================================================================

    def test_null_coercion(self):
        """Test that None can be coerced to numeric types as a null scalar."""
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

        # Integer overflow
        overflow_cases = [
            (128, pa.int8()),
            (-129, pa.int8()),
            (256, pa.uint8()),
            (32768, pa.int16()),
            (2**62, pa.int32()),
        ]
        self._run_error_tests(overflow_cases, pa.ArrowInvalid)

        # Negative to unsigned
        negative_to_unsigned_cases = [
            (-1, pa.uint8()),
            (-1, pa.uint16()),
            (-1, pa.uint32()),
            (-1, pa.uint64()),
        ]
        for value, target_type in negative_to_unsigned_cases:
            with self.assertRaises(OverflowError):
                pa.scalar(value, type=target_type)

        # Integer precision loss in float32 (2^24 + 1)
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(16777217, type=pa.float32())

        # NaN/Inf to integer
        nan_inf_cases = [
            (float("nan"), pa.int64()),
            (float("inf"), pa.int64()),
            (float("-inf"), pa.int64()),
        ]
        self._run_error_tests(nan_inf_cases, pa.ArrowInvalid)

        # Float to decimal
        float_to_decimal_cases = [
            (42.5, pa.decimal128(10, 2)),
            (0.0, pa.decimal128(10, 2)),
            (3.14, pa.decimal128(10, 2)),
            (float("nan"), pa.decimal128(10, 2)),
        ]
        self._run_error_tests(float_to_decimal_cases, pa.ArrowTypeError)

        # Decimal precision loss
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(Decimal("123.456"), type=pa.decimal128(10, 2))

        # Decimal to float
        with self.assertRaises(pa.ArrowInvalid):
            pa.scalar(Decimal("123.45"), type=pa.float64())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
