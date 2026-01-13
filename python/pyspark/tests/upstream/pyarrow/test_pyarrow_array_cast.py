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
Tests for PyArrow's pa.Array.cast() method with default arguments.

This test suite is part of SPARK-54936 to monitor upstream PyArrow behavior.
It tests all combinations of source type -> target type to ensure PySpark's
assumptions about PyArrow's casting behavior remain valid across versions.

## Type Conversion Matrix (pa.Array.cast with default safe=True)

### Comprehensive Type Coverage:
- **Integers**: int8, int16, int32, int64, uint8, uint16, uint32, uint64
- **Floats**: float16, float32, float64
- **Strings**: string, large_string
- **Binary**: binary, large_binary
- **Decimals**: decimal128, decimal256
- **Dates**: date32, date64
- **Timestamps**: timestamp[s/ms/us/ns]
- **Durations**: duration[s/ms/us/ns]
- **Times**: time32[s/ms], time64[us/ns]
- **Lists**: list, large_list, fixed_size_list
- **Complex**: struct, map
- **NumPy**: np.int8-64, np.uint8-64, np.float16/32/64
- **Pandas**: pd.Int64Dtype(), pd.Float64Dtype(), pd.ArrowDtype()

### Conversion Matrix:

| From \\ To         | int8-64 | uint8-64 | float16-64 | bool | string/large | binary/large | decimal128/256 | date32/64 | timestamp | duration | time | list/large | struct | map |
|-------------------|---------|----------|------------|------|--------------|--------------|----------------|-----------|-----------|----------|------|------------|--------|-----|
| **int8-64**       | ✓       | ✓        | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **uint8-64**      | ✓       | ✓        | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **float16-64**    | ✓ᴺᵀ     | ✓ᴺᵀ      | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **bool**          | ✓       | ✓        | ✓⁽ᶠ³²⁺⁾    | ✓    | ✓            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **string/large**  | ✓       | ✓        | ✓          | ✓    | ✓            | ✓            | ✓              | ✓         | ✓         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **binary/large**  | ✗       | ✗        | ✗          | ✗    | ✓            | ✓            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **decimal128/256**| ✓       | ✓        | ✓          | ✗    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **date32/64**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✓         | ✓         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **timestamp**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✓         | ✓ᵁᴾ       | ✗        | ✗    | ✗          | ✗      | ✗   |
| **duration**      | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✗         | ✗         | ✓ᵁᴾ      | ✗    | ✗          | ✗      | ✗   |
| **time32/64**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✗         | ✗         | ✗        | ✓ᵁᴾ  | ✗          | ✗      | ✗   |
| **list/large**    | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✓ᴱᴸ        | ✗      | ✗   |
| **struct**        | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✓ᴱᴸ    | ✗   |
| **map**           | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✓ᴱᴸ |

Legend:
- ✓      = Allowed (no precision loss with safe=True)
- ✓ᴺᵀ    = No Truncation: only if no truncation (e.g., 1.0→1 ok, 1.5→1 fails)
- ✓ᵁᴾ    = Upcast only: converting to higher precision unit (s→ms ok, ms→s may fail)
- ✓ᴱᴸ    = Element-wise: element/field types must be castable
- ✓⁽ᶠ³²⁺⁾ = float32 and above (bool->float16 not supported)
- ✗      = Not allowed / raises ArrowInvalid

Notes:
1. With default safe=True, PyArrow prevents precision loss
2. Float→Int requires whole numbers (1.0 ok, 1.5 fails)
3. Timestamp/Duration/Time conversions to lower precision may lose data
4. Large int64 values may exceed float64 safe range (±2^53)
5. Nested type casts recursively cast element types
6. string/large_string and binary/large_binary are interchangeable
7. decimal128 and decimal256 require sufficient precision for int64 (≥21 digits)
"""

import unittest
import math
from datetime import datetime, date

from pyspark.testing.utils import (
    have_pandas,
    have_numpy,
    have_pyarrow,
    pandas_requirement_message,
    numpy_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowArrayCastTests(unittest.TestCase):
    """Test pa.Array.cast() with default arguments for all type combinations."""

    def test_all_integer_type_casts(self):
        """Test casting between all integer types (int8-64, uint8-64)."""
        import pyarrow as pa

        # All integer types with test values
        # (type, test_values, type_name)
        int_types = [
            (pa.int8(), [1, 2, 3, None], "int8"),
            (pa.int16(), [1, 2, 3, None], "int16"),
            (pa.int32(), [1, 2, 3, None], "int32"),
            (pa.int64(), [1, 2, 3, None], "int64"),
            (pa.uint8(), [1, 2, 3, None], "uint8"),
            (pa.uint16(), [1, 2, 3, None], "uint16"),
            (pa.uint32(), [1, 2, 3, None], "uint32"),
            (pa.uint64(), [1, 2, 3, None], "uint64"),
        ]

        # Test all int -> int conversions
        for source_type, source_values, source_name in int_types:
            source_arr = pa.array(source_values, type=source_type)
            for target_type, _, target_name in int_types:
                result = source_arr.cast(target_type)
                self.assertEqual(result.type, target_type)
                self.assertEqual(result[0].as_py(), 1)
                self.assertEqual(result[1].as_py(), 2)
                self.assertEqual(result[2].as_py(), 3)
                self.assertIsNone(result[3].as_py())

    def test_all_float_type_casts(self):
        """Test casting between all float types (float16, float32, float64)."""
        import pyarrow as pa

        # All float types with test values
        float_types = [
            (pa.float16(), [1.5, 2.5, 3.5, None], "float16"),
            (pa.float32(), [1.5, 2.5, 3.5, None], "float32"),
            (pa.float64(), [1.5, 2.5, 3.5, None], "float64"),
        ]

        # Test all float -> float conversions
        for source_type, source_values, source_name in float_types:
            source_arr = pa.array(source_values, type=source_type)
            for target_type, _, target_name in float_types:
                result = source_arr.cast(target_type)
                self.assertEqual(result.type, target_type)
                # float16 has lower precision
                if source_name == "float16" or target_name == "float16":
                    self.assertAlmostEqual(result[0].as_py(), 1.5, places=2)
                    self.assertAlmostEqual(result[1].as_py(), 2.5, places=2)
                    self.assertAlmostEqual(result[2].as_py(), 3.5, places=2)
                else:
                    self.assertAlmostEqual(result[0].as_py(), 1.5, places=5)
                    self.assertAlmostEqual(result[1].as_py(), 2.5, places=5)
                    self.assertAlmostEqual(result[2].as_py(), 3.5, places=5)
                self.assertIsNone(result[3].as_py())

    def test_integer_to_float_casts(self):
        """Test casting all integer types to all float types."""
        import pyarrow as pa

        int_types = [
            (pa.int8(), [1, 2, 3, None], "int8"),
            (pa.int16(), [1, 2, 3, None], "int16"),
            (pa.int32(), [1, 2, 3, None], "int32"),
            (pa.int64(), [1, 2, 3, None], "int64"),
            (pa.uint8(), [1, 2, 3, None], "uint8"),
            (pa.uint16(), [1, 2, 3, None], "uint16"),
            (pa.uint32(), [1, 2, 3, None], "uint32"),
            (pa.uint64(), [1, 2, 3, None], "uint64"),
        ]

        float_types = [
            (pa.float16(), "float16"),
            (pa.float32(), "float32"),
            (pa.float64(), "float64"),
        ]

        for source_type, source_values, source_name in int_types:
            source_arr = pa.array(source_values, type=source_type)
            for target_type, target_name in float_types:
                result = source_arr.cast(target_type)
                self.assertEqual(result.type, target_type)
                self.assertAlmostEqual(result[0].as_py(), 1.0, places=2)
                self.assertAlmostEqual(result[1].as_py(), 2.0, places=2)
                self.assertAlmostEqual(result[2].as_py(), 3.0, places=2)
                self.assertIsNone(result[3].as_py())

    def test_float_to_integer_casts(self):
        """Test casting float types to integer types (only whole numbers with safe=True)."""
        import pyarrow as pa

        # Use whole numbers for safe casting
        float_types = [
            (pa.float16(), [1.0, 2.0, 3.0, None], "float16"),
            (pa.float32(), [1.0, 2.0, 3.0, None], "float32"),
            (pa.float64(), [1.0, 2.0, 3.0, None], "float64"),
        ]

        int_types = [
            (pa.int8(), "int8"),
            (pa.int16(), "int16"),
            (pa.int32(), "int32"),
            (pa.int64(), "int64"),
            (pa.uint8(), "uint8"),
            (pa.uint16(), "uint16"),
            (pa.uint32(), "uint32"),
            (pa.uint64(), "uint64"),
        ]

        for source_type, source_values, source_name in float_types:
            source_arr = pa.array(source_values, type=source_type)
            for target_type, target_name in int_types:
                result = source_arr.cast(target_type)
                self.assertEqual(result.type, target_type)
                self.assertEqual(result[0].as_py(), 1)
                self.assertEqual(result[1].as_py(), 2)
                self.assertEqual(result[2].as_py(), 3)
                self.assertIsNone(result[3].as_py())

    def test_numeric_to_bool_casts(self):
        """Test casting all numeric types to boolean."""
        import pyarrow as pa

        numeric_types = [
            (pa.int8(), [0, 1, 2, None], "int8"),
            (pa.int32(), [0, 1, 2, None], "int32"),
            (pa.int64(), [0, 1, 2, None], "int64"),
            (pa.float32(), [0.0, 1.0, 2.0, None], "float32"),
            (pa.float64(), [0.0, 1.0, 2.0, None], "float64"),
        ]

        for source_type, source_values, source_name in numeric_types:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(pa.bool_())
            self.assertEqual(result.type, pa.bool_())
            self.assertEqual(result[0].as_py(), False)
            self.assertEqual(result[1].as_py(), True)
            self.assertEqual(result[2].as_py(), True)
            self.assertIsNone(result[3].as_py())

    def test_bool_to_numeric_casts(self):
        """Test casting boolean to all numeric types."""
        import pyarrow as pa

        arr_bool = pa.array([True, False, True, None], type=pa.bool_())

        numeric_types = [
            (pa.int8(), "int8"),
            (pa.int16(), "int16"),
            (pa.int32(), "int32"),
            (pa.int64(), "int64"),
            (pa.uint8(), "uint8"),
            (pa.uint16(), "uint16"),
            (pa.uint32(), "uint32"),
            (pa.uint64(), "uint64"),
            # float16 not supported for bool->float cast
            (pa.float32(), "float32"),
            (pa.float64(), "float64"),
        ]

        for target_type, target_name in numeric_types:
            result = arr_bool.cast(target_type)
            self.assertEqual(result.type, target_type)
            self.assertEqual(result[0].as_py(), 1)
            self.assertEqual(result[1].as_py(), 0)
            self.assertEqual(result[2].as_py(), 1)
            self.assertIsNone(result[3].as_py())

    def test_numeric_to_string_casts(self):
        """Test casting all numeric types to string."""
        import pyarrow as pa

        test_cases = [
            (pa.int8(), [1, 2, 3, None], ["1", "2", "3", None]),
            (pa.int32(), [1, 2, 3, None], ["1", "2", "3", None]),
            (pa.int64(), [1, 2, 3, None], ["1", "2", "3", None]),
            (pa.uint32(), [1, 2, 3, None], ["1", "2", "3", None]),
            (pa.float32(), [1.5, 2.5, 3.5, None], ["1.5", "2.5", "3.5", None]),
            (pa.float64(), [1.5, 2.5, 3.5, None], ["1.5", "2.5", "3.5", None]),
            (pa.bool_(), [True, False, None], ["true", "false", None]),
        ]

        for source_type, source_values, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(pa.string())
            self.assertEqual(result.type, pa.string())
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_string_to_numeric_casts(self):
        """Test casting string to all numeric types."""
        import pyarrow as pa

        test_cases = [
            (["1", "2", "3", None], pa.int8(), [1, 2, 3, None]),
            (["1", "2", "3", None], pa.int32(), [1, 2, 3, None]),
            (["1", "2", "3", None], pa.int64(), [1, 2, 3, None]),
            (["1", "2", "3", None], pa.uint32(), [1, 2, 3, None]),
            (["1.5", "2.5", "3.5", None], pa.float32(), [1.5, 2.5, 3.5, None]),
            (["1.5", "2.5", "3.5", None], pa.float64(), [1.5, 2.5, 3.5, None]),
            (["true", "false", "1", "0", None], pa.bool_(), [True, False, True, False, None]),
        ]

        for source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=pa.string())
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                if isinstance(expected_values[i], float):
                    self.assertAlmostEqual(result[i].as_py(), expected_values[i], places=5)
                else:
                    self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_string_binary_casts(self):
        """Test casting between string types and binary types."""
        import pyarrow as pa

        test_cases = [
            # string <-> binary
            (pa.string(), ["hello", "world", None], pa.binary(), [b"hello", b"world", None]),
            (pa.binary(), [b"hello", b"world", None], pa.string(), ["hello", "world", None]),
            # string <-> large_string
            (pa.string(), ["hello", "world", None], pa.large_string(), ["hello", "world", None]),
            (pa.large_string(), ["hello", "world", None], pa.string(), ["hello", "world", None]),
            # binary <-> large_binary
            (
                pa.binary(),
                [b"hello", b"world", None],
                pa.large_binary(),
                [b"hello", b"world", None],
            ),
            (
                pa.large_binary(),
                [b"hello", b"world", None],
                pa.binary(),
                [b"hello", b"world", None],
            ),
            # string <-> large_binary
            (pa.string(), ["hello", "world", None], pa.large_binary(), [b"hello", b"world", None]),
            (pa.large_string(), ["hello", "world", None], pa.binary(), [b"hello", b"world", None]),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_temporal_date_casts(self):
        """Test casting between date types."""
        import pyarrow as pa

        test_cases = [
            # date32 <-> date64
            (
                pa.date32(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
                pa.date64(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
            ),
            (
                pa.date64(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
                pa.date32(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
            ),
            # date -> string
            (
                pa.date32(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
                pa.string(),
                ["2024-01-01", "2024-12-31", None],
            ),
            (
                pa.date64(),
                [date(2024, 1, 1), date(2024, 12, 31), None],
                pa.string(),
                ["2024-01-01", "2024-12-31", None],
            ),
            # date -> timestamp
            (
                pa.date32(),
                [date(2024, 1, 1), None],
                pa.timestamp("s"),
                [datetime(2024, 1, 1), None],
            ),
            (
                pa.date64(),
                [date(2024, 1, 1), None],
                pa.timestamp("ms"),
                [datetime(2024, 1, 1), None],
            ),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_temporal_timestamp_unit_casts(self):
        """Test casting between timestamp units (upcast only with safe=True)."""
        import pyarrow as pa

        dt_s = datetime(2024, 1, 1, 12, 30, 45)
        dt_ms = datetime(2024, 1, 1, 12, 30, 45, 123000)

        test_cases = [
            # Upcast (safe)
            (pa.timestamp("s"), [dt_s, None], pa.timestamp("ms")),
            (pa.timestamp("s"), [dt_s, None], pa.timestamp("us")),
            (pa.timestamp("s"), [dt_s, None], pa.timestamp("ns")),
            (pa.timestamp("ms"), [dt_ms, None], pa.timestamp("us")),
            (pa.timestamp("ms"), [dt_ms, None], pa.timestamp("ns")),
            (pa.timestamp("us"), [dt_s, None], pa.timestamp("ns")),
            # timestamp -> string
            (pa.timestamp("s"), [dt_s, None], pa.string()),
            (pa.timestamp("ms"), [dt_ms, None], pa.string()),
        ]

        for source_type, source_values, target_type in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            self.assertIsNone(result[1].as_py())

    def test_temporal_duration_and_time_casts(self):
        """Test casting duration and time types."""
        import pyarrow as pa

        test_cases = [
            # duration upcasts
            (pa.duration("s"), [1, 2, 3, None], pa.duration("ms")),
            (pa.duration("s"), [1, 2, 3, None], pa.duration("us")),
            (pa.duration("ms"), [1000, 2000, 3000, None], pa.duration("us")),
            # time upcasts
            (pa.time32("s"), [3600, 7200, None], pa.time64("us")),
            (pa.time32("s"), [3600, 7200, None], pa.time64("ns")),
            (pa.time64("us"), [3600000000, 7200000000, None], pa.time64("ns")),
        ]

        for source_type, source_values, target_type in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            self.assertIsNone(result[len(source_values) - 1].as_py())

    def test_decimal_casts(self):
        """Test casting to/from decimal types (decimal128 and decimal256)."""
        import pyarrow as pa

        test_cases = [
            # numeric -> decimal128
            (pa.int8(), [1, 2, 3, None], pa.decimal128(12, 2)),
            (pa.int32(), [1, 2, 3, None], pa.decimal128(12, 2)),
            (pa.int64(), [1, 2, 3, None], pa.decimal128(21, 2)),  # int64 needs precision >= 21
            (pa.float32(), [1.5, 2.5, 3.5, None], pa.decimal128(12, 2)),
            (pa.float64(), [1.5, 2.5, 3.5, None], pa.decimal128(12, 2)),
            # decimal128 -> numeric
            (pa.decimal128(10, 0), [1, 2, 3, None], pa.int32()),
            (pa.decimal128(10, 0), [1, 2, 3, None], pa.int64()),
            (pa.decimal128(10, 1), [1, 2, 3, None], pa.float32()),
            (pa.decimal128(10, 1), [1, 2, 3, None], pa.float64()),
            # decimal128 <-> decimal256
            (pa.decimal128(10, 2), [1, 2, 3, None], pa.decimal256(20, 2)),
            (pa.decimal256(20, 2), [1, 2, 3, None], pa.decimal128(10, 2)),
            # string -> decimal
            (pa.string(), ["1.5", "2.5", "3.5", None], pa.decimal128(12, 2)),
            (pa.string(), ["1.5", "2.5", "3.5", None], pa.decimal256(20, 2)),
        ]

        for source_type, source_values, target_type in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            self.assertIsNone(result[3].as_py())

    def test_list_type_casts(self):
        """Test casting between list types (list, large_list, fixed_size_list)."""
        import pyarrow as pa

        test_cases = [
            # Element type casts
            (
                pa.list_(pa.int32()),
                [[1, 2, 3], [4, 5], None],
                pa.list_(pa.int64()),
                [[1, 2, 3], [4, 5], None],
            ),
            (
                pa.list_(pa.int32()),
                [[1, 2, 3], [4, 5], None],
                pa.list_(pa.float64()),
                [[1.0, 2.0, 3.0], [4.0, 5.0], None],
            ),
            (
                pa.list_(pa.float32()),
                [[1.0, 2.0], None],
                pa.list_(pa.float64()),
                [[1.0, 2.0], None],
            ),
            # list <-> large_list
            (
                pa.list_(pa.int32()),
                [[1, 2, 3], [4, 5], None],
                pa.large_list(pa.int32()),
                [[1, 2, 3], [4, 5], None],
            ),
            (
                pa.large_list(pa.int32()),
                [[1, 2, 3], [4, 5], None],
                pa.list_(pa.int32()),
                [[1, 2, 3], [4, 5], None],
            ),
            # list -> fixed_size_list
            (
                pa.list_(pa.int32()),
                [[1, 2, 3], [4, 5, 6], None],
                pa.list_(pa.int32(), 3),
                [[1, 2, 3], [4, 5, 6], None],
            ),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_struct_type_casts(self):
        """Test casting struct types (field types must be castable)."""
        import pyarrow as pa

        test_cases = [
            # Field type casts
            (
                pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                pa.struct([("a", pa.int64()), ("b", pa.int64())]),
                [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
            ),
            (
                pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                pa.struct([("a", pa.float64()), ("b", pa.float64())]),
                [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}, None],
            ),
            (
                pa.struct([("x", pa.float32())]),
                [{"x": 1.5}, {"x": 2.5}, None],
                pa.struct([("x", pa.float64())]),
                [{"x": 1.5}, {"x": 2.5}, None],
            ),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_map_type_casts(self):
        """Test casting map types (key/value types must be castable)."""
        import pyarrow as pa

        test_cases = [
            # Value type casts
            (
                pa.map_(pa.string(), pa.int32()),
                [[("a", 1), ("b", 2)], [("c", 3)], None],
                pa.map_(pa.string(), pa.int64()),
                [[("a", 1), ("b", 2)], [("c", 3)], None],
            ),
            (
                pa.map_(pa.string(), pa.int32()),
                [[("a", 1), ("b", 2)], [("c", 3)], None],
                pa.map_(pa.string(), pa.float64()),
                [[("a", 1.0), ("b", 2.0)], [("c", 3.0)], None],
            ),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_deeply_nested_types(self):
        """Test casting deeply nested types (multiple levels of nesting)."""
        import pyarrow as pa

        test_cases = [
            # Nested list: list<list<int32>> -> list<list<int64>>
            (
                pa.list_(pa.list_(pa.int32())),
                [[[1, 2], [3, 4]], [[5, 6]], None],
                pa.list_(pa.list_(pa.int64())),
                [[[1, 2], [3, 4]], [[5, 6]], None],
            ),
            # Nested list with type change: list<list<int32>> -> list<list<float64>>
            (
                pa.list_(pa.list_(pa.int32())),
                [[[1, 2], [3, 4]], [[5, 6]], None],
                pa.list_(pa.list_(pa.float64())),
                [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0]], None],
            ),
            # Triple nested list: list<list<list<int32>>> -> list<list<list<int64>>>
            (
                pa.list_(pa.list_(pa.list_(pa.int32()))),
                [[[[1, 2], [3]], [[4, 5]]], None],
                pa.list_(pa.list_(pa.list_(pa.int64()))),
                [[[[1, 2], [3]], [[4, 5]]], None],
            ),
            # Nested struct: struct<a: struct<b: int32>> -> struct<a: struct<b: int64>>
            (
                pa.struct([("a", pa.struct([("b", pa.int32())]))]),
                [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
                pa.struct([("a", pa.struct([("b", pa.int64())]))]),
                [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
            ),
            # Nested struct with type change: struct<x: struct<y: int32>> -> struct<x: struct<y: float64>>
            (
                pa.struct([("x", pa.struct([("y", pa.int32())]))]),
                [{"x": {"y": 1}}, {"x": {"y": 2}}, None],
                pa.struct([("x", pa.struct([("y", pa.float64())]))]),
                [{"x": {"y": 1.0}}, {"x": {"y": 2.0}}, None],
            ),
            # List of structs: list<struct<a: int32>> -> list<struct<a: int64>>
            (
                pa.list_(pa.struct([("a", pa.int32())])),
                [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
                pa.list_(pa.struct([("a", pa.int64())])),
                [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
            ),
            # Struct with list field: struct<vals: list<int32>> -> struct<vals: list<int64>>
            (
                pa.struct([("vals", pa.list_(pa.int32()))]),
                [{"vals": [1, 2, 3]}, {"vals": [4, 5]}, None],
                pa.struct([("vals", pa.list_(pa.int64()))]),
                [{"vals": [1, 2, 3]}, {"vals": [4, 5]}, None],
            ),
            # Map with list values: map<string, list<int32>> -> map<string, list<int64>>
            (
                pa.map_(pa.string(), pa.list_(pa.int32())),
                [[("a", [1, 2]), ("b", [3])], [("c", [4, 5])], None],
                pa.map_(pa.string(), pa.list_(pa.int64())),
                [[("a", [1, 2]), ("b", [3])], [("c", [4, 5])], None],
            ),
            # Struct with multiple nested fields: struct<a: list<int32>, b: struct<c: int32>>
            (
                pa.struct([("a", pa.list_(pa.int32())), ("b", pa.struct([("c", pa.int32())]))]),
                [{"a": [1, 2], "b": {"c": 3}}, {"a": [4], "b": {"c": 5}}, None],
                pa.struct([("a", pa.list_(pa.int64())), ("b", pa.struct([("c", pa.int64())]))]),
                [{"a": [1, 2], "b": {"c": 3}}, {"a": [4], "b": {"c": 5}}, None],
            ),
            # List of maps: list<map<string, int32>> -> list<map<string, int64>>
            (
                pa.list_(pa.map_(pa.string(), pa.int32())),
                [[[("a", 1), ("b", 2)]], [[("c", 3)]], None],
                pa.list_(pa.map_(pa.string(), pa.int64())),
                [[[("a", 1), ("b", 2)]], [[("c", 3)]], None],
            ),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    def test_special_float_values(self):
        """Test casting with NaN, Inf, -Inf."""
        import pyarrow as pa

        test_cases = [
            # float with special values -> string
            (
                pa.float32(),
                [1.0, float("nan"), float("inf"), float("-inf"), None],
                pa.string(),
                ["1", "nan", "inf", "-inf", None],
            ),
            (
                pa.float64(),
                [1.0, float("nan"), float("inf"), float("-inf"), None],
                pa.string(),
                ["1", "nan", "inf", "-inf", None],
            ),
            # float16 -> float64 with special values
            (pa.float16(), [1.0, float("inf"), None], pa.float64(), None),  # Checked manually
            # float32 -> float64 with special values
            (
                pa.float32(),
                [1.0, float("nan"), float("inf"), None],
                pa.float64(),
                None,
            ),  # Checked manually
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)

            if expected_values is not None:
                for i in range(len(expected_values) - 1):
                    self.assertEqual(result[i].as_py(), expected_values[i])
                self.assertIsNone(result[len(expected_values) - 1].as_py())
            else:
                # Manual checks for float->float with special values
                self.assertAlmostEqual(result[0].as_py(), 1.0, places=2)
                if len(source_values) > 2 and math.isnan(source_values[1]):
                    self.assertTrue(math.isnan(result[1].as_py()))
                    self.assertTrue(math.isinf(result[2].as_py()))
                else:
                    self.assertTrue(math.isinf(result[1].as_py()))

        # string with special values -> float
        for target_type in [pa.float32(), pa.float64()]:
            arr = pa.array(["1.0", "nan", "inf", "-inf", None], type=pa.string())
            result = arr.cast(target_type)
            self.assertEqual(result[0].as_py(), 1.0)
            self.assertTrue(math.isnan(result[1].as_py()))
            self.assertTrue(math.isinf(result[2].as_py()) and result[2].as_py() > 0)
            self.assertTrue(math.isinf(result[3].as_py()) and result[3].as_py() < 0)
            self.assertIsNone(result[4].as_py())

    def test_null_and_empty_arrays(self):
        """Test casting with null and empty arrays."""
        import pyarrow as pa

        test_cases = [
            # All nulls
            (pa.int8(), [None, None, None], pa.int64()),
            (pa.int32(), [None, None, None], pa.float64()),
            (pa.int32(), [None, None, None], pa.string()),
            (pa.float64(), [None, None, None], pa.int32()),
            # Empty arrays
            (pa.int8(), [], pa.int64()),
            (pa.int32(), [], pa.float64()),
            (pa.string(), [], pa.int32()),
            (pa.float64(), [], pa.string()),
        ]

        for source_type, source_values, target_type in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            self.assertEqual(len(result), len(source_values))
            for i in range(len(source_values)):
                self.assertIsNone(result[i].as_py())

    def test_boundary_values(self):
        """Test casting with boundary values for all integer types."""
        import pyarrow as pa

        test_cases = [
            # int8 boundaries
            (pa.int8(), [127, -128, None], pa.int16(), [127, -128, None]),
            (pa.int8(), [127, -128, None], pa.int64(), [127, -128, None]),
            # int16 boundaries
            (pa.int16(), [32767, -32768, None], pa.int32(), [32767, -32768, None]),
            # int32 boundaries
            (
                pa.int32(),
                [2147483647, -2147483648, None],
                pa.int64(),
                [2147483647, -2147483648, None],
            ),
            (
                pa.int32(),
                [2147483647, -2147483648, None],
                pa.float64(),
                [2147483647.0, -2147483648.0, None],
            ),
            # int64 to float64 (within safe range)
            (
                pa.int64(),
                [9007199254740992, -9007199254740992, None],
                pa.float64(),
                [9007199254740992.0, -9007199254740992.0, None],
            ),
            # uint boundaries
            (pa.uint8(), [255, 0, None], pa.uint16(), [255, 0, None]),
            (pa.uint16(), [65535, 0, None], pa.uint32(), [65535, 0, None]),
        ]

        for source_type, source_values, target_type, expected_values in test_cases:
            arr = pa.array(source_values, type=source_type)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)
            for i in range(len(expected_values) - 1):
                self.assertEqual(result[i].as_py(), expected_values[i])
            self.assertIsNone(result[len(expected_values) - 1].as_py())

    @unittest.skipIf(
        not have_pandas or not have_numpy, pandas_requirement_message or numpy_requirement_message
    )
    def test_numpy_pandas_dtypes_to_pyarrow(self):
        """Test casting arrays created from NumPy and Pandas dtypes."""
        import pyarrow as pa
        import pandas as pd
        import numpy as np

        # NumPy dtypes -> PyArrow casts
        numpy_test_cases = [
            # NumPy int types
            (np.array([1, 2, 3], dtype=np.int8), pa.int16()),
            (np.array([1, 2, 3], dtype=np.int32), pa.int64()),
            (np.array([1, 2, 3], dtype=np.int64), pa.float64()),
            # NumPy float types
            (np.array([1.5, 2.5, 3.5], dtype=np.float32), pa.float64()),
            (np.array([1.5, 2.5, 3.5], dtype=np.float64), pa.float32()),
            # NumPy uint types
            (np.array([1, 2, 3], dtype=np.uint8), pa.uint16()),
            (np.array([1, 2, 3], dtype=np.uint32), pa.uint64()),
        ]

        for np_arr, target_type in numpy_test_cases:
            arr = pa.array(np_arr)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)

        # Pandas nullable dtypes -> PyArrow casts
        pandas_test_cases = [
            # Pandas Int64Dtype
            (pd.Series([1, 2, 3, pd.NA], dtype=pd.Int64Dtype()), pa.int32()),
            (pd.Series([1, 2, 3, pd.NA], dtype=pd.Int64Dtype()), pa.float64()),
            # Pandas Float64Dtype
            (pd.Series([1.5, 2.5, 3.5, pd.NA], dtype=pd.Float64Dtype()), pa.float32()),
            # Pandas ArrowDtype
            (pd.Series([1, 2, 3, None], dtype=pd.ArrowDtype(pa.int32())), pa.int64()),
            (pd.Series([1, 2, 3, None], dtype=pd.ArrowDtype(pa.int64())), pa.float64()),
            (pd.Series([1.5, 2.5, 3.5, None], dtype=pd.ArrowDtype(pa.float64())), pa.float32()),
        ]

        for pd_series, target_type in pandas_test_cases:
            arr = pa.Array.from_pandas(pd_series)
            result = arr.cast(target_type)
            self.assertEqual(result.type, target_type)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
