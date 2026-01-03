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
Unit tests for unified type coercion in PySpark.

These tests verify that the CoercionPolicy enum and DataType.coerce() method
correctly handle type coercion with different policies:
- PERMISSIVE: matches legacy pickle behavior (returns None for most type mismatches)
- WARN: same as PERMISSIVE but logs warnings when Arrow would behave differently

Note: STRICT policy skips coercion entirely in worker.py, so coerce() is never
called with STRICT. These unit tests only cover PERMISSIVE and WARN.

The goal is to enable Arrow by default without breaking existing code.
"""

import array
import datetime
import unittest
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    CoercionPolicy,
)


class CoercionPolicyTests(unittest.TestCase):
    """Tests for the CoercionPolicy enum."""

    def test_policy_values(self):
        """Test that all expected policy values exist."""
        # With StrEnum and auto(), values are lowercase names
        self.assertEqual(CoercionPolicy.PERMISSIVE, "permissive")
        self.assertEqual(CoercionPolicy.WARN, "warn")
        self.assertEqual(CoercionPolicy.STRICT, "strict")

    def test_policy_from_string(self):
        """Test creating policy from string value."""
        self.assertEqual(CoercionPolicy("permissive"), CoercionPolicy.PERMISSIVE)
        self.assertEqual(CoercionPolicy("warn"), CoercionPolicy.WARN)
        self.assertEqual(CoercionPolicy("strict"), CoercionPolicy.STRICT)


class BooleanCoercionTests(unittest.TestCase):
    """Tests for BooleanType coercion."""

    def setUp(self):
        self.boolean_type = BooleanType()

    def test_bool_to_boolean_permissive_and_warn(self):
        """bool -> boolean should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.boolean_type.coerce(True, policy), True)
            self.assertEqual(self.boolean_type.coerce(False, policy), False)

    def test_none_to_boolean_permissive_and_warn(self):
        """None -> boolean should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.boolean_type.coerce(None, policy))

    def test_int_to_boolean_permissive(self):
        """int -> boolean: PERMISSIVE returns None (pickle behavior)."""
        self.assertIsNone(self.boolean_type.coerce(1, CoercionPolicy.PERMISSIVE))
        self.assertIsNone(self.boolean_type.coerce(0, CoercionPolicy.PERMISSIVE))

    def test_int_to_boolean_warn(self):
        """int -> boolean: WARN returns None but logs warning."""
        with self.assertWarns(UserWarning):
            result = self.boolean_type.coerce(1, CoercionPolicy.WARN)
        self.assertIsNone(result)

    def test_float_to_boolean_permissive(self):
        """float -> boolean: PERMISSIVE returns None (pickle behavior)."""
        self.assertIsNone(self.boolean_type.coerce(1.0, CoercionPolicy.PERMISSIVE))
        self.assertIsNone(self.boolean_type.coerce(0.0, CoercionPolicy.PERMISSIVE))

    def test_string_to_boolean_permissive(self):
        """str -> boolean: PERMISSIVE returns None."""
        self.assertIsNone(self.boolean_type.coerce("true", CoercionPolicy.PERMISSIVE))


class IntegerCoercionTests(unittest.TestCase):
    """Tests for integer types (ByteType, ShortType, IntegerType, LongType)."""

    def setUp(self):
        self.int_types = [ByteType(), ShortType(), IntegerType(), LongType()]

    def test_int_to_int_permissive_and_warn(self):
        """int -> int should work for PERMISSIVE and WARN."""
        for int_type in self.int_types:
            for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
                self.assertEqual(int_type.coerce(1, policy), 1)
                self.assertEqual(int_type.coerce(0, policy), 0)
                self.assertEqual(int_type.coerce(-1, policy), -1)

    def test_none_to_int_permissive_and_warn(self):
        """None -> int should return None for PERMISSIVE and WARN."""
        for int_type in self.int_types:
            for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
                self.assertIsNone(int_type.coerce(None, policy))

    def test_bool_to_int_permissive(self):
        """bool -> int: PERMISSIVE returns None (pickle behavior)."""
        for int_type in self.int_types:
            self.assertIsNone(int_type.coerce(True, CoercionPolicy.PERMISSIVE))
            self.assertIsNone(int_type.coerce(False, CoercionPolicy.PERMISSIVE))

    def test_float_to_int_permissive(self):
        """float -> int: PERMISSIVE returns None (pickle behavior)."""
        for int_type in self.int_types:
            self.assertIsNone(int_type.coerce(1.0, CoercionPolicy.PERMISSIVE))
            self.assertIsNone(int_type.coerce(1.9, CoercionPolicy.PERMISSIVE))

    def test_decimal_to_int_permissive(self):
        """Decimal -> int: PERMISSIVE returns None (pickle behavior)."""
        for int_type in self.int_types:
            self.assertIsNone(int_type.coerce(Decimal(1), CoercionPolicy.PERMISSIVE))

    def test_string_to_int_permissive(self):
        """str -> int: PERMISSIVE returns None."""
        for int_type in self.int_types:
            self.assertIsNone(int_type.coerce("1", CoercionPolicy.PERMISSIVE))


class FloatCoercionTests(unittest.TestCase):
    """Tests for FloatType and DoubleType coercion."""

    def setUp(self):
        self.float_types = [FloatType(), DoubleType()]

    def test_float_to_float_permissive_and_warn(self):
        """float -> float should work for PERMISSIVE and WARN."""
        for float_type in self.float_types:
            for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
                self.assertEqual(float_type.coerce(1.0, policy), 1.0)
                self.assertEqual(float_type.coerce(0.0, policy), 0.0)

    def test_none_to_float_permissive_and_warn(self):
        """None -> float should return None for PERMISSIVE and WARN."""
        for float_type in self.float_types:
            for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
                self.assertIsNone(float_type.coerce(None, policy))

    def test_int_to_float_permissive(self):
        """int -> float: PERMISSIVE returns None (pickle behavior)."""
        for float_type in self.float_types:
            self.assertIsNone(float_type.coerce(1, CoercionPolicy.PERMISSIVE))

    def test_bool_to_float_permissive(self):
        """bool -> float: PERMISSIVE returns None (pickle behavior)."""
        for float_type in self.float_types:
            self.assertIsNone(float_type.coerce(True, CoercionPolicy.PERMISSIVE))
            self.assertIsNone(float_type.coerce(False, CoercionPolicy.PERMISSIVE))

    def test_decimal_to_float_permissive(self):
        """Decimal -> float: PERMISSIVE returns None (pickle behavior)."""
        for float_type in self.float_types:
            self.assertIsNone(float_type.coerce(Decimal(1), CoercionPolicy.PERMISSIVE))


class StringCoercionTests(unittest.TestCase):
    """Tests for StringType coercion."""

    def setUp(self):
        self.string_type = StringType()

    def test_str_to_string_permissive_and_warn(self):
        """str -> string should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.string_type.coerce("hello", policy), "hello")
            self.assertEqual(self.string_type.coerce("", policy), "")

    def test_none_to_string_permissive_and_warn(self):
        """None -> string should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.string_type.coerce(None, policy))

    def test_int_to_string_permissive_and_warn(self):
        """int -> string: all paths convert via str()."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.string_type.coerce(1, policy), "1")

    def test_bool_to_string_permissive(self):
        """bool -> string: PERMISSIVE returns 'true'/'false' (Java toString)."""
        self.assertEqual(
            self.string_type.coerce(True, CoercionPolicy.PERMISSIVE), "true"
        )
        self.assertEqual(
            self.string_type.coerce(False, CoercionPolicy.PERMISSIVE), "false"
        )

    def test_float_to_string_permissive_and_warn(self):
        """float -> string: converted via str()."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.string_type.coerce(1.0, policy), "1.0")


class DateCoercionTests(unittest.TestCase):
    """Tests for DateType coercion."""

    def setUp(self):
        self.date_type = DateType()

    def test_date_to_date_permissive_and_warn(self):
        """date -> date should work for PERMISSIVE and WARN."""
        date_val = datetime.date(1970, 1, 1)
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.date_type.coerce(date_val, policy), date_val)

    def test_none_to_date_permissive_and_warn(self):
        """None -> date should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.date_type.coerce(None, policy))

    def test_datetime_to_date_permissive_and_warn(self):
        """datetime -> date: PERMISSIVE/WARN extract date part."""
        dt_val = datetime.datetime(1970, 1, 1, 12, 30, 45)
        expected = datetime.date(1970, 1, 1)
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.date_type.coerce(dt_val, policy), expected)

    def test_int_to_date_permissive(self):
        """int -> date: PERMISSIVE raises TypeError (pickle behavior)."""
        with self.assertRaises(TypeError):
            self.date_type.coerce(1, CoercionPolicy.PERMISSIVE)


class TimestampCoercionTests(unittest.TestCase):
    """Tests for TimestampType coercion."""

    def setUp(self):
        self.timestamp_type = TimestampType()

    def test_datetime_to_timestamp_permissive_and_warn(self):
        """datetime -> timestamp should work for PERMISSIVE and WARN."""
        dt_val = datetime.datetime(1970, 1, 1, 0, 0, 0)
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.timestamp_type.coerce(dt_val, policy), dt_val)

    def test_none_to_timestamp_permissive_and_warn(self):
        """None -> timestamp should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.timestamp_type.coerce(None, policy))

    def test_date_to_timestamp_permissive_and_warn(self):
        """date -> timestamp: raises TypeError for PERMISSIVE and WARN."""
        date_val = datetime.date(1970, 1, 1)
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            with self.assertRaises(TypeError):
                self.timestamp_type.coerce(date_val, policy)

    def test_int_to_timestamp_permissive_and_warn(self):
        """int -> timestamp: raises TypeError for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            with self.assertRaises(TypeError):
                self.timestamp_type.coerce(1, policy)


class BinaryCoercionTests(unittest.TestCase):
    """Tests for BinaryType coercion."""

    def setUp(self):
        self.binary_type = BinaryType()

    def test_bytes_to_binary_permissive_and_warn(self):
        """bytes -> binary should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.binary_type.coerce(b"ABC", policy), b"ABC")

    def test_bytearray_to_binary_permissive_and_warn(self):
        """bytearray -> binary: PERMISSIVE/WARN convert to bytes."""
        ba = bytearray([65, 66, 67])
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.binary_type.coerce(ba, policy), b"ABC")

    def test_none_to_binary_permissive_and_warn(self):
        """None -> binary should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.binary_type.coerce(None, policy))

    def test_str_to_binary_permissive(self):
        """str -> binary: PERMISSIVE encodes (pickle behavior)."""
        self.assertEqual(
            self.binary_type.coerce("a", CoercionPolicy.PERMISSIVE), b"a"
        )


class ArrayCoercionTests(unittest.TestCase):
    """Tests for ArrayType coercion."""

    def setUp(self):
        self.array_int_type = ArrayType(IntegerType())

    def test_list_to_array_permissive_and_warn(self):
        """list -> array should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.array_int_type.coerce([1, 2, 3], policy), [1, 2, 3])

    def test_tuple_to_array_permissive_and_warn(self):
        """tuple -> array: PERMISSIVE/WARN convert to list."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.array_int_type.coerce((1, 2, 3), policy), [1, 2, 3])

    def test_none_to_array_permissive_and_warn(self):
        """None -> array should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.array_int_type.coerce(None, policy))

    def test_python_array_to_array_permissive_and_warn(self):
        """array.array -> array: PERMISSIVE/WARN convert to list."""
        arr = array.array("i", [1, 2, 3])
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.array_int_type.coerce(arr, policy), [1, 2, 3])

    def test_bytearray_to_int_array_permissive_and_warn(self):
        """bytearray -> array<int>: PERMISSIVE/WARN convert to list."""
        ba = bytearray([65, 66, 67])
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.array_int_type.coerce(ba, policy), [65, 66, 67])


class StructCoercionTests(unittest.TestCase):
    """Tests for StructType coercion."""

    def setUp(self):
        self.struct_type = StructType([StructField("_1", IntegerType())])

    def test_row_to_struct_permissive_and_warn(self):
        """Row -> struct should work for PERMISSIVE and WARN."""
        row = Row(_1=1)
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            result = self.struct_type.coerce(row, policy)
            self.assertEqual(result._1, 1)

    def test_tuple_to_struct_permissive_and_warn(self):
        """tuple -> struct: PERMISSIVE/WARN convert to Row."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            result = self.struct_type.coerce((1,), policy)
            self.assertEqual(result._1, 1)

    def test_dict_to_struct_permissive_and_warn(self):
        """dict -> struct: PERMISSIVE/WARN convert to Row."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            result = self.struct_type.coerce({"_1": 1}, policy)
            self.assertEqual(result._1, 1)

    def test_none_to_struct_permissive_and_warn(self):
        """None -> struct should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.struct_type.coerce(None, policy))

    def test_list_to_struct_permissive(self):
        """list -> struct: PERMISSIVE converts (pickle behavior)."""
        result = self.struct_type.coerce([1], CoercionPolicy.PERMISSIVE)
        self.assertEqual(result._1, 1)


class MapCoercionTests(unittest.TestCase):
    """Tests for MapType coercion."""

    def setUp(self):
        self.map_type = MapType(StringType(), IntegerType())

    def test_dict_to_map_permissive_and_warn(self):
        """dict -> map should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(self.map_type.coerce({"a": 1}, policy), {"a": 1})

    def test_none_to_map_permissive_and_warn(self):
        """None -> map should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.map_type.coerce(None, policy))

    def test_other_to_map_permissive(self):
        """other -> map: PERMISSIVE returns None."""
        self.assertIsNone(self.map_type.coerce([1, 2], CoercionPolicy.PERMISSIVE))


class DecimalCoercionTests(unittest.TestCase):
    """Tests for DecimalType coercion."""

    def setUp(self):
        self.decimal_type = DecimalType(10, 0)

    def test_decimal_to_decimal_permissive_and_warn(self):
        """Decimal -> decimal should work for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertEqual(
                self.decimal_type.coerce(Decimal("123"), policy), Decimal("123")
            )

    def test_none_to_decimal_permissive_and_warn(self):
        """None -> decimal should return None for PERMISSIVE and WARN."""
        for policy in [CoercionPolicy.PERMISSIVE, CoercionPolicy.WARN]:
            self.assertIsNone(self.decimal_type.coerce(None, policy))

    def test_int_to_decimal_permissive(self):
        """int -> decimal: PERMISSIVE returns None (pickle behavior)."""
        self.assertIsNone(self.decimal_type.coerce(1, CoercionPolicy.PERMISSIVE))


class DefaultPolicyTests(unittest.TestCase):
    """Tests that coerce() defaults to PERMISSIVE policy."""

    def test_default_policy_is_permissive(self):
        """coerce() without policy should behave like PERMISSIVE."""
        boolean_type = BooleanType()
        # int -> boolean: PERMISSIVE returns None (pickle behavior)
        self.assertIsNone(boolean_type.coerce(1))

        int_type = IntegerType()
        # float -> int: PERMISSIVE returns None (pickle behavior)
        self.assertIsNone(int_type.coerce(1.0))

        date_type = DateType()
        # int -> date: PERMISSIVE raises TypeError (pickle behavior)
        with self.assertRaises(TypeError):
            date_type.coerce(1)


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
