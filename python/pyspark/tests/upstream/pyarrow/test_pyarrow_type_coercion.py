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

from decimal import Decimal
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


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowTypeCoercionTests(unittest.TestCase):
    """Test PyArrow's type coercion behavior for pa.array with explicit type parameter."""

    def test_int_to_float_coercion(self):
        """Test that integers can be coerced to float types."""
        import pyarrow as pa

        # int -> float64
        a = pa.array([1, 2, 3], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])
        for v in a.to_pylist():
            self.assertIsInstance(v, float)

        # int -> float32
        a = pa.array([1, 2, 3], type=pa.float32())
        self.assertEqual(a.type, pa.float32())
        values = a.to_pylist()
        self.assertAlmostEqual(values[0], 1.0, places=5)
        self.assertAlmostEqual(values[1], 2.0, places=5)
        self.assertAlmostEqual(values[2], 3.0, places=5)

        # int -> float16
        a = pa.array([1, 2, 3], type=pa.float16())
        self.assertEqual(a.type, pa.float16())

    def test_float_to_int_coercion(self):
        """Test that floats can be coerced to integer types (truncation)."""
        import pyarrow as pa

        # float -> int64 (whole numbers)
        a = pa.array([1.0, 2.0, 3.0], type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])
        for v in a.to_pylist():
            self.assertIsInstance(v, int)

        # float -> int32
        a = pa.array([1.0, 2.0, 3.0], type=pa.int32())
        self.assertEqual(a.type, pa.int32())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # float -> int8
        a = pa.array([1.0, 2.0, 3.0], type=pa.int8())
        self.assertEqual(a.type, pa.int8())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_int_to_decimal_coercion(self):
        """Test that integers can be coerced to decimal128 types."""
        import pyarrow as pa

        # int -> decimal128 with scale 0
        a = pa.array([1, 2, 3], type=pa.decimal128(10, 0))
        self.assertEqual(a.type, pa.decimal128(10, 0))
        self.assertEqual(a.to_pylist(), [Decimal("1"), Decimal("2"), Decimal("3")])
        for v in a.to_pylist():
            self.assertIsInstance(v, Decimal)

        # int -> decimal128 with scale > 0
        a = pa.array([1, 2, 3], type=pa.decimal128(10, 2))
        self.assertEqual(a.type, pa.decimal128(10, 2))
        self.assertEqual(
            a.to_pylist(), [Decimal("1.00"), Decimal("2.00"), Decimal("3.00")]
        )

        # int -> decimal128 with larger precision
        a = pa.array([1, 2, 3], type=pa.decimal128(38, 10))
        self.assertEqual(a.type, pa.decimal128(38, 10))

        # int -> decimal256
        a = pa.array([1, 2, 3], type=pa.decimal256(50, 0))
        self.assertEqual(a.type, pa.decimal256(50, 0))
        self.assertEqual(a.to_pylist(), [Decimal("1"), Decimal("2"), Decimal("3")])

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

        # Explicit casting via pc.cast works
        int_arr = pa.array([1, 2, 3])

        str_arr = pc.cast(int_arr, pa.string())
        self.assertEqual(str_arr.type, pa.string())
        self.assertEqual(str_arr.to_pylist(), ["1", "2", "3"])
        for v in str_arr.to_pylist():
            self.assertIsInstance(v, str)

        large_str_arr = pc.cast(int_arr, pa.large_string())
        self.assertEqual(large_str_arr.type, pa.large_string())
        self.assertEqual(large_str_arr.to_pylist(), ["1", "2", "3"])

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
        # 0 -> False, non-zero -> True
        self.assertEqual(bool_arr.to_pylist(), [False, True, True])

    def test_int_size_narrowing_coercion(self):
        """Test that integers can be narrowed to smaller integer types."""
        import pyarrow as pa

        # int64 -> int8
        a = pa.array([1, 2, 3], type=pa.int8())
        self.assertEqual(a.type, pa.int8())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int64 -> int16
        a = pa.array([1, 2, 3], type=pa.int16())
        self.assertEqual(a.type, pa.int16())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int64 -> int32
        a = pa.array([1, 2, 3], type=pa.int32())
        self.assertEqual(a.type, pa.int32())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_int_to_unsigned_coercion(self):
        """Test that non-negative integers can be coerced to unsigned types."""
        import pyarrow as pa

        # int -> uint8
        a = pa.array([1, 2, 3], type=pa.uint8())
        self.assertEqual(a.type, pa.uint8())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int -> uint16
        a = pa.array([1, 2, 3], type=pa.uint16())
        self.assertEqual(a.type, pa.uint16())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int -> uint32
        a = pa.array([1, 2, 3], type=pa.uint32())
        self.assertEqual(a.type, pa.uint32())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # int -> uint64
        a = pa.array([1, 2, 3], type=pa.uint64())
        self.assertEqual(a.type, pa.uint64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

    def test_null_handling_in_coercion(self):
        """Test that None values are preserved during type coercion."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # int with None -> float64
        a = pa.array([1, None, 3], type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, None, 3.0])

        # int with None -> decimal128
        a = pa.array([1, None, 3], type=pa.decimal128(10, 0))
        self.assertEqual(a.type, pa.decimal128(10, 0))
        self.assertEqual(a.to_pylist(), [Decimal("1"), None, Decimal("3")])

        # int with None -> int32 (narrowing)
        a = pa.array([1, None, 3], type=pa.int32())
        self.assertEqual(a.type, pa.int32())
        self.assertEqual(a.to_pylist(), [1, None, 3])

        # explicit cast to string with None
        int_arr = pa.array([1, None, 3])
        str_arr = pc.cast(int_arr, pa.string())
        self.assertEqual(str_arr.to_pylist(), ["1", None, "3"])

    def test_string_to_numeric_requires_explicit_cast(self):
        """Test that strings cannot be implicitly coerced to numeric types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # string -> int does NOT work implicitly
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array(["1", "2", "3"], type=pa.int64())

        # string -> float does NOT work implicitly
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError)):
            pa.array(["1.0", "2.0", "3.0"], type=pa.float64())

        # Explicit casting via pc.cast works
        str_arr = pa.array(["1", "2", "3"])

        int_arr = pc.cast(str_arr, pa.int64())
        self.assertEqual(int_arr.type, pa.int64())
        self.assertEqual(int_arr.to_pylist(), [1, 2, 3])

        float_arr = pc.cast(pa.array(["1.0", "2.0", "3.0"]), pa.float64())
        self.assertEqual(float_arr.type, pa.float64())
        self.assertEqual(float_arr.to_pylist(), [1.0, 2.0, 3.0])

    def test_decimal_to_float_requires_explicit_cast(self):
        """Test that Python Decimal cannot be implicitly coerced to float types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # Decimal -> float64 does NOT work implicitly
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([Decimal("1.5"), Decimal("2.5"), Decimal("3.5")], type=pa.float64())

        # Explicit casting via pc.cast works
        dec_arr = pa.array([Decimal("1.5"), Decimal("2.5"), Decimal("3.5")])
        float_arr = pc.cast(dec_arr, pa.float64())
        self.assertEqual(float_arr.type, pa.float64())
        self.assertEqual(float_arr.to_pylist(), [1.5, 2.5, 3.5])

    def test_float_to_decimal_requires_explicit_cast(self):
        """Test that floats cannot be implicitly coerced to decimal types."""
        import pyarrow as pa
        import pyarrow.compute as pc

        # float -> decimal128 does NOT work implicitly
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([1.5, 2.5, 3.5], type=pa.decimal128(10, 2))

        # Explicit casting via pc.cast works
        float_arr = pa.array([1.5, 2.5, 3.5])
        dec_arr = pc.cast(float_arr, pa.decimal128(10, 2))
        self.assertEqual(dec_arr.type, pa.decimal128(10, 2))
        self.assertEqual(
            dec_arr.to_pylist(), [Decimal("1.50"), Decimal("2.50"), Decimal("3.50")]
        )

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_coercion(self):
        """Test type coercion from pandas Series."""
        import pyarrow as pa
        import pandas as pd

        # pandas int64 -> float64
        s = pd.Series([1, 2, 3])
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, 2.0, 3.0])

        # pandas float64 -> int64
        s = pd.Series([1.0, 2.0, 3.0])
        a = pa.array(s, type=pa.int64())
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # pandas int64 -> decimal128 does NOT work implicitly (numpy-backed)
        s = pd.Series([1, 2, 3])
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(s, type=pa.decimal128(10, 0))

        # pandas nullable Int64 with None -> float64
        s = pd.Series([1, None, 3], dtype=pd.Int64Dtype())
        a = pa.array(s, type=pa.float64())
        self.assertEqual(a.type, pa.float64())
        self.assertEqual(a.to_pylist(), [1.0, None, 3.0])

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
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # numpy int64 -> int8 (narrowing)
        a = pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.int8())
        self.assertEqual(a.type, pa.int8())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # numpy int64 -> decimal128 does NOT work implicitly
        with self.assertRaises(pa.ArrowInvalid):
            pa.array(np.array([1, 2, 3], dtype=np.int64), type=pa.decimal128(10, 0))

    def test_list_type_coercion(self):
        """Test type coercion for nested list types."""
        import pyarrow as pa

        # list of int -> list of float
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.float64()))
        self.assertEqual(a.type, pa.list_(pa.float64()))
        self.assertEqual(a.to_pylist(), [[1.0, 2.0], [3.0, 4.0]])

        # list of int -> large_list of int
        a = pa.array([[1, 2], [3, 4]], type=pa.large_list(pa.int64()))
        self.assertEqual(a.type, pa.large_list(pa.int64()))
        self.assertEqual(a.to_pylist(), [[1, 2], [3, 4]])

        # list of int -> fixed_size_list
        a = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64(), 2))
        self.assertEqual(a.type, pa.list_(pa.int64(), 2))
        self.assertEqual(a.to_pylist(), [[1, 2], [3, 4]])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
