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
import unittest

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


# Test pa.array type inference for various input types.
# This test monitors the behavior of PyArrow's type inference to ensure
# PySpark's assumptions about PyArrow behavior remain valid across versions.
#
# Key input types tested:
# 1. nullable data (with None values)
# 2. plain Python instances (list, tuple, array)
# 3. pandas instances (pd.Series)
# 4. numpy instances (np.array)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowTypeInferenceTests(unittest.TestCase):
    """Test PyArrow's type inference behavior for pa.array."""

    def test_nullable_data(self):
        """Test type inference with nullable data (None values) for all types."""
        import pyarrow as pa

        # Single None value
        a = pa.array([None])
        self.assertEqual(a.type, pa.null())

        # Integers with None
        a = pa.array([1, 2, 3])
        self.assertEqual(a.type, pa.int64())

        a = pa.array([1, 2, 3, None])
        self.assertEqual(a.type, pa.int64())

        # Floats with None
        a = pa.array([1.0, 2.0, 3.0])
        self.assertEqual(a.type, pa.float64())

        a = pa.array([1.0, 2.0, None])
        self.assertEqual(a.type, pa.float64())

        # Strings with None
        a = pa.array(["a", "b", "c"])
        self.assertEqual(a.type, pa.string())

        a = pa.array(["a", "b", None])
        self.assertEqual(a.type, pa.string())

        # Booleans with None
        a = pa.array([True, False, None])
        self.assertEqual(a.type, pa.bool_())

        # Date with None
        a = pa.array([datetime.date(2024, 1, 1), None])
        self.assertEqual(a.type, pa.date32())

        # Datetime with None
        a = pa.array([datetime.datetime(2024, 1, 1, 12, 0, 0), None])
        self.assertEqual(a.type, pa.timestamp("us"))

        # Time with None
        a = pa.array([datetime.time(12, 30, 0), None])
        self.assertEqual(a.type, pa.time64("us"))

        # Timedelta with None
        a = pa.array([datetime.timedelta(days=1), None])
        self.assertEqual(a.type, pa.duration("us"))

        # Binary with None
        a = pa.array([b"hello", None])
        self.assertEqual(a.type, pa.binary())

        # Decimal with None
        a = pa.array([Decimal("1.23"), None])
        self.assertEqual(a.type, pa.decimal128(3, 2))

    def test_plain_python_list(self):
        """Test type inference from Python lists for all types."""
        import pyarrow as pa

        # Integer list
        a = pa.array([1, 2, 3])
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # Float list
        a = pa.array([1.0, 2.0, 3.0])
        self.assertEqual(a.type, pa.float64())

        # Mixed int and float
        a = pa.array([1, 2.0, 3])
        self.assertEqual(a.type, pa.float64())

        # String list
        a = pa.array(["a", "b", "c"])
        self.assertEqual(a.type, pa.string())

        # Boolean list
        a = pa.array([True, False, True])
        self.assertEqual(a.type, pa.bool_())

        # Nested list (list of lists)
        a = pa.array([[1, 2], [3, 4]])
        self.assertEqual(a.type, pa.list_(pa.int64()))

        # Date list
        a = pa.array([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)])
        self.assertEqual(a.type, pa.date32())

        # Datetime list (naive - no timezone)
        a = pa.array([datetime.datetime(2024, 1, 1, 12, 0, 0), datetime.datetime(2024, 1, 2, 12, 0, 0)])
        self.assertEqual(a.type, pa.timestamp("us"))

        # Time list
        a = pa.array([datetime.time(12, 30, 0), datetime.time(13, 45, 0)])
        self.assertEqual(a.type, pa.time64("us"))

        # Timedelta list (duration)
        a = pa.array([datetime.timedelta(days=1), datetime.timedelta(hours=2)])
        self.assertEqual(a.type, pa.duration("us"))

        # Binary list
        a = pa.array([b"hello", b"world"])
        self.assertEqual(a.type, pa.binary())

        # Decimal list
        a = pa.array([Decimal("1.23"), Decimal("4.56")])
        self.assertEqual(a.type, pa.decimal128(3, 2))

        a = pa.array([Decimal("123456.789"), Decimal("987654.321")])
        self.assertEqual(a.type, pa.decimal128(9, 3))

    def test_plain_python_tuple(self):
        """Test type inference from Python tuples for all types."""
        import pyarrow as pa

        # Tuple of integers
        a = pa.array((1, 2, 3))
        self.assertEqual(a.type, pa.int64())
        self.assertEqual(a.to_pylist(), [1, 2, 3])

        # Tuple of floats
        a = pa.array((1.0, 2.0, 3.0))
        self.assertEqual(a.type, pa.float64())

        # Tuple of strings
        a = pa.array(("a", "b", "c"))
        self.assertEqual(a.type, pa.string())

        # Tuple of booleans
        a = pa.array((True, False, True))
        self.assertEqual(a.type, pa.bool_())

        # Tuple of dates
        a = pa.array((datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))
        self.assertEqual(a.type, pa.date32())

        # Tuple of datetimes
        a = pa.array((datetime.datetime(2024, 1, 1, 12, 0, 0), datetime.datetime(2024, 1, 2, 12, 0, 0)))
        self.assertEqual(a.type, pa.timestamp("us"))

        # Tuple of times
        a = pa.array((datetime.time(12, 30, 0), datetime.time(13, 45, 0)))
        self.assertEqual(a.type, pa.time64("us"))

        # Tuple of timedeltas
        a = pa.array((datetime.timedelta(days=1), datetime.timedelta(hours=2)))
        self.assertEqual(a.type, pa.duration("us"))

        # Tuple of bytes
        a = pa.array((b"hello", b"world"))
        self.assertEqual(a.type, pa.binary())

        # Tuple of decimals
        a = pa.array((Decimal("1.23"), Decimal("4.56")))
        self.assertEqual(a.type, pa.decimal128(3, 2))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series(self):
        """Test type inference from pandas Series for all types."""
        import pyarrow as pa
        import pandas as pd

        # ========== numpy-backed Series (default) ==========

        # Integer Series (numpy int64)
        s = pd.Series([1, 2, 3])
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())

        # Float Series (numpy float64)
        s = pd.Series([1.0, 2.0, 3.0])
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())

        # String Series (object dtype)
        s = pd.Series(["a", "b", "c"])
        a = pa.array(s)
        self.assertEqual(a.type, pa.string())

        # Boolean Series (numpy bool)
        s = pd.Series([True, False, True])
        a = pa.array(s)
        self.assertEqual(a.type, pa.bool_())

        # Series with None (converts to float due to numpy)
        s = pd.Series([1, 2, None])
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())

        # Date Series (object dtype with date objects)
        s = pd.Series([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)])
        a = pa.array(s)
        self.assertEqual(a.type, pa.date32())

        # Datetime Series (pandas Timestamp, numpy datetime64[ns])
        s = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"]))
        a = pa.array(s)
        self.assertEqual(a.type, pa.timestamp("ns"))

        # Timedelta Series (numpy timedelta64[ns])
        s = pd.Series(pd.to_timedelta(["1 day", "2 hours"]))
        a = pa.array(s)
        self.assertEqual(a.type, pa.duration("ns"))

        # Binary Series (object dtype with bytes)
        s = pd.Series([b"hello", b"world"])
        a = pa.array(s)
        self.assertEqual(a.type, pa.binary())

        # ========== Pandas nullable extension types ==========

        # Nullable Integer types (all sizes)
        s = pd.Series([1, 2, 3], dtype=pd.Int8Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int8())

        s = pd.Series([1, 2, 3], dtype=pd.Int16Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int16())

        s = pd.Series([1, 2, 3], dtype=pd.Int32Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int32())

        s = pd.Series([1, 2, 3], dtype=pd.Int64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())

        # Nullable Integer with None
        s = pd.Series([1, 2, None], dtype=pd.Int64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())

        # Unsigned Integer types (all sizes)
        s = pd.Series([1, 2, 3], dtype=pd.UInt8Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.uint8())

        s = pd.Series([1, 2, 3], dtype=pd.UInt16Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.uint16())

        s = pd.Series([1, 2, 3], dtype=pd.UInt32Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.uint32())

        s = pd.Series([1, 2, 3], dtype=pd.UInt64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.uint64())

        # Nullable Float types
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.float32())

        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())

        s = pd.Series([1.0, 2.0, None], dtype=pd.Float64Dtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())

        # Nullable Boolean type
        s = pd.Series([True, False, True], dtype=pd.BooleanDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.bool_())

        s = pd.Series([True, False, None], dtype=pd.BooleanDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.bool_())

        # Nullable String type
        s = pd.Series(["a", "b", "c"], dtype=pd.StringDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.string())

        s = pd.Series(["a", "b", None], dtype=pd.StringDtype())
        a = pa.array(s)
        self.assertEqual(a.type, pa.string())

        # ========== PyArrow-backed Series (pd.ArrowDtype) ==========

        # ArrowDtype - integers
        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.int8())

        s = pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.int64())

        # ArrowDtype - floats
        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.ArrowDtype(pa.float32()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.float32())

        s = pd.Series([1.0, 2.0, 3.0], dtype=pd.ArrowDtype(pa.float64()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.float64())

        # ArrowDtype - string (large_string)
        s = pd.Series(["a", "b", "c"], dtype=pd.ArrowDtype(pa.large_string()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.large_string())

        # ArrowDtype - binary (large_binary)
        s = pd.Series([b"hello", b"world"], dtype=pd.ArrowDtype(pa.large_binary()))
        a = pa.array(s)
        self.assertEqual(a.type, pa.large_binary())

        # ArrowDtype - date
        s = pd.Series(
            [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            dtype=pd.ArrowDtype(pa.date32()),
        )
        a = pa.array(s)
        self.assertEqual(a.type, pa.date32())

        # ArrowDtype - timestamp with timezone
        tz = "UTC"
        pa_type = pa.timestamp("us", tz=tz)
        s = pd.Series(
            [datetime.datetime(2024, 1, 1, 12, 0, 0), datetime.datetime(2024, 1, 2, 12, 0, 0)],
            dtype=pd.ArrowDtype(pa_type),
        )
        a = pa.array(s)
        self.assertEqual(a.type, pa_type)

    def test_numpy_array(self):
        """Test type inference from numpy arrays for all types."""
        import pyarrow as pa
        import numpy as np

        # Integer array (various sizes)
        a = pa.array(np.array([1, 2, 3], dtype=np.int8))
        self.assertEqual(a.type, pa.int8())

        a = pa.array(np.array([1, 2, 3], dtype=np.int16))
        self.assertEqual(a.type, pa.int16())

        a = pa.array(np.array([1, 2, 3], dtype=np.int32))
        self.assertEqual(a.type, pa.int32())

        a = pa.array(np.array([1, 2, 3], dtype=np.int64))
        self.assertEqual(a.type, pa.int64())

        # Unsigned integer array
        a = pa.array(np.array([1, 2, 3], dtype=np.uint8))
        self.assertEqual(a.type, pa.uint8())

        a = pa.array(np.array([1, 2, 3], dtype=np.uint16))
        self.assertEqual(a.type, pa.uint16())

        a = pa.array(np.array([1, 2, 3], dtype=np.uint32))
        self.assertEqual(a.type, pa.uint32())

        a = pa.array(np.array([1, 2, 3], dtype=np.uint64))
        self.assertEqual(a.type, pa.uint64())

        # Float array (various sizes)
        a = pa.array(np.array([1.0, 2.0, 3.0], dtype=np.float16))
        self.assertEqual(a.type, pa.float16())

        a = pa.array(np.array([1.0, 2.0, 3.0], dtype=np.float32))
        self.assertEqual(a.type, pa.float32())

        a = pa.array(np.array([1.0, 2.0, 3.0], dtype=np.float64))
        self.assertEqual(a.type, pa.float64())

        # Boolean array
        a = pa.array(np.array([True, False, True], dtype=np.bool_))
        self.assertEqual(a.type, pa.bool_())

        # String array (numpy uses object dtype for strings)
        a = pa.array(np.array(["a", "b", "c"]))
        self.assertEqual(a.type, pa.string())

        # Datetime array with day resolution
        a = pa.array(np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[D]"))
        self.assertEqual(a.type, pa.date32())

        # Datetime array with second resolution
        a = pa.array(np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[s]"))
        self.assertEqual(a.type, pa.timestamp("s"))

        # Datetime array with millisecond resolution
        a = pa.array(np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[ms]"))
        self.assertEqual(a.type, pa.timestamp("ms"))

        # Datetime array with microsecond resolution
        a = pa.array(np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[us]"))
        self.assertEqual(a.type, pa.timestamp("us"))

        # Datetime array with nanosecond resolution
        a = pa.array(np.array(["2024-01-01T12:00:00", "2024-01-02T12:00:00"], dtype="datetime64[ns]"))
        self.assertEqual(a.type, pa.timestamp("ns"))

        # Timedelta array
        a = pa.array(np.array([1, 2, 3], dtype="timedelta64[s]"))
        self.assertEqual(a.type, pa.duration("s"))

        a = pa.array(np.array([1, 2, 3], dtype="timedelta64[ms]"))
        self.assertEqual(a.type, pa.duration("ms"))

        a = pa.array(np.array([1, 2, 3], dtype="timedelta64[us]"))
        self.assertEqual(a.type, pa.duration("us"))

        a = pa.array(np.array([1, 2, 3], dtype="timedelta64[ns]"))
        self.assertEqual(a.type, pa.duration("ns"))

    def test_nested_and_complex_types(self):
        """Test type inference for nested and complex types (struct, map, list)."""
        import pyarrow as pa

        # ========== List types ==========

        # List of integers
        a = pa.array([[1, 2], [3, 4, 5]])
        self.assertEqual(a.type, pa.list_(pa.int64()))

        # List of floats
        a = pa.array([[1.0, 2.0], [3.0]])
        self.assertEqual(a.type, pa.list_(pa.float64()))

        # List of strings
        a = pa.array([["a", "b"], ["c"]])
        self.assertEqual(a.type, pa.list_(pa.string()))

        # List with None
        a = pa.array([[1, 2], None, [3]])
        self.assertEqual(a.type, pa.list_(pa.int64()))

        # Nested list (list of lists)
        a = pa.array([[[1, 2], [3]], [[4, 5]]])
        self.assertEqual(a.type, pa.list_(pa.list_(pa.int64())))

        # List with explicit large_list type
        a = pa.array([[1, 2], [3, 4]], type=pa.large_list(pa.int64()))
        self.assertEqual(a.type, pa.large_list(pa.int64()))

        # Fixed size list
        a = pa.array([[1, 2, 3], [4, 5, 6]], type=pa.list_(pa.int64(), 3))
        self.assertEqual(a.type, pa.list_(pa.int64(), 3))

        # ========== Struct types ==========

        # Struct from list of dicts
        a = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
        expected_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        self.assertEqual(a.type, expected_type)

        # Struct with None values
        a = pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}])
        self.assertEqual(a.type, expected_type)

        # Struct with nested types
        a = pa.array([{"x": 1, "y": [1, 2]}, {"x": 2, "y": [3, 4, 5]}])
        expected_type = pa.struct([("x", pa.int64()), ("y", pa.list_(pa.int64()))])
        self.assertEqual(a.type, expected_type)

        # ========== Map types (requires explicit type) ==========

        # Map type with explicit specification
        map_type = pa.map_(pa.string(), pa.int64())
        a = pa.array([[("a", 1), ("b", 2)], [("c", 3)]], type=map_type)
        self.assertEqual(a.type, map_type)

        # ========== Large string/binary types (explicit) ==========

        # large_string
        a = pa.array(["hello", "world"], type=pa.large_string())
        self.assertEqual(a.type, pa.large_string())

        # large_binary
        a = pa.array([b"hello", b"world"], type=pa.large_binary())
        self.assertEqual(a.type, pa.large_binary())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
