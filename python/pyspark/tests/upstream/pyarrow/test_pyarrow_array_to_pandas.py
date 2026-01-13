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
Tests for PyArrow pa.Array.to_pandas with default arguments.

This module tests how PyArrow converts Arrow arrays to pandas Series using
default arguments. PySpark uses pa.Array.to_pandas() to convert Arrow data
to numpy-backed pandas DataFrames for pandas UDFs.

With default arguments, to_pandas() converts Arrow arrays to numpy-backed
pandas Series, which is important for PySpark's type handling.

Key behaviors tested:
- Nullable integers become float64 with NaN for null values (numpy limitation)
- Nested types (struct, list, map) are converted to object dtype with Python objects
- ArrowDtype conversion (types_mapper=pd.ArrowDtype) is out of scope as it's not
  the default behavior; it may be covered in a separate test module.
"""

import datetime
import math
import unittest
from decimal import Decimal

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message if not have_pyarrow else pandas_requirement_message,
)
class PyArrowArrayToPandasTests(unittest.TestCase):
    """Test pa.Array.to_pandas with default arguments."""

    def test_integer_types(self):
        """Test conversion of integer arrays to pandas."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # (arrow_type, values, expected_numpy_dtype)
        test_cases = [
            (pa.int8(), [1, 2, 3], np.int8),
            (pa.int16(), [1, 2, 3], np.int16),
            (pa.int32(), [1, 2, 3], np.int32),
            (pa.int64(), [1, 2, 3], np.int64),
            (pa.uint8(), [1, 2, 3], np.uint8),
            (pa.uint16(), [1, 2, 3], np.uint16),
            (pa.uint32(), [1, 2, 3], np.uint32),
            (pa.uint64(), [1, 2, 3], np.uint64),
        ]

        for arrow_type, values, expected_dtype in test_cases:
            arr = pa.array(values, type=arrow_type)
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            self.assertEqual(
                series.dtype,
                expected_dtype,
                f"Dtype mismatch for {arrow_type}: expected {expected_dtype}, got {series.dtype}",
            )
            self.assertEqual(series.tolist(), values)

    def test_integer_with_nulls(self):
        """Test conversion of nullable integer arrays - converts to float64 by default."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # With nulls, integers convert to float64 by default (numpy doesn't have nullable int)
        arr = pa.array([1, None, 3], type=pa.int64())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # Default behavior: nullable integers become float64
        self.assertEqual(series.dtype, np.float64)
        self.assertEqual(series[0], 1.0)
        self.assertTrue(math.isnan(series[1]))
        self.assertEqual(series[2], 3.0)

    def test_float_types(self):
        """Test conversion of float arrays to pandas."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # (arrow_type, values, expected_dtype)
        test_cases = [
            (pa.float32(), [1.5, 2.5, 3.5], np.float32),
            (pa.float64(), [1.5, 2.5, 3.5], np.float64),
            (pa.float64(), [1.5, None, 3.5], np.float64),  # nulls become NaN
            (pa.float64(), [float("inf"), float("-inf"), float("nan")], np.float64),
        ]
        for arrow_type, values, expected_dtype in test_cases:
            with self.subTest(arrow_type=arrow_type, values=values):
                arr = pa.array(values, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, expected_dtype)

        # Verify null becomes NaN
        arr = pa.array([1.5, None, 3.5], type=pa.float64())
        series = arr.to_pandas()
        self.assertTrue(math.isnan(series[1]))

    def test_boolean_types(self):
        """Test conversion of boolean arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        # Without nulls - bool dtype
        arr = pa.array([True, False, True], type=pa.bool_())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, bool)
        self.assertEqual(series.tolist(), [True, False, True])

        # With nulls - object dtype
        arr = pa.array([True, None, False], type=pa.bool_())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), [True, None, False])

    def test_string_and_binary_types(self):
        """Test conversion of string and binary arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        # (data, arrow_type, expected_values)
        test_cases = [
            (["hello", "world", ""], pa.string(), ["hello", "world", ""]),
            (["hello", None, "world"], pa.string(), ["hello", None, "world"]),
            ([b"hello", b"world", b""], pa.binary(), [b"hello", b"world", b""]),
            (["hello", "world"], pa.large_string(), ["hello", "world"]),
            ([b"hello", b"world"], pa.large_binary(), [b"hello", b"world"]),
        ]
        for data, arrow_type, expected in test_cases:
            with self.subTest(arrow_type=arrow_type):
                arr = pa.array(data, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, object)
                self.assertEqual(series.tolist(), expected)

    def test_date_and_time_types(self):
        """Test conversion of date and time arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        dates = [datetime.date(2024, 1, 15), datetime.date(2024, 6, 30)]
        dates_with_null = [datetime.date(2024, 1, 15), None, datetime.date(2024, 6, 30)]
        times = [datetime.time(12, 30, 45, 123456), datetime.time(18, 0, 0)]

        # (data, arrow_type) - all become object dtype
        test_cases = [
            (dates, pa.date32()),
            (dates, pa.date64()),
            (dates_with_null, pa.date32()),
            (times, pa.time32("s")),
            (times, pa.time32("ms")),
            (times, pa.time64("us")),
            (times, pa.time64("ns")),
        ]
        for data, arrow_type in test_cases:
            with self.subTest(arrow_type=arrow_type):
                arr = pa.array(data, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, object)
                self.assertEqual(len(series), len(data))

    def test_timestamp_types(self):
        """Test conversion of timestamp arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        timestamps = [
            datetime.datetime(2024, 1, 15, 12, 30, 45),
            datetime.datetime(2024, 6, 30, 18, 0, 0),
        ]

        # Timezone-naive timestamps with various units
        for unit in ["s", "ms", "us", "ns"]:
            with self.subTest(unit=unit, tz=None):
                arr = pa.array(timestamps, type=pa.timestamp(unit))
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertTrue(str(series.dtype).startswith("datetime64"))

        # Timezone-aware timestamp
        from zoneinfo import ZoneInfo

        tz_timestamps = [
            datetime.datetime(2024, 1, 15, 12, 30, 45, tzinfo=ZoneInfo("UTC")),
            datetime.datetime(2024, 6, 30, 18, 0, 0, tzinfo=ZoneInfo("UTC")),
        ]
        arr = pa.array(tz_timestamps, type=pa.timestamp("us", tz="UTC"))
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertIsNotNone(series.dt.tz)

    def test_duration_types(self):
        """Test conversion of duration arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        durations = [
            datetime.timedelta(days=1, hours=2),
            datetime.timedelta(seconds=3600),
        ]

        for unit in ["s", "ms", "us", "ns"]:
            with self.subTest(unit=unit):
                arr = pa.array(durations, type=pa.duration(unit))
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertTrue(str(series.dtype).startswith("timedelta64"))

    def test_decimal_and_null_types(self):
        """Test conversion of decimal and null arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        # Decimal128
        decimals = [Decimal("123.45"), Decimal("-999.99"), Decimal("0.00")]
        arr = pa.array(decimals, type=pa.decimal128(10, 2))
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), decimals)

        # Null type
        arr = pa.array([None, None, None], type=pa.null())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertTrue(all(v is None for v in series))

    def test_empty_and_chunked_arrays(self):
        """Test conversion of empty and chunked arrays to pandas."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # Empty array
        arr = pa.array([], type=pa.int64())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(len(series), 0)
        self.assertEqual(series.dtype, np.int64)

        # Chunked array
        chunk1 = pa.array([1, 2, 3], type=pa.int64())
        chunk2 = pa.array([4, 5, 6], type=pa.int64())
        chunked = pa.chunked_array([chunk1, chunk2])
        series = chunked.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, np.int64)
        self.assertEqual(series.tolist(), [1, 2, 3, 4, 5, 6])

    # =========================================================================
    # Nested types - struct, list, map combinations
    # These tests are important because PySpark has encountered OOM and SEGFAULT
    # issues with nested types during Arrow to pandas conversion.
    # =========================================================================

    def test_list_types(self):
        """Test conversion of list arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        # (data, arrow_type, expected_length)
        test_cases = [
            # Simple list
            ([[1, 2], [3, 4, 5], []], pa.list_(pa.int64()), 3),
            # List of lists
            ([[[1, 2], [3]], [[4, 5, 6]], []], pa.list_(pa.list_(pa.int64())), 3),
            # List of lists with nulls
            ([[[1, None], [3]], None, [[None]]], pa.list_(pa.list_(pa.int64())), 3),
            # 3-level nested lists
            ([[[[1, 2]], [[3, 4]]], [[[5]]]], pa.list_(pa.list_(pa.list_(pa.int64()))), 2),
            # Large list (64-bit offsets)
            ([[1, 2, 3], [4, 5], []], pa.large_list(pa.int64()), 3),
            # Fixed-size list
            ([[1, 2, 3], [4, 5, 6]], pa.list_(pa.int64(), 3), 2),
            # List of strings with nulls
            ([["a", None, "b"], None, ["c"], []], pa.list_(pa.string()), 4),
        ]
        for data, arrow_type, expected_len in test_cases:
            with self.subTest(arrow_type=arrow_type):
                arr = pa.array(data, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, object)
                self.assertEqual(len(series), expected_len)

    def test_struct_types(self):
        """Test conversion of struct arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        simple_struct = pa.struct([("a", pa.int64()), ("b", pa.string())])
        nested_struct = pa.struct([("id", pa.int64()), ("inner", simple_struct)])

        level3 = pa.struct([("value", pa.int64())])
        level2 = pa.struct([("level3", level3)])
        level1 = pa.struct([("level2", level2)])

        struct_with_list = pa.struct([("name", pa.string()), ("values", pa.list_(pa.int64()))])

        # (data, arrow_type, expected_length)
        test_cases = [
            # Simple struct
            ([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], simple_struct, 2),
            # Struct in struct
            (
                [{"id": 1, "inner": {"a": 10, "b": "x"}}, {"id": 2, "inner": {"a": 20, "b": "y"}}],
                nested_struct,
                2,
            ),
            # Struct with nulls at various levels
            (
                [{"id": 1, "inner": {"a": 10, "b": "x"}}, {"id": 2, "inner": None}, None],
                nested_struct,
                3,
            ),
            # 3-level nested struct
            ([{"level2": {"level3": {"value": 42}}}], level1, 1),
            # Struct containing list
            (
                [{"name": "a", "values": [1, 2, 3]}, {"name": "b", "values": [4, 5]}],
                struct_with_list,
                2,
            ),
            # All nulls
            ([None, None, None], simple_struct, 3),
        ]
        for data, arrow_type, expected_len in test_cases:
            with self.subTest(arrow_type=arrow_type):
                arr = pa.array(data, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, object)
                self.assertEqual(len(series), expected_len)

    def test_list_of_structs(self):
        """Test conversion of list containing struct elements."""
        import pandas as pd
        import pyarrow as pa

        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        arr = pa.array(
            [[{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], [{"x": 3, "y": "c"}]],
            type=pa.list_(struct_type),
        )
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(len(series), 2)
        self.assertEqual(series[0][0]["x"], 1)
        self.assertEqual(series[0][0]["y"], "a")

    def test_map_types(self):
        """Test conversion of map types."""
        import pandas as pd
        import pyarrow as pa

        value_struct = pa.struct([("x", pa.int64()), ("y", pa.float64())])
        map_type = pa.map_(pa.string(), pa.int64())

        # (data, arrow_type, expected_length)
        test_cases = [
            # Simple map
            ([[("a", 1), ("b", 2)], [("c", 3)]], pa.map_(pa.string(), pa.int64()), 2),
            # Map with struct values
            (
                [[("key1", {"x": 1, "y": 1.5}), ("key2", {"x": 2, "y": 2.5})]],
                pa.map_(pa.string(), value_struct),
                1,
            ),
            # List of maps
            ([[[("a", 1)], [("b", 2)]], [[("c", 3)]]], pa.list_(map_type), 2),
        ]
        for data, arrow_type, expected_len in test_cases:
            with self.subTest(arrow_type=arrow_type):
                arr = pa.array(data, type=arrow_type)
                series = arr.to_pandas()
                self.assertIsInstance(series, pd.Series)
                self.assertEqual(series.dtype, object)
                self.assertEqual(len(series), expected_len)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
