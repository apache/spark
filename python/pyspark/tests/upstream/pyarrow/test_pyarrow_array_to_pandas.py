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

        test_cases = [
            (pa.float32(), [1.5, 2.5, 3.5], np.float32),
            (pa.float64(), [1.5, 2.5, 3.5], np.float64),
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

    def test_float_with_nulls(self):
        """Test conversion of nullable float arrays - nulls become NaN."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([1.5, None, 3.5], type=pa.float64())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, np.float64)
        self.assertEqual(series[0], 1.5)
        self.assertTrue(math.isnan(series[1]))
        self.assertEqual(series[2], 3.5)

    def test_float_special_values(self):
        """Test conversion of float arrays with inf and nan."""
        import numpy as np
        import pyarrow as pa

        arr = pa.array([float("inf"), float("-inf"), float("nan")], type=pa.float64())
        series = arr.to_pandas()
        self.assertEqual(series.dtype, np.float64)
        self.assertEqual(series[0], float("inf"))
        self.assertEqual(series[1], float("-inf"))
        self.assertTrue(math.isnan(series[2]))

    def test_boolean_type(self):
        """Test conversion of boolean arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([True, False, True], type=pa.bool_())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, bool)
        self.assertEqual(series.tolist(), [True, False, True])

    def test_boolean_with_nulls(self):
        """Test conversion of nullable boolean arrays - becomes object dtype."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([True, None, False], type=pa.bool_())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # Nullable booleans become object dtype
        self.assertEqual(series.dtype, object)
        self.assertEqual(series[0], True)
        self.assertIsNone(series[1])
        self.assertEqual(series[2], False)

    def test_string_type(self):
        """Test conversion of string arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array(["hello", "world", ""], type=pa.string())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), ["hello", "world", ""])

    def test_string_with_nulls(self):
        """Test conversion of nullable string arrays."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array(["hello", None, "world"], type=pa.string())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series[0], "hello")
        self.assertIsNone(series[1])
        self.assertEqual(series[2], "world")

    def test_binary_type(self):
        """Test conversion of binary arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([b"hello", b"world", b""], type=pa.binary())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), [b"hello", b"world", b""])

    def test_large_string_and_binary(self):
        """Test conversion of large_string and large_binary arrays."""
        import pandas as pd
        import pyarrow as pa

        # large_string
        arr = pa.array(["hello", "world"], type=pa.large_string())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), ["hello", "world"])

        # large_binary
        arr = pa.array([b"hello", b"world"], type=pa.large_binary())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        self.assertEqual(series.tolist(), [b"hello", b"world"])

    def test_date_types(self):
        """Test conversion of date32 and date64 arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        dates = [datetime.date(2024, 1, 15), datetime.date(2024, 6, 30)]

        # Both date32 and date64 become object dtype with datetime.date objects
        for date_type in [pa.date32(), pa.date64()]:
            arr = pa.array(dates, type=date_type)
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            self.assertEqual(
                series.dtype,
                object,
                f"Dtype mismatch for {date_type}",
            )
            self.assertEqual(series[0], datetime.date(2024, 1, 15))
            self.assertEqual(series[1], datetime.date(2024, 6, 30))

    def test_date_with_nulls(self):
        """Test conversion of nullable date arrays."""
        import pandas as pd
        import pyarrow as pa

        dates = [datetime.date(2024, 1, 15), None, datetime.date(2024, 6, 30)]
        arr = pa.array(dates, type=pa.date32())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # With nulls, object dtype with None for null
        self.assertEqual(series.dtype, object)
        self.assertEqual(series[0], datetime.date(2024, 1, 15))
        self.assertIsNone(series[1])
        self.assertEqual(series[2], datetime.date(2024, 6, 30))

    def test_time_types(self):
        """Test conversion of time32 and time64 arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        time_val = datetime.time(12, 30, 45, 123456)
        times = [time_val, datetime.time(18, 0, 0)]

        # time32 with second and millisecond resolution
        for unit in ["s", "ms"]:
            arr = pa.array(times, type=pa.time32(unit))
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            self.assertEqual(
                series.dtype,
                object,
                f"Dtype mismatch for time32[{unit}]",
            )

        # time64 with microsecond and nanosecond resolution
        for unit in ["us", "ns"]:
            arr = pa.array(times, type=pa.time64(unit))
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            self.assertEqual(
                series.dtype,
                object,
                f"Dtype mismatch for time64[{unit}]",
            )

    def test_timestamp_naive(self):
        """Test conversion of timezone-naive timestamp arrays."""
        import pandas as pd
        import pyarrow as pa

        timestamps = [
            datetime.datetime(2024, 1, 15, 12, 30, 45),
            datetime.datetime(2024, 6, 30, 18, 0, 0),
        ]

        for unit in ["s", "ms", "us", "ns"]:
            arr = pa.array(timestamps, type=pa.timestamp(unit))
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            # Timestamps convert to datetime64[ns] by default
            self.assertTrue(
                str(series.dtype).startswith("datetime64"),
                f"Expected datetime64 for timestamp[{unit}], got {series.dtype}",
            )

    def test_timestamp_with_timezone(self):
        """Test conversion of timezone-aware timestamp arrays."""
        import pandas as pd
        import pyarrow as pa
        from zoneinfo import ZoneInfo

        timestamps = [
            datetime.datetime(2024, 1, 15, 12, 30, 45, tzinfo=ZoneInfo("UTC")),
            datetime.datetime(2024, 6, 30, 18, 0, 0, tzinfo=ZoneInfo("UTC")),
        ]
        arr = pa.array(timestamps, type=pa.timestamp("us", tz="UTC"))
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # Timezone-aware timestamps preserve timezone
        self.assertIsNotNone(series.dt.tz)

    def test_duration_type(self):
        """Test conversion of duration arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        durations = [
            datetime.timedelta(days=1, hours=2),
            datetime.timedelta(seconds=3600),
        ]

        for unit in ["s", "ms", "us", "ns"]:
            arr = pa.array(durations, type=pa.duration(unit))
            series = arr.to_pandas()
            self.assertIsInstance(series, pd.Series)
            # Durations convert to timedelta64[ns]
            self.assertTrue(
                str(series.dtype).startswith("timedelta64"),
                f"Expected timedelta64 for duration[{unit}], got {series.dtype}",
            )

    def test_decimal_type(self):
        """Test conversion of decimal arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        decimals = [Decimal("123.45"), Decimal("-999.99"), Decimal("0.00")]
        arr = pa.array(decimals, type=pa.decimal128(10, 2))
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # Decimals become object dtype with Decimal objects
        self.assertEqual(series.dtype, object)
        self.assertEqual(series[0], Decimal("123.45"))
        self.assertEqual(series[1], Decimal("-999.99"))
        self.assertEqual(series[2], Decimal("0.00"))

    def test_null_type(self):
        """Test conversion of null arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([None, None, None], type=pa.null())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        # Null arrays become object dtype with None values
        self.assertEqual(series.dtype, object)
        self.assertTrue(all(v is None for v in series))

    def test_list_type(self):
        """Test conversion of list arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([[1, 2], [3, 4, 5], []], type=pa.list_(pa.int64()))
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        # List elements are converted to numpy arrays
        self.assertEqual(len(series[0]), 2)
        self.assertEqual(len(series[1]), 3)
        self.assertEqual(len(series[2]), 0)

    def test_struct_type(self):
        """Test conversion of struct arrays to pandas."""
        import pandas as pd
        import pyarrow as pa

        struct_type = pa.struct([("a", pa.int64()), ("b", pa.string())])
        arr = pa.array(
            [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
            type=struct_type,
        )
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, object)
        # Struct elements become dicts
        self.assertEqual(series[0]["a"], 1)
        self.assertEqual(series[0]["b"], "x")

    def test_empty_array(self):
        """Test conversion of empty arrays to pandas."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        arr = pa.array([], type=pa.int64())
        series = arr.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(len(series), 0)
        self.assertEqual(series.dtype, np.int64)

    def test_chunked_array(self):
        """Test conversion of chunked arrays to pandas."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        chunk1 = pa.array([1, 2, 3], type=pa.int64())
        chunk2 = pa.array([4, 5, 6], type=pa.int64())
        chunked = pa.chunked_array([chunk1, chunk2])
        series = chunked.to_pandas()
        self.assertIsInstance(series, pd.Series)
        self.assertEqual(series.dtype, np.int64)
        self.assertEqual(series.tolist(), [1, 2, 3, 4, 5, 6])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
