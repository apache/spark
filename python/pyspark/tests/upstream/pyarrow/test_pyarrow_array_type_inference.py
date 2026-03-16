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
Test pa.array type inference for various input types.

This test monitors the behavior of PyArrow's type inference to ensure
PySpark's assumptions about PyArrow behavior remain valid across versions.

Test cases are organized by input category:
1. Nullable data - with None values
2. Plain Python instances - list, tuple, dict (struct)
3. Pandas instances - numpy-backed Series, nullable extension types, ArrowDtype
4. NumPy array - all numeric dtypes, datetime64, timedelta64
5. Nested types - list of list, struct, map
6. Explicit type specification - large_list, fixed_size_list, map_, large_string, large_binary
"""

import datetime
import unittest
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowArrayTypeInferenceTests(unittest.TestCase):
    """Test PyArrow's type inference behavior for pa.array."""

    # =========================================================================
    # Helper method
    # =========================================================================
    def _run_inference_tests(self, cases, use_expected_as_explicit=False):
        """Run type inference tests from a list of (data, expected_type) tuples.

        If use_expected_as_explicit is True, pass expected_type as explicit type to pa.array().
        """
        import pyarrow as pa

        for data, expected_type in cases:
            if use_expected_as_explicit:
                arr = pa.array(data, type=expected_type)
            else:
                arr = pa.array(data)
            self.assertEqual(arr.type, expected_type)

    # =========================================================================
    # 1. NULLABLE DATA - Test None value handling
    # =========================================================================
    def test_nullable_data(self):
        """Test type inference with nullable data (None values)."""
        import pyarrow as pa

        cases = [
            ([None], pa.null()),
            ([1, 2, 3], pa.int64()),
            ([1, 2, 3, None], pa.int64()),
            ([1.0, 2.0, 3.0], pa.float64()),
            ([1.0, 2.0, None], pa.float64()),
            (["a", "b", "c"], pa.string()),
            (["a", "b", None], pa.string()),
            ([True, False, None], pa.bool_()),
            ([datetime.date(2024, 1, 1), None], pa.date32()),
            ([datetime.datetime(2024, 1, 1, 12, 0, 0), None], pa.timestamp("us")),
            ([datetime.time(12, 30, 0), None], pa.time64("us")),
            ([datetime.timedelta(days=1), None], pa.duration("us")),
            ([b"hello", None], pa.binary()),
            ([Decimal("1.23"), None], pa.decimal128(3, 2)),
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 2. PLAIN PYTHON INSTANCES - list, tuple, dict
    # =========================================================================
    # 2a. Python list
    # -------------------------------------------------------------------------
    def test_plain_python_list(self):
        """Test type inference from Python lists."""
        import math
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = ZoneInfo("America/Los_Angeles")
        int64_max = 2**63 - 1
        int64_min = -(2**63)
        epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)
        epoch_date = datetime.date(1970, 1, 1)
        date1 = datetime.date(2024, 1, 1)
        date2 = datetime.date(2024, 1, 2)
        date_min = datetime.date(1, 1, 1)
        date_max = datetime.date(9999, 12, 31)
        dt1 = datetime.datetime(2024, 1, 1, 12)
        dt2 = datetime.datetime(2024, 1, 2, 12)
        dt1_sg = datetime.datetime(2024, 1, 1, 12, tzinfo=sg)
        dt2_sg = datetime.datetime(2024, 1, 2, 12, tzinfo=sg)
        dt1_la = datetime.datetime(2024, 1, 1, 12, tzinfo=la)
        dt2_la = datetime.datetime(2024, 1, 2, 12, tzinfo=la)
        time1 = datetime.time(12, 30, 0)
        time2 = datetime.time(13, 45, 0)
        time_min = datetime.time(0, 0, 0, 0)
        time_max = datetime.time(23, 59, 59, 999999)
        td1 = datetime.timedelta(days=1)
        td2 = datetime.timedelta(hours=2)
        td_zero = datetime.timedelta(0)
        td_neg = datetime.timedelta(days=-1)

        cases = [
            # Integer
            ([1, 2, 3], pa.int64()),
            ([int64_max], pa.int64()),
            ([int64_min], pa.int64()),
            ([int64_min, 0, int64_max], pa.int64()),
            ([0], pa.int64()),
            ([-1, -2, -3], pa.int64()),
            # Float
            ([1.0, 2.0, 3.0], pa.float64()),
            ([1, 2.0, 3], pa.float64()),
            ([float("nan")], pa.float64()),
            ([float("inf")], pa.float64()),
            ([float("-inf")], pa.float64()),
            ([float("nan"), float("inf"), float("-inf")], pa.float64()),
            ([1.0, float("nan"), 3.0], pa.float64()),
            ([1.7976931348623157e308], pa.float64()),
            ([2.2250738585072014e-308], pa.float64()),
            ([0.0, -0.0], pa.float64()),
            # String
            (["a", "b", "c"], pa.string()),
            (["你好", "世界", "中文"], pa.string()),
            (["こんにちは", "世界"], pa.string()),
            (["안녕하세요", "세계"], pa.string()),
            (["😀", "🎉", "🚀", "❤️"], pa.string()),
            (["Hello 👋", "World 🌍"], pa.string()),
            (["مرحبا", "العالم"], pa.string()),
            (["Привет", "Мир"], pa.string()),
            (["Café", "Résumé"], pa.string()),
            (["Grüß Gott", "Größe"], pa.string()),
            (["", "a", ""], pa.string()),
            (["  ", "\t", "\n"], pa.string()),
            (["foo\u200bbar"], pa.string()),
            # Boolean
            ([True, False, True], pa.bool_()),
            # Nested list
            ([[1, 2], [3, 4]], pa.list_(pa.int64())),
            # Temporal
            ([date1, date2], pa.date32()),
            ([date_min], pa.date32()),
            ([date_max], pa.date32()),
            ([dt1, dt2], pa.timestamp("us")),
            ([epoch], pa.timestamp("us")),
            ([epoch_date], pa.date32()),
            ([time1, time2], pa.time64("us")),
            ([time_min], pa.time64("us")),
            ([time_max], pa.time64("us")),
            ([td1, td2], pa.duration("us")),
            ([td_zero], pa.duration("us")),
            ([td_neg], pa.duration("us")),
            # Timezone-aware
            ([dt1_sg, dt2_sg], pa.timestamp("us", tz=sg)),
            ([dt1_la, dt2_la], pa.timestamp("us", tz=la)),
            # Binary and decimal
            ([b"hello", b"world"], pa.binary()),
            ([bytearray(b"hello"), bytearray(b"world")], pa.binary()),
            ([memoryview(b"foo"), memoryview(b"bar")], pa.binary()),
            ([b"a", bytearray(b"b"), memoryview(b"c")], pa.binary()),
            ([Decimal("1.23"), Decimal("4.56")], pa.decimal128(3, 2)),
            ([Decimal("123456.789"), Decimal("987654.321")], pa.decimal128(9, 3)),
        ]
        self._run_inference_tests(cases)

        # Verify NaN/Inf behavior
        arr = pa.array([float("nan")])
        self.assertTrue(math.isnan(arr.to_pylist()[0]))
        self.assertEqual(pa.array([float("inf")]).to_pylist()[0], float("inf"))

        # Verify overflow behavior (values > int64 max)
        with self.assertRaises(OverflowError):
            pa.array([2**63])  # Just over int64 max

        # Verify date boundary - Python date has min/max limits
        # date32 can represent much wider range, but Python date is limited to 1-9999 years
        # Test that date_min and date_max are accepted (already tested in cases above)
        # Test ValueError when trying to create date beyond Python's limits
        with self.assertRaises(ValueError):
            datetime.date(10000, 1, 1)  # Year 10000 exceeds Python date max
        with self.assertRaises(ValueError):
            datetime.date(0, 1, 1)  # Year 0 is before Python date min

    # -------------------------------------------------------------------------
    # 2b. Python tuple
    # -------------------------------------------------------------------------
    def test_plain_python_tuple(self):
        """Test type inference from Python tuples."""
        import pyarrow as pa

        date1 = datetime.date(2024, 1, 1)
        date2 = datetime.date(2024, 1, 2)
        dt1 = datetime.datetime(2024, 1, 1, 12)
        dt2 = datetime.datetime(2024, 1, 2, 12)
        time1 = datetime.time(12, 30, 0)
        time2 = datetime.time(13, 45, 0)
        td1 = datetime.timedelta(days=1)
        td2 = datetime.timedelta(hours=2)

        cases = [
            ((1, 2, 3), pa.int64()),
            ((1.0, 2.0, 3.0), pa.float64()),
            (("a", "b", "c"), pa.string()),
            ((True, False, True), pa.bool_()),
            ((date1, date2), pa.date32()),
            ((dt1, dt2), pa.timestamp("us")),
            ((time1, time2), pa.time64("us")),
            ((td1, td2), pa.duration("us")),
            ((b"hello", b"world"), pa.binary()),
            ((Decimal("1.23"), Decimal("4.56")), pa.decimal128(3, 2)),
        ]
        self._run_inference_tests(cases)

    # -------------------------------------------------------------------------
    # 2c. Python dict (struct)
    # -------------------------------------------------------------------------
    def test_plain_python_dict(self):
        """Test type inference from Python dicts (inferred as struct)."""
        import pyarrow as pa

        date1 = datetime.date(2024, 1, 1)
        dt1 = datetime.datetime(2024, 1, 1, 12)

        cases = [
            # Basic dict
            ([{}], pa.struct([])),
            ([{"x": 1}], pa.struct([("x", pa.int64())])),
            ([{"x": "a"}], pa.struct([("x", pa.string())])),
            (
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
            # Dict with None values
            (
                [{"a": 3, "b": None, "c": "s"}],
                pa.struct([("a", pa.int64()), ("b", pa.null()), ("c", pa.string())]),
            ),
            (
                [{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
            # Dict with mixed/different keys
            (
                [{"a": 3, "b": "s"}, {"x": 5, "a": 1.0}],
                pa.struct([("a", pa.float64()), ("b", pa.string()), ("x", pa.int64())]),
            ),
            # Dict with nested values
            (
                [{"x": 1, "y": [1, 2]}, {"x": 2, "y": [3, 4, 5]}],
                pa.struct([("x", pa.int64()), ("y", pa.list_(pa.int64()))]),
            ),
            ([{"outer": {"inner": 1}}], pa.struct([("outer", pa.struct([("inner", pa.int64())]))])),
            # Dict with various value types
            ([{"flag": True}, {"flag": False}], pa.struct([("flag", pa.bool_())])),
            ([{"val": 1.5}, {"val": 2.5}], pa.struct([("val", pa.float64())])),
            ([{"d": date1}], pa.struct([("d", pa.date32())])),
            ([{"ts": dt1}], pa.struct([("ts", pa.timestamp("us"))])),
            ([{"b": b"hello"}], pa.struct([("b", pa.binary())])),
            ([{"d": Decimal("1.23")}], pa.struct([("d", pa.decimal128(3, 2))])),
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 3. PANDAS INSTANCES - Series with various dtypes
    # =========================================================================
    # 3a. Pandas Series (numpy-backed)
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_numpy_backed(self):
        """Test type inference from numpy-backed pandas Series."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        sg = ZoneInfo("Asia/Singapore")
        la = "America/Los_Angeles"
        date1 = datetime.date(2024, 1, 1)
        date2 = datetime.date(2024, 1, 2)
        dt1_sg = datetime.datetime(2024, 1, 1, 12, tzinfo=sg)
        dt2_sg = datetime.datetime(2024, 1, 2, 12, tzinfo=sg)
        ts1_la = pd.Timestamp("2024-01-01 12:00:00", tz=la)
        ts2_la = pd.Timestamp("2024-01-02 12:00:00", tz=la)

        cases = [
            # Integer
            (pd.Series([1, 2, 3]), pa.int64()),
            (pd.Series([1, 2, None]), pa.float64()),
            # Float
            (pd.Series([1.0, 2.0, 3.0]), pa.float64()),
            (pd.Series([np.nan, 1.0, 2.0]), pa.float64()),
            (pd.Series([np.inf, 1.0, 2.0]), pa.float64()),
            (pd.Series([-np.inf, 1.0, 2.0]), pa.float64()),
            # String
            (pd.Series(["a", "b", "c"]), pa.string()),
            # Boolean
            (pd.Series([True, False, True]), pa.bool_()),
            # Temporal
            (pd.Series([date1, date2]), pa.date32()),
            (pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"])), pa.timestamp("ns")),
            (pd.Series([pd.Timestamp("1970-01-01")]), pa.timestamp("ns")),
            (pd.Series([pd.Timestamp.min]), pa.timestamp("ns")),
            (pd.Series([pd.Timestamp.max]), pa.timestamp("ns")),
            (pd.Series(pd.to_timedelta(["1 day", "2 hours"])), pa.duration("ns")),
            (pd.Series([pd.Timedelta(0)]), pa.duration("ns")),
            (pd.Series([pd.Timedelta.min]), pa.duration("ns")),
            (pd.Series([pd.Timedelta.max]), pa.duration("ns")),
            # Timezone-aware
            (pd.Series([dt1_sg, dt2_sg]), pa.timestamp("ns", tz="Asia/Singapore")),
            (pd.Series([ts1_la, ts2_la]), pa.timestamp("ns", tz=la)),
            # Binary
            (pd.Series([b"hello", b"world"]), pa.binary()),
            # Nested
            (pd.Series([[1, 2], [3, 4, 5]]), pa.list_(pa.int64())),
            (pd.Series([[1.0, 2.0], [3.0]]), pa.list_(pa.float64())),
            (pd.Series([["a", "b"], ["c"]]), pa.list_(pa.string())),
            (pd.Series([[1, 2], None, [3]]), pa.list_(pa.int64())),
        ]
        self._run_inference_tests(cases)

        # Verify pandas Timestamp range boundaries
        # pd.Timestamp.min and pd.Timestamp.max are already tested above
        # Test that dates outside Python date range raise ValueError in pandas
        with self.assertRaises((ValueError, pd.errors.OutOfBoundsDatetime)):
            pd.to_datetime(["10000-01-01"])  # Beyond Python date max

    # -------------------------------------------------------------------------
    # 3b. Pandas nullable extension types
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_nullable_extension(self):
        """Test type inference from pandas nullable extension types."""
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        cases = [
            # Integer
            (pd.Series([1, 2, 3], dtype=pd.Int8Dtype()), pa.int8()),
            (pd.Series([1, 2, 3], dtype=pd.Int16Dtype()), pa.int16()),
            (pd.Series([1, 2, 3], dtype=pd.Int32Dtype()), pa.int32()),
            (pd.Series([1, 2, 3], dtype=pd.Int64Dtype()), pa.int64()),
            (pd.Series([1, 2, None], dtype=pd.Int64Dtype()), pa.int64()),
            # Unsigned Integer
            (pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()), pa.uint8()),
            (pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()), pa.uint16()),
            (pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()), pa.uint32()),
            (pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()), pa.uint64()),
            # Float
            (pd.Series([1.0, 2.0, 3.0], dtype=pd.Float32Dtype()), pa.float32()),
            (pd.Series([1.0, 2.0, 3.0], dtype=pd.Float64Dtype()), pa.float64()),
            (pd.Series([1.0, 2.0, None], dtype=pd.Float64Dtype()), pa.float64()),
            (pd.Series([np.nan, 1.0], dtype=pd.Float64Dtype()), pa.float64()),
            (pd.Series([np.inf, 1.0], dtype=pd.Float64Dtype()), pa.float64()),
            # Boolean
            (pd.Series([True, False, True], dtype=pd.BooleanDtype()), pa.bool_()),
            (pd.Series([True, False, None], dtype=pd.BooleanDtype()), pa.bool_()),
            # String
            (pd.Series(["a", "b", "c"], dtype=pd.StringDtype()), pa.string()),
            (pd.Series(["a", "b", None], dtype=pd.StringDtype()), pa.string()),
        ]
        self._run_inference_tests(cases)

    # -------------------------------------------------------------------------
    # 3c. Pandas ArrowDtype
    # -------------------------------------------------------------------------
    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_pandas_series_arrow_dtype(self):
        """Test type inference from PyArrow-backed pandas Series (ArrowDtype)."""
        import pandas as pd
        import pyarrow as pa

        date1 = datetime.date(2024, 1, 1)
        date2 = datetime.date(2024, 1, 2)
        dt1 = datetime.datetime(2024, 1, 1, 12)
        dt2 = datetime.datetime(2024, 1, 2, 12)

        cases = [
            # Integers
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int8())), pa.int8()),
            (pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())), pa.int64()),
            # Floats
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float32())), pa.float32()),
            (pd.Series([1.0, 2.0], dtype=pd.ArrowDtype(pa.float64())), pa.float64()),
            # Large string/binary
            (pd.Series(["a", "b"], dtype=pd.ArrowDtype(pa.large_string())), pa.large_string()),
            (pd.Series([b"a", b"b"], dtype=pd.ArrowDtype(pa.large_binary())), pa.large_binary()),
            # Date
            (pd.Series([date1, date2], dtype=pd.ArrowDtype(pa.date32())), pa.date32()),
            # Timestamp with timezone
            (
                pd.Series([dt1, dt2], dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC"))),
                pa.timestamp("us", tz="UTC"),
            ),
            (
                pd.Series([dt1, dt2], dtype=pd.ArrowDtype(pa.timestamp("us", tz="Asia/Singapore"))),
                pa.timestamp("us", tz="Asia/Singapore"),
            ),
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 4. NUMPY ARRAY - all numeric dtypes, datetime64, timedelta64
    # =========================================================================
    def test_numpy_array(self):
        """Test type inference from numpy arrays."""
        import numpy as np
        import pyarrow as pa

        date_strs = ["2024-01-01", "2024-01-02"]
        datetime_strs = ["2024-01-01T12", "2024-01-02T12"]
        dt_nat_us = np.datetime64("2024-01-01T12:00", "us")
        nat_us = np.datetime64("NaT", "us")

        cases = [
            # Signed integers
            (np.array([1, 2, 3], dtype=np.int8), pa.int8()),
            (np.array([127], dtype=np.int8), pa.int8()),
            (np.array([-128], dtype=np.int8), pa.int8()),
            (np.array([-128, 0, 127], dtype=np.int8), pa.int8()),
            (np.array([1, 2, 3], dtype=np.int16), pa.int16()),
            (np.array([32767], dtype=np.int16), pa.int16()),
            (np.array([-32768], dtype=np.int16), pa.int16()),
            (np.array([1, 2, 3], dtype=np.int32), pa.int32()),
            (np.array([2147483647], dtype=np.int32), pa.int32()),
            (np.array([-2147483648], dtype=np.int32), pa.int32()),
            (np.array([1, 2, 3], dtype=np.int64), pa.int64()),
            # Unsigned integers
            (np.array([1, 2, 3], dtype=np.uint8), pa.uint8()),
            (np.array([255], dtype=np.uint8), pa.uint8()),
            (np.array([1, 2, 3], dtype=np.uint16), pa.uint16()),
            (np.array([65535], dtype=np.uint16), pa.uint16()),
            (np.array([1, 2, 3], dtype=np.uint32), pa.uint32()),
            (np.array([4294967295], dtype=np.uint32), pa.uint32()),
            (np.array([1, 2, 3], dtype=np.uint64), pa.uint64()),
            (np.array([2**64 - 1], dtype=np.uint64), pa.uint64()),
            # Floats
            (np.array([1.0, 2.0, 3.0], dtype=np.float16), pa.float16()),
            (np.array([1.0, 2.0, 3.0], dtype=np.float32), pa.float32()),
            (np.array([1.0, 2.0, 3.0], dtype=np.float64), pa.float64()),
            (np.array([1.0, 2.0, None], dtype=np.float64), pa.float64()),
            (np.array([np.nan, 1.0, 2.0]), pa.float64()),
            (np.array([np.nan, 1.0, None], dtype=np.float64), pa.float64()),
            (np.array([np.inf, 1.0, 2.0]), pa.float64()),
            (np.array([np.inf, 1.0, None], dtype=np.float64), pa.float64()),
            (np.array([-np.inf, 1.0, 2.0]), pa.float64()),
            (np.array([np.nan, np.inf, -np.inf]), pa.float64()),
            # Boolean
            (np.array([True, False, True], dtype=np.bool_), pa.bool_()),
            # String
            (np.array(["a", "b", "c"]), pa.string()),
            # Datetime64
            (np.array(date_strs, dtype="datetime64[D]"), pa.date32()),
            (np.array(["1970-01-01"], dtype="datetime64[D]"), pa.date32()),
            (np.array(["0001-01-01"], dtype="datetime64[D]"), pa.date32()),
            (np.array(["9999-12-31"], dtype="datetime64[D]"), pa.date32()),
            (np.array(datetime_strs, dtype="datetime64[s]"), pa.timestamp("s")),
            (np.array(datetime_strs, dtype="datetime64[ms]"), pa.timestamp("ms")),
            (np.array(datetime_strs, dtype="datetime64[us]"), pa.timestamp("us")),
            (np.array(datetime_strs, dtype="datetime64[ns]"), pa.timestamp("ns")),
            # Timedelta64
            (np.array([1, 2, 3], dtype="timedelta64[s]"), pa.duration("s")),
            (np.array([1, 2, 3], dtype="timedelta64[ms]"), pa.duration("ms")),
            (np.array([1, 2, 3], dtype="timedelta64[us]"), pa.duration("us")),
            (np.array([1, 2, 3], dtype="timedelta64[ns]"), pa.duration("ns")),
            # NaT with explicit unit
            ([np.datetime64("NaT", "ns")], pa.timestamp("ns")),
            ([np.datetime64("NaT", "us")], pa.timestamp("us")),
            ([np.timedelta64("NaT", "ns")], pa.duration("ns")),
            ([dt_nat_us, nat_us], pa.timestamp("us")),
        ]
        self._run_inference_tests(cases)

        # Verify NaT values produce null_count=1
        self.assertEqual(pa.array([np.datetime64("NaT", "ns")]).null_count, 1)
        self.assertEqual(pa.array([np.timedelta64("NaT", "ns")]).null_count, 1)

        # Verify datetime64[ns] overflow/wrap behavior
        # numpy datetime64[ns] has limited range (~1677-09-21 to ~2262-04-11)
        # Dates outside this range will overflow/wrap around
        overflow_before = np.array(["1600-01-01"], dtype="datetime64[ns]")
        overflow_after = np.array(["2300-01-01"], dtype="datetime64[ns]")
        # Verify wrapped values: 1600 wraps to ~2184, 2300 wraps to ~1715
        self.assertEqual(str(overflow_before[0])[:4], "2184")
        self.assertEqual(str(overflow_after[0])[:4], "1715")
        # Verify PyArrow accepts and preserves the wrapped values
        self.assertEqual(pa.array(overflow_before).type, pa.timestamp("ns"))
        self.assertEqual(pa.array(overflow_before)[0].as_py(), overflow_before[0])
        self.assertEqual(pa.array(overflow_after).type, pa.timestamp("ns"))
        self.assertEqual(pa.array(overflow_after)[0].as_py(), overflow_after[0])

    # =========================================================================
    # 5. NESTED TYPES - list of list, struct, map
    # =========================================================================
    def test_nested_list_types(self):
        """Test type inference for nested list types."""
        import pyarrow as pa

        cases = [
            # List types
            ([[1, 2], [3, 4, 5]], pa.list_(pa.int64())),
            ([[1.0, 2.0], [3.0]], pa.list_(pa.float64())),
            ([["a", "b"], ["c"]], pa.list_(pa.string())),
            ([[True, False], [True]], pa.list_(pa.bool_())),
            ([[1, 2], None, [3]], pa.list_(pa.int64())),
            ([[[1, 2], [3]], [[4, 5]]], pa.list_(pa.list_(pa.int64()))),
            ([[[[1]]]], pa.list_(pa.list_(pa.list_(pa.int64())))),
            ([[1, 2.0], [3, 4.0]], pa.list_(pa.float64())),
            # Struct types
            ([{"items": [1, 2, 3]}], pa.struct([("items", pa.list_(pa.int64()))])),
            ([{"outer": {"inner": 1}}], pa.struct([("outer", pa.struct([("inner", pa.int64())]))])),
            (
                [{"items": [1, 2], "nested": {"val": "a"}}],
                pa.struct(
                    [("items", pa.list_(pa.int64())), ("nested", pa.struct([("val", pa.string())]))]
                ),
            ),
            # List of struct
            ([[{"x": 1}, {"x": 2}]], pa.list_(pa.struct([("x", pa.int64())]))),
            ([[[{"x": 1}]]], pa.list_(pa.list_(pa.struct([("x", pa.int64())])))),
        ]
        self._run_inference_tests(cases)

    # =========================================================================
    # 6. EXPLICIT TYPE SPECIFICATION - large_list, fixed_size_list, map_, etc.
    # =========================================================================
    def test_explicit_type_specification(self):
        """Test types that require explicit specification.

        - Large types (large_list, large_string, large_binary) are NEVER auto-inferred.
        - Map types cannot be inferred without explicit type.
        """
        import pyarrow as pa

        cases = [
            # Large types
            ([[1, 2], [3, 4]], pa.large_list(pa.int64())),
            (["hello", "world"], pa.large_string()),
            ([b"hello", b"world"], pa.large_binary()),
            # Fixed size types
            ([[1, 2, 3], [4, 5, 6]], pa.list_(pa.int64(), 3)),
            # Map types (cannot be inferred, require explicit type)
            ([[("a", 1), ("b", 2)]], pa.map_(pa.string(), pa.int64())),
            ([[(1, "a"), (2, "b")]], pa.map_(pa.int64(), pa.string())),
            ([[("k", [1, 2, 3])]], pa.map_(pa.string(), pa.list_(pa.int64()))),
            ([[("k", {"x": 1})]], pa.map_(pa.string(), pa.struct([("x", pa.int64())]))),
            ([[[("a", 1)], [("b", 2)]]], pa.list_(pa.map_(pa.string(), pa.int64()))),
        ]
        self._run_inference_tests(cases, use_expected_as_explicit=True)

        # Map types cannot be inferred without explicit type - raises error
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("a", 1), ("b", 2)]])  # map_str_int
        with self.assertRaises(pa.ArrowInvalid):
            pa.array([[(1, "a"), (2, "b")]])  # map_int_str
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("k", [1, 2, 3])]])  # map_of_list
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[("k", {"x": 1})]])  # map_of_struct
        with self.assertRaises(pa.ArrowTypeError):
            pa.array([[[("a", 1)], [("b", 2)]]])  # list_of_map


if __name__ == "__main__":
    from pyspark.testing import main

    main()
