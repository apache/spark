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

import os
import datetime
from zoneinfo import ZoneInfo
import unittest

from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


# Test PYARROW_IGNORE_TIMEZONE introduced in PyArrow 2.0,
# https://arrow.apache.org/blog/2020/10/22/2.0.0-release/
# Conversion of timezone aware datetimes to and/from pyarrow arrays including pandas
# now round-trip preserving timezone. To use the old behavior (e.g. for spark) set
# the environment variable PYARROW_IGNORE_TIMEZONE to a truthy
# value (i.e. PYARROW_IGNORE_TIMEZONE=1)

# Summary:
# 1, pa.array and pa.Array.from_pandas
#    a, when PYARROW_IGNORE_TIMEZONE=1, and input is list[datetime.datetime|pd.Timestamp]
#       the timezone is used to infer the pyarrow datatype,
#       but not used in computing the underlying value (or treated as UTC time);
#    b, PYARROW_IGNORE_TIMEZONE takes no effect when the input is a Pandas Series;
# 2, In pa.scalar, PYARROW_IGNORE_TIMEZONE takes no effect.


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowIgnoreTimeZoneTests(unittest.TestCase):
    def test_timezone_with_python(self):
        import pyarrow as pa

        tz = "Asia/Singapore"
        ts1 = datetime.datetime(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo(tz))
        ts2 = datetime.datetime(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo("UTC"))
        pa_type = pa.timestamp("us", tz=tz)

        os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
        for s in [
            pa.scalar(ts1),
            pa.scalar(ts1, type=pa_type),
        ]:
            self.assertEqual(s.type, pa_type)
            self.assertEqual(s.as_py(), ts1)

        # when PYARROW_IGNORE_TIMEZONE=1 and input is list[datetime.datetime]
        # tzinfo is used to infer the datatype, but not used in computing the underlying value
        for a in [
            pa.array([ts1]),
            pa.array([ts1], type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0]]:
                self.assertNotEqual(v, ts1)
                self.assertEqual(v, ts2)

        for a in [
            pa.array([pa.scalar(ts1)]),
            pa.array([pa.scalar(ts1)], type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0]]:
                self.assertEqual(v, ts1)

        del os.environ["PYARROW_IGNORE_TIMEZONE"]
        for s in [
            pa.scalar(ts1),
            pa.scalar(ts1, type=pa_type),
        ]:
            self.assertEqual(s.type, pa_type)
            self.assertEqual(s.as_py(), ts1)

        for a in [
            pa.array([ts1]),
            pa.array([ts1], type=pa_type),
            pa.array([pa.scalar(ts1)]),
            pa.array([pa.scalar(ts1)], type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0]]:
                self.assertEqual(v, ts1)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_timezone_with_pandas(self):
        import pyarrow as pa
        import pandas as pd

        tz = "Asia/Singapore"
        ts1 = pd.Timestamp(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo(tz))
        ts2 = pd.Timestamp(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo("UTC"))
        pa_type = pa.timestamp("us", tz=tz)

        # numpy-backed series (with pandas extension type)
        # The corresponding numpy type np.datetime64 is timezone-naive, so no need to test
        ser1 = pd.Series([ts1], dtype=pd.DatetimeTZDtype("us", tz=tz))
        self.assertEqual(ser1.dtype.unit, "us")
        self.assertEqual(ser1.dtype.tz.zone, tz)

        # pyarrow-backed series
        ser2 = pd.Series([ts1], dtype=pd.ArrowDtype(pa_type))
        self.assertEqual(ser2.dtype.pyarrow_dtype, pa_type)

        os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
        for s in [
            pa.scalar(ts1),
            pa.scalar(ts1, type=pa_type),
        ]:
            self.assertEqual(s.type, pa_type)
            self.assertEqual(s.as_py(), ts1)

        # when PYARROW_IGNORE_TIMEZONE=1 and input is list[pd.Timestamp]
        # tzinfo is used to infer the datatype, but not used in computing the underlying value
        for a in [
            pa.array([ts1]),
            pa.array([ts1], type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0], a.to_pandas()[0]]:
                self.assertNotEqual(v, ts1)
                self.assertEqual(v, ts2)

        for a in [
            pa.array([pa.scalar(ts1)]),
            pa.array([pa.scalar(ts1)], type=pa_type),
            pa.array(ser1),
            pa.array(ser1, type=pa_type),
            pa.Array.from_pandas(ser1),
            pa.Array.from_pandas(ser1, type=pa_type),
            pa.array(ser2),
            pa.array(ser2, type=pa_type),
            pa.Array.from_pandas(ser2),
            pa.Array.from_pandas(ser2, type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0], a.to_pandas()[0]]:
                self.assertEqual(v, ts1)

        del os.environ["PYARROW_IGNORE_TIMEZONE"]
        for s in [
            pa.scalar(ts1),
            pa.scalar(ts1, type=pa_type),
        ]:
            self.assertEqual(s.type, pa_type)
            self.assertEqual(s.as_py(), ts1)

        for a in [
            pa.array([ts1]),
            pa.array([ts1], type=pa_type),
            pa.array([pa.scalar(ts1)]),
            pa.array([pa.scalar(ts1)], type=pa_type),
            pa.array(ser1),
            pa.array(ser1, type=pa_type),
            pa.Array.from_pandas(ser1),
            pa.Array.from_pandas(ser1, type=pa_type),
            pa.array(ser2),
            pa.array(ser2, type=pa_type),
            pa.Array.from_pandas(ser2),
            pa.Array.from_pandas(ser2, type=pa_type),
        ]:
            self.assertEqual(a.type, pa_type)
            for v in [a[0].as_py(), a.to_pylist()[0], a.to_pandas()[0]]:
                self.assertEqual(v, ts1)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
