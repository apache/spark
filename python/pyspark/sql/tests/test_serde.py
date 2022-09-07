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
import shutil
import tempfile
import time

from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, DecimalType, BinaryType
from pyspark.testing.sqlutils import ReusedSQLTestCase, UTCOffsetTimezone


class SerdeTests(ReusedSQLTestCase):
    def test_serialize_nested_array_and_map(self):
        d = [Row(lst=[Row(a=1, b="s")], d={"key": Row(c=1.0, d="2")})]
        rdd = self.sc.parallelize(d)
        df = self.spark.createDataFrame(rdd)
        row = df.head()
        self.assertEqual(1, len(row.lst))
        self.assertEqual(1, row.lst[0].a)
        self.assertEqual("2", row.d["key"].d)

        lst = df.rdd.map(lambda x: x.lst).first()
        self.assertEqual(1, len(lst))
        self.assertEqual("s", lst[0].b)

        d = df.rdd.map(lambda x: x.d).first()
        self.assertEqual(1, len(d))
        self.assertEqual(1.0, d["key"].c)

        row = df.rdd.map(lambda x: x.d["key"]).first()
        self.assertEqual(1.0, row.c)
        self.assertEqual("2", row.d)

    def test_select_null_literal(self):
        df = self.spark.sql("select null as col")
        self.assertEqual(Row(col=None), df.first())

    def test_struct_in_map(self):
        d = [Row(m={Row(i=1): Row(s="")})]
        df = self.sc.parallelize(d).toDF()
        k, v = list(df.head().m.items())[0]
        self.assertEqual(1, k.i)
        self.assertEqual("", v.s)

    def test_filter_with_datetime(self):
        time = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000)
        date = time.date()
        row = Row(date=date, time=time)
        df = self.spark.createDataFrame([row])
        self.assertEqual(1, df.filter(df.date == date).count())
        self.assertEqual(1, df.filter(df.time == time).count())
        self.assertEqual(0, df.filter(df.date > date).count())
        self.assertEqual(0, df.filter(df.time > time).count())

    def test_filter_with_datetime_timezone(self):
        dt1 = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000, tzinfo=UTCOffsetTimezone(0))
        dt2 = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000, tzinfo=UTCOffsetTimezone(1))
        row = Row(date=dt1)
        df = self.spark.createDataFrame([row])
        self.assertEqual(0, df.filter(df.date == dt2).count())
        self.assertEqual(1, df.filter(df.date > dt2).count())
        self.assertEqual(0, df.filter(df.date < dt2).count())

    def test_time_with_timezone(self):
        day = datetime.date.today()
        now = datetime.datetime.now()
        ts = time.mktime(now.timetuple())
        # class in __main__ is not serializable
        from pyspark.testing.sqlutils import UTCOffsetTimezone

        utc = UTCOffsetTimezone()
        utcnow = datetime.datetime.utcfromtimestamp(ts)  # without microseconds
        # add microseconds to utcnow (keeping year,month,day,hour,minute,second)
        utcnow = datetime.datetime(*(utcnow.timetuple()[:6] + (now.microsecond, utc)))
        df = self.spark.createDataFrame([(day, now, utcnow)])
        day1, now1, utcnow1 = df.first()
        self.assertEqual(day1, day)
        self.assertEqual(now, now1)
        self.assertEqual(now, utcnow1)

    # regression test for SPARK-19561
    def test_datetime_at_epoch(self):
        epoch = datetime.datetime.fromtimestamp(0)
        df = self.spark.createDataFrame([Row(date=epoch)])
        first = df.select("date", lit(epoch).alias("lit_date")).first()
        self.assertEqual(first["date"], epoch)
        self.assertEqual(first["lit_date"], epoch)

    def test_decimal(self):
        from decimal import Decimal

        schema = StructType([StructField("decimal", DecimalType(10, 5))])
        df = self.spark.createDataFrame([(Decimal("3.14159"),)], schema)
        row = df.select(df.decimal + 1).first()
        self.assertEqual(row[0], Decimal("4.14159"))
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.parquet(tmpPath)
        df2 = self.spark.read.parquet(tmpPath)
        row = df2.first()
        self.assertEqual(row[0], Decimal("3.14159"))

    def test_BinaryType_serialization(self):
        # Pyrolite version <= 4.9 could not serialize BinaryType with Python3 SPARK-17808
        # The empty bytearray is test for SPARK-21534.
        schema = StructType([StructField("mybytes", BinaryType())])
        data = [
            [bytearray(b"here is my data")],
            [bytearray(b"and here is some more")],
            [bytearray(b"")],
        ]
        df = self.spark.createDataFrame(data, schema=schema)
        df.collect()

    def test_int_array_serialization(self):
        # Note that this test seems dependent on parallelism.
        data = self.spark.sparkContext.parallelize([[1, 2, 3, 4]] * 100, numSlices=12)
        df = self.spark.createDataFrame(data, "array<integer>")
        self.assertEqual(len(list(filter(lambda r: None in r.value, df.collect()))), 0)

    def test_bytes_as_binary_type(self):
        df = self.spark.createDataFrame([[b"abcd"]], "col binary")
        self.assertEqual(df.first().col, bytearray(b"abcd"))


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_serde import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
