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

from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase
from pyspark.sql.types import StringType
from pyspark.testing.sqlutils import have_pandas
from pyspark.sql.types import (
    ByteType,
    ShortType,
    IntegerType,
    FloatType,
    DayTimeIntervalType,
    StringType,
    DoubleType,
    LongType,
    DecimalType,
    BinaryType,
    BooleanType,
)

if have_pandas:
    from pyspark.sql.connect.functions import lit
    import pandas


class SparkConnectTests(SparkConnectSQLTestCase):
    def test_column_operator(self):
        # SPARK-41351: Column needs to support !=
        df = self.connect.range(10)
        self.assertEqual(9, len(df.filter(df.id != lit(1)).collect()))

    def test_columns(self):
        # SPARK-41036: test `columns` API for python client.
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)
        self.assertEqual(["id", "name"], df.columns)

        self.assert_eq(
            df.filter(df.name.rlike("20")).toPandas(), df2.filter(df2.name.rlike("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.like("20")).toPandas(), df2.filter(df2.name.like("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.ilike("20")).toPandas(), df2.filter(df2.name.ilike("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.contains("20")).toPandas(),
            df2.filter(df2.name.contains("20")).toPandas(),
        )
        self.assert_eq(
            df.filter(df.name.startswith("2")).toPandas(),
            df2.filter(df2.name.startswith("2")).toPandas(),
        )
        self.assert_eq(
            df.filter(df.name.endswith("0")).toPandas(),
            df2.filter(df2.name.endswith("0")).toPandas(),
        )
        self.assert_eq(
            df.select(df.name.substr(0, 1).alias("col")).toPandas(),
            df2.select(df2.name.substr(0, 1).alias("col")).toPandas(),
        )
        df3 = self.connect.sql("SELECT cast(null as int) as name")
        df4 = self.spark.sql("SELECT cast(null as int) as name")
        self.assert_eq(
            df3.filter(df3.name.isNull()).toPandas(),
            df4.filter(df4.name.isNull()).toPandas(),
        )
        self.assert_eq(
            df3.filter(df3.name.isNotNull()).toPandas(),
            df4.filter(df4.name.isNotNull()).toPandas(),
        )

    def test_simple_binary_expressions(self):
        """Test complex expression"""
        df = self.connect.read.table(self.tbl_name)
        pd = df.select(df.id).where(df.id % lit(30) == lit(0)).sort(df.id.asc()).toPandas()
        self.assertEqual(len(pd.index), 4)

        res = pandas.DataFrame(data={"id": [0, 30, 60, 90]})
        self.assert_(pd.equals(res), f"{pd.to_string()} != {res.to_string()}")

    def test_cast(self):
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)

        self.assert_eq(
            df.select(df.id.cast("string")).toPandas(), df2.select(df2.id.cast("string")).toPandas()
        )

        for x in [
            StringType(),
            BinaryType(),
            ShortType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            ByteType(),
            DecimalType(10, 2),
            BooleanType(),
            DayTimeIntervalType(),
        ]:
            self.assert_eq(
                df.select(df.id.cast(x)).toPandas(), df2.select(df2.id.cast(x)).toPandas()
            )


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_connect_column import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
