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
from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    import pandas as pd
    from pyspark.sql.connect.functions import lit


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
        pdf = df.select(df.id).where(df.id % lit(30) == lit(0)).sort(df.id.asc()).toPandas()
        self.assertEqual(len(pdf.index), 4)

        res = pd.DataFrame(data={"id": [0, 30, 60, 90]})
        self.assert_(pdf.equals(res), f"{pdf.to_string()} != {res.to_string()}")

    def test_literal_integers(self):
        cdf = self.connect.range(0, 1)
        sdf = self.spark.range(0, 1)

        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF
        from pyspark.sql.connect.column import JVM_INT_MIN, JVM_INT_MAX, JVM_LONG_MIN, JVM_LONG_MAX

        cdf1 = cdf.select(
            CF.lit(0),
            CF.lit(1),
            CF.lit(-1),
            CF.lit(JVM_INT_MAX),
            CF.lit(JVM_INT_MIN),
            CF.lit(JVM_INT_MAX + 1),
            CF.lit(JVM_INT_MIN - 1),
            CF.lit(JVM_LONG_MAX),
            CF.lit(JVM_LONG_MIN),
            CF.lit(JVM_LONG_MAX - 1),
            CF.lit(JVM_LONG_MIN + 1),
        )

        sdf1 = sdf.select(
            SF.lit(0),
            SF.lit(1),
            SF.lit(-1),
            SF.lit(JVM_INT_MAX),
            SF.lit(JVM_INT_MIN),
            SF.lit(JVM_INT_MAX + 1),
            SF.lit(JVM_INT_MIN - 1),
            SF.lit(JVM_LONG_MAX),
            SF.lit(JVM_LONG_MIN),
            SF.lit(JVM_LONG_MAX - 1),
            SF.lit(JVM_LONG_MIN + 1),
        )

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assert_eq(cdf1.toPandas(), sdf1.toPandas())

        with self.assertRaisesRegex(
            ValueError,
            "integer 9223372036854775808 out of bounds",
        ):
            cdf.select(CF.lit(JVM_LONG_MAX + 1)).show()

        with self.assertRaisesRegex(
            ValueError,
            "integer -9223372036854775809 out of bounds",
        ):
            cdf.select(CF.lit(JVM_LONG_MIN - 1)).show()

    def test_cast(self):
        # SPARK-41412: test basic Column.cast
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)

        self.assert_eq(
            df.select(df.id.cast("string")).toPandas(), df2.select(df2.id.cast("string")).toPandas()
        )

        # Test if the arguments can be passed properly.
        # Do not need to check individual behaviour for the ANSI mode thoroughly.
        with self.sql_conf({"spark.sql.ansi.enabled": False}):
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

    def test_unsupported_functions(self):
        # SPARK-41225: Disable unsupported functions.
        c = self.connect.range(1).id
        for f in (
            "otherwise",
            "over",
            "isin",
            "when",
            "getItem",
            "astype",
            "between",
            "getField",
            "withField",
            "dropFields",
        ):
            with self.assertRaises(NotImplementedError):
                getattr(c, f)()

        with self.assertRaises(NotImplementedError):
            c["a"]

        with self.assertRaises(TypeError):
            for x in c:
                pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_connect_column import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
