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
from typing import Any
import unittest
import tempfile

from pyspark.testing.sqlutils import have_pandas, SQLTestUtils

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

if have_pandas:
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.utils import ReusedPySparkTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectSQLTestCase(PandasOnSparkTestCase, ReusedPySparkTestCase, SQLTestUtils):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    if have_pandas:
        connect: RemoteSparkSession
    tbl_name: str
    tbl_name_empty: str
    df_text: "DataFrame"
    spark: SparkSession

    @classmethod
    def setUpClass(cls: Any):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.testDataStr = [Row(key=str(i)) for i in range(100)]
        cls.df = cls.sc.parallelize(cls.testData).toDF()
        cls.df_text = cls.sc.parallelize(cls.testDataStr).toDF()

        cls.tbl_name = "test_connect_basic_table_1"
        cls.tbl_name_empty = "test_connect_basic_table_empty"

        # Cleanup test data
        cls.spark_connect_clean_up_test_data()
        # Load test data
        cls.spark_connect_load_test_data()

    @classmethod
    def tearDownClass(cls: Any) -> None:
        cls.spark_connect_clean_up_test_data()
        ReusedPySparkTestCase.tearDownClass()

    @classmethod
    def spark_connect_load_test_data(cls: Any):
        # Setup Remote Spark Session
        cls.connect = RemoteSparkSession.builder.remote().getOrCreate()
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to create global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.write.saveAsTable(cls.tbl_name)
        empty_table_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
            ]
        )
        emptyRDD = cls.spark.sparkContext.emptyRDD()
        empty_df = cls.spark.createDataFrame(emptyRDD, empty_table_schema)
        empty_df.write.saveAsTable(cls.tbl_name_empty)

    @classmethod
    def spark_connect_clean_up_test_data(cls: Any) -> None:
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name_empty))


class SparkConnectFunctionTests(SparkConnectSQLTestCase):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def test_sorting_functions_with_column(self):
        from pyspark.sql.connect import functions as CF
        from pyspark.sql.connect.column import Column

        funs = [
            CF.asc_nulls_first,
            CF.asc_nulls_last,
            CF.desc_nulls_first,
            CF.desc_nulls_last,
        ]
        exprs = [CF.col("x"), "x"]

        for fun in funs:
            for _expr in exprs:
                res = fun(_expr)
                self.assertIsInstance(res, Column)
                self.assertIn(f"""{fun.__name__.replace("_", " ").upper()}'""", str(res))

        for _expr in exprs:
            res = CF.asc(_expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""ASC NULLS FIRST'""", str(res))

        for _expr in exprs:
            res = CF.desc(_expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""DESC NULLS LAST'""", str(res))

    def test_sort_with_nulls_order(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (true, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|null|
        # | true|null| 2.0|
        # | null|   3| 3.0|
        # +-----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for c in ["a", "b", "c"]:
            self.assert_eq(
                cdf.orderBy(CF.asc(c)).toPandas(),
                sdf.orderBy(SF.asc(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.asc_nulls_first(c)).toPandas(),
                sdf.orderBy(SF.asc_nulls_first(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.asc_nulls_last(c)).toPandas(),
                sdf.orderBy(SF.asc_nulls_last(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc(c)).toPandas(),
                sdf.orderBy(SF.desc(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc_nulls_first(c)).toPandas(),
                sdf.orderBy(SF.desc_nulls_first(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc_nulls_last(c)).toPandas(),
                sdf.orderBy(SF.desc_nulls_last(c)).toPandas(),
            )

    def test_math_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (true, NULL, 2.0), (NULL, 3, 3.5)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|null|
        # | true|null| 2.0|
        # | null|   3| 3.5|
        # +-----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for cfunc, sfunc in [
            (CF.abs, SF.abs),
            (CF.acos, SF.acos),
            (CF.acosh, SF.acosh),
            (CF.asin, SF.asin),
            (CF.asinh, SF.asinh),
            (CF.atan, SF.atan),
            (CF.atanh, SF.atanh),
            (CF.bin, SF.bin),
            (CF.cbrt, SF.cbrt),
            (CF.ceil, SF.ceil),
            (CF.cos, SF.cos),
            (CF.cosh, SF.cosh),
            (CF.cot, SF.cot),
            (CF.csc, SF.csc),
            (CF.degrees, SF.degrees),
            (CF.exp, SF.exp),
            (CF.expm1, SF.expm1),
            (CF.factorial, SF.factorial),
            (CF.floor, SF.floor),
            (CF.hex, SF.hex),
            (CF.log, SF.log),
            (CF.log10, SF.log10),
            (CF.log1p, SF.log1p),
            (CF.log2, SF.log2),
            (CF.radians, SF.radians),
            (CF.rint, SF.rint),
            (CF.sec, SF.sec),
            (CF.signum, SF.signum),
            (CF.sin, SF.sin),
            (CF.sinh, SF.sinh),
            (CF.sqrt, SF.sqrt),
            (CF.tan, SF.tan),
            (CF.tanh, SF.tanh),
            (CF.unhex, SF.unhex),
        ]:
            self.assert_eq(
                cdf.select(cfunc("b"), cfunc(cdf.c)).toPandas(),
                sdf.select(sfunc("b"), sfunc(sdf.c)).toPandas(),
            )

        self.assert_eq(
            cdf.select(CF.atan2("b", cdf.c)).toPandas(),
            sdf.select(SF.atan2("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.bround("b", 1)).toPandas(),
            sdf.select(SF.bround("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.conv("b", 2, 16)).toPandas(),
            sdf.select(SF.conv("b", 2, 16)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.hypot("b", cdf.c)).toPandas(),
            sdf.select(SF.hypot("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.pmod("b", cdf.c)).toPandas(),
            sdf.select(SF.pmod("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.pow("b", cdf.c)).toPandas(),
            sdf.select(SF.pow("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.round("b", 1)).toPandas(),
            sdf.select(SF.round("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftleft("b", 1)).toPandas(),
            sdf.select(SF.shiftleft("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftright("b", 1)).toPandas(),
            sdf.select(SF.shiftright("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftrightunsigned("b", 1)).toPandas(),
            sdf.select(SF.shiftrightunsigned("b", 1)).toPandas(),
        )


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_function import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
