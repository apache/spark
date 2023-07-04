# -*- encoding: utf-8 -*-
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

import unittest
from prettytable import PrettyTable

from pyspark.sql.functions import sha2
from pyspark.errors import (
    AnalysisException,
    ParseException,
    PySparkAssertionError,
    IllegalArgumentException,
    SparkUpgradeException,
)
from pyspark.testing.utils import assertDataFrameEqual, blue, red
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, pandas_requirement_message
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date, unix_timestamp, from_unixtime
from pyspark.sql.types import StringType, ArrayType, LongType, StructType, MapType, StructField


class UtilsTestsMixin:
    def test_assert_equal_inttype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2)

    def test_assert_equal_arraytype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("john", ["Python", "Java"]),
                ("jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("john", ["Python", "Java"]),
                ("jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2)

    def test_assert_notequal_arraytype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("John", ["Python", "Java"]),
                ("Jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("John", ["Python", "Java"]),
                ("Jane", ["Scala", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )

        expected_error_table = PrettyTable(["df", "expected"])
        expected_error_table.add_row(
            [red(df1.sort(df1.columns).collect()[0]), red(df2.sort(df2.columns).collect()[0])]
        )
        expected_error_table.add_row(
            [blue(df1.sort(df1.columns).collect()[1]), blue(df2.sort(df2.columns).collect()[1])]
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            error_class="DIFFERENT_DATAFRAME",
            message_parameters={"error_table": expected_error_table.get_string()},
        )

    def test_assert_equal_maptype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", {"id": 222342203655477580}),
                ("student2", {"id": 422322203155477692}),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("properties", MapType(StringType(), LongType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", {"id": 222342203655477580}),
                ("student2", {"id": 422322203155477692}),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("properties", MapType(StringType(), LongType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, ignore_row_order=False)

    def test_assert_equal_nullrow(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                (None, None),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                (None, None),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2)

    def test_assert_notequal_nullval(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 2000),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", None),
            ],
            schema=["id", "amount"],
        )

        expected_error_table = PrettyTable(["df", "expected"])
        expected_error_table.add_row(
            [blue(df1.sort(df1.columns).collect()[0]), blue(df2.sort(df2.columns).collect()[0])]
        )
        expected_error_table.add_row(
            [red(df1.sort(df1.columns).collect()[1]), red(df2.sort(df2.columns).collect()[1])]
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            error_class="DIFFERENT_DATAFRAME",
            message_parameters={"error_table": expected_error_table.get_string()},
        )

    def test_assert_equal_nulldf(self):
        df1 = None
        df2 = None

        assertDataFrameEqual(df1, df2)

    def test_ignore_row_order(self):
        # test that row order is ignored by default
        df1 = self.spark.createDataFrame(
            data=[
                ("2", 3000.00),
                ("1", 1000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2)

    def remove_non_word_characters(self, col):
        return F.regexp_replace(col, "[^\\w\\s]+", "")

    def test_remove_non_word_characters_long(self):
        source_data = [("jo&&se",), ("**li**",), ("#::luisa",), (None,)]
        source_df = self.spark.createDataFrame(source_data, ["name"])

        actual_df = source_df.withColumn(
            "clean_name", self.remove_non_word_characters(F.col("name"))
        )

        expected_data = [("jo&&se", "jose"), ("**li**", "li"), ("#::luisa", "luisa"), (None, None)]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "clean_name"])

        assertDataFrameEqual(actual_df, expected_df)

    def test_assert_pyspark_approx_equal(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.0000001),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2)

    def test_assert_pyspark_df_not_equal(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1001.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        expected_error_table = PrettyTable(["df", "expected"])
        expected_error_table.add_row([red(df1.collect()[0]), red(df2.collect()[0])])
        expected_error_table.add_row([blue(df1.collect()[1]), blue(df2.collect()[1])])

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            error_class="DIFFERENT_DATAFRAME",
            message_parameters={"error_table": expected_error_table.get_string()},
        )

        def test_assert_notequal_schema(self):
            df1 = self.spark.createDataFrame(
                data=[
                    (1, 1000),
                    (2, 3000),
                ],
                schema=["id", "amount"],
            )
            df2 = self.spark.createDataFrame(
                data=[
                    ("1", 1000),
                    ("2", 3000),
                ],
                schema=["id", "amount"],
            )

            with self.assertRaises(PySparkAssertionError) as pe:
                assertDataFrameEqual(df1, df2)

            self.check_error(
                exception=pe.exception,
                error_class="DIFFERENT_SCHEMA",
                message_parameters={"df_schema": df1.schema, "expected_schema": df2.schema},
            )


class UtilsTests(ReusedSQLTestCase, UtilsTestsMixin):
    def test_capture_analysis_exception(self):
        self.assertRaises(AnalysisException, lambda: self.spark.sql("select abc"))
        self.assertRaises(AnalysisException, lambda: self.df.selectExpr("a + b"))

    def test_capture_user_friendly_exception(self):
        try:
            self.spark.sql("select `中文字段`")
        except AnalysisException as e:
            self.assertRegex(str(e), ".*UNRESOLVED_COLUMN.*`中文字段`.*")

    def test_spark_upgrade_exception(self):
        # SPARK-32161 : Test case to Handle SparkUpgradeException in pythonic way
        df = self.spark.createDataFrame([("2014-31-12",)], ["date_str"])
        df2 = df.select(
            "date_str", to_date(from_unixtime(unix_timestamp("date_str", "yyyy-dd-aa")))
        )
        self.assertRaises(SparkUpgradeException, df2.collect)

    def test_capture_parse_exception(self):
        self.assertRaises(ParseException, lambda: self.spark.sql("abc"))

    def test_capture_illegalargument_exception(self):
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Setting negative mapred.reduce.tasks",
            lambda: self.spark.sql("SET mapred.reduce.tasks=-1"),
        )
        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        self.assertRaisesRegex(
            IllegalArgumentException,
            "1024 is not in the permitted values",
            lambda: df.select(sha2(df.a, 1024)).collect(),
        )
        try:
            df.select(sha2(df.a, 1024)).collect()
        except IllegalArgumentException as e:
            self.assertRegex(e.desc, "1024 is not in the permitted values")
            self.assertRegex(e.stackTrace, "org.apache.spark.sql.functions")

    def test_get_error_class_state(self):
        # SPARK-36953: test CapturedException.getErrorClass and getSqlState (from SparkThrowable)
        try:
            self.spark.sql("""SELECT a""")
        except AnalysisException as e:
            self.assertEquals(e.getErrorClass(), "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")
            self.assertEquals(e.getSqlState(), "42703")


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_utils import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
