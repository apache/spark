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
from pyspark.errors import (
    AnalysisException,
    ArithmeticException,
    QueryContextType,
    NumberFormatException,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
)


class DataFrameQueryContextTestsMixin:
    def test_dataframe_query_context(self):
        # SPARK-47274: Add more useful contexts for PySpark DataFrame API errors.
        with self.sql_conf({"spark.sql.ansi.enabled": True}):
            df = self.spark.range(10)

            # DataFrameQueryContext with pysparkLoggingInfo - divide
            with self.assertRaises(ArithmeticException) as pe:
                df.withColumn("div_zero", df.id / 0).collect()
            self.check_error(
                exception=pe.exception,
                error_class="DIVIDE_BY_ZERO",
                message_parameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - plus
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("plus_invalid_type", df.id + "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - minus
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("minus_invalid_type", df.id - "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - multiply
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("multiply_invalid_type", df.id * "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - mod
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("mod_invalid_type", df.id % "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mod__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - equalTo
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("equalTo_invalid_type", df.id == "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__eq__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - lt
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("lt_invalid_type", df.id < "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__lt__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - leq
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("leq_invalid_type", df.id <= "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__le__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - geq
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("geq_invalid_type", df.id >= "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__ge__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - gt
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("gt_invalid_type", df.id > "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__gt__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - eqNullSafe
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("eqNullSafe_invalid_type", df.id.eqNullSafe("string")).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="eqNullSafe",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseOR
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseOR_invalid_type", df.id.bitwiseOR("string")).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="bitwiseOR",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseAND
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseAND_invalid_type", df.id.bitwiseAND("string")).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="bitwiseAND",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseXOR
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseXOR_invalid_type", df.id.bitwiseXOR("string")).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="bitwiseXOR",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - chained (`divide` is problematic)
            with self.assertRaises(ArithmeticException) as pe:
                df.withColumn("multiply_ten", df.id * 10).withColumn(
                    "divide_zero", df.id / 0
                ).withColumn("plus_ten", df.id + 10).withColumn("minus_ten", df.id - 10).collect()
            self.check_error(
                exception=pe.exception,
                error_class="DIVIDE_BY_ZERO",
                message_parameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - chained (`plus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("multiply_ten", df.id * 10).withColumn(
                    "divide_ten", df.id / 10
                ).withColumn("plus_string", df.id + "string").withColumn(
                    "minus_ten", df.id - 10
                ).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - chained (`minus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("multiply_ten", df.id * 10).withColumn(
                    "divide_ten", df.id / 10
                ).withColumn("plus_ten", df.id + 10).withColumn(
                    "minus_string", df.id - "string"
                ).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - chained (`multiply` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("multiply_string", df.id * "string").withColumn(
                    "divide_ten", df.id / 10
                ).withColumn("plus_ten", df.id + 10).withColumn("minus_ten", df.id - 10).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

            # Multiple expressions in df.select (`divide` is problematic)
            with self.assertRaises(ArithmeticException) as pe:
                df.select(df.id - 10, df.id + 4, df.id / 0, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="DIVIDE_BY_ZERO",
                message_parameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # Multiple expressions in df.select (`plus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - 10, df.id + "string", df.id / 10, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # Multiple expressions in df.select (`minus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - "string", df.id + 4, df.id / 10, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # Multiple expressions in df.select (`multiply` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - 10, df.id + 4, df.id / 10, df.id * "string").collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

            # Multiple expressions with pre-declared expressions (`divide` is problematic)
            a = df.id / 10
            b = df.id / 0
            with self.assertRaises(ArithmeticException) as pe:
                df.select(a, df.id + 4, b, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="DIVIDE_BY_ZERO",
                message_parameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # Multiple expressions with pre-declared expressions (`plus` is problematic)
            a = df.id + "string"
            b = df.id + 4
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id / 10, a, b, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # Multiple expressions with pre-declared expressions (`minus` is problematic)
            a = df.id - "string"
            b = df.id - 5
            with self.assertRaises(NumberFormatException) as pe:
                df.select(a, df.id / 10, b, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # Multiple expressions with pre-declared expressions (`multiply` is problematic)
            a = df.id * "string"
            b = df.id * 10
            with self.assertRaises(NumberFormatException) as pe:
                df.select(a, df.id / 10, b, df.id + 5).collect()
            self.check_error(
                exception=pe.exception,
                error_class="CAST_INVALID_INPUT",
                message_parameters={
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

    def test_sql_query_context(self):
        with self.sql_conf({"spark.sql.ansi.enabled": True}):
            # SQLQueryContext
            with self.assertRaises(ArithmeticException) as pe:
                self.spark.sql("select 10/0").collect()
            self.check_error(
                exception=pe.exception,
                error_class="DIVIDE_BY_ZERO",
                message_parameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.SQL,
            )

            # No QueryContext
            with self.assertRaises(AnalysisException) as pe:
                self.spark.sql("select * from non-existing-table")
            self.check_error(
                exception=pe.exception,
                error_class="INVALID_IDENTIFIER",
                message_parameters={"ident": "non-existing-table"},
                query_context_type=None,
            )


class DataFrameQueryContextTests(DataFrameQueryContextTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_dataframe_query_context import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
