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

from pyspark.errors import (
    AnalysisException,
    ArithmeticException,
    QueryContextType,
    NumberFormatException,
    ArrayIndexOutOfBoundsException,
)
from pyspark.sql import functions as sf
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
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - plus
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("plus_invalid_type", df.id + "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - minus
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("minus_invalid_type", df.id - "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - multiply
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("multiply_invalid_type", df.id * "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - mod
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("mod_invalid_type", df.id % "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mod__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - equalTo
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("equalTo_invalid_type", df.id == "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__eq__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - lt
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("lt_invalid_type", df.id < "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__lt__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - leq
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("leq_invalid_type", df.id <= "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__le__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - geq
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("geq_invalid_type", df.id >= "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__ge__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - gt
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("gt_invalid_type", df.id > "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__gt__",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - eqNullSafe
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("eqNullSafe_invalid_type", df.id.eqNullSafe("string")).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="eqNullSafe",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseOR
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseOR_invalid_type", df.id.bitwiseOR("string")).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="bitwiseOR",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseAND
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseAND_invalid_type", df.id.bitwiseAND("string")).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="bitwiseAND",
            )

            # DataFrameQueryContext with pysparkLoggingInfo - bitwiseXOR
            with self.assertRaises(NumberFormatException) as pe:
                df.withColumn("bitwiseXOR_invalid_type", df.id.bitwiseXOR("string")).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__mul__",
            )

            # Multiple expressions in df.select (`divide` is problematic)
            with self.assertRaises(ArithmeticException) as pe:
                df.select(df.id - 10, df.id + 4, df.id / 0, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

            # Multiple expressions in df.select (`plus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - 10, df.id + "string", df.id / 10, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__add__",
            )

            # Multiple expressions in df.select (`minus` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - "string", df.id + 4, df.id / 10, df.id * 5).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
                },
                query_context_type=QueryContextType.DataFrame,
                fragment="__sub__",
            )

            # Multiple expressions in df.select (`multiply` is problematic)
            with self.assertRaises(NumberFormatException) as pe:
                df.select(df.id - 10, df.id + 4, df.id / 10, df.id * "string").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="CAST_INVALID_INPUT",
                messageParameters={
                    "ansiConfig": '"spark.sql.ansi.enabled"',
                    "expression": "'string'",
                    "sourceType": '"STRING"',
                    "targetType": '"BIGINT"',
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
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.SQL,
            )

            # No QueryContext
            with self.assertRaises(AnalysisException) as pe:
                self.spark.sql("select * from non-existing-table")
            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_IDENTIFIER",
                messageParameters={"ident": "non-existing-table"},
                query_context_type=None,
            )

    def test_query_context_complex(self):
        with self.sql_conf({"spark.sql.ansi.enabled": True}):
            # SQLQueryContext
            with self.assertRaises(ArithmeticException) as pe:
                self.spark.sql("select (10/0)*100").collect()
            self.check_error(
                exception=pe.exception,
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.SQL,
            )

            # DataFrameQueryContext
            df = self.spark.range(10)
            with self.assertRaises(ArithmeticException) as pe:
                df.withColumn("div_zero", (df.id / 0) * 10).collect()
            self.check_error(
                exception=pe.exception,
                errorClass="DIVIDE_BY_ZERO",
                messageParameters={"config": '"spark.sql.ansi.enabled"'},
                query_context_type=QueryContextType.DataFrame,
                fragment="__truediv__",
            )

    def test_dataframe_query_context_col(self):
        with self.assertRaises(AnalysisException) as pe:
            self.spark.range(1).select(sf.col("id") + sf.col("idd")).show()

        self.check_error(
            exception=pe.exception,
            errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION",
            messageParameters={"objectName": "`idd`", "proposal": "`id`"},
            query_context_type=QueryContextType.DataFrame,
            fragment="col",
        )

    def test_with_origin_is_reentrant(self):
        # A PySpark API can invoke another decorated API internally, for example
        # `Column.__and__` calls `lit`. The nested call must neither overwrite nor clear the
        # outer origin, so that the outermost call site, the one closest to the user code,
        # is the one reported. This mirrors the JVM side `withOrigin`.
        from pyspark.errors.utils import _with_origin, current_origin

        observed = []

        @_with_origin
        def inner():
            observed.append(current_origin().fragment)

        @_with_origin
        def outer():
            inner()
            # Still set after the nested call returns, so the expression built here
            # is still attributed to `outer`.
            observed.append(current_origin().fragment)

        outer()
        self.assertEqual(observed, ["outer", "outer"])
        # Fully cleared once the outermost call completes.
        self.assertIsNone(current_origin().fragment)
        self.assertIsNone(current_origin().call_site)

    def test_dataframe_query_context_functions(self):
        # Expressions are commonly built from `pyspark.sql.functions` rather than from
        # `Column` methods. Those functions used to capture no call site, so the context fell
        # back to the JVM stack trace, which points at py4j reflection internals.
        try:
            self.spark.range(1).select(sf.element_at(sf.array(sf.lit(1)), 5)).collect()
            self.fail("Expected the query to fail")
        except ArrayIndexOutOfBoundsException as e:
            call_site = e.getQueryContext()[0].callSite()

        # A Python `<file>:<line>` call site, rather than the JVM stack trace fallback such
        # as "java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)".
        # Note that this test file itself lives under the PySpark package, whose frames are
        # filtered out of the call site, so the innermost reported frame is its caller.
        self.assertRegex(call_site, r"\.py:[0-9]+$")

    def test_dataframe_query_context_functions_coverage(self):
        # Public functions are decorated, while the ones taking a user defined callable or a
        # raw expression string are deliberately left alone.
        from pyspark.errors.utils import _ORIGIN_IGNORED_FUNCTIONS, _is_origin_wrapped

        self.assertTrue(_is_origin_wrapped(sf.split))
        self.assertTrue(_is_origin_wrapped(sf.upper))
        self.assertTrue(_is_origin_wrapped(sf.to_date))

        for name in _ORIGIN_IGNORED_FUNCTIONS:
            func = getattr(sf, name, None)
            if func is not None:
                self.assertFalse(_is_origin_wrapped(func), name)

        # Aliases stay identical to what they alias, rather than becoming a separate wrapper.
        for alias, aliased in [
            ("negate", "negative"),
            ("column", "col"),
            ("power", "pow"),
            ("random", "rand"),
        ]:
            self.assertIs(getattr(sf, alias), getattr(sf, aliased), alias)


class DataFrameQueryContextTests(DataFrameQueryContextTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
