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
Additional Spark functions used in pandas-on-Spark.
"""
from pyspark.sql.column import Column
from pyspark.sql.utils import is_remote


def product(col: Column, dropna: bool) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_product",
            col,  # type: ignore[arg-type]
            lit(dropna),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasProduct(col._jc, dropna))


def stddev(col: Column, ddof: int) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_stddev",
            col,  # type: ignore[arg-type]
            lit(ddof),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasStddev(col._jc, ddof))


def var(col: Column, ddof: int) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_var",
            col,  # type: ignore[arg-type]
            lit(ddof),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasVariance(col._jc, ddof))


def skew(col: Column) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_skew",
            col,  # type: ignore[arg-type]
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasSkewness(col._jc))


def kurt(col: Column) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_kurt",
            col,  # type: ignore[arg-type]
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasKurtosis(col._jc))


def mode(col: Column, dropna: bool) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_mode",
            col,  # type: ignore[arg-type]
            lit(dropna),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasMode(col._jc, dropna))


def covar(col1: Column, col2: Column, ddof: int) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "pandas_covar",
            col1,  # type: ignore[arg-type]
            col2,  # type: ignore[arg-type]
            lit(ddof),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.pandasCovar(col1._jc, col2._jc, ddof))


def ewm(col: Column, alpha: float, ignore_na: bool) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "ewm",
            col,  # type: ignore[arg-type]
            lit(alpha),
            lit(ignore_na),
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.ewm(col._jc, alpha, ignore_na))


def null_index(col: Column) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "null_index",
            col,  # type: ignore[arg-type]
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.nullIndex(col._jc))


def timestampdiff(unit: str, start: Column, end: Column) -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

        return _invoke_function_over_columns(  # type: ignore[return-value]
            "timestampdiff",
            lit(unit),
            start,  # type: ignore[arg-type]
            end,  # type: ignore[arg-type]
        )

    else:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.timestampDiff(unit, start._jc, end._jc))
