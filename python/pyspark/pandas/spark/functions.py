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
from pyspark.sql import Column, functions as F
from pyspark.sql.utils import is_remote
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


def _invoke_internal_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

        return _invoke_function_over_columns(name, *cols)

    else:
        from pyspark.sql.classic.column import _to_seq, _to_java_column
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(sc._jvm.PythonSQLUtils.internalFn(name, _to_seq(sc, cols, _to_java_column)))


def timestamp_ntz_to_long(col: Column) -> Column:
    return _invoke_internal_function_over_columns("timestamp_ntz_to_long", col)


def product(col: Column, dropna: bool) -> Column:
    return _invoke_internal_function_over_columns("pandas_product", col, F.lit(dropna))


def stddev(col: Column, ddof: int) -> Column:
    return _invoke_internal_function_over_columns("pandas_stddev", col, F.lit(ddof))


def var(col: Column, ddof: int) -> Column:
    return _invoke_internal_function_over_columns("pandas_var", col, F.lit(ddof))


def skew(col: Column) -> Column:
    return _invoke_internal_function_over_columns("pandas_skew", col)


def kurt(col: Column) -> Column:
    return _invoke_internal_function_over_columns("pandas_kurt", col)


def mode(col: Column, dropna: bool) -> Column:
    return _invoke_internal_function_over_columns("pandas_mode", col, F.lit(dropna))


def covar(col1: Column, col2: Column, ddof: int) -> Column:
    return _invoke_internal_function_over_columns("pandas_covar", col1, col2, F.lit(ddof))


def ewm(col: Column, alpha: float, ignorena: bool) -> Column:
    return _invoke_internal_function_over_columns("ewm", col, F.lit(alpha), F.lit(ignorena))


def null_index(col: Column) -> Column:
    return _invoke_internal_function_over_columns("null_index", col)


def distributed_sequence_id() -> Column:
    return _invoke_internal_function_over_columns("distributed_sequence_id")


def collect_top_k(col: Column, num: int, reverse: bool) -> Column:
    return _invoke_internal_function_over_columns("collect_top_k", col, F.lit(num), F.lit(reverse))


def array_binary_search(col: Column, value: Column) -> Column:
    return _invoke_internal_function_over_columns("array_binary_search", col, value)


def make_interval(unit: str, e: Union[Column, int, float]) -> Column:
    unit_mapping = {
        "YEAR": "years",
        "MONTH": "months",
        "WEEK": "weeks",
        "DAY": "days",
        "HOUR": "hours",
        "MINUTE": "mins",
        "SECOND": "secs",
    }
    return F.make_interval(**{unit_mapping[unit]: F.lit(e)})
