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

from pyspark.sql import Column, functions as F, is_remote

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName

"""
Internal Spark functions used in Pandas API on Spark & PySpark Native Plotting.
"""


class InternalFunction:
    @staticmethod
    def _invoke_internal_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
        if is_remote():
            from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

            return _invoke_function_over_columns(name, *cols)

        else:
            from pyspark.sql.classic.column import Column, _to_seq, _to_java_column
            from pyspark import SparkContext

            sc = SparkContext._active_spark_context
            return Column(
                sc._jvm.PythonSQLUtils.internalFn(  # type: ignore
                    name, _to_seq(sc, cols, _to_java_column)  # type: ignore
                )
            )

    @staticmethod
    def timestamp_ntz_to_long(col: Column) -> Column:
        return InternalFunction._invoke_internal_function_over_columns("timestamp_ntz_to_long", col)

    @staticmethod
    def product(col: Column, dropna: bool) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "pandas_product", col, F.lit(dropna)
        )

    @staticmethod
    def stddev(col: Column, ddof: int) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "pandas_stddev", col, F.lit(ddof)
        )

    @staticmethod
    def var(col: Column, ddof: int) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "pandas_var", col, F.lit(ddof)
        )

    @staticmethod
    def skew(col: Column) -> Column:
        return InternalFunction._invoke_internal_function_over_columns("pandas_skew", col)

    @staticmethod
    def kurt(col: Column) -> Column:
        return InternalFunction._invoke_internal_function_over_columns("pandas_kurt", col)

    @staticmethod
    def mode(col: Column, dropna: bool) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "pandas_mode", col, F.lit(dropna)
        )

    @staticmethod
    def covar(col1: Column, col2: Column, ddof: int) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "pandas_covar", col1, col2, F.lit(ddof)
        )

    @staticmethod
    def ewm(col: Column, alpha: float, ignorena: bool) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "ewm", col, F.lit(alpha), F.lit(ignorena)
        )

    @staticmethod
    def null_index(col: Column) -> Column:
        return InternalFunction._invoke_internal_function_over_columns("null_index", col)

    @staticmethod
    def distributed_id() -> Column:
        return InternalFunction._invoke_internal_function_over_columns("distributed_id")

    @staticmethod
    def distributed_sequence_id() -> Column:
        return InternalFunction._invoke_internal_function_over_columns("distributed_sequence_id")

    @staticmethod
    def collect_top_k(col: Column, num: int, reverse: bool) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "collect_top_k", col, F.lit(num), F.lit(reverse)
        )

    @staticmethod
    def array_binary_search(col: Column, value: Column) -> Column:
        return InternalFunction._invoke_internal_function_over_columns(
            "array_binary_search", col, value
        )

    @staticmethod
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

    @staticmethod
    def vector_get(vec: Column, idx: Column) -> Column:
        unwrapped = F.unwrap_udt(vec)
        is_dense = unwrapped.getField("type") == F.lit(1)
        values = unwrapped.getField("values")
        size = F.when(is_dense, F.array_size(values)).otherwise(unwrapped.getField("size"))
        sparse_idx = InternalFunction.array_binary_search(unwrapped.getField("indices"), idx)
        value = (
            F.when(is_dense, F.get(values, idx))
            .when(sparse_idx >= 0, F.get(values, sparse_idx))
            .otherwise(F.lit(0.0))
        )

        return F.when((0 <= idx) & (idx < size), value).otherwise(
            F.raise_error(F.printf(F.lit("Vector index must be in [0, %s), but got %s"), size, idx))
        )

    # The main different between this function and the `array_max` + `array_position`:
    # This function ignores NaN/NULL values, while `array_max` treats NaN values as largest values.
    @staticmethod
    def array_argmax(arr: Column) -> Column:
        def merge(acc: Column, vv: Column) -> Column:
            v = acc.getField("v")
            i = acc.getField("i")
            j = acc.getField("j")
            return F.when(
                (~vv.isNaN()) & (~vv.isNull()) & (vv > v),
                F.struct(vv.alias("v"), j.alias("i"), j + 1),
            ).otherwise(F.struct(v.alias("v"), i.alias("i"), j + 1))

        return F.aggregate(
            arr,
            F.struct(
                F.lit(float("-inf")).alias("v"),  # max value
                F.lit(-1).alias("i"),  # index of max value
                F.lit(0).alias("j"),  # current index
            ),
            merge,
            lambda acc: acc.getField("i"),
        )
