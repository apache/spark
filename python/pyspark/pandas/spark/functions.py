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
Additional Spark functions used in Koalas.
"""

from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal


__all__ = ["percentile_approx"]


def percentile_approx(col, percentage, accuracy=10000):
    """
    Returns the approximate percentile value of numeric column col at the given percentage.
    The value of percentage must be between 0.0 and 1.0.

    The accuracy parameter (default: 10000)
    is a positive numeric literal which controls approximation accuracy at the cost of memory.
    Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error
    of the approximation.

    When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
    In this case, returns the approximate percentile array of column col
    at the given percentage array.

    Ported from Spark 3.1.
    """
    sc = SparkContext._active_spark_context

    if isinstance(percentage, (list, tuple)):
        # A local list
        percentage = sc._jvm.functions.array(
            _to_seq(sc, [_create_column_from_literal(x) for x in percentage])
        )
    elif isinstance(percentage, Column):
        # Already a Column
        percentage = _to_java_column(percentage)
    else:
        # Probably scalar
        percentage = _create_column_from_literal(percentage)

    accuracy = (
        _to_java_column(accuracy)
        if isinstance(accuracy, Column)
        else _create_column_from_literal(accuracy)
    )

    return _call_udf(sc, "percentile_approx", _to_java_column(col), percentage, accuracy)


def array_repeat(col, count):
    """
    Collection function: creates an array containing a column repeated count times.

    Ported from Spark 3.0.
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.functions.array_repeat(
            _to_java_column(col), _to_java_column(count) if isinstance(count, Column) else count
        )
    )


def repeat(col, n):
    """
    Repeats a string column n times, and returns it as a new string column.
    """
    sc = SparkContext._active_spark_context
    n = _to_java_column(n) if isinstance(n, Column) else _create_column_from_literal(n)
    return _call_udf(sc, "repeat", _to_java_column(col), n)


def _call_udf(sc, name, *cols):
    return Column(sc._jvm.functions.callUDF(name, _make_arguments(sc, *cols)))


def _make_arguments(sc, *cols):
    java_arr = sc._gateway.new_array(sc._jvm.Column, len(cols))
    for i, col in enumerate(cols):
        java_arr[i] = col
    return java_arr
