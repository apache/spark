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
from typing import Any, Union, no_type_check

import numpy as np

from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.column import (
    Column,
    _to_java_column,
    _create_column_from_literal,
)
from pyspark.sql.types import (
    ByteType,
    FloatType,
    IntegerType,
    LongType,
)


def skew(col: Column) -> Column:
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.PythonSQLUtils.pandasSkewness(col._jc))


def kurt(col: Column) -> Column:
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.PythonSQLUtils.pandasKurtosis(col._jc))


def mode(col: Column, dropna: bool) -> Column:
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.PythonSQLUtils.pandasMode(col._jc, dropna))


def repeat(col: Column, n: Union[int, Column]) -> Column:
    """
    Repeats a string column n times, and returns it as a new string column.
    """
    sc = SparkContext._active_spark_context
    n = _to_java_column(n) if isinstance(n, Column) else _create_column_from_literal(n)
    return _call_udf(sc, "repeat", _to_java_column(col), n)


def date_part(field: Union[str, Column], source: Column) -> Column:
    """
    Extracts a part of the date/timestamp or interval source.
    """
    sc = SparkContext._active_spark_context
    field = (
        _to_java_column(field) if isinstance(field, Column) else _create_column_from_literal(field)
    )
    return _call_udf(sc, "date_part", field, _to_java_column(source))


def lit(literal: Any) -> Column:
    """
    Creates a Column of literal value.
    """
    if isinstance(literal, np.generic):
        scol = F.lit(literal.item())
        if isinstance(literal, np.int64):
            return scol.astype(LongType())
        elif isinstance(literal, np.int32):
            return scol.astype(IntegerType())
        elif isinstance(literal, np.int8) or isinstance(literal, np.byte):
            return scol.astype(ByteType())
        elif isinstance(literal, np.float32):
            return scol.astype(FloatType())
        else:  # TODO: Complete mappings between numpy literals and Spark data types
            return scol
    else:
        return F.lit(literal)


@no_type_check
def _call_udf(sc, name, *cols):
    return Column(sc._jvm.functions.callUDF(name, _make_arguments(sc, *cols)))


@no_type_check
def _make_arguments(sc, *cols):
    java_arr = sc._gateway.new_array(sc._jvm.Column, len(cols))
    for i, col in enumerate(cols):
        java_arr[i] = col
    return java_arr
