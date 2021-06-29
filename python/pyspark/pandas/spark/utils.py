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
Helpers and utilities to deal with PySpark instances
"""
from typing import overload

from pyspark.sql.types import DecimalType, StructType, MapType, ArrayType, StructField, DataType


@overload
def as_nullable_spark_type(dt: StructType) -> StructType:
    ...


@overload
def as_nullable_spark_type(dt: ArrayType) -> ArrayType:
    ...


@overload
def as_nullable_spark_type(dt: MapType) -> MapType:
    ...


@overload
def as_nullable_spark_type(dt: DataType) -> DataType:
    ...


def as_nullable_spark_type(dt: DataType) -> DataType:
    """
    Returns a nullable schema or data types.

    Examples
    --------
    >>> from pyspark.sql.types import *
    >>> as_nullable_spark_type(StructType([
    ...     StructField("A", IntegerType(), True),
    ...     StructField("B", FloatType(), False)]))  # doctest: +NORMALIZE_WHITESPACE
    StructType(List(StructField(A,IntegerType,true),StructField(B,FloatType,true)))

    >>> as_nullable_spark_type(StructType([
    ...     StructField("A",
    ...         StructType([
    ...             StructField('a',
    ...                 MapType(IntegerType(),
    ...                 ArrayType(IntegerType(), False), False), False),
    ...             StructField('b', StringType(), True)])),
    ...     StructField("B", FloatType(), False)]))  # doctest: +NORMALIZE_WHITESPACE
    StructType(List(StructField(A,StructType(List(StructField(a,MapType(IntegerType,ArrayType\
(IntegerType,true),true),true),StructField(b,StringType,true))),true),\
StructField(B,FloatType,true)))
    """
    if isinstance(dt, StructType):
        new_fields = []
        for field in dt.fields:
            new_fields.append(
                StructField(
                    field.name,
                    as_nullable_spark_type(field.dataType),
                    nullable=True,
                    metadata=field.metadata,
                )
            )
        return StructType(new_fields)
    elif isinstance(dt, ArrayType):
        return ArrayType(as_nullable_spark_type(dt.elementType), containsNull=True)
    elif isinstance(dt, MapType):
        return MapType(
            as_nullable_spark_type(dt.keyType),
            as_nullable_spark_type(dt.valueType),
            valueContainsNull=True,
        )
    else:
        return dt


@overload
def force_decimal_precision_scale(
    dt: StructType, *, precision: int = ..., scale: int = ...
) -> StructType:
    ...


@overload
def force_decimal_precision_scale(
    dt: ArrayType, *, precision: int = ..., scale: int = ...
) -> ArrayType:
    ...


@overload
def force_decimal_precision_scale(
    dt: MapType, *, precision: int = ..., scale: int = ...
) -> MapType:
    ...


@overload
def force_decimal_precision_scale(
    dt: DataType, *, precision: int = ..., scale: int = ...
) -> DataType:
    ...


def force_decimal_precision_scale(
    dt: DataType, *, precision: int = 38, scale: int = 18
) -> DataType:
    """
    Returns a data type with a fixed decimal type.

    The precision and scale of the decimal type are fixed with the given values.

    Examples
    --------
    >>> from pyspark.sql.types import *
    >>> force_decimal_precision_scale(StructType([
    ...     StructField("A", DecimalType(10, 0), True),
    ...     StructField("B", DecimalType(14, 7), False)]))  # doctest: +NORMALIZE_WHITESPACE
    StructType(List(StructField(A,DecimalType(38,18),true),StructField(B,DecimalType(38,18),false)))

    >>> force_decimal_precision_scale(StructType([
    ...     StructField("A",
    ...         StructType([
    ...             StructField('a',
    ...                 MapType(DecimalType(5, 0),
    ...                 ArrayType(DecimalType(20, 0), False), False), False),
    ...             StructField('b', StringType(), True)])),
    ...     StructField("B", DecimalType(30, 15), False)]),
    ...     precision=30, scale=15)  # doctest: +NORMALIZE_WHITESPACE
    StructType(List(StructField(A,StructType(List(StructField(a,MapType(DecimalType(30,15),\
ArrayType(DecimalType(30,15),false),false),false),StructField(b,StringType,true))),true),\
StructField(B,DecimalType(30,15),false)))
    """
    if isinstance(dt, StructType):
        new_fields = []
        for field in dt.fields:
            new_fields.append(
                StructField(
                    field.name,
                    force_decimal_precision_scale(field.dataType, precision=precision, scale=scale),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        return StructType(new_fields)
    elif isinstance(dt, ArrayType):
        return ArrayType(
            force_decimal_precision_scale(dt.elementType, precision=precision, scale=scale),
            containsNull=dt.containsNull,
        )
    elif isinstance(dt, MapType):
        return MapType(
            force_decimal_precision_scale(dt.keyType, precision=precision, scale=scale),
            force_decimal_precision_scale(dt.valueType, precision=precision, scale=scale),
            valueContainsNull=dt.valueContainsNull,
        )
    elif isinstance(dt, DecimalType):
        return DecimalType(precision=precision, scale=scale)
    else:
        return dt


def _test() -> None:
    import doctest
    import sys
    import pyspark.pandas.spark.utils

    globs = pyspark.pandas.spark.utils.__dict__.copy()
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.spark.utils,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
