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

import numbers
from abc import ABCMeta
from itertools import chain
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql import functions as F, Column
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    FractionalType,
    IntegralType,
    MapType,
    NullType,
    NumericType,
    StringType,
    StructType,
    TimestampType,
    UserDefinedType,
)
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.typedef import extension_dtypes
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)

if extension_dtypes_available:
    from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

if extension_float_dtypes_available:
    from pandas import Float32Dtype, Float64Dtype

if extension_object_dtypes_available:
    from pandas import BooleanDtype, StringDtype


def is_valid_operand_for_numeric_arithmetic(operand: Any, *, allow_bool: bool = True) -> bool:
    """Check whether the `operand` is valid for arithmetic operations against numerics."""
    from pyspark.pandas.base import IndexOpsMixin

    if isinstance(operand, numbers.Number):
        return not isinstance(operand, bool) or allow_bool
    elif isinstance(operand, IndexOpsMixin):
        if isinstance(operand.dtype, CategoricalDtype):
            return False
        else:
            return isinstance(operand.spark.data_type, NumericType) or (
                allow_bool and isinstance(operand.spark.data_type, BooleanType)
            )
    else:
        return False


def transform_boolean_operand_to_numeric(
    operand: Any, spark_type: Optional[DataType] = None
) -> Any:
    """Transform boolean operand to numeric.

    If the `operand` is:
        - a boolean IndexOpsMixin, transform the `operand` to the `spark_type`.
        - a boolean literal, transform to the int value.
    Otherwise, return the operand as it is.
    """
    from pyspark.pandas.base import IndexOpsMixin

    if isinstance(operand, IndexOpsMixin) and isinstance(operand.spark.data_type, BooleanType):
        assert spark_type, "spark_type must be provided if the operand is a boolean IndexOpsMixin"
        return operand.spark.transform(lambda scol: scol.cast(spark_type))
    elif isinstance(operand, bool):
        return int(operand)
    else:
        return operand


def _as_categorical_type(
    index_ops: IndexOpsLike, dtype: CategoricalDtype, spark_type: DataType
) -> IndexOpsLike:
    """Cast `index_ops` to categorical dtype, given `dtype` and `spark_type`."""
    assert isinstance(dtype, CategoricalDtype)
    if dtype.categories is None:
        codes, uniques = index_ops.factorize()
        return codes._with_new_scol(
            codes.spark.column,
            field=codes._internal.data_fields[0].copy(dtype=CategoricalDtype(categories=uniques)),
        )
    else:
        categories = dtype.categories
        if len(categories) == 0:
            scol = SF.lit(-1)
        else:
            kvs = chain(
                *[(SF.lit(category), SF.lit(code)) for code, category in enumerate(categories)]
            )
            map_scol = F.create_map(*kvs)

            scol = F.coalesce(map_scol.getItem(index_ops.spark.column), SF.lit(-1))
        return index_ops._with_new_scol(
            scol.cast(spark_type).alias(index_ops._internal.data_fields[0].name),
            field=index_ops._internal.data_fields[0].copy(
                dtype=dtype, spark_type=spark_type, nullable=False
            ),
        )


def _as_bool_type(index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
    """Cast `index_ops` to BooleanType Spark type, given `dtype`."""
    from pyspark.pandas.internal import InternalField

    if isinstance(dtype, extension_dtypes):
        scol = index_ops.spark.column.cast(BooleanType())
    else:
        scol = F.when(index_ops.spark.column.isNull(), SF.lit(False)).otherwise(
            index_ops.spark.column.cast(BooleanType())
        )
    return index_ops._with_new_scol(
        scol.alias(index_ops._internal.data_spark_column_names[0]),
        field=InternalField(dtype=dtype),
    )


def _as_string_type(
    index_ops: IndexOpsLike, dtype: Union[str, type, Dtype], *, null_str: str = str(None)
) -> IndexOpsLike:
    """Cast `index_ops` to StringType Spark type, given `dtype` and `null_str`,
    representing null Spark column.
    """
    from pyspark.pandas.internal import InternalField

    if isinstance(dtype, extension_dtypes):
        scol = index_ops.spark.column.cast(StringType())
    else:
        casted = index_ops.spark.column.cast(StringType())
        scol = F.when(index_ops.spark.column.isNull(), null_str).otherwise(casted)
    return index_ops._with_new_scol(
        scol.alias(index_ops._internal.data_spark_column_names[0]),
        field=InternalField(dtype=dtype),
    )


def _as_other_type(
    index_ops: IndexOpsLike, dtype: Union[str, type, Dtype], spark_type: DataType
) -> IndexOpsLike:
    """Cast `index_ops` to a `dtype` (`spark_type`) that needs no pre-processing.

    Destination types that need pre-processing: CategoricalDtype, BooleanType, and StringType.
    """
    from pyspark.pandas.internal import InternalField

    need_pre_process = (
        isinstance(dtype, CategoricalDtype)
        or isinstance(spark_type, BooleanType)
        or isinstance(spark_type, StringType)
    )
    assert not need_pre_process, "Pre-processing is needed before the type casting."

    scol = index_ops.spark.column.cast(spark_type)
    return index_ops._with_new_scol(
        scol.alias(index_ops._internal.data_spark_column_names[0]),
        field=InternalField(dtype=dtype),
    )


class DataTypeOps(object, metaclass=ABCMeta):
    """The base class for binary operations of pandas-on-Spark objects (of different data types)."""

    def __new__(cls, dtype: Dtype, spark_type: DataType) -> "DataTypeOps":
        from pyspark.pandas.data_type_ops.binary_ops import BinaryOps
        from pyspark.pandas.data_type_ops.boolean_ops import BooleanOps, BooleanExtensionOps
        from pyspark.pandas.data_type_ops.categorical_ops import CategoricalOps
        from pyspark.pandas.data_type_ops.complex_ops import ArrayOps, MapOps, StructOps
        from pyspark.pandas.data_type_ops.date_ops import DateOps
        from pyspark.pandas.data_type_ops.datetime_ops import DatetimeOps
        from pyspark.pandas.data_type_ops.null_ops import NullOps
        from pyspark.pandas.data_type_ops.num_ops import (
            DecimalOps,
            FractionalExtensionOps,
            FractionalOps,
            IntegralExtensionOps,
            IntegralOps,
        )
        from pyspark.pandas.data_type_ops.string_ops import StringOps, StringExtensionOps
        from pyspark.pandas.data_type_ops.udt_ops import UDTOps

        if isinstance(dtype, CategoricalDtype):
            return object.__new__(CategoricalOps)
        elif isinstance(spark_type, DecimalType):
            return object.__new__(DecimalOps)
        elif isinstance(spark_type, FractionalType):
            if extension_float_dtypes_available and type(dtype) in [Float32Dtype, Float64Dtype]:
                return object.__new__(FractionalExtensionOps)
            else:
                return object.__new__(FractionalOps)
        elif isinstance(spark_type, IntegralType):
            if extension_dtypes_available and type(dtype) in [
                Int8Dtype,
                Int16Dtype,
                Int32Dtype,
                Int64Dtype,
            ]:
                return object.__new__(IntegralExtensionOps)
            else:
                return object.__new__(IntegralOps)
        elif isinstance(spark_type, StringType):
            if extension_object_dtypes_available and isinstance(dtype, StringDtype):
                return object.__new__(StringExtensionOps)
            else:
                return object.__new__(StringOps)
        elif isinstance(spark_type, BooleanType):
            if extension_object_dtypes_available and isinstance(dtype, BooleanDtype):
                return object.__new__(BooleanExtensionOps)
            else:
                return object.__new__(BooleanOps)
        elif isinstance(spark_type, TimestampType):
            return object.__new__(DatetimeOps)
        elif isinstance(spark_type, DateType):
            return object.__new__(DateOps)
        elif isinstance(spark_type, BinaryType):
            return object.__new__(BinaryOps)
        elif isinstance(spark_type, ArrayType):
            return object.__new__(ArrayOps)
        elif isinstance(spark_type, MapType):
            return object.__new__(MapOps)
        elif isinstance(spark_type, StructType):
            return object.__new__(StructOps)
        elif isinstance(spark_type, NullType):
            return object.__new__(NullOps)
        elif isinstance(spark_type, UserDefinedType):
            return object.__new__(UDTOps)
        else:
            raise TypeError("Type %s was not understood." % dtype)

    def __init__(self, dtype: Dtype, spark_type: DataType):
        self.dtype = dtype
        self.spark_type = spark_type

    @property
    def pretty_name(self) -> str:
        raise NotImplementedError()

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def __and__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Bitwise and can not be applied to %s." % self.pretty_name)

    def __or__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Bitwise or can not be applied to %s." % self.pretty_name)

    def rand(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return left.__and__(right)

    def ror(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return left.__or__(right)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary - can not be applied to %s." % self.pretty_name)

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("abs() can not be applied to %s." % self.pretty_name)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("< can not be applied to %s." % self.pretty_name)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("<= can not be applied to %s." % self.pretty_name)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("> can not be applied to %s." % self.pretty_name)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError(">= can not be applied to %s." % self.pretty_name)

    def eq(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__eq__)(left, right)

    def ne(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__ne__)(left, right)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary ~ can not be applied to %s." % self.pretty_name)

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col.replace({np.nan: None})

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull(),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        raise TypeError("astype can not be applied to %s." % self.pretty_name)
