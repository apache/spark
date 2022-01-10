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
from typing import Any, Union

import numpy as np
import pandas as pd
from pandas.api.types import (  # type: ignore[attr-defined]
    is_bool_dtype,
    is_integer_dtype,
    CategoricalDtype,
)

from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import column_op, IndexOpsMixin, numpy_column_op
from pyspark.pandas.config import get_option
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    is_valid_operand_for_numeric_arithmetic,
    transform_boolean_operand_to_numeric,
    _as_bool_type,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
    _sanitize_list_like,
    _is_valid_for_logical_operator,
    _is_boolean_type,
)
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.typedef.typehints import extension_dtypes, pandas_on_spark_type
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import (
    BooleanType,
    DataType,
    StringType,
)


def _non_fractional_astype(
    index_ops: IndexOpsLike, dtype: Dtype, spark_type: DataType
) -> IndexOpsLike:
    if isinstance(dtype, CategoricalDtype):
        return _as_categorical_type(index_ops, dtype, spark_type)
    elif isinstance(spark_type, BooleanType):
        return _as_bool_type(index_ops, dtype)
    elif isinstance(spark_type, StringType):
        return _as_string_type(index_ops, dtype, null_str="NaN")
    else:
        return _as_other_type(index_ops, dtype, spark_type)


class NumericOps(DataTypeOps):
    """The class for binary operations of numeric pandas-on-Spark objects."""

    @property
    def pretty_name(self) -> str:
        return "numerics"

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Addition can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(Column.__add__)(left, right)

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Subtraction can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(Column.__sub__)(left, right)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Modulo can not be applied to given types.")

        def mod(left: Column, right: Any) -> Column:
            return ((left % right) + right) % right

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(mod)(left, right)

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Exponentiation can not be applied to given types.")

        def pow_func(left: Column, right: Any) -> Column:
            return (
                F.when(left == 1, left)
                .when(SF.lit(right) == 0, 1)
                .otherwise(Column.__pow__(left, right))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(pow_func)(left, right)

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Addition can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__radd__)(left, right)

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Subtraction can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rsub__)(left, right)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Multiplication can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rmul__)(left, right)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Exponentiation can not be applied to given types.")

        def rpow_func(left: Column, right: Any) -> Column:
            return F.when(SF.lit(right == 1), right).otherwise(Column.__rpow__(left, right))

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rpow_func)(left, right)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Modulo can not be applied to given types.")

        def rmod(left: Column, right: Any) -> Column:
            return ((right % left) + left) % left

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rmod)(left, right)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(-operand.spark.column, field=operand._internal.data_fields[0])

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(
            F.abs(operand.spark.column), field=operand._internal.data_fields[0]
        )

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(Column.__lt__)(left, right)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(Column.__le__)(left, right)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(Column.__ge__)(left, right)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(Column.__gt__)(left, right)


class IntegralOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    LongType, IntegerType, ByteType and ShortType.
    """

    def xor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right ^ left
        elif _is_valid_for_logical_operator(right):
            right_is_boolean = _is_boolean_type(right)

            def xor_func(left: Column, right: Any) -> Column:
                if not isinstance(right, Column):
                    if pd.isna(right):
                        right = SF.lit(None)
                    else:
                        right = SF.lit(right)
                return (
                    left.bitwiseXOR(right.cast("integer")).cast("boolean")
                    if right_is_boolean
                    else left.bitwiseXOR(right)
                )

            return column_op(xor_func)(left, right)
        else:
            raise TypeError("XOR can not be applied to given types.")

    @property
    def pretty_name(self) -> str:
        return "integrals"

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType):
            return column_op(SF.repeat)(right, left)

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(Column.__mul__)(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")

        def truediv(left: Column, right: Any) -> Column:
            return F.when(
                SF.lit(right != 0) | SF.lit(right).isNull(), left.__div__(right)
            ).otherwise(SF.lit(np.inf).__div__(left))

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")

        def floordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    SF.lit(right != 0) | SF.lit(right).isNull(), F.floor(left.__div__(right))
                ).otherwise(SF.lit(np.inf).__div__(left))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: Column, right: Any) -> Column:
            return F.when(left == 0, SF.lit(np.inf).__div__(right)).otherwise(
                SF.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(left == 0), SF.lit(np.inf).__div__(right)).otherwise(
                F.floor(SF.lit(right).__div__(left))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(
            F.bitwise_not(operand.spark.column), field=operand._internal.data_fields[0]
        )

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)
        return _non_fractional_astype(index_ops, dtype, spark_type)


class FractionalOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    FloatType, DoubleType.
    """

    @property
    def pretty_name(self) -> str:
        return "fractions"

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(Column.__mul__)(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")

        def truediv(left: Column, right: Any) -> Column:
            return F.when(
                SF.lit(right != 0) | SF.lit(right).isNull(), left.__div__(right)
            ).otherwise(
                F.when(SF.lit(left == np.inf) | SF.lit(left == -np.inf), left).otherwise(
                    SF.lit(np.inf).__div__(left)
                )
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")

        def floordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    SF.lit(right != 0) | SF.lit(right).isNull(), F.floor(left.__div__(right))
                ).otherwise(
                    F.when(SF.lit(left == np.inf) | SF.lit(left == -np.inf), left).otherwise(
                        SF.lit(np.inf).__div__(left)
                    )
                )
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: Column, right: Any) -> Column:
            return F.when(left == 0, SF.lit(np.inf).__div__(right)).otherwise(
                SF.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(left == 0), SF.lit(np.inf).__div__(right)).otherwise(
                F.when(SF.lit(left) == np.nan, np.nan).otherwise(
                    F.floor(SF.lit(right).__div__(left))
                )
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull() | F.isnan(index_ops.spark.column),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def nan_to_null(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        # Special handle floating point types because Spark's count treats nan as a valid value,
        # whereas pandas count doesn't include nan.
        return index_ops._with_new_scol(
            F.nanvl(index_ops.spark.column, SF.lit(None)),
            field=index_ops._internal.data_fields[0].copy(nullable=True),
        )

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if is_integer_dtype(dtype) and not isinstance(dtype, extension_dtypes):
            if get_option("compute.eager_check") and index_ops.hasnans:
                raise ValueError(
                    "Cannot convert %s with missing values to integer" % self.pretty_name
                )

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            if isinstance(dtype, extension_dtypes):
                scol = index_ops.spark.column.cast(spark_type)
            else:
                scol = F.when(
                    index_ops.spark.column.isNull() | F.isnan(index_ops.spark.column),
                    SF.lit(True),
                ).otherwise(index_ops.spark.column.cast(spark_type))
            return index_ops._with_new_scol(
                scol.alias(index_ops._internal.data_spark_column_names[0]),
                field=index_ops._internal.data_fields[0].copy(dtype=dtype, spark_type=spark_type),
            )
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(np.nan))
        else:
            return _as_other_type(index_ops, dtype, spark_type)


class DecimalOps(FractionalOps):
    """
    The class for decimal operations of pandas-on-Spark objects with spark type:
    DecimalType.
    """

    @property
    def pretty_name(self) -> str:
        return "decimal"

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("< can not be applied to %s." % self.pretty_name)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("<= can not be applied to %s." % self.pretty_name)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("> can not be applied to %s." % self.pretty_name)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError(">= can not be applied to %s." % self.pretty_name)

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull(),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def nan_to_null(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops.copy()

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)
        if is_integer_dtype(dtype) and not isinstance(dtype, extension_dtypes):
            if get_option("compute.eager_check") and index_ops.hasnans:
                raise ValueError(
                    "Cannot convert %s with missing values to integer" % self.pretty_name
                )
        return _non_fractional_astype(index_ops, dtype, spark_type)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Exponentiation can not be applied to given types.")

        def rpow_func(left: Column, right: Any) -> Column:
            return (
                F.when(left.isNull(), np.nan)
                .when(SF.lit(right == 1), right)
                .otherwise(Column.__rpow__(left, right))
            )

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rpow_func)(left, right)


class IntegralExtensionOps(IntegralOps):
    """
    The class for binary operations of pandas-on-Spark objects with one of the
    - spark types:
        LongType, IntegerType, ByteType and ShortType
    - dtypes:
        Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype
    """

    def xor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        raise TypeError("XOR can not be applied to given types.")

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col.astype(self.dtype)

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)
        if get_option("compute.eager_check"):
            if is_integer_dtype(dtype) and not isinstance(dtype, extension_dtypes):
                if index_ops.hasnans:
                    raise ValueError(
                        "Cannot convert %s with missing values to integer" % self.pretty_name
                    )
            elif is_bool_dtype(dtype) and not isinstance(dtype, extension_dtypes):
                if index_ops.hasnans:
                    raise ValueError(
                        "Cannot convert %s with missing values to bool" % self.pretty_name
                    )
        return _non_fractional_astype(index_ops, dtype, spark_type)


class FractionalExtensionOps(FractionalOps):
    """
    The class for binary operations of pandas-on-Spark objects with one of the
    - spark types:
        FloatType, DoubleType and DecimalType
    - dtypes:
        Float32Dtype, Float64Dtype
    """

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col.astype(self.dtype)

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)
        if get_option("compute.eager_check"):
            if is_integer_dtype(dtype) and not isinstance(dtype, extension_dtypes):
                if index_ops.hasnans:
                    raise ValueError(
                        "Cannot convert %s with missing values to integer" % self.pretty_name
                    )
            elif is_bool_dtype(dtype) and not isinstance(dtype, extension_dtypes):
                if index_ops.hasnans:
                    raise ValueError(
                        "Cannot convert %s with missing values to bool" % self.pretty_name
                    )

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            if isinstance(dtype, extension_dtypes):
                scol = index_ops.spark.column.cast(spark_type)
            else:
                scol = F.when(
                    index_ops.spark.column.isNull() | F.isnan(index_ops.spark.column),
                    SF.lit(True),
                ).otherwise(index_ops.spark.column.cast(spark_type))
            return index_ops._with_new_scol(
                scol.alias(index_ops._internal.data_spark_column_names[0]),
                field=index_ops._internal.data_fields[0].copy(dtype=dtype, spark_type=spark_type),
            )
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(np.nan))
        else:
            return _as_other_type(index_ops, dtype, spark_type)
