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

import pandas as pd
from pandas.api.types import CategoricalDtype, is_integer_dtype  # type: ignore[attr-defined]
from pandas.core.dtypes.common import is_numeric_dtype

from pyspark.pandas.base import column_op, IndexOpsMixin
from pyspark.pandas.config import get_option
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    is_valid_operand_for_numeric_arithmetic,
    transform_boolean_operand_to_numeric,
    _as_bool_type,
    _as_categorical_type,
    _as_other_type,
    _sanitize_list_like,
    _is_valid_for_logical_operator,
    _is_boolean_type,
)
from pyspark.pandas.typedef.typehints import as_spark_type, extension_dtypes, pandas_on_spark_type
from pyspark.pandas.utils import is_ansi_mode_enabled
from pyspark.sql import functions as F, Column as PySparkColumn
from pyspark.sql.types import BooleanType, StringType
from pyspark.errors import PySparkValueError


class BooleanOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: BooleanType.
    """

    @property
    def pretty_name(self) -> str:
        return "bools"

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name
            )

        if isinstance(right, bool):
            return left.__or__(right)
        elif isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left + right
        else:
            assert isinstance(right, IndexOpsMixin)
            if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, BooleanType):
                return left.__or__(right)
            else:
                left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
                return left + right

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left - right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
            return left - right

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, bool):
            return left.__and__(right)
        elif isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left * right
        else:
            assert isinstance(right, IndexOpsMixin)
            if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, BooleanType):
                return left.__and__(right)
            else:
                left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
                return left * right

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left / right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
            return left / right

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left // right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
            return left // right

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left % right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
            return left % right

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return left**right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, spark_type=right.spark.data_type)
            return left**right

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, bool):
            return left.__or__(right)
        elif isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right + left
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name
            )

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right - left
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name
            )

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, bool):
            return left.__and__(right)
        elif isinstance(right, numbers.Number):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right * left
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name
            )

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right / left
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name
            )

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right // left
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name
            )

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right**left
        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name
            )

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = transform_boolean_operand_to_numeric(left, spark_type=as_spark_type(type(right)))
            return right % left
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name
            )

    def __and__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if (
            is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
            and self.dtype == bool
            and right is None
        ):
            raise TypeError("AND can not be applied to given types.")
        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right.__and__(left)
        else:

            def and_func(left: PySparkColumn, right: Any) -> PySparkColumn:
                try:
                    is_null = pd.isna(right)
                except PySparkValueError:
                    # Complaining `PySparkValueError` means that `right` is a Column.
                    is_null = False

                right = F.lit(None) if is_null else F.lit(right)
                scol = left & right
                return F.when(scol.isNull(), False).otherwise(scol)

            return column_op(and_func)(left, right)

    def xor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if (
            is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
            and self.dtype == bool
            and right is None
        ):
            raise TypeError("XOR can not be applied to given types.")
        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right ^ left
        elif _is_valid_for_logical_operator(right):

            def xor_func(left: PySparkColumn, right: Any) -> PySparkColumn:
                try:
                    is_null = pd.isna(right)
                except PySparkValueError:
                    # Complaining `PySparkValueError` means that `right` is a Column.
                    is_null = False

                right = F.lit(None) if is_null else F.lit(right)
                scol = left.cast("integer").bitwiseXOR(right.cast("integer")).cast("boolean")
                return F.when(scol.isNull(), False).otherwise(scol)

            return column_op(xor_func)(left, right)
        else:
            raise TypeError("XOR can not be applied to given types.")

    def __or__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if (
            is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
            and self.dtype == bool
            and right is None
        ):
            raise TypeError("OR can not be applied to given types.")
        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right.__or__(left)
        else:

            def or_func(left: PySparkColumn, right: Any) -> PySparkColumn:
                try:
                    is_null = pd.isna(right)
                except PySparkValueError:
                    # Complaining `PySparkValueError` means that `right` is a Column.
                    is_null = False

                if is_null:
                    return F.lit(False)
                else:
                    scol = left | F.lit(right)
                    return F.when(left.isNull() | scol.isNull(), False).otherwise(scol)

            return column_op(or_func)(left, right)

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            return _as_bool_type(index_ops, dtype)
        elif isinstance(spark_type, StringType):
            if isinstance(dtype, extension_dtypes):
                scol = F.when(
                    index_ops.spark.column.isNotNull(),
                    F.when(index_ops.spark.column, "True").otherwise("False"),
                )
                nullable = index_ops.spark.nullable
            else:
                null_str = str(pd.NA) if isinstance(self, BooleanExtensionOps) else str(None)
                casted = F.when(index_ops.spark.column, "True").otherwise("False")
                scol = F.when(index_ops.spark.column.isNull(), null_str).otherwise(casted)
                nullable = False
            return index_ops._with_new_scol(
                scol,
                field=index_ops._internal.data_fields[0].copy(
                    dtype=dtype, spark_type=spark_type, nullable=nullable  # type: ignore[arg-type]
                ),
            )
        else:
            is_ansi = is_ansi_mode_enabled(index_ops._internal.spark_frame.sparkSession)
            if is_ansi and get_option("compute.eager_check"):
                if is_integer_dtype(dtype) and not isinstance(dtype, extension_dtypes):
                    if index_ops.hasnans:
                        raise ValueError(
                            "Cannot convert %s with missing values to integer" % self.pretty_name
                        )
            return _as_other_type(index_ops, dtype, spark_type)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        return ~operand

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand

    def eq(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if is_ansi_mode_enabled(left._internal.spark_frame.sparkSession):
            # Handle bool vs. non-bool numeric comparisons
            left_is_bool = _is_boolean_type(left)
            right_is_non_bool_numeric = is_numeric_dtype(right) and not _is_boolean_type(right)

            if left_is_bool and right_is_non_bool_numeric:
                if isinstance(right, numbers.Number):
                    left = transform_boolean_operand_to_numeric(
                        left, spark_type=as_spark_type(type(right))
                    )
                else:
                    left = transform_boolean_operand_to_numeric(
                        left, spark_type=right.spark.data_type
                    )

        return super().eq(left, right)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(PySparkColumn.__lt__)(left, right)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(PySparkColumn.__le__)(left, right)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(PySparkColumn.__ge__)(left, right)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return column_op(PySparkColumn.__gt__)(left, right)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(~operand.spark.column, field=operand._internal.data_fields[0])


class BooleanExtensionOps(BooleanOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type BooleanType,
    and dtype BooleanDtype.
    """

    @property
    def pretty_name(self) -> str:
        return "booleans"

    def __and__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        def and_func(left: PySparkColumn, right: Any) -> PySparkColumn:
            try:
                is_null = pd.isna(right)
            except PySparkValueError:
                # Complaining `PySparkValueError` means that `right` is a Column.
                is_null = False

            right = F.lit(None) if is_null else F.lit(right)
            return left & right

        return column_op(and_func)(left, right)

    def __or__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        def or_func(left: PySparkColumn, right: Any) -> PySparkColumn:
            try:
                is_null = pd.isna(right)
            except PySparkValueError:
                # Complaining `PySparkValueError` means that `right` is a Column.
                is_null = False

            right = F.lit(None) if is_null else F.lit(right)
            return left | right

        return column_op(or_func)(left, right)

    def xor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        if _is_boolean_type(right):

            def xor_func(left: PySparkColumn, right: Any) -> PySparkColumn:
                try:
                    is_null = pd.isna(right)
                except PySparkValueError:
                    # Complaining `PySparkValueError` means that `right` is a Column.
                    is_null = False

                right = F.lit(None) if is_null else F.lit(right)
                return left.cast("integer").bitwiseXOR(right.cast("integer")).cast("boolean")

            return column_op(xor_func)(left, right)
        else:
            raise TypeError("XOR can not be applied to given types.")

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col.astype(self.dtype)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary - can not be applied to %s." % self.pretty_name)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary ~ can not be applied to %s." % self.pretty_name)

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("abs() can not be applied to %s." % self.pretty_name)
