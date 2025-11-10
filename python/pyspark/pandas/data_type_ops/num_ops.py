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

import decimal
import numbers
from typing import Any, Union, Callable, cast

import numpy as np
import pandas as pd
from pandas.api.types import (  # type: ignore[attr-defined]
    is_bool_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    CategoricalDtype,
    is_list_like,
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
    _should_return_all_false,
)
from pyspark.pandas.typedef.typehints import extension_dtypes, pandas_on_spark_type, as_spark_type
from pyspark.pandas.utils import is_ansi_mode_enabled
from pyspark.sql import functions as F, Column as PySparkColumn
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DecimalType,
    StringType,
)
from pyspark.errors import PySparkValueError

# For Supporting Spark Connect
from pyspark.sql.utils import pyspark_column_op


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


def _cast_back_float(
    expr: PySparkColumn, left_dtype: Union[str, type, Dtype], right: Any
) -> PySparkColumn:
    """
    Cast the result expression back to the original float dtype if needed.

    This function ensures pandas on Spark matches pandas behavior when performing
    arithmetic operations involving float32 and numeric values. In such cases, under ANSI mode,
    Spark implicitly widen float32 to float64, when the other operand is a numeric type
    but not float32 (e.g., int, bool), which deviates from pandas behavior where the result
    retains float32.
    """
    is_left_float = is_float_dtype(left_dtype)
    is_right_numeric = isinstance(right, (int, float, bool)) or (
        hasattr(right, "dtype") and is_numeric_dtype(right.dtype)
    )
    if is_left_float and is_right_numeric:
        return expr.cast(as_spark_type(left_dtype))
    return expr


def _is_decimal_float_mixed(left: IndexOpsLike, right: Any) -> bool:
    left_is_decimal = isinstance(left.spark.data_type, DecimalType)
    left_is_float = is_float_dtype(left.dtype)

    if isinstance(right, IndexOpsMixin):
        right_is_float = is_float_dtype(right.dtype)
        right_is_decimal = isinstance(right.spark.data_type, DecimalType)
    else:
        # scalar
        right_is_float = isinstance(right, (float, np.floating))
        right_is_decimal = isinstance(right, decimal.Decimal)

    return (left_is_decimal and right_is_float) or (left_is_float and right_is_decimal)


class NumericOps(DataTypeOps):
    """The class for binary operations of numeric pandas-on-Spark objects."""

    @property
    def pretty_name(self) -> str:
        return "numerics"

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Addition can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        def wrapped_add(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__add__(lc, rc)
            if is_ansi_mode_enabled(spark_session):
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_add)(left, new_right)

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Subtraction can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        def wrapped_sub(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__sub__(lc, rc)
            if is_ansi_mode_enabled(spark_session):
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_sub)(left, new_right)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Modulo can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession

        def mod(left_op: PySparkColumn, right_op: Any) -> PySparkColumn:
            if is_ansi_mode_enabled(spark_session):
                expr = F.when(F.lit(right_op == 0), F.lit(None)).otherwise(
                    ((left_op % right_op) + right_op) % right_op
                )
                expr = _cast_back_float(expr, left.dtype, right)
            else:
                expr = ((left_op % right_op) + right_op) % right_op
            return expr

        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        return column_op(mod)(left, new_right)

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Exponentiation can not be applied to given types.")

        def pow_func(left: PySparkColumn, right: Any) -> PySparkColumn:
            return (
                F.when(left == 1, left)
                .when(F.lit(right) == 0, 1)
                .otherwise(PySparkColumn.__pow__(left, right))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return column_op(pow_func)(left, right)

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Addition can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        new_right = transform_boolean_operand_to_numeric(right)

        def wrapped_radd(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__radd__(lc, rc)
            if is_ansi_mode_enabled(spark_session):
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_radd)(left, new_right)

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Subtraction can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        new_right = transform_boolean_operand_to_numeric(right)

        def wrapped_rsub(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__rsub__(lc, rc)
            if is_ansi_mode_enabled(spark_session):
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_rsub)(left, new_right)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Multiplication can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        new_right = transform_boolean_operand_to_numeric(right)

        def wrapped_rmul(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__mul__(lc, rc)
            if is_ansi_mode_enabled(spark_session):
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_rmul)(left, new_right)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Exponentiation can not be applied to given types.")

        def rpow_func(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(F.lit(right == 1), right).otherwise(PySparkColumn.__rpow__(left, right))

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rpow_func)(left, right)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Modulo can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession

        new_right = transform_boolean_operand_to_numeric(right)

        def safe_rmod(left_op: PySparkColumn, right_op: Any) -> PySparkColumn:
            if is_ansi_mode_enabled(spark_session):
                # Java-style modulo -> Python-style modulo
                result = F.when(
                    left_op != 0, ((F.lit(right_op) % left_op) + left_op) % left_op
                ).otherwise(F.lit(None))
                result = _cast_back_float(result, left.dtype, right)
                return result
            else:
                return ((right_op % left_op) + left_op) % left_op

        return column_op(safe_rmod)(left, new_right)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(-operand.spark.column, field=operand._internal.data_fields[0])

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        return operand._with_new_scol(
            F.abs(operand.spark.column), field=operand._internal.data_fields[0]
        )

    def eq(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        # We can directly use `super().eq` when given object is list, tuple, dict or set.
        if not isinstance(right, IndexOpsMixin) and is_list_like(right):
            return super().eq(left, right)
        else:
            if is_ansi_mode_enabled(left._internal.spark_frame.sparkSession):
                if _should_return_all_false(left, right):
                    left_scol = left._with_new_scol(F.lit(False))
                    if isinstance(right, IndexOpsMixin):
                        # When comparing with another Series/Index, drop the name
                        # to align with pandas behavior
                        return left_scol.rename(None)  # type: ignore[attr-defined]
                    else:
                        # When comparing with scalar-like, keep the name of left operand
                        return cast(SeriesOrIndex, left_scol)
                if _is_boolean_type(right):  # numeric vs. bool
                    right = transform_boolean_operand_to_numeric(
                        right, spark_type=left.spark.data_type
                    )
            return pyspark_column_op("__eq__", left, right, fillna=False)

    def ne(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__ne__", left, right, fillna=True)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__lt__", left, right, fillna=False)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__le__", left, right, fillna=False)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__ge__", left, right, fillna=False)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__gt__", left, right, fillna=False)


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

            def xor_func(left: PySparkColumn, right: Any) -> PySparkColumn:
                try:
                    is_null = pd.isna(right)
                except PySparkValueError:
                    # Complaining `PySparkValueError` means that `right` is a Column.
                    is_null = False

                right = F.lit(None) if is_null else F.lit(right)
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
            return column_op(F.repeat)(right, left)

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        return column_op(PySparkColumn.__mul__)(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        def truediv(left: PySparkColumn, right: Any) -> PySparkColumn:
            if is_ansi_mode_enabled(spark_session):
                return F.when(
                    F.lit(right == 0),
                    F.when(left < 0, F.lit(float("-inf")))
                    .when(left > 0, F.lit(float("inf")))
                    .otherwise(F.lit(np.nan)),
                ).otherwise(left / right)
            else:
                return F.when(
                    F.lit(right != 0) | F.lit(right).isNull(),
                    left.__div__(right),
                ).otherwise(F.lit(np.inf).__div__(left))

        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")
        spark_session = left._internal.spark_frame.sparkSession
        use_try_divide = is_ansi_mode_enabled(spark_session)

        def fallback_div(x: PySparkColumn, y: PySparkColumn) -> PySparkColumn:
            return x.__div__(y)

        safe_div: Callable[[PySparkColumn, PySparkColumn], PySparkColumn] = (
            F.try_divide if use_try_divide else fallback_div
        )

        def floordiv(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(F.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    F.lit(right != 0) | F.lit(right).isNull(),
                    F.floor(left.__div__(right)),
                ).otherwise(safe_div(F.lit(np.inf), left))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(left == 0, F.lit(np.inf).__div__(right)).otherwise(
                F.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(F.lit(left == 0), F.lit(np.inf).__div__(right)).otherwise(
                F.floor(F.lit(right).__div__(left))
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

        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Multiplication can not be applied to given types.")
        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)

        def wrapped_mul(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = PySparkColumn.__mul__(lc, rc)
            if is_ansi:
                expr = _cast_back_float(expr, left.dtype, right)
            return expr

        return column_op(wrapped_mul)(left, new_right)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Modulo can not be applied to given types.")

        return super().mod(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")
        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("True division can not be applied to given types.")
        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        left_dtype = left.dtype

        def truediv(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            if is_ansi:
                expr = F.when(
                    F.lit(rc == 0),
                    F.when(lc < 0, F.lit(float("-inf")))
                    .when(lc > 0, F.lit(float("inf")))
                    .otherwise(F.lit(np.nan)),
                ).otherwise(lc / rc)
            else:
                expr = F.when(
                    F.lit(rc != 0) | F.lit(rc).isNull(),
                    lc.__div__(rc),
                ).otherwise(
                    F.when(F.lit(lc == np.inf) | F.lit(lc == -np.inf), lc).otherwise(
                        F.lit(np.inf).__div__(lc)
                    )
                )
            return _cast_back_float(expr, left_dtype, right)

        return numpy_column_op(truediv)(left, new_right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")
        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Floor division can not be applied to given types.")
        left_dtype = left.dtype

        def fallback_div(x: PySparkColumn, y: PySparkColumn) -> PySparkColumn:
            return x.__div__(y)

        safe_div: Callable[[PySparkColumn, PySparkColumn], PySparkColumn] = (
            F.try_divide if is_ansi else fallback_div
        )

        def floordiv(lc: PySparkColumn, rc: Any) -> PySparkColumn:
            expr = F.when(F.lit(rc is np.nan), np.nan).otherwise(
                F.when(
                    F.lit(rc != 0) | F.lit(rc).isNull(),
                    F.floor(lc.__div__(rc)),
                ).otherwise(
                    F.when(F.lit(lc == np.inf) | F.lit(lc == -np.inf), lc).otherwise(
                        safe_div(F.lit(np.inf), lc)
                    )
                )
            )
            return _cast_back_float(expr, left_dtype, right)

        new_right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(floordiv)(left, new_right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(left == 0, F.lit(np.inf).__div__(right)).otherwise(
                F.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: PySparkColumn, right: Any) -> PySparkColumn:
            return F.when(F.lit(left == 0), F.lit(np.inf).__div__(right)).otherwise(
                F.when(F.lit(left) == np.nan, np.nan).otherwise(F.floor(F.lit(right).__div__(left)))
            )

        right = transform_boolean_operand_to_numeric(right, spark_type=left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Multiplication can not be applied to given types.")
        return super().rmul(left, right)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        is_ansi = is_ansi_mode_enabled(left._internal.spark_frame.sparkSession)
        if is_ansi and _is_decimal_float_mixed(left, right):
            raise TypeError("Modulo can not be applied to given types.")

        return super().rmod(left, right)

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
            F.nanvl(index_ops.spark.column, F.lit(None)),
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
                    F.lit(True),
                ).otherwise(index_ops.spark.column.cast(spark_type))
            return index_ops._with_new_scol(
                scol.alias(index_ops._internal.data_spark_column_names[0]),
                field=index_ops._internal.data_fields[0].copy(
                    dtype=dtype, spark_type=spark_type  # type: ignore[arg-type]
                ),
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

        def rpow_func(left: PySparkColumn, right: Any) -> PySparkColumn:
            return (
                F.when(left.isNull(), np.nan)
                .when(F.lit(right == 1), right)
                .otherwise(PySparkColumn.__rpow__(left, right))
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
                    F.lit(True),
                ).otherwise(index_ops.spark.column.cast(spark_type))
            return index_ops._with_new_scol(
                scol.alias(index_ops._internal.data_spark_column_names[0]),
                field=index_ops._internal.data_fields[0].copy(
                    dtype=dtype, spark_type=spark_type  # type: ignore[arg-type]
                ),
            )
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(np.nan))
        else:
            return _as_other_type(index_ops, dtype, spark_type)
