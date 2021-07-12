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
from typing import cast, Any, Union

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import column_op, IndexOpsMixin, numpy_column_op
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    is_valid_operand_for_numeric_arithmetic,
    transform_boolean_operand_to_numeric,
    _as_bool_type,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
)
from pyspark.pandas.internal import InternalField
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.typedef import extension_dtypes, pandas_on_spark_type
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import (
    BooleanType,
    StringType,
)


class NumericOps(DataTypeOps):
    """The class for binary operations of numeric pandas-on-Spark objects."""

    @property
    def pretty_name(self) -> str:
        return "numerics"

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Addition can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__add__)(left, right)

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Subtraction can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__sub__)(left, right)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Modulo can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def mod(left: Column, right: Any) -> Column:
            return ((left % right) + right) % right

        return column_op(mod)(left, right)

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Exponentiation can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def pow_func(left: Column, right: Any) -> Column:
            return F.when(left == 1, left).otherwise(Column.__pow__(left, right))

        return column_op(pow_func)(left, right)

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Addition can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__radd__)(left, right)

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Subtraction can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rsub__)(left, right)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Multiplication can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rmul__)(left, right)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Exponentiation can not be applied to given types.")

        def rpow_func(left: Column, right: Any) -> Column:
            return F.when(SF.lit(right == 1), right).otherwise(Column.__rpow__(left, right))

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rpow_func)(left, right)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Modulo can not be applied to given types.")

        def rmod(left: Column, right: Any) -> Column:
            return ((right % left) + left) % left

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rmod)(left, right)

    # TODO(SPARK-36003): Implement unary operator `invert` as below
    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise NotImplementedError("Unary ~ can not be applied to %s." % self.pretty_name)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        from pyspark.pandas.base import column_op

        return cast(IndexOpsLike, column_op(Column.__neg__)(operand))

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        from pyspark.pandas.base import column_op

        return cast(IndexOpsLike, column_op(F.abs)(operand))

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__lt__)(left, right)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__le__)(left, right)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__ge__)(left, right)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        return column_op(Column.__gt__)(left, right)


class IntegralOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    LongType, IntegerType, ByteType and ShortType.
    """

    @property
    def pretty_name(self) -> str:
        return "integrals"

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType):
            return column_op(SF.repeat)(right, left)

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__mul__)(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def truediv(left: Column, right: Any) -> Column:
            return F.when(
                SF.lit(right != 0) | SF.lit(right).isNull(), left.__div__(right)
            ).otherwise(SF.lit(np.inf).__div__(left))

        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def floordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    SF.lit(right != 0) | SF.lit(right).isNull(), F.floor(left.__div__(right))
                ).otherwise(SF.lit(np.inf).__div__(left))
            )

        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: Column, right: Any) -> Column:
            return F.when(left == 0, SF.lit(np.inf).__div__(right)).otherwise(
                SF.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(left == 0), SF.lit(np.inf).__div__(right)).otherwise(
                F.floor(SF.lit(right).__div__(left))
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            return _as_bool_type(index_ops, dtype)
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(np.nan))
        else:
            return _as_other_type(index_ops, dtype, spark_type)


class FractionalOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    FloatType, DoubleType.
    """

    @property
    def pretty_name(self) -> str:
        return "fractions"

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__mul__)(left, right)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("True division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def truediv(left: Column, right: Any) -> Column:
            return F.when(
                SF.lit(right != 0) | SF.lit(right).isNull(), left.__div__(right)
            ).otherwise(
                F.when(SF.lit(left == np.inf) | SF.lit(left == -np.inf), left).otherwise(
                    SF.lit(np.inf).__div__(left)
                )
            )

        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("Floor division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

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

        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("True division can not be applied to given types.")

        def rtruediv(left: Column, right: Any) -> Column:
            return F.when(left == 0, SF.lit(np.inf).__div__(right)).otherwise(
                SF.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if not isinstance(right, numbers.Number):
            raise TypeError("Floor division can not be applied to given types.")

        def rfloordiv(left: Column, right: Any) -> Column:
            return F.when(SF.lit(left == 0), SF.lit(np.inf).__div__(right)).otherwise(
                F.when(SF.lit(left) == np.nan, np.nan).otherwise(
                    F.floor(SF.lit(right).__div__(left))
                )
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull() | F.isnan(index_ops.spark.column),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

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
                field=InternalField(dtype=dtype),
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

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary ~ can not be applied to %s." % self.pretty_name)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("< can not be applied to %s." % self.pretty_name)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("<= can not be applied to %s." % self.pretty_name)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("> can not be applied to %s." % self.pretty_name)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError(">= can not be applied to %s." % self.pretty_name)

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull(),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            return _as_bool_type(index_ops, dtype)
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(np.nan))
        else:
            return _as_other_type(index_ops, dtype, spark_type)


class IntegralExtensionOps(IntegralOps):
    """
    The class for binary operations of pandas-on-Spark objects with one of the
    - spark types:
        LongType, IntegerType, ByteType and ShortType
    - dtypes:
        Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype
    """

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col.astype(self.dtype)


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
