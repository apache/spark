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
from typing import TYPE_CHECKING, Union

import numpy as np

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    TimestampType,
)

from pyspark.pandas.base import column_op, IndexOpsMixin, numpy_column_op
from pyspark.pandas.data_type_ops.base import (
    is_valid_operand_for_numeric_arithmetic,
    DataTypeOps,
    transform_boolean_operand_to_numeric,
)
from pyspark.pandas.spark import functions as SF
from pyspark.sql.column import Column


if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


class NumericOps(DataTypeOps):
    """The class for binary operations of numeric pandas-on-Spark objects."""

    @property
    def pretty_name(self) -> str:
        return "numerics"

    def add(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("string addition can only be applied to string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("addition can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__add__)(left, right)

    def sub(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("subtraction can not be applied to string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("subtraction can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__sub__)(left, right)

    def mod(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("modulo can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("modulo can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def mod(left, right):
            return ((left % right) + right) % right

        return column_op(mod)(left, right)

    def pow(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("exponentiation can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("exponentiation can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def pow_func(left, right):
            return F.when(left == 1, left).otherwise(Column.__pow__(left, right))

        return column_op(pow_func)(left, right)

    def radd(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("string addition can only be applied to string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("addition can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__radd__)(left, right)

    def rsub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("subtraction can not be applied to string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("subtraction can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rsub__)(left, right)

    def rmul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("multiplication can not be applied to a string literal.")
        if not isinstance(right, numbers.Number):
            raise TypeError("multiplication can not be applied to given types.")
        right = transform_boolean_operand_to_numeric(right)
        return column_op(Column.__rmul__)(left, right)

    def rpow(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("exponentiation can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("exponentiation can not be applied to given types.")

        def rpow_func(left, right):
            return F.when(F.lit(right == 1), right).otherwise(Column.__rpow__(left, right))

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rpow_func)(left, right)

    def rmod(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("modulo can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("modulo can not be applied to given types.")

        def rmod(left, right):
            return ((right % left) + left) % left

        right = transform_boolean_operand_to_numeric(right)
        return column_op(rmod)(left, right)


class IntegralOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    LongType, IntegerType, ByteType and ShortType.
    """

    @property
    def pretty_name(self) -> str:
        return "integrals"

    def mul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("multiplication can not be applied to a string literal.")

        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, TimestampType):
            raise TypeError("multiplication can not be applied to date times.")

        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType):
            return column_op(SF.repeat)(right, left)

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__mul__)(left, right)

    def truediv(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def truediv(left, right):
            return F.when(F.lit(right != 0) | F.lit(right).isNull(), left.__div__(right)).otherwise(
                F.lit(np.inf).__div__(left)
            )

        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def floordiv(left, right):
            return F.when(F.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    F.lit(right != 0) | F.lit(right).isNull(), F.floor(left.__div__(right))
                ).otherwise(F.lit(np.inf).__div__(left))
            )

        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("division can not be applied to given types.")

        def rtruediv(left, right):
            return F.when(left == 0, F.lit(np.inf).__div__(right)).otherwise(
                F.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("division can not be applied to given types.")

        def rfloordiv(left, right):
            return F.when(F.lit(left == 0), F.lit(np.inf).__div__(right)).otherwise(
                F.floor(F.lit(right).__div__(left))
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)


class FractionalOps(NumericOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark types:
    FloatType, DoubleType and DecimalType.
    """

    @property
    def pretty_name(self) -> str:
        return "fractions"

    def mul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("multiplication can not be applied to a string literal.")

        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, TimestampType):
            raise TypeError("multiplication can not be applied to date times.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("multiplication can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        return column_op(Column.__mul__)(left, right)

    def truediv(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def truediv(left, right):
            return F.when(F.lit(right != 0) | F.lit(right).isNull(), left.__div__(right)).otherwise(
                F.when(F.lit(left == np.inf) | F.lit(left == -np.inf), left).otherwise(
                    F.lit(np.inf).__div__(left)
                )
            )

        return numpy_column_op(truediv)(left, right)

    def floordiv(self, left, right) -> Union["Series", "Index"]:
        if (
            isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType)
        ) or isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")

        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError("division can not be applied to given types.")

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)

        def floordiv(left, right):
            return F.when(F.lit(right is np.nan), np.nan).otherwise(
                F.when(
                    F.lit(right != 0) | F.lit(right).isNull(), F.floor(left.__div__(right))
                ).otherwise(
                    F.when(F.lit(left == np.inf) | F.lit(left == -np.inf), left).otherwise(
                        F.lit(np.inf).__div__(left)
                    )
                )
            )

        return numpy_column_op(floordiv)(left, right)

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("division can not be applied to given types.")

        def rtruediv(left, right):
            return F.when(left == 0, F.lit(np.inf).__div__(right)).otherwise(
                F.lit(right).__truediv__(left)
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rtruediv)(left, right)

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, str):
            raise TypeError("division can not be applied on string series or literals.")
        if not isinstance(right, numbers.Number):
            raise TypeError("division can not be applied to given types.")

        def rfloordiv(left, right):
            return F.when(F.lit(left == 0), F.lit(np.inf).__div__(right)).otherwise(
                F.when(F.lit(left) == np.nan, np.nan).otherwise(F.floor(F.lit(right).__div__(left)))
            )

        right = transform_boolean_operand_to_numeric(right, left.spark.data_type)
        return numpy_column_op(rfloordiv)(left, right)
