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

import pandas as pd

from pyspark import sql as spark
from pyspark.pandas.base import column_op, IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    is_valid_operand_for_numeric_arithmetic,
    DataTypeOps,
    transform_boolean_operand_to_numeric,
)
from pyspark.pandas.typedef import extension_dtypes
from pyspark.pandas.typedef.typehints import as_spark_type
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


class BooleanOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: BooleanType.
    """

    @property
    def pretty_name(self) -> str:
        return "booleans"

    def add(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name
            )

        if isinstance(right, bool):
            return left.__or__(right)
        elif isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left + right
        else:
            assert isinstance(right, IndexOpsMixin)
            if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, BooleanType):
                return left.__or__(right)
            else:
                left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
                return left + right

    def sub(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left - right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left - right

    def mul(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right):
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, bool):
            return left.__and__(right)
        elif isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left * right
        else:
            assert isinstance(right, IndexOpsMixin)
            if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, BooleanType):
                return left.__and__(right)
            else:
                left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
                return left * right

    def truediv(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left / right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left / right

    def floordiv(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left // right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left // right

    def mod(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left % right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left % right

    def pow(self, left, right) -> Union["Series", "Index"]:
        if not is_valid_operand_for_numeric_arithmetic(right, allow_bool=False):
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name
            )
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left ** right
        else:
            assert isinstance(right, IndexOpsMixin)
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left ** right

    def radd(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, bool):
            return left.__or__(right)
        elif isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right + left
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name
            )

    def rsub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right - left
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name
            )

    def rmul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, bool):
            return left.__and__(right)
        elif isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right * left
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name
            )

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right / left
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name
            )

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right // left
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name
            )

    def rpow(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right ** left
        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name
            )

    def rmod(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number) and not isinstance(right, bool):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right % left
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name
            )

    def __and__(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right.__and__(left)
        else:

            def and_func(left, right):
                if not isinstance(right, spark.Column):
                    if pd.isna(right):
                        right = F.lit(None)
                    else:
                        right = F.lit(right)
                scol = left & right
                return F.when(scol.isNull(), False).otherwise(scol)

            return column_op(and_func)(left, right)

    def __or__(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, extension_dtypes):
            return right.__or__(left)
        else:

            def or_func(left, right):
                if not isinstance(right, spark.Column) and pd.isna(right):
                    return F.lit(False)
                else:
                    scol = left | F.lit(right)
                    return F.when(left.isNull() | scol.isNull(), False).otherwise(scol)

            return column_op(or_func)(left, right)


class BooleanExtensionOps(BooleanOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type BooleanType,
    and dtype BooleanDtype.
    """

    def __and__(self, left, right) -> Union["Series", "Index"]:
        def and_func(left, right):
            if not isinstance(right, spark.Column):
                if pd.isna(right):
                    right = F.lit(None)
                else:
                    right = F.lit(right)
            return left & right

        return column_op(and_func)(left, right)

    def __or__(self, left, right) -> Union["Series", "Index"]:
        def or_func(left, right):
            if not isinstance(right, spark.Column):
                if pd.isna(right):
                    right = F.lit(None)
                else:
                    right = F.lit(right)
            return left | right

        return column_op(or_func)(left, right)
