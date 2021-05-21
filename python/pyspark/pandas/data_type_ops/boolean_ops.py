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
from pandas.api.types import CategoricalDtype

from pyspark.pandas.base import column_op, IndexOpsMixin, numpy_column_op
from pyspark.pandas.data_type_ops.base import DataTypeOps, transform_boolean_operand_to_numeric
from pyspark.pandas.typedef.typehints import as_spark_type
from pyspark.sql import Column, functions as F
from pyspark.sql.types import NumericType

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


def is_numeric_index_ops(index_ops: IndexOpsMixin) -> bool:
    """Check if the given index_ops is numeric IndexOpsMixin."""
    return isinstance(index_ops.spark.data_type, NumericType) and (
        not isinstance(index_ops.dtype, CategoricalDtype))


class BooleanOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: BooleanType.
    """

    @property
    def pretty_name(self) -> str:
        return 'booleans'

    def add(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__add__)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return column_op(Column.__add__)(left, right)
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name)

    def sub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__sub__)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return column_op(Column.__sub__)(left, right)
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name)

    def mul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__mul__)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return column_op(Column.__mul__)(left, right)
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name)

    def truediv(self, left, right) -> Union["Series", "Index"]:
        def truediv(left, right):
            return F.when(F.lit(right != 0) | F.lit(right).isNull(), left.__div__(right)).otherwise(
                F.when(F.lit(left == np.inf) | F.lit(left == -np.inf), left).otherwise(
                    F.lit(np.inf).__div__(left)
                )
            )

        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return numpy_column_op(truediv)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return numpy_column_op(truediv)(left, right)
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name)

    def floordiv(self, left, right) -> Union["Series", "Index"]:
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

        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return numpy_column_op(floordiv)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return numpy_column_op(floordiv)(left, right)
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name)

    def mod(self, left, right) -> Union["Series", "Index"]:
        def mod(left, right):
            return ((left % right) + right) % right

        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(mod)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return column_op(mod)(left, right)
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name)

    def pow(self, left, right) -> Union["Series", "Index"]:
        def pow_func(left, right):
            return F.when(left == 1, left).otherwise(Column.__pow__(left, right))

        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(pow_func)(left, right)
        elif isinstance(right, IndexOpsMixin) and is_numeric_index_ops(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return column_op(pow_func)(left, right)

        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name)

    def radd(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__radd__)(left, right)
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name)

    def rsub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__rsub__)(left, right)
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name)

    def rmul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return column_op(Column.__rmul__)(left, right)
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name)

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))

            def rtruediv(left, right):
                return F.when(left == 0, F.lit(np.inf).__div__(right)).otherwise(
                    F.lit(right).__truediv__(left)
                )

            return numpy_column_op(rtruediv)(left, right)
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name)

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))

            def rfloordiv(left, right):
                return F.when(F.lit(left == 0), F.lit(np.inf).__div__(right)).otherwise(
                    F.when(F.lit(left) == np.nan, np.nan).otherwise(
                        F.floor(F.lit(right).__div__(left))
                    )
                )

            return numpy_column_op(rfloordiv)(left, right)
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name)

    def rpow(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))

            def rpow_func(left, right):
                return F.when(F.lit(right == 1), right).otherwise(Column.__rpow__(left, right))

            return column_op(rpow_func)(left, right)
        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name)

    def rmod(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))

            def rmod(left, right):
                return ((right % left) + left) % left

            return column_op(rmod)(left, right)
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name)
