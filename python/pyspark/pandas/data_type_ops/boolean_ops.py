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

from pyspark.pandas.data_type_ops.base import DataTypeOps, transform_boolean_operand_to_numeric
from pyspark.pandas.data_type_ops.num_ops import is_valid_operand_for_numeric_arithmetic
from pyspark.pandas.typedef.typehints import as_spark_type

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


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
            return left + right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left + right
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name)

    def sub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left - right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left - right
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name)

    def mul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left * right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left * right
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name)

    def truediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left / right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left / right
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name)

    def floordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left // right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left // right
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name)

    def mod(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left % right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left % right
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name)

    def pow(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return left ** right
        elif is_valid_operand_for_numeric_arithmetic(right):
            left = transform_boolean_operand_to_numeric(left, right.spark.data_type)
            return left ** right
        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name)

    def radd(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right + left
        else:
            raise TypeError(
                "Addition can not be applied to %s and the given type." % self.pretty_name)

    def rsub(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right - left
        else:
            raise TypeError(
                "Subtraction can not be applied to %s and the given type." % self.pretty_name)

    def rmul(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right * left
        else:
            raise TypeError(
                "Multiplication can not be applied to %s and the given type." % self.pretty_name)

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right / left
        else:
            raise TypeError(
                "True division can not be applied to %s and the given type." % self.pretty_name)

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right // left
        else:
            raise TypeError(
                "Floor division can not be applied to %s and the given type." % self.pretty_name)

    def rpow(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right ** left
        else:
            raise TypeError(
                "Exponentiation can not be applied to %s and the given type." % self.pretty_name)

    def rmod(self, left, right) -> Union["Series", "Index"]:
        if isinstance(right, numbers.Number):
            left = left.spark.transform(lambda scol: scol.cast(as_spark_type(type(right))))
            return right % left
        else:
            raise TypeError(
                "Modulo can not be applied to %s and the given type." % self.pretty_name)
