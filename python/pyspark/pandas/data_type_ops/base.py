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
from typing import Any, Optional, TYPE_CHECKING, Union

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
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

import pyspark.sql.types as types
from pyspark.pandas.typedef import Dtype
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available

if extension_object_dtypes_available:
    from pandas import BooleanDtype

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


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
    operand: Any, spark_type: Optional[types.DataType] = None
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


class DataTypeOps(object, metaclass=ABCMeta):
    """The base class for binary operations of pandas-on-Spark objects (of different data types)."""

    def __new__(cls, dtype: Dtype, spark_type: DataType):
        from pyspark.pandas.data_type_ops.binary_ops import BinaryOps
        from pyspark.pandas.data_type_ops.boolean_ops import BooleanOps, BooleanExtensionOps
        from pyspark.pandas.data_type_ops.categorical_ops import CategoricalOps
        from pyspark.pandas.data_type_ops.complex_ops import ArrayOps, MapOps, StructOps
        from pyspark.pandas.data_type_ops.date_ops import DateOps
        from pyspark.pandas.data_type_ops.datetime_ops import DatetimeOps
        from pyspark.pandas.data_type_ops.null_ops import NullOps
        from pyspark.pandas.data_type_ops.num_ops import IntegralOps, FractionalOps
        from pyspark.pandas.data_type_ops.string_ops import StringOps
        from pyspark.pandas.data_type_ops.udt_ops import UDTOps

        if isinstance(dtype, CategoricalDtype):
            return object.__new__(CategoricalOps)
        elif isinstance(spark_type, FractionalType):
            return object.__new__(FractionalOps)
        elif isinstance(spark_type, IntegralType):
            return object.__new__(IntegralOps)
        elif isinstance(spark_type, StringType):
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

    def add(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def sub(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def mul(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def truediv(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def floordiv(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def mod(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def pow(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def radd(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def rsub(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def rmul(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def rtruediv(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def rfloordiv(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def rmod(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def rpow(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def __and__(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Bitwise and can not be applied to %s." % self.pretty_name)

    def __or__(self, left, right) -> Union["Series", "Index"]:
        raise TypeError("Bitwise or can not be applied to %s." % self.pretty_name)

    def rand(self, left, right) -> Union["Series", "Index"]:
        return left.__and__(right)

    def ror(self, left, right) -> Union["Series", "Index"]:
        return left.__or__(right)

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col.replace({np.nan: None})
