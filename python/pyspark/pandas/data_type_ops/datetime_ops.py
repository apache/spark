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

import datetime
import warnings
from typing import Any, Union, cast

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql import Column, functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    TimestampType,
    TimestampNTZType,
    NumericType,
)
from pyspark.sql.utils import pyspark_column_op
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.sql.internal import InternalFunction as SF
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
    _sanitize_list_like,
)
from pyspark.pandas.typedef import pandas_on_spark_type


class DatetimeOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: TimestampType.
    """

    @property
    def pretty_name(self) -> str:
        return "datetimes"

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        # Note that timestamp subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' from 'datetime64[ns]'s subtraction.
        msg = (
            "Note that there is a behavior difference of timestamp subtraction. "
            "The timestamp subtraction returns an integer in seconds, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, IndexOpsMixin) and isinstance(
            right.spark.data_type, (TimestampType, TimestampNTZType)
        ):
            warnings.warn(msg, UserWarning)
            return left.astype("long") - right.astype("long")
        elif isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    left.astype("long").spark.column
                    - self._cast_spark_column_timestamp_to_long(F.lit(right)),
                    field=left._internal.data_fields[0].copy(
                        dtype=np.dtype("int64"), spark_type=LongType()
                    ),
                ),
            )
        else:
            raise TypeError("Datetime subtraction can only be applied to datetime series.")

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        # Note that timestamp subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' from 'datetime64[ns]'s subtraction.
        msg = (
            "Note that there is a behavior difference of timestamp subtraction. "
            "The timestamp subtraction returns an integer in seconds, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    self._cast_spark_column_timestamp_to_long(F.lit(right))
                    - left.astype("long").spark.column,
                    field=left._internal.data_fields[0].copy(
                        dtype=np.dtype("int64"), spark_type=LongType()
                    ),
                ),
            )
        else:
            raise TypeError("Datetime subtraction can only be applied to datetime series.")

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__lt__", left, right)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__le__", left, right)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__ge__", left, right)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return pyspark_column_op("__gt__", left, right)

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            raise TypeError("cannot astype a %s to [bool]" % self.pretty_name)
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(pd.NaT))
        else:
            return _as_other_type(index_ops, dtype, spark_type)

    def _cast_spark_column_timestamp_to_long(self, scol: Column) -> Column:
        return scol.cast(LongType())


class DatetimeNTZOps(DatetimeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type:
    TimestampNTZType.
    """

    def _cast_spark_column_timestamp_to_long(self, scol: Column) -> Column:
        return SF.timestamp_ntz_to_long(scol)

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, NumericType):
            from pyspark.pandas.internal import InternalField

            scol = self._cast_spark_column_timestamp_to_long(index_ops.spark.column).cast(
                spark_type
            )
            return index_ops._with_new_scol(scol, field=InternalField(dtype=dtype))
        else:
            return super(DatetimeNTZOps, self).astype(index_ops, dtype)
