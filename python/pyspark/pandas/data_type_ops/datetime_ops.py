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

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql import functions as F, Column
from pyspark.sql.types import BooleanType, StringType, TimestampType

from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_bool_type,
    _as_categorical_type,
    _as_other_type,
)
from pyspark.pandas.internal import InternalField
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.typedef import as_spark_type, extension_dtypes, pandas_on_spark_type


class DatetimeOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: TimestampType.
    """

    @property
    def pretty_name(self) -> str:
        return "datetimes"

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        # Note that timestamp subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' from 'datetime64[ns]'s subtraction.
        msg = (
            "Note that there is a behavior difference of timestamp subtraction. "
            "The timestamp subtraction returns an integer in seconds, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, TimestampType):
            warnings.warn(msg, UserWarning)
            return left.astype("long") - right.astype("long")
        elif isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return cast(
                SeriesOrIndex,
                left.spark.transform(
                    lambda scol: scol.astype("long") - SF.lit(right).cast(as_spark_type("long"))
                ),
            )
        else:
            raise TypeError("Datetime subtraction can only be applied to datetime series.")

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
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
                left.spark.transform(
                    lambda scol: SF.lit(right).cast(as_spark_type("long")) - scol.astype("long")
                ),
            )
        else:
            raise TypeError("Datetime subtraction can only be applied to datetime series.")

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

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            return _as_bool_type(index_ops, dtype)
        elif isinstance(spark_type, StringType):
            if isinstance(dtype, extension_dtypes):
                # seems like a pandas' bug?
                scol = F.when(index_ops.spark.column.isNull(), str(pd.NaT)).otherwise(
                    index_ops.spark.column.cast(spark_type)
                )
            else:
                null_str = str(pd.NaT)
                casted = index_ops.spark.column.cast(spark_type)
                scol = F.when(index_ops.spark.column.isNull(), null_str).otherwise(casted)
            return index_ops._with_new_scol(
                scol.alias(index_ops._internal.data_spark_column_names[0]),
                field=InternalField(dtype=dtype),
            )
        else:
            return _as_other_type(index_ops, dtype, spark_type)
