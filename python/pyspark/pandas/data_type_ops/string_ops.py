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

from typing import Any, Union, cast

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql import functions as F
from pyspark.sql.types import IntegralType, StringType
from pyspark.sql.utils import pyspark_column_op
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import column_op, IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
    _sanitize_list_like,
)
from pyspark.pandas.typedef import extension_dtypes, pandas_on_spark_type
from pyspark.sql.types import BooleanType


class StringOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: StringType.
    """

    @property
    def pretty_name(self) -> str:
        return "strings"

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, str):
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    F.concat(left.spark.column, F.lit(right)), field=left._internal.data_fields[0]
                ),
            )
        elif isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, StringType):
            return column_op(F.concat)(left, right)
        else:
            raise TypeError("Addition can not be applied to given types.")

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, int):
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    F.repeat(left.spark.column, right), field=left._internal.data_fields[0]
                ),
            )
        elif (
            isinstance(right, IndexOpsMixin)
            and isinstance(right.spark.data_type, IntegralType)
            and not isinstance(right.dtype, CategoricalDtype)
        ):
            return column_op(F.repeat)(left, right)
        else:
            raise TypeError("Multiplication can not be applied to given types.")

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, str):
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    F.concat(F.lit(right), left.spark.column), field=left._internal.data_fields[0]
                ),
            )
        else:
            raise TypeError("Addition can not be applied to given types.")

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        if isinstance(right, int):
            return cast(
                SeriesOrIndex,
                left._with_new_scol(
                    F.repeat(left.spark.column, right), field=left._internal.data_fields[0]
                ),
            )
        else:
            raise TypeError("Multiplication can not be applied to given types.")

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

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)

        if isinstance(spark_type, BooleanType):
            if isinstance(dtype, extension_dtypes):
                scol = index_ops.spark.column.cast(spark_type)
            else:
                scol = F.when(index_ops.spark.column.isNull(), F.lit(False)).otherwise(
                    F.length(index_ops.spark.column) > 0
                )
            return index_ops._with_new_scol(
                scol,
                field=index_ops._internal.data_fields[0].copy(
                    dtype=dtype, spark_type=spark_type  # type: ignore[arg-type]
                ),
            )
        elif isinstance(spark_type, StringType):
            null_str = str(pd.NA) if isinstance(self, StringExtensionOps) else str(None)
            return _as_string_type(index_ops, dtype, null_str=null_str)
        else:
            return _as_other_type(index_ops, dtype, spark_type)


class StringExtensionOps(StringOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type StringType,
    and dtype StringDtype.
    """

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col.astype(self.dtype)
