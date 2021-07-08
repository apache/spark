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

from typing import Any, Union

from pandas.api.types import CategoricalDtype

from pyspark.pandas._typing import Dtype, IndexOpsLike
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_bool_type,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
)
from pyspark.pandas._typing import SeriesOrIndex
from pyspark.pandas.typedef import pandas_on_spark_type
from pyspark.sql import Column
from pyspark.sql.types import BooleanType, StringType


class NullOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with Spark type: NullType.
    """

    @property
    def pretty_name(self) -> str:
        return "nulls"

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

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            return _as_bool_type(index_ops, dtype)
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype)
        else:
            return _as_other_type(index_ops, dtype, spark_type)
