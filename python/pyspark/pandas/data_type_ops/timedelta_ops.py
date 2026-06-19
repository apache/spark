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

from datetime import timedelta
from typing import Any, Union

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.loose_version import LooseVersion
from pyspark.sql.types import (
    BooleanType,
    DayTimeIntervalType,
    StringType,
)
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
    _sanitize_list_like,
)
from pyspark.pandas.typedef import pandas_on_spark_type
from pyspark.sql.utils import pyspark_column_op


class TimedeltaOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: DayTimeIntervalType.
    """

    @property
    def pretty_name(self) -> str:
        return "timedelta"

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

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        if LooseVersion(pd.__version__) < "3.0.0":
            return col
        else:
            return col.astype(self.dtype)

    def _with_inferred_unit(
        self, result: SeriesOrIndex, left: IndexOpsLike, right: Any
    ) -> SeriesOrIndex:
        # Before pandas 3.0.0, timedelta64 is always nanosecond resolution, so there is no
        # unit to infer. From pandas 3.0.0, arithmetic between timedeltas promotes to the
        # finer resolution of the operands. The computed Spark column is always a
        # DayTimeIntervalType (microsecond), so the result field would otherwise be reported
        # as timedelta64[us]; here we override its dtype to match pandas.
        if LooseVersion(pd.__version__) < "3.0.0":
            return result

        def unit_of(obj: Any) -> str:
            if isinstance(obj, IndexOpsMixin):
                dtype = obj.dtype
                if isinstance(dtype, np.dtype) and np.issubdtype(dtype, np.timedelta64):
                    return np.datetime_data(dtype)[0]
            # Fall back to microseconds for datetime.timedelta scalars and for object-backed
            # interval columns, which carry no numpy timedelta64 resolution. This matches the
            # microsecond resolution of the underlying DayTimeIntervalType.
            return "us"

        promoted = np.promote_types(
            np.dtype("timedelta64[%s]" % unit_of(left)),
            np.dtype("timedelta64[%s]" % unit_of(right)),
        )
        # Spark's DayTimeIntervalType stores microseconds and cannot represent finer
        # resolutions, so cap nanosecond promotion at microseconds.
        if np.datetime_data(promoted)[0] == "ns":
            promoted = np.dtype("timedelta64[us]")

        field = result._internal.data_fields[0]
        return result._with_new_scol(result.spark.column, field=field.copy(dtype=promoted))

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        if (
            isinstance(right, IndexOpsMixin)
            and isinstance(right.spark.data_type, DayTimeIntervalType)
            or isinstance(right, timedelta)
        ):
            result = pyspark_column_op("__sub__", left, right)
            return self._with_inferred_unit(result, left, right)
        else:
            raise TypeError("Timedelta subtraction can only be applied to timedelta series.")

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)

        if isinstance(right, timedelta):
            result = pyspark_column_op("__rsub__", left, right)
            return self._with_inferred_unit(result, left, right)
        else:
            raise TypeError("Timedelta subtraction can only be applied to timedelta series.")

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
