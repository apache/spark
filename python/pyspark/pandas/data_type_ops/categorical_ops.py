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

from itertools import chain
from typing import cast, Any, Callable, Union

import pandas as pd
import numpy as np
from pandas.api.types import is_list_like, CategoricalDtype

from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import column_op, IndexOpsMixin
from pyspark.pandas.data_type_ops.base import DataTypeOps
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.typedef import pandas_on_spark_type
from pyspark.sql import functions as F
from pyspark.sql.column import Column


class CategoricalOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with categorical types.
    """

    @property
    def pretty_name(self) -> str:
        return "categoricals"

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return pd.Series(
            pd.Categorical.from_codes(
                col.replace(np.nan, -1).astype(int),
                categories=cast(CategoricalDtype, self.dtype).categories,
                ordered=cast(CategoricalDtype, self.dtype).ordered,
            )
        )

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col.cat.codes

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        dtype, _ = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype) and cast(CategoricalDtype, dtype).categories is None:
            return index_ops.copy()

        categories = cast(CategoricalDtype, index_ops.dtype).categories
        if len(categories) == 0:
            scol = SF.lit(None)
        else:
            kvs = chain(
                *[(SF.lit(code), SF.lit(category)) for code, category in enumerate(categories)]
            )
            map_scol = F.create_map(*kvs)
            scol = map_scol.getItem(index_ops.spark.column)
        return index_ops._with_new_scol(scol).astype(dtype)

    def eq(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__eq__, is_equality_comparison=True)

    def ne(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__ne__, is_equality_comparison=True)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__lt__)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__le__)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__gt__)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        return _compare(left, right, Column.__ge__)


def _compare(
    left: IndexOpsLike,
    right: Any,
    f: Callable[..., Column],
    *,
    is_equality_comparison: bool = False
) -> SeriesOrIndex:
    """
    Compare a Categorical operand `left` to `right` with the given Spark Column function.

    Parameters
    ----------
    left: A Categorical operand
    right: The other operand to compare with
    f : The Spark Column function to apply
    is_equality_comparison: True if it is equality comparison, ie. == or !=. False by default.

    Returns
    -------
    SeriesOrIndex
    """
    if isinstance(right, IndexOpsMixin) and isinstance(right.dtype, CategoricalDtype):
        if not is_equality_comparison:
            if not cast(CategoricalDtype, left.dtype).ordered:
                raise TypeError("Unordered Categoricals can only compare equality or not.")
        # Check if categoricals have the same dtype, same categories, and same ordered
        if hash(left.dtype) != hash(right.dtype):
            raise TypeError("Categoricals can only be compared if 'categories' are the same.")
        return column_op(f)(left, right)
    elif not is_list_like(right):
        categories = cast(CategoricalDtype, left.dtype).categories
        if right not in categories:
            raise TypeError("Cannot compare a Categorical with a scalar, which is not a category.")
        right_code = categories.get_loc(right)
        return column_op(f)(left, right_code)
    else:
        raise TypeError("Cannot compare a Categorical with the given type.")
