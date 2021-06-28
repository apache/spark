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
from typing import Union

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.pandas.data_type_ops.base import DataTypeOps, T_IndexOps
from pyspark.pandas.typedef import Dtype, pandas_on_spark_type
from pyspark.sql import functions as F


class CategoricalOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with categorical types.
    """

    @property
    def pretty_name(self) -> str:
        return "categoricals"

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return pd.Categorical.from_codes(
            col, categories=self.dtype.categories, ordered=self.dtype.ordered
        )

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col.cat.codes

    def astype(self, index_ops: T_IndexOps, dtype: Union[str, type, Dtype]) -> T_IndexOps:
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype) and dtype.categories is None:
            return index_ops.copy()

        categories = index_ops.dtype.categories
        if len(categories) == 0:
            scol = F.lit(None)
        else:
            kvs = chain(
                *[(F.lit(code), F.lit(category)) for code, category in enumerate(categories)]
            )
            map_scol = F.create_map(*kvs)
            scol = map_scol.getItem(index_ops.spark.column)
        return index_ops._with_new_scol(
            scol.alias(index_ops._internal.data_spark_column_names[0])
        ).astype(dtype)
