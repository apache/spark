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

from typing import (
    Dict,
    List,
    Sequence,
    Union,
    TYPE_CHECKING,
    overload,
    cast,
)

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.column import (
    Column,
    scalar_function,
)
from pyspark.sql.connect.functions import col, lit
from pyspark.sql.group import GroupedData as PySparkGroupedData

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame


class GroupedData(object):
    def __init__(self, df: "DataFrame", *grouping_cols: Union[Column, str]) -> None:
        self._df = df
        self._grouping_cols = [x if isinstance(x, Column) else df[x] for x in grouping_cols]

    @overload
    def agg(self, *exprs: Column) -> "DataFrame":
        ...

    @overload
    def agg(self, __exprs: Dict[str, str]) -> "DataFrame":
        ...

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # Convert the dict into key value pairs
            measures = [scalar_function(exprs[0][k], col(k)) for k in exprs[0]]
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            measures = cast(List[Column], list(exprs))

        res = DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                measures=measures,
            ),
            session=self._df._session,
        )
        return res

    agg.__doc__ = PySparkGroupedData.agg.__doc__

    def _map_cols_to_expression(self, fun: str, param: Union[Column, str]) -> Sequence[Column]:
        return [
            scalar_function(fun, col(param)) if isinstance(param, str) else param,
        ]

    def min(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("min", col)
        return self.agg(*expr)

    min.__doc__ = PySparkGroupedData.min.__doc__

    def max(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("max", col)
        return self.agg(*expr)

    max.__doc__ = PySparkGroupedData.max.__doc__

    def sum(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("sum", col)
        return self.agg(*expr)

    sum.__doc__ = PySparkGroupedData.sum.__doc__

    def avg(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("avg", col)
        return self.agg(*expr)

    avg.__doc__ = PySparkGroupedData.avg.__doc__

    def count(self) -> "DataFrame":
        return self.agg(scalar_function("count", lit(1)))

    count.__doc__ = PySparkGroupedData.count.__doc__


GroupedData.__doc__ = PySparkGroupedData.__doc__
