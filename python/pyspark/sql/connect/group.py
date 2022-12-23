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
    Any,
    Dict,
    List,
    Sequence,
    Union,
    TYPE_CHECKING,
    Optional,
    overload,
    cast,
)

from pyspark.sql.group import GroupedData as PySparkGroupedData

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.column import Column, scalar_function
from pyspark.sql.connect.functions import col, lit

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import LiteralType
    from pyspark.sql.connect.dataframe import DataFrame


class GroupedData:
    def __init__(
        self,
        df: "DataFrame",
        group_type: str,
        grouping_cols: Sequence["Column"],
        pivot_col: Optional["Column"] = None,
        pivot_values: Optional[Sequence["LiteralType"]] = None,
    ) -> None:
        from pyspark.sql.connect.dataframe import DataFrame

        assert isinstance(df, DataFrame)
        self._df = df

        assert isinstance(group_type, str) and group_type in ["groupby", "rollup", "cube", "pivot"]
        self._group_type = group_type

        assert isinstance(grouping_cols, list) and all(isinstance(g, Column) for g in grouping_cols)
        self._grouping_cols: List[Column] = grouping_cols

        self._pivot_col: Optional["Column"] = None
        self._pivot_values: Optional[List[Any]] = None
        if group_type == "pivot":
            assert pivot_col is not None and isinstance(pivot_col, Column)
            assert pivot_values is None or isinstance(pivot_values, list)
            self._pivot_col = pivot_col
            self._pivot_values = pivot_values

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
            aggregate_cols = [scalar_function(exprs[0][k], col(k)) for k in exprs[0]]
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            aggregate_cols = cast(List[Column], list(exprs))

        res = DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                group_type=self._group_type,
                grouping_cols=self._grouping_cols,
                aggregate_cols=aggregate_cols,
                pivot_col=self._pivot_col,
                pivot_values=self._pivot_values,
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

    def pivot(self, pivot_col: str, values: Optional[List["LiteralType"]] = None) -> "GroupedData":
        if self._group_type != "groupby":
            if self._group_type == "pivot":
                raise Exception("Repeated PIVOT operation is not supported!")
            else:
                raise Exception(f"PIVOT after {self._group_type.upper()} is not supported!")

        if not isinstance(pivot_col, str):
            raise TypeError(
                f"pivot_col should be a str, but got {type(pivot_col).__name__} {pivot_col}"
            )

        if values is not None:
            if not isinstance(values, list):
                raise TypeError(
                    f"values should be a list, but got {type(values).__name__} {values}"
                )
            for v in values:
                if not isinstance(v, (bool, float, int, str)):
                    raise TypeError(
                        f"value should be a bool, float, int or str, but got {type(v).__name__} {v}"
                    )

        return GroupedData(
            df=self._df,
            group_type="pivot",
            grouping_cols=self._grouping_cols,
            pivot_col=self._df[pivot_col],
            pivot_values=values,
        )

    pivot.__doc__ = PySparkGroupedData.pivot.__doc__


GroupedData.__doc__ = PySparkGroupedData.__doc__
