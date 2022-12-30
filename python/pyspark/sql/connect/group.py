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
from pyspark.sql.types import NumericType

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
            # There is a special case for count(*) which is rewritten into count(1).
            # Convert the dict into key value pairs
            aggregate_cols = [
                scalar_function(
                    exprs[0][k], lit(1) if exprs[0][k] == "count" and k == "*" else col(k)
                )
                for k in exprs[0]
            ]
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            aggregate_cols = cast(List[Column], list(exprs))

        return DataFrame.withPlan(
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

    agg.__doc__ = PySparkGroupedData.agg.__doc__

    def _numeric_agg(self, function: str, cols: Sequence[str]) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        assert isinstance(function, str) and function in ["min", "max", "avg", "sum"]

        assert isinstance(cols, list) and all(isinstance(c, str) for c in cols)

        schema = self._df.schema

        numerical_cols: List[str] = [
            field.name for field in schema.fields if isinstance(field.dataType, NumericType)
        ]

        agg_cols: List[str] = []

        if len(cols) > 0:
            invalid_cols = [c for c in cols if c not in numerical_cols]
            if len(invalid_cols) > 0:
                raise TypeError(
                    f"{invalid_cols} are not numeric columns. "
                    f"Numeric aggregation function can only be applied on numeric columns."
                )
            agg_cols = cols
        else:
            # if no column is provided, then all numerical columns are selected
            agg_cols = numerical_cols

        return DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                group_type=self._group_type,
                grouping_cols=self._grouping_cols,
                aggregate_cols=[scalar_function(function, col(c)) for c in agg_cols],
                pivot_col=self._pivot_col,
                pivot_values=self._pivot_values,
            ),
            session=self._df._session,
        )

    def min(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("min", list(cols))

    min.__doc__ = PySparkGroupedData.min.__doc__

    def max(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("max", list(cols))

    max.__doc__ = PySparkGroupedData.max.__doc__

    def sum(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("sum", list(cols))

    sum.__doc__ = PySparkGroupedData.sum.__doc__

    def avg(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("avg", list(cols))

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


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

    os.chdir(os.environ["SPARK_HOME"])

    if should_test_connect:
        import pyspark.sql.connect.group

        globs = pyspark.sql.connect.group.__dict__.copy()
        # Works around to create a regular Spark session
        sc = SparkContext("local[4]", "sql.connect.group tests", conf=SparkConf())
        globs["_spark"] = PySparkSession(sc, options={"spark.app.name": "sql.connect.group tests"})

        # Creates a remote Spark session.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        globs["spark"] = PySparkSession.builder.remote("sc://localhost").getOrCreate()

        (failure_count, test_count) = doctest.testmod(
            pyspark.sql.connect.group,
            globs=globs,
            optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
        )
        globs["_spark"].stop()
        globs["spark"].stop()
        if failure_count:
            sys.exit(-1)
    else:
        print(
            f"Skipping pyspark.sql.connect.group doctests: {connect_requirement_message}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    _test()
