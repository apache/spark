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
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be:

        1. built-in aggregation functions, such as `avg`, `max`, `min`, `sum`, `count`

        2. group aggregate pandas UDFs, created with :func:`pyspark.sql.functions.pandas_udf`

           .. note:: There is no partial aggregation with group aggregate UDFs, i.e.,
               a full shuffle is required. Also, all the data of a group will be loaded into
               memory, so the user should be aware of the potential OOM risk if data is skewed
               and certain groups are too large to fit in memory.

           .. seealso:: :func:`pyspark.sql.functions.pandas_udf`

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        exprs : dict
            a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        Notes
        -----
        Built-in aggregation functions and group aggregate pandas UDFs cannot be mixed
        in a single call to this function.

        Examples
        --------
        >>> from pyspark.sql import functions as F
        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name).agg({"*": "count"}).sort("name").show()
        +-----+--------+
        | name|count(1)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       2|
        +-----+--------+

        Group-by name, and calculate the minimum age.

        >>> df.groupBy(df.name).agg(F.min(df.age)).sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+
        """
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

    def _map_cols_to_expression(self, fun: str, param: Union[Column, str]) -> Sequence[Column]:
        return [
            scalar_function(fun, col(param)) if isinstance(param, str) else param,
        ]

    def min(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("min", col)
        return self.agg(*expr)

    def max(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("max", col)
        return self.agg(*expr)

    def sum(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("sum", col)
        return self.agg(*expr)

    def avg(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("avg", col)
        return self.agg(*expr)

    def count(self) -> "DataFrame":
        return self.agg(scalar_function("count", lit(1)))
