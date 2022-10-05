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
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)

import pandas

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.column import (
    ColumnOrString,
    ColumnRef,
    Expression,
    ExpressionOrString,
    LiteralExpression,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import RemoteSparkSession


ColumnOrName = Union[ColumnRef, str]


class GroupingFrame(object):

    MeasuresType = Union[Sequence[Tuple[ExpressionOrString, str]], Dict[str, str]]
    OptMeasuresType = Optional[MeasuresType]

    def __init__(self, df: "DataFrame", *grouping_cols: Union[ColumnRef, str]) -> None:
        self._df = df
        self._grouping_cols = [x if isinstance(x, ColumnRef) else df[x] for x in grouping_cols]

    def agg(self, exprs: MeasuresType = None) -> "DataFrame":

        # Normalize the dictionary into a list of tuples.
        if isinstance(exprs, Dict):
            measures = list(exprs.items())
        elif isinstance(exprs, List):
            measures = exprs
        else:
            measures = []

        res = DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                measures=measures,
            ),
            session=self._df._session,
        )
        return res

    def _map_cols_to_dict(self, fun: str, cols: List[Union[ColumnRef, str]]) -> Dict[str, str]:
        return {x if isinstance(x, str) else cast(ColumnRef, x).name(): fun for x in cols}

    def min(self, *cols: Union[ColumnRef, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("min", list(cols))
        return self.agg(expr)

    def max(self, *cols: Union[ColumnRef, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("max", list(cols))
        return self.agg(expr)

    def sum(self, *cols: Union[ColumnRef, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("sum", list(cols))
        return self.agg(expr)

    def count(self) -> "DataFrame":
        return self.agg([(LiteralExpression(1), "count")])


class DataFrame(object):
    """Every DataFrame object essentially is a Relation that is refined using the
    member functions. Calling a method on a dataframe will essentially return a copy
    of the DataFrame with the changes applied.
    """

    def __init__(self, data: List[Any] = None, schema: List[str] = None):
        """Creates a new data frame"""
        self._schema = schema
        self._plan: Optional[plan.LogicalPlan] = None
        self._cache: Dict[str, Any] = {}
        self._session: "RemoteSparkSession" = None

    @classmethod
    def withPlan(cls, plan: plan.LogicalPlan, session=None) -> "DataFrame":
        """Main initialization method used to construct a new data frame with a child plan."""
        new_frame = DataFrame()
        new_frame._plan = plan
        new_frame._session = session
        return new_frame

    def select(self, *cols: ColumnRef) -> "DataFrame":
        return DataFrame.withPlan(plan.Project(self._plan, *cols), session=self._session)

    def agg(self, exprs: Dict[str, str]) -> "DataFrame":
        return self.groupBy().agg(exprs)

    def alias(self, alias):
        return DataFrame.withPlan(plan.Project(self._plan).withAlias(alias), session=self._session)

    def approxQuantile(self, col, probabilities, relativeError):
        ...

    def colRegex(self, regex) -> "DataFrame":
        ...

    @property
    def columns(self) -> List[str]:
        """Returns the list of columns of the current data frame."""
        if self._plan is None:
            return []
        if "columns" not in self._cache and self._plan is not None:
            pdd = self.limit(0).collect()
            # Translate to standard pytho array
            self._cache["columns"] = pdd.columns.values
        return self._cache["columns"]

    def count(self):
        """Returns the number of rows in the data frame"""
        return self.agg([(LiteralExpression(1), "count")]).collect().iloc[0, 0]

    def crossJoin(self, other):
        ...

    def coalesce(self, num_partitions: int) -> "DataFrame":
        ...

    def describe(self, cols):
        ...

    def distinct(self) -> "DataFrame":
        """Returns all distinct rows."""
        all_cols = self.columns()
        gf = self.groupBy(*all_cols)
        return gf.agg()

    def drop(self, *cols: ColumnOrString):
        all_cols = self.columns()
        dropped = set([c.name() if isinstance(c, ColumnRef) else self[c].name() for c in cols])
        filter(lambda x: x in dropped, all_cols)

    def filter(self, condition: Expression) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Filter(child=self._plan, filter=condition), session=self._session
        )

    def first(self):
        return self.head(1)

    def groupBy(self, *cols: ColumnOrString):
        return GroupingFrame(self, *cols)

    def head(self, n: int):
        self.limit(n)
        return self.collect()

    def join(self, other, on, how=None):
        return DataFrame.withPlan(
            plan.Join(left=self._plan, right=other._plan, on=on, how=how),
            session=self._session,
        )

    def limit(self, n):
        return DataFrame.withPlan(plan.Limit(child=self._plan, limit=n), session=self._session)

    def sort(self, *cols: ColumnOrName):
        """Sort by a specific column"""
        return DataFrame.withPlan(plan.Sort(self._plan, *cols), session=self._session)

    def show(self, n: int, truncate: Optional[Union[bool, int]], vertical: Optional[bool]):
        ...

    def union(self, other) -> "DataFrame":
        return self.unionAll(other)

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        if other._plan is None:
            raise ValueError("Argument to Union does not contain a valid plan.")
        return DataFrame.withPlan(plan.UnionAll(self._plan, other._plan), session=self._session)

    def where(self, condition):
        return self.filter(condition)

    def _get_alias(self):
        p = self._plan
        while p is not None:
            if isinstance(p, plan.Project) and p.alias:
                return p.alias
            p = p._child

    def __getattr__(self, name) -> "ColumnRef":
        return self[name]

    def __getitem__(self, name) -> "ColumnRef":
        # Check for alias
        alias = self._get_alias()
        return ColumnRef(alias, name)

    def _print_plan(self) -> str:
        if self._plan:
            return self._plan.print()
        return ""

    def collect(self):
        raise NotImplementedError("Please use toPandas().")

    def toPandas(self) -> pandas.DataFrame:
        query = self._plan.collect(self._session)
        return self._session._to_pandas(query)

    def explain(self) -> str:
        query = self._plan.collect(self._session)
        return self._session.analyze(query).explain_string
