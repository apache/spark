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
    Tuple,
    Union,
    TYPE_CHECKING,
    overload,
    Callable,
    cast,
    Type,
)

import sys
import random
import pandas
import warnings
from collections.abc import Iterable

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.sql.types import DataType, StructType, Row

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.group import GroupedData
from pyspark.sql.connect.readwriter import DataFrameWriter
from pyspark.sql.connect.column import (
    Column,
    scalar_function,
    sql_expression,
)
from pyspark.sql.connect.expressions import UnresolvedRegex
from pyspark.sql.connect.functions import col, lit
from pyspark.sql.dataframe import (
    DataFrame as PySparkDataFrame,
    DataFrameNaFunctions as PySparkDataFrameNaFunctions,
    DataFrameStatFunctions as PySparkDataFrameStatFunctions,
)

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName, LiteralType, OptionalPrimitiveType
    from pyspark.sql.connect.session import SparkSession


class DataFrame:
    def __init__(
        self,
        session: "SparkSession",
        schema: Optional[StructType] = None,
    ):
        """Creates a new data frame"""
        self._schema = schema
        self._plan: Optional[plan.LogicalPlan] = None
        self._session: "SparkSession" = session

    def __repr__(self) -> str:
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    @property
    def write(self) -> "DataFrameWriter":
        assert self._plan is not None
        return DataFrameWriter(self._plan, self._session)

    write.__doc__ = PySparkDataFrame.write.__doc__

    def isEmpty(self) -> bool:
        return len(self.take(1)) == 0

    isEmpty.__doc__ = PySparkDataFrame.isEmpty.__doc__

    def select(self, *cols: "ColumnOrName") -> "DataFrame":
        return DataFrame.withPlan(plan.Project(self._plan, *cols), session=self._session)

    select.__doc__ = PySparkDataFrame.select.__doc__

    def selectExpr(self, *expr: Union[str, List[str]]) -> "DataFrame":
        sql_expr = []
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]  # type: ignore[assignment]
        for element in expr:
            if isinstance(element, str):
                sql_expr.append(sql_expression(element))
            else:
                sql_expr.extend([sql_expression(e) for e in element])

        return DataFrame.withPlan(plan.Project(self._plan, *sql_expr), session=self._session)

    selectExpr.__doc__ = PySparkDataFrame.selectExpr.__doc__

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        if not exprs:
            raise ValueError("Argument 'exprs' must not be empty")

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            measures = [scalar_function(f, col(e)) for e, f in exprs[0].items()]
            return self.groupBy().agg(*measures)
        else:
            # other expressions
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Expression"
            exprs = cast(Tuple[Column, ...], exprs)
            return self.groupBy().agg(*exprs)

    agg.__doc__ = PySparkDataFrame.agg.__doc__

    def alias(self, alias: str) -> "DataFrame":
        return DataFrame.withPlan(plan.SubqueryAlias(self._plan, alias), session=self._session)

    alias.__doc__ = PySparkDataFrame.alias.__doc__

    def colRegex(self, colName: str) -> Column:
        return Column(UnresolvedRegex(colName))

    colRegex.__doc__ = PySparkDataFrame.colRegex.__doc__

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    dtypes.__doc__ = PySparkDataFrame.dtypes.__doc__

    @property
    def columns(self) -> List[str]:
        if self._plan is None:
            return []

        return self.schema.names

    columns.__doc__ = PySparkDataFrame.columns.__doc__

    @property
    def sparkSession(self) -> "SparkSession":
        return self._session

    sparkSession.__doc__ = PySparkDataFrame.sparkSession.__doc__

    def count(self) -> int:
        pdd = self.agg(scalar_function("count", lit(1))).toPandas()
        return pdd.iloc[0, 0]

    count.__doc__ = PySparkDataFrame.count.__doc__

    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot cartesian join when self._plan is empty.")
        if other._plan is None:
            raise Exception("Cannot cartesian join when other._plan is empty.")

        return DataFrame.withPlan(
            plan.Join(left=self._plan, right=other._plan, on=None, how="cross"),
            session=self._session,
        )

    crossJoin.__doc__ = PySparkDataFrame.crossJoin.__doc__

    def coalesce(self, numPartitions: int) -> "DataFrame":
        if not numPartitions > 0:
            raise ValueError("numPartitions must be positive.")
        return DataFrame.withPlan(
            plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=False),
            self._session,
        )

    coalesce.__doc__ = PySparkDataFrame.coalesce.__doc__

    def repartition(self, numPartitions: int) -> "DataFrame":
        if not numPartitions > 0:
            raise ValueError("numPartitions must be positive.")
        return DataFrame.withPlan(
            plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=True),
            self._session,
        )

    repartition.__doc__ = PySparkDataFrame.repartition.__doc__

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "DataFrame":
        if subset is None:
            return DataFrame.withPlan(
                plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
            )
        else:
            return DataFrame.withPlan(
                plan.Deduplicate(child=self._plan, column_names=subset), session=self._session
            )

    dropDuplicates.__doc__ = PySparkDataFrame.dropDuplicates.__doc__

    drop_duplicates = dropDuplicates

    def distinct(self) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
        )

    distinct.__doc__ = PySparkDataFrame.distinct.__doc__

    def drop(self, *cols: "ColumnOrName") -> "DataFrame":
        _cols = list(cols)
        if any(not isinstance(c, (str, Column)) for c in _cols):
            raise TypeError(
                f"'cols' must contains strings or Columns, but got {type(cols).__name__}"
            )
        if len(_cols) == 0:
            raise ValueError("'cols' must be non-empty")

        return DataFrame.withPlan(
            plan.Drop(
                child=self._plan,
                columns=_cols,
            ),
            session=self._session,
        )

    drop.__doc__ = PySparkDataFrame.drop.__doc__

    def filter(self, condition: Union[Column, str]) -> "DataFrame":
        if isinstance(condition, str):
            expr = sql_expression(condition)
        else:
            expr = condition
        return DataFrame.withPlan(plan.Filter(child=self._plan, filter=expr), session=self._session)

    filter.__doc__ = PySparkDataFrame.filter.__doc__

    def first(self) -> Optional[Row]:
        return self.head()

    first.__doc__ = PySparkDataFrame.first.__doc__

    def groupBy(self, *cols: "ColumnOrName") -> GroupedData:
        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            else:
                raise TypeError(
                    f"groupBy requires all cols be Column or str, but got {type(c).__name__} {c}"
                )

        return GroupedData(df=self, group_type="groupby", grouping_cols=_cols)

    groupBy.__doc__ = PySparkDataFrame.groupBy.__doc__

    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":
        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            else:
                raise TypeError(
                    f"rollup requires all cols be Column or str, but got {type(c).__name__} {c}"
                )

        return GroupedData(df=self, group_type="rollup", grouping_cols=_cols)

    rollup.__doc__ = PySparkDataFrame.rollup.__doc__

    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            else:
                raise TypeError(
                    f"cube requires all cols be Column or str, but got {type(c).__name__} {c}"
                )

        return GroupedData(df=self, group_type="cube", grouping_cols=_cols)

    cube.__doc__ = PySparkDataFrame.cube.__doc__

    @overload
    def head(self) -> Optional[Row]:
        ...

    @overload
    def head(self, n: int) -> List[Row]:
        ...

    def head(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    head.__doc__ = PySparkDataFrame.head.__doc__

    def take(self, num: int) -> List[Row]:
        return self.limit(num).collect()

    take.__doc__ = PySparkDataFrame.take.__doc__

    # TODO: extend `on` to also be type List[Column].
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot join when self._plan is empty.")
        if other._plan is None:
            raise Exception("Cannot join when other._plan is empty.")

        return DataFrame.withPlan(
            plan.Join(left=self._plan, right=other._plan, on=on, how=how),
            session=self._session,
        )

    join.__doc__ = PySparkDataFrame.join.__doc__

    def limit(self, n: int) -> "DataFrame":
        return DataFrame.withPlan(plan.Limit(child=self._plan, limit=n), session=self._session)

    limit.__doc__ = PySparkDataFrame.limit.__doc__

    def tail(self, num: int) -> List[Row]:
        return DataFrame.withPlan(
            plan.Tail(child=self._plan, limit=num), session=self._session
        ).collect()

    tail.__doc__ = PySparkDataFrame.tail.__doc__

    def sort(self, *cols: "ColumnOrName") -> "DataFrame":
        return DataFrame.withPlan(
            plan.Sort(self._plan, columns=list(cols), is_global=True), session=self._session
        )

    sort.__doc__ = PySparkDataFrame.sort.__doc__

    orderBy = sort

    def sortWithinPartitions(self, *cols: "ColumnOrName") -> "DataFrame":
        return DataFrame.withPlan(
            plan.Sort(self._plan, columns=list(cols), is_global=False), session=self._session
        )

    sortWithinPartitions.__doc__ = PySparkDataFrame.sortWithinPartitions.__doc__

    def sample(
        self,
        fraction: float,
        *,
        withReplacement: bool = False,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        if not isinstance(fraction, float):
            raise TypeError(f"'fraction' must be float, but got {type(fraction).__name__}")
        if not isinstance(withReplacement, bool):
            raise TypeError(
                f"'withReplacement' must be bool, but got {type(withReplacement).__name__}"
            )
        if seed is not None and not isinstance(seed, int):
            raise TypeError(f"'seed' must be None or int, but got {type(seed).__name__}")

        return DataFrame.withPlan(
            plan.Sample(
                child=self._plan,
                lower_bound=0.0,
                upper_bound=fraction,
                with_replacement=withReplacement,
                seed=seed,
            ),
            session=self._session,
        )

    sample.__doc__ = PySparkDataFrame.sample.__doc__

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        return self.withColumnsRenamed({existing: new})

    withColumnRenamed.__doc__ = PySparkDataFrame.withColumnRenamed.__doc__

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "DataFrame":
        if not isinstance(colsMap, dict):
            raise TypeError("colsMap must be dict of existing column name and new column name.")

        return DataFrame.withPlan(plan.RenameColumnsNameByName(self._plan, colsMap), self._session)

    withColumnsRenamed.__doc__ = PySparkDataFrame.withColumnsRenamed.__doc__

    def _show_string(
        self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False
    ) -> str:
        if not isinstance(n, int) or isinstance(n, bool):
            raise TypeError("Parameter 'n' (number of rows) must be an int")
        if not isinstance(vertical, bool):
            raise TypeError("Parameter 'vertical' must be a bool")

        _truncate: int = -1
        if isinstance(truncate, bool) and truncate:
            _truncate = 20
        else:
            try:
                _truncate = int(truncate)
            except ValueError:
                raise TypeError(
                    "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )

        pdf = DataFrame.withPlan(
            plan.ShowString(child=self._plan, numRows=n, truncate=_truncate, vertical=vertical),
            session=self._session,
        ).toPandas()
        assert pdf is not None
        return pdf["show_string"][0]

    def withColumns(self, colsMap: Dict[str, Column]) -> "DataFrame":
        if not isinstance(colsMap, dict):
            raise TypeError("colsMap must be dict of column name and column.")

        return DataFrame.withPlan(
            plan.WithColumns(self._plan, colsMap),
            session=self._session,
        )

    withColumns.__doc__ = PySparkDataFrame.withColumns.__doc__

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        if not isinstance(col, Column):
            raise TypeError("col should be Column")
        return DataFrame.withPlan(
            plan.WithColumns(self._plan, {colName: col}),
            session=self._session,
        )

    withColumn.__doc__ = PySparkDataFrame.withColumn.__doc__

    def unpivot(
        self,
        ids: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> "DataFrame":
        def to_jcols(
            cols: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]]
        ) -> List["ColumnOrName"]:
            if cols is None:
                lst = []
            elif isinstance(cols, tuple):
                lst = list(cols)
            elif isinstance(cols, list):
                lst = cols
            else:
                lst = [cols]
            return lst

        return DataFrame.withPlan(
            plan.Unpivot(
                self._plan, to_jcols(ids), to_jcols(values), variableColumnName, valueColumnName
            ),
            self._session,
        )

    unpivot.__doc__ = PySparkDataFrame.unpivot.__doc__

    melt = unpivot

    def hint(self, name: str, *params: Any) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Hint(self._plan, name, list(params)),
            session=self._session,
        )

    hint.__doc__ = PySparkDataFrame.hint.__doc__

    def randomSplit(
        self,
        weights: List[float],
        seed: Optional[int] = None,
    ) -> List["DataFrame"]:
        for w in weights:
            if w < 0.0:
                raise ValueError("Weights must be positive. Found weight value: %s" % w)
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        total = sum(weights)
        if total <= 0:
            raise ValueError("Sum of weights must be positive, but got: %s" % w)
        proportions = list(map(lambda x: x / total, weights))
        normalizedCumWeights = [0.0]
        for v in proportions:
            normalizedCumWeights.append(normalizedCumWeights[-1] + v)
        j = 1
        length = len(normalizedCumWeights)
        splits = []
        while j < length:
            lowerBound = normalizedCumWeights[j - 1]
            upperBound = normalizedCumWeights[j]
            samplePlan = DataFrame.withPlan(
                plan.Sample(
                    child=self._plan,
                    lower_bound=lowerBound,
                    upper_bound=upperBound,
                    with_replacement=False,
                    seed=int(seed),
                    force_stable_sort=True,
                ),
                session=self._session,
            )
            splits.append(samplePlan)
            j += 1

        return splits

    randomSplit.__doc__ = PySparkDataFrame.randomSplit.__doc__

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        print(self._show_string(n, truncate, vertical))

    show.__doc__ = PySparkDataFrame.show.__doc__

    def union(self, other: "DataFrame") -> "DataFrame":
        return self.unionAll(other)

    union.__doc__ = PySparkDataFrame.union.__doc__

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        if other._plan is None:
            raise ValueError("Argument to Union does not contain a valid plan.")
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "union", is_all=True), session=self._session
        )

    unionAll.__doc__ = PySparkDataFrame.unionAll.__doc__

    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":
        if other._plan is None:
            raise ValueError("Argument to UnionByName does not contain a valid plan.")
        return DataFrame.withPlan(
            plan.SetOperation(
                self._plan, other._plan, "union", is_all=True, by_name=allowMissingColumns
            ),
            session=self._session,
        )

    unionByName.__doc__ = PySparkDataFrame.unionByName.__doc__

    def subtract(self, other: "DataFrame") -> "DataFrame":
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "except", is_all=False),
            session=self._session,
        )

    subtract.__doc__ = PySparkDataFrame.subtract.__doc__

    def exceptAll(self, other: "DataFrame") -> "DataFrame":
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "except", is_all=True), session=self._session
        )

    exceptAll.__doc__ = PySparkDataFrame.exceptAll.__doc__

    def intersect(self, other: "DataFrame") -> "DataFrame":
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "intersect", is_all=False),
            session=self._session,
        )

    intersect.__doc__ = PySparkDataFrame.intersect.__doc__

    def intersectAll(self, other: "DataFrame") -> "DataFrame":
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "intersect", is_all=True),
            session=self._session,
        )

    intersectAll.__doc__ = PySparkDataFrame.intersectAll.__doc__

    def where(self, condition: Union[Column, str]) -> "DataFrame":
        return self.filter(condition)

    where.__doc__ = PySparkDataFrame.where.__doc__

    @property
    def na(self) -> "DataFrameNaFunctions":
        return DataFrameNaFunctions(self)

    na.__doc__ = PySparkDataFrame.na.__doc__

    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        if not isinstance(value, (float, int, str, bool, dict)):
            raise TypeError(
                f"value should be a float, int, string, bool or dict, "
                f"but got {type(value).__name__}"
            )
        if isinstance(value, dict):
            if len(value) == 0:
                raise ValueError("value dict can not be empty")
            for c, v in value.items():
                if not isinstance(c, str):
                    raise TypeError(
                        f"key type of dict should be string, but got {type(c).__name__}"
                    )
                if not isinstance(v, (bool, int, float, str)):
                    raise TypeError(
                        f"value type of dict should be float, int, string or bool, "
                        f"but got {type(v).__name__}"
                    )

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise TypeError(
                            f"cols should be a str, tuple[str] or list[str], "
                            f"but got {type(c).__name__}"
                        )
                _cols = list(subset)
            else:
                raise TypeError(
                    f"cols should be a str, tuple[str] or list[str], "
                    f"but got {type(subset).__name__}"
                )

        if isinstance(value, dict):
            _cols = list(value.keys())
            _values = [value[c] for c in _cols]
        else:
            _values = [value]

        return DataFrame.withPlan(
            plan.NAFill(child=self._plan, cols=_cols, values=_values),
            session=self._session,
        )

    fillna.__doc__ = PySparkDataFrame.fillna.__doc__

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        min_non_nulls: Optional[int] = None

        if how is not None:
            if not isinstance(how, str):
                raise TypeError(f"how should be a str, but got {type(how).__name__}")
            if how == "all":
                min_non_nulls = 1
            elif how == "any":
                min_non_nulls = None
            else:
                raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if thresh is not None:
            if not isinstance(thresh, int):
                raise TypeError(f"thresh should be a int, but got {type(thresh).__name__}")

            # 'thresh' overwrites 'how'
            min_non_nulls = thresh

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise TypeError(
                            f"cols should be a str, tuple[str] or list[str], "
                            f"but got {type(c).__name__}"
                        )
                _cols = list(subset)
            else:
                raise TypeError(
                    f"cols should be a str, tuple[str] or list[str], "
                    f"but got {type(subset).__name__}"
                )

        return DataFrame.withPlan(
            plan.NADrop(child=self._plan, cols=_cols, min_non_nulls=min_non_nulls),
            session=self._session,
        )

    dropna.__doc__ = PySparkDataFrame.dropna.__doc__

    def replace(
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> "DataFrame":
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Helper functions
        def all_of(types: Union[Type, Tuple[Type, ...]]) -> Callable[[Iterable], bool]:
            """Given a type or tuple of types and a sequence of xs
            check if each x is instance of type(s)

            >>> all_of(bool)([True, False])
            True
            >>> all_of(str)(["a", 1])
            False
            """

            def all_of_(xs: Iterable) -> bool:
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types + (dict,)):
            raise TypeError(
                "to_replace should be a bool, float, int, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace))
            )

        if (
            not isinstance(value, valid_types)
            and value is not None
            and not isinstance(to_replace, dict)
        ):
            raise TypeError(
                "If to_replace is not a dict, value should be "
                "a bool, float, int, string, list, tuple or None. "
                "Got {0}".format(type(value))
            )

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError(
                    "to_replace and value lists should be of the same length. "
                    "Got {0} and {1}".format(len(to_replace), len(value))
                )

        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise TypeError(
                "subset should be a list or tuple of column names, "
                "column name or None. Got {0}".format(type(subset))
            )

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, cast("Iterable[Optional[Union[float, str]]]", value)))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(
            all_of_type(rep_dict.keys())
            and all_of_type(x for x in rep_dict.values() if x is not None)
            for all_of_type in [all_of_bool, all_of_str, all_of_numeric]
        ):
            raise ValueError("Mixed type replacements are not supported")

        return DataFrame.withPlan(
            plan.NAReplace(child=self._plan, cols=subset, replacements=rep_dict),
            session=self._session,
        )

    replace.__doc__ = PySparkDataFrame.replace.__doc__

    @property
    def stat(self) -> "DataFrameStatFunctions":
        return DataFrameStatFunctions(self)

    stat.__doc__ = PySparkDataFrame.stat.__doc__

    def summary(self, *statistics: str) -> "DataFrame":
        _statistics: List[str] = list(statistics)
        for s in _statistics:
            if not isinstance(s, str):
                raise TypeError(f"'statistics' must be list[str], but got {type(s).__name__}")
        return DataFrame.withPlan(
            plan.StatSummary(child=self._plan, statistics=_statistics),
            session=self._session,
        )

    summary.__doc__ = PySparkDataFrame.summary.__doc__

    def describe(self, *cols: str) -> "DataFrame":
        _cols: List[str] = list(cols)
        for s in _cols:
            if not isinstance(s, str):
                raise TypeError(f"'cols' must be list[str], but got {type(s).__name__}")
        return DataFrame.withPlan(
            plan.StatDescribe(child=self._plan, cols=_cols),
            session=self._session,
        )

    describe.__doc__ = PySparkDataFrame.describe.__doc__

    def crosstab(self, col1: str, col2: str) -> "DataFrame":
        if not isinstance(col1, str):
            raise TypeError(f"'col1' must be str, but got {type(col1).__name__}")
        if not isinstance(col2, str):
            raise TypeError(f"'col2' must be str, but got {type(col2).__name__}")
        return DataFrame.withPlan(
            plan.StatCrosstab(child=self._plan, col1=col1, col2=col2),
            session=self._session,
        )

    crosstab.__doc__ = PySparkDataFrame.crosstab.__doc__

    def _get_alias(self) -> Optional[str]:
        p = self._plan
        while p is not None:
            if isinstance(p, plan.Project) and p.alias:
                return p.alias
            p = p._child
        return None

    def __getattr__(self, name: str) -> "Column":
        return self[name]

    def __getitem__(self, name: str) -> "Column":
        # Check for alias
        alias = self._get_alias()
        if alias is not None:
            return col(alias)
        else:
            return col(name)

    def _print_plan(self) -> str:
        if self._plan:
            return self._plan.print()
        return ""

    def collect(self) -> List[Row]:
        pdf = self.toPandas()
        if pdf is not None:
            return list(pdf.apply(lambda row: Row(**row), axis=1))
        else:
            return []

    collect.__doc__ = PySparkDataFrame.collect.__doc__

    def toPandas(self) -> "pandas.DataFrame":
        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._to_pandas(query)

    toPandas.__doc__ = PySparkDataFrame.toPandas.__doc__

    @property
    def schema(self) -> StructType:
        if self._schema is None:
            if self._plan is not None:
                query = self._plan.to_proto(self._session.client)
                if self._session is None:
                    raise Exception("Cannot analyze without SparkSession.")
                self._schema = self._session.client.schema(query)
                return self._schema
            else:
                raise Exception("Empty plan.")
        else:
            return self._schema

    schema.__doc__ = PySparkDataFrame.schema.__doc__

    def isLocal(self) -> bool:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).is_local

    isLocal.__doc__ = PySparkDataFrame.isLocal.__doc__

    @property
    def isStreaming(self) -> bool:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).is_streaming

    isStreaming.__doc__ = PySparkDataFrame.isStreaming.__doc__

    def _tree_string(self) -> str:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).tree_string

    def printSchema(self) -> None:
        print(self._tree_string())

    printSchema.__doc__ = PySparkDataFrame.printSchema.__doc__

    def inputFiles(self) -> List[str]:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).input_files

    inputFiles.__doc__ = PySparkDataFrame.inputFiles.__doc__

    def to(self, schema: DataType) -> "DataFrame":
        assert schema is not None
        return DataFrame.withPlan(
            plan.ToSchema(child=self._plan, schema=schema),
            session=self._session,
        )

    to.__doc__ = PySparkDataFrame.to.__doc__

    def toDF(self, *cols: str) -> "DataFrame":
        return DataFrame.withPlan(plan.RenameColumns(self._plan, list(cols)), self._session)

    toDF.__doc__ = PySparkDataFrame.toDF.__doc__

    def transform(self, func: Callable[..., "DataFrame"], *args: Any, **kwargs: Any) -> "DataFrame":
        result = func(self, *args, **kwargs)
        assert isinstance(
            result, DataFrame
        ), "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        return result

    transform.__doc__ = PySparkDataFrame.transform.__doc__

    def _explain_string(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> str:
        if extended is not None and mode is not None:
            raise ValueError("extended and mode should not be set together.")

        # For the no argument case: df.explain()
        is_no_argument = extended is None and mode is None

        # For the cases below:
        #   explain(True)
        #   explain(extended=False)
        is_extended_case = isinstance(extended, bool) and mode is None

        # For the case when extended is mode:
        #   df.explain("formatted")
        is_extended_as_mode = isinstance(extended, str) and mode is None

        # For the mode specified:
        #   df.explain(mode="formatted")
        is_mode_case = extended is None and isinstance(mode, str)

        if not (is_no_argument or is_extended_case or is_extended_as_mode or is_mode_case):
            argtypes = [str(type(arg)) for arg in [extended, mode] if arg is not None]
            raise TypeError(
                "extended (optional) and mode (optional) should be a string "
                "and bool; however, got [%s]." % ", ".join(argtypes)
            )

        # Sets an explain mode depending on a given argument
        if is_no_argument:
            explain_mode = "simple"
        elif is_extended_case:
            explain_mode = "extended" if extended else "simple"
        elif is_mode_case:
            explain_mode = cast(str, mode)
        elif is_extended_as_mode:
            explain_mode = cast(str, extended)

        if self._plan is not None:
            query = self._plan.to_proto(self._session.client)
            if self._session is None:
                raise Exception("Cannot analyze without SparkSession.")
            return self._session.client.explain_string(query, explain_mode)
        else:
            return ""

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        print(self._explain_string(extended=extended, mode=mode))

    explain.__doc__ = PySparkDataFrame.explain.__doc__

    def createTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=False
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    createTempView.__doc__ = PySparkDataFrame.createTempView.__doc__

    def createOrReplaceTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=True
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    createOrReplaceTempView.__doc__ = PySparkDataFrame.createOrReplaceTempView.__doc__

    def createGlobalTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=False
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    createGlobalTempView.__doc__ = PySparkDataFrame.createGlobalTempView.__doc__

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=True
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    createOrReplaceGlobalTempView.__doc__ = PySparkDataFrame.createOrReplaceGlobalTempView.__doc__

    def rdd(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("RDD Support for Spark Connect is not implemented.")

    def unpersist(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("unpersist() is not implemented.")

    def cache(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("cache() is not implemented.")

    def persist(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("persist() is not implemented.")

    def withWatermark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("withWatermark() is not implemented.")

    def observe(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("observe() is not implemented.")

    def foreach(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreach() is not implemented.")

    def foreachPartition(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreachPartition() is not implemented.")

    def toLocalIterator(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("toLocalIterator() is not implemented.")

    def checkpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("checkpoint() is not implemented.")

    def localCheckpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("localCheckpoint() is not implemented.")

    def to_pandas_on_spark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("to_pandas_on_spark() is not implemented.")

    def pandas_api(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("pandas_api() is not implemented.")

    def registerTempTable(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("registerTempTable() is not implemented.")

    def storageLevel(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("storageLevel() is not implemented.")

    def mapInPandas(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("mapInPandas() is not implemented.")

    def mapInArrow(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("mapInArrow() is not implemented.")

    def writeStream(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("writeStream() is not implemented.")

    def toJSON(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("toJSON() is not implemented.")

    def _repr_html_(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("_repr_html_() is not implemented.")

    def semanticHash(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("semanticHash() is not implemented.")

    def sameSemantics(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("sameSemantics() is not implemented.")

    # SparkConnect specific API
    def offset(self, n: int) -> "DataFrame":
        """Returns a new :class: `DataFrame` by skipping the first `n` rows.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        :class:`DataFrame`
            Subset of the records
        """
        return DataFrame.withPlan(plan.Offset(child=self._plan, offset=n), session=self._session)

    @classmethod
    def withPlan(cls, plan: plan.LogicalPlan, session: "SparkSession") -> "DataFrame":
        """
        Main initialization method used to construct a new data frame with a child plan.
        This is for internal purpose.
        """
        new_frame = DataFrame(session=session)
        new_frame._plan = plan
        return new_frame


class DataFrameNaFunctions:
    def __init__(self, df: DataFrame):
        self.df = df

    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def replace(
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self.df.replace(to_replace, value, subset)

    replace.__doc__ = DataFrame.replace.__doc__


DataFrameNaFunctions.__doc__ = PySparkDataFrameNaFunctions.__doc__


class DataFrameStatFunctions:
    def __init__(self, df: DataFrame):
        self.df = df

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__


DataFrameStatFunctions.__doc__ = PySparkDataFrameStatFunctions.__doc__
