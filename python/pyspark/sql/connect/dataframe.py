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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    Sequence,
    TYPE_CHECKING,
    overload,
    Callable,
    cast,
    Type,
)

import sys
import random
import pandas
import pyarrow as pa
import json
import warnings
from collections.abc import Iterable

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.sql.observation import Observation
from pyspark.sql.types import Row, StructType
from pyspark.sql.dataframe import (
    DataFrame as PySparkDataFrame,
    DataFrameNaFunctions as PySparkDataFrameNaFunctions,
    DataFrameStatFunctions as PySparkDataFrameStatFunctions,
)

from pyspark.errors import PySparkTypeError, PySparkAttributeError
from pyspark.errors.exceptions.connect import SparkConnectException
from pyspark.rdd import PythonEvalType
from pyspark.storagelevel import StorageLevel
import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.group import GroupedData
from pyspark.sql.connect.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import UnresolvedRegex
from pyspark.sql.connect.functions import (
    _to_col_with_plan_id,
    _to_col,
    _invoke_function,
    col,
    lit,
    expr as sql_expression,
)
from pyspark.sql.connect.types import from_arrow_schema


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        ColumnOrName,
        LiteralType,
        PrimitiveType,
        OptionalPrimitiveType,
        PandasMapIterFunction,
        ArrowMapIterFunction,
    )
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
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

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
            measures = [_invoke_function(f, col(e)) for e, f in exprs[0].items()]
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
        if not isinstance(colName, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "colName", "arg_type": type(colName).__name__},
            )
        if self._plan is not None:
            return Column(UnresolvedRegex(colName, self._plan._plan_id))
        else:
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
        pdd = self.agg(_invoke_function("count", lit(1))).toPandas()
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

    @overload
    def repartition(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartition(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    def repartition(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        if isinstance(numPartitions, int):
            if not numPartitions > 0:
                raise ValueError("numPartitions must be positive.")
            if len(cols) == 0:
                return DataFrame.withPlan(
                    plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=True),
                    self._session,
                )
            else:
                return DataFrame.withPlan(
                    plan.RepartitionByExpression(self._plan, numPartitions, list(cols)),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            return DataFrame.withPlan(
                plan.RepartitionByExpression(self._plan, None, list(cols)),
                self.sparkSession,
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )

    repartition.__doc__ = PySparkDataFrame.repartition.__doc__

    @overload
    def repartitionByRange(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartitionByRange(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    def repartitionByRange(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        def _convert_col(col: "ColumnOrName") -> "ColumnOrName":
            from pyspark.sql.connect.expressions import SortOrder, ColumnReference

            if isinstance(col, Column):
                if isinstance(col._expr, SortOrder):
                    return col
                else:
                    return Column(SortOrder(col._expr))
            else:
                return Column(SortOrder(ColumnReference(col)))

        if isinstance(numPartitions, int):
            if not numPartitions > 0:
                raise ValueError("numPartitions must be positive.")
            if len(cols) == 0:
                raise ValueError("At least one partition-by expression must be specified.")
            else:
                sort = []
                sort.extend([_convert_col(c) for c in cols])
                return DataFrame.withPlan(
                    plan.RepartitionByExpression(self._plan, numPartitions, sort),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            sort = []
            sort.extend([_convert_col(c) for c in cols])
            return DataFrame.withPlan(
                plan.RepartitionByExpression(self._plan, None, sort),
                self.sparkSession,
            )
        else:
            raise TypeError("numPartitions should be an int, string or Column")

    repartitionByRange.__doc__ = PySparkDataFrame.repartitionByRange.__doc__

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "DataFrame":
        if subset is not None and not isinstance(subset, (list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_TUPLE",
                message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
            )

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
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

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

    groupby = groupBy

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
        if how is not None and isinstance(how, str):
            how = how.lower().replace("_", "")

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

    def _sort_cols(
        self, cols: Sequence[Union[str, Column, List[Union[str, Column]]]], kwargs: Dict[str, Any]
    ) -> List[Column]:
        """Return a JVM Seq of Columns that describes the sort order"""
        if cols is None:
            raise ValueError("should sort by at least one column")

        _cols: List[Column] = []
        if len(cols) == 1 and isinstance(cols[0], list):
            _cols = [_to_col(c) for c in cols[0]]
        else:
            _cols = [_to_col(cast("ColumnOrName", c)) for c in cols]

        ascending = kwargs.get("ascending", True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                _cols = [c.desc() for c in _cols]
        elif isinstance(ascending, list):
            _cols = [c if asc else c.desc() for asc, c in zip(ascending, _cols)]
        else:
            raise TypeError("ascending can only be boolean or list, but got %s" % type(ascending))

        return _cols

    def sort(
        self, *cols: Union[str, Column, List[Union[str, Column]]], **kwargs: Any
    ) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Sort(
                self._plan,
                columns=self._sort_cols(cols, kwargs),
                is_global=True,
            ),
            session=self._session,
        )

    sort.__doc__ = PySparkDataFrame.sort.__doc__

    orderBy = sort

    def sortWithinPartitions(
        self, *cols: Union[str, Column, List[Union[str, Column]]], **kwargs: Any
    ) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Sort(
                self._plan,
                columns=self._sort_cols(cols, kwargs),
                is_global=False,
            ),
            session=self._session,
        )

    sortWithinPartitions.__doc__ = PySparkDataFrame.sortWithinPartitions.__doc__

    def sample(
        self,
        withReplacement: Optional[Union[float, bool]] = None,
        fraction: Optional[Union[int, float]] = None,
        seed: Optional[int] = None,
    ) -> "DataFrame":

        # For the cases below:
        #   sample(True, 0.5 [, seed])
        #   sample(True, fraction=0.5 [, seed])
        #   sample(withReplacement=False, fraction=0.5 [, seed])
        is_withReplacement_set = type(withReplacement) == bool and isinstance(fraction, float)

        # For the case below:
        #   sample(faction=0.5 [, seed])
        is_withReplacement_omitted_kwargs = withReplacement is None and isinstance(fraction, float)

        # For the case below:
        #   sample(0.5 [, seed])
        is_withReplacement_omitted_args = isinstance(withReplacement, float)

        if not (
            is_withReplacement_set
            or is_withReplacement_omitted_kwargs
            or is_withReplacement_omitted_args
        ):
            argtypes = [
                str(type(arg)) for arg in [withReplacement, fraction, seed] if arg is not None
            ]
            raise TypeError(
                "withReplacement (optional), fraction (required) and seed (optional)"
                " should be a bool, float and number; however, "
                "got [%s]." % ", ".join(argtypes)
            )

        if is_withReplacement_omitted_args:
            if fraction is not None:
                seed = cast(int, fraction)
            fraction = withReplacement
            withReplacement = None

        if withReplacement is None:
            withReplacement = False

        seed = int(seed) if seed is not None else None

        return DataFrame.withPlan(
            plan.Sample(
                child=self._plan,
                lower_bound=0.0,
                upper_bound=fraction,  # type: ignore[arg-type]
                with_replacement=withReplacement,  # type: ignore[arg-type]
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
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "colsMap", "arg_type": type(colsMap).__name__},
            )

        return DataFrame.withPlan(plan.WithColumnsRenamed(self._plan, colsMap), self._session)

    withColumnsRenamed.__doc__ = PySparkDataFrame.withColumnsRenamed.__doc__

    def _show_string(
        self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False
    ) -> str:
        if not isinstance(n, int) or isinstance(n, bool):
            raise PySparkTypeError(
                error_class="NOT_INT",
                message_parameters={"arg_name": "n", "arg_type": type(n).__name__},
            )
        if not isinstance(vertical, bool):
            raise PySparkTypeError(
                error_class="NOT_BOOL",
                message_parameters={"arg_name": "vertical", "arg_type": type(vertical).__name__},
            )

        _truncate: int = -1
        if isinstance(truncate, bool) and truncate:
            _truncate = 20
        else:
            try:
                _truncate = int(truncate)
            except ValueError:
                raise PySparkTypeError(
                    error_class="NOT_BOOL",
                    message_parameters={
                        "arg_name": "truncate",
                        "arg_type": type(truncate).__name__,
                    },
                )

        pdf = DataFrame.withPlan(
            plan.ShowString(child=self._plan, num_rows=n, truncate=_truncate, vertical=vertical),
            session=self._session,
        ).toPandas()
        assert pdf is not None
        return pdf["show_string"][0]

    def withColumns(self, colsMap: Dict[str, Column]) -> "DataFrame":
        if not isinstance(colsMap, dict):
            raise TypeError("colsMap must be dict of column name and column.")

        names: List[str] = []
        columns: List[Column] = []
        for columnName, column in colsMap.items():
            names.append(columnName)
            columns.append(column)

        return DataFrame.withPlan(
            plan.WithColumns(
                self._plan,
                columnNames=names,
                columns=columns,
            ),
            session=self._session,
        )

    withColumns.__doc__ = PySparkDataFrame.withColumns.__doc__

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        if not isinstance(col, Column):
            raise TypeError("col should be Column")
        return DataFrame.withPlan(
            plan.WithColumns(
                self._plan,
                columnNames=[colName],
                columns=[col],
            ),
            session=self._session,
        )

    withColumn.__doc__ = PySparkDataFrame.withColumn.__doc__

    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> "DataFrame":
        if not isinstance(metadata, dict):
            raise TypeError("metadata should be a dict")

        return DataFrame.withPlan(
            plan.WithColumns(
                self._plan,
                columnNames=[columnName],
                columns=[self[columnName]],
                metadata=[json.dumps(metadata)],
            ),
            session=self._session,
        )

    withMetadata.__doc__ = PySparkDataFrame.withMetadata.__doc__

    def unpivot(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> "DataFrame":
        assert ids is not None, "ids must not be None"

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
                self._plan,
                to_jcols(ids),
                to_jcols(values) if values is not None else None,
                variableColumnName,
                valueColumnName,
            ),
            self._session,
        )

    unpivot.__doc__ = PySparkDataFrame.unpivot.__doc__

    melt = unpivot

    def hint(
        self, name: str, *parameters: Union["PrimitiveType", List["PrimitiveType"]]
    ) -> "DataFrame":
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]  # type: ignore[assignment]

        if not isinstance(name, str):
            raise TypeError("name should be provided as str, got {0}".format(type(name)))

        allowed_types = (str, list, float, int)
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise TypeError(
                    "all parameters should be in {0}, got {1} of type {2}".format(
                        allowed_types, p, type(p)
                    )
                )

        return DataFrame.withPlan(
            plan.Hint(self._plan, name, list(parameters)),
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
                    deterministic_order=True,
                ),
                session=self._session,
            )
            splits.append(samplePlan)
            j += 1

        return splits

    randomSplit.__doc__ = PySparkDataFrame.randomSplit.__doc__

    def observe(
        self,
        observation: Union["Observation", str],
        *exprs: Column,
    ) -> "DataFrame":
        if len(exprs) == 0:
            raise ValueError("'exprs' should not be empty")
        if not all(isinstance(c, Column) for c in exprs):
            raise ValueError("all 'exprs' should be Column")

        if isinstance(observation, Observation):
            return DataFrame.withPlan(
                plan.CollectMetrics(self._plan, str(observation._name), list(exprs)),
                self._session,
            )
        elif isinstance(observation, str):
            return DataFrame.withPlan(
                plan.CollectMetrics(self._plan, observation, list(exprs)),
                self._session,
            )
        else:
            raise ValueError("'observation' should be either `Observation` or `str`.")

    observe.__doc__ = PySparkDataFrame.observe.__doc__

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
                self._plan,
                other._plan,
                "union",
                by_name=True,
                allow_missing_columns=allowMissingColumns,
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
        if not isinstance(condition, (str, Column)):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )
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
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR",
                message_parameters={"arg_name": "value", "arg_type": type(value).__name__},
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
                raise PySparkTypeError(
                    error_class="NOT_LIST_OR_TUPLE",
                    message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
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
                raise PySparkTypeError(
                    error_class="NOT_LIST_OR_STR_OR_TUPLE",
                    message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
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
                raise PySparkTypeError(
                    error_class="ARGUMENT_REQUIRED",
                    message_parameters={"arg_name": "value", "condition": "`to_replace` is dict"},
                )

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
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
                message_parameters={
                    "arg_name": "to_replace",
                    "arg_type": type(to_replace).__name__,
                },
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

    def describe(self, *cols: Union[str, List[str]]) -> "DataFrame":
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]  # type: ignore[assignment]

        _cols = []
        for column in cols:
            if isinstance(column, str):
                _cols.append(column)
            else:
                _cols.extend([s for s in column])
        return DataFrame.withPlan(
            plan.StatDescribe(child=self._plan, cols=_cols),
            session=self._session,
        )

    describe.__doc__ = PySparkDataFrame.describe.__doc__

    def cov(self, col1: str, col2: str) -> float:
        if not isinstance(col1, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        pdf = DataFrame.withPlan(
            plan.StatCov(child=self._plan, col1=col1, col2=col2),
            session=self._session,
        ).toPandas()

        assert pdf is not None
        return pdf["cov"][0]

    cov.__doc__ = PySparkDataFrame.cov.__doc__

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        if not isinstance(col1, str):
            raise TypeError("col1 should be a string.")
        if not isinstance(col2, str):
            raise TypeError("col2 should be a string.")
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise ValueError(
                "Currently only the calculation of the Pearson Correlation "
                + "coefficient is supported."
            )
        pdf = DataFrame.withPlan(
            plan.StatCorr(child=self._plan, col1=col1, col2=col2, method=method),
            session=self._session,
        ).toPandas()

        assert pdf is not None
        return pdf["corr"][0]

    corr.__doc__ = PySparkDataFrame.corr.__doc__

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        if not isinstance(col, (str, list, tuple)):
            raise TypeError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [cast(str, col)]

        for c in col:
            if not isinstance(c, str):
                raise TypeError("columns should be strings, but got %r" % type(c))

        if not isinstance(probabilities, (list, tuple)):
            raise TypeError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int) in [0,1].")

        if not isinstance(relativeError, (float, int)):
            raise TypeError("relativeError should be numerical (float, int)")
        if relativeError < 0:
            raise ValueError("relativeError should be >= 0.")
        relativeError = float(relativeError)
        pdf = DataFrame.withPlan(
            plan.StatApproxQuantile(
                child=self._plan,
                cols=list(col),
                probabilities=probabilities,
                relativeError=relativeError,
            ),
            session=self._session,
        ).toPandas()

        assert pdf is not None
        jaq = pdf["approx_quantile"][0]
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    approxQuantile.__doc__ = PySparkDataFrame.approxQuantile.__doc__

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

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> "DataFrame":
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise TypeError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame.withPlan(
            plan.StatFreqItems(child=self._plan, cols=cols, support=support),
            session=self._session,
        )

    freqItems.__doc__ = PySparkDataFrame.freqItems.__doc__

    def sampleBy(
        self, col: "ColumnOrName", fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> "DataFrame":
        from pyspark.sql.connect.expressions import ColumnReference

        if isinstance(col, str):
            col = Column(ColumnReference(col))
        elif not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        if not isinstance(fractions, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "fractions", "arg_type": type(fractions).__name__},
            )
        for k, v in fractions.items():
            if not isinstance(k, (float, int, str)):
                raise PySparkTypeError(
                    error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "fractions",
                        "arg_type": type(fractions).__name__,
                        "allowed_types": "float, int, str",
                        "return_type": type(k).__name__,
                    },
                )
            fractions[k] = float(v)
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        return DataFrame.withPlan(
            plan.StatSampleBy(child=self._plan, col=col, fractions=fractions, seed=seed),
            session=self._session,
        )

    sampleBy.__doc__ = PySparkDataFrame.sampleBy.__doc__

    def _get_alias(self) -> Optional[str]:
        p = self._plan
        while p is not None:
            if isinstance(p, plan.Project) and p.alias:
                return p.alias
            p = p._child
        return None

    def __getattr__(self, name: str) -> "Column":
        if name in ["_jseq", "_jdf", "_jmap", "_jcols"]:
            raise PySparkAttributeError(
                error_class="JVM_ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
            )
        return self[name]

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> "DataFrame":
        ...

    def __getitem__(self, item: Union[int, str, Column, List, Tuple]) -> Union[Column, "DataFrame"]:
        if isinstance(item, str):
            # Check for alias
            alias = self._get_alias()
            if self._plan is None:
                raise SparkConnectException("Cannot analyze on empty plan.")
            return _to_col_with_plan_id(
                col=alias if alias is not None else item,
                plan_id=self._plan._plan_id,
            )
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            return col(self.columns[item])
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def _print_plan(self) -> str:
        if self._plan:
            return self._plan.print()
        return ""

    def collect(self) -> List[Row]:
        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session.client)
        table, schema = self._session.client.to_table(query)

        schema = schema or from_arrow_schema(table.schema)

        assert schema is not None and isinstance(schema, StructType)

        from pyspark.sql.connect.conversion import ArrowTableToRowsConversion

        return ArrowTableToRowsConversion.convert(table, schema)

    collect.__doc__ = PySparkDataFrame.collect.__doc__

    def toPandas(self) -> "pandas.DataFrame":
        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client.to_pandas(query)

    toPandas.__doc__ = PySparkDataFrame.toPandas.__doc__

    @property
    def schema(self) -> StructType:
        if self._plan is not None:
            query = self._plan.to_proto(self._session.client)
            if self._session is None:
                raise Exception("Cannot analyze without SparkSession.")
            return self._session.client.schema(query)
        else:
            raise Exception("Empty plan.")

    schema.__doc__ = PySparkDataFrame.schema.__doc__

    def isLocal(self) -> bool:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="is_local", plan=query).is_local
        assert result is not None
        return result

    isLocal.__doc__ = PySparkDataFrame.isLocal.__doc__

    @property
    def isStreaming(self) -> bool:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="is_streaming", plan=query).is_streaming
        assert result is not None
        return result

    isStreaming.__doc__ = PySparkDataFrame.isStreaming.__doc__

    def _tree_string(self) -> str:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="tree_string", plan=query).tree_string
        assert result is not None
        return result

    def printSchema(self) -> None:
        print(self._tree_string())

    printSchema.__doc__ = PySparkDataFrame.printSchema.__doc__

    def inputFiles(self) -> List[str]:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="input_files", plan=query).input_files
        assert result is not None
        return result

    inputFiles.__doc__ = PySparkDataFrame.inputFiles.__doc__

    def to(self, schema: StructType) -> "DataFrame":
        assert schema is not None
        return DataFrame.withPlan(
            plan.ToSchema(child=self._plan, schema=schema),
            session=self._session,
        )

    to.__doc__ = PySparkDataFrame.to.__doc__

    def toDF(self, *cols: str) -> "DataFrame":
        return DataFrame.withPlan(plan.ToDF(self._plan, list(cols)), self._session)

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

    def cache(self) -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot cache on empty plan.")
        relation = self._plan.plan(self._session.client)
        self._session.client._analyze(method="persist", relation=relation)
        return self

    cache.__doc__ = PySparkDataFrame.cache.__doc__

    def persist(
        self,
        storageLevel: StorageLevel = (StorageLevel.MEMORY_AND_DISK_DESER),
    ) -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot persist on empty plan.")
        relation = self._plan.plan(self._session.client)
        self._session.client._analyze(
            method="persist", relation=relation, storage_level=storageLevel
        )
        return self

    persist.__doc__ = PySparkDataFrame.persist.__doc__

    @property
    def storageLevel(self) -> StorageLevel:
        if self._plan is None:
            raise Exception("Cannot persist on empty plan.")
        relation = self._plan.plan(self._session.client)
        storage_level = self._session.client._analyze(
            method="get_storage_level", relation=relation
        ).storage_level
        assert storage_level is not None
        return storage_level

    storageLevel.__doc__ = PySparkDataFrame.storageLevel.__doc__

    def unpersist(self, blocking: bool = False) -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot unpersist on empty plan.")
        relation = self._plan.plan(self._session.client)
        self._session.client._analyze(method="unpersist", relation=relation, blocking=blocking)
        return self

    unpersist.__doc__ = PySparkDataFrame.unpivot.__doc__

    @property
    def is_cached(self) -> bool:
        return self.storageLevel != StorageLevel.NONE

    def withWatermark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("withWatermark() is not implemented.")

    def foreach(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreach() is not implemented.")

    def foreachPartition(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreachPartition() is not implemented.")

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]:
        from pyspark.sql.connect.conversion import ArrowTableToRowsConversion

        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session.client)

        schema: Optional[StructType] = None
        for schema_or_table in self._session.client.to_table_as_iterator(query):
            if isinstance(schema_or_table, StructType):
                assert schema is None
                schema = schema_or_table
            else:
                assert isinstance(schema_or_table, pa.Table)
                table = schema_or_table
                if schema is None:
                    schema = from_arrow_schema(table.schema)
                yield from ArrowTableToRowsConversion.convert(table, schema)

    toLocalIterator.__doc__ = PySparkDataFrame.toLocalIterator.__doc__

    def checkpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("checkpoint() is not implemented.")

    def localCheckpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("localCheckpoint() is not implemented.")

    def to_pandas_on_spark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("to_pandas_on_spark() is not implemented.")

    def pandas_api(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("pandas_api() is not implemented.")

    def registerTempTable(self, name: str) -> None:
        warnings.warn("Deprecated in 2.0, use createOrReplaceTempView instead.", FutureWarning)
        self.createOrReplaceTempView(name)

    registerTempTable.__doc__ = PySparkDataFrame.registerTempTable.__doc__

    def _map_partitions(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        evalType: int,
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction

        if self._plan is None:
            raise Exception("Cannot mapInPandas when self._plan is empty.")

        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=evalType,
        )

        return DataFrame.withPlan(
            plan.MapPartitions(child=self._plan, function=udf_obj, cols=self.columns),
            session=self._session,
        )

    def mapInPandas(
        self, func: "PandasMapIterFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        return self._map_partitions(func, schema, PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)

    mapInPandas.__doc__ = PySparkDataFrame.mapInPandas.__doc__

    def mapInArrow(
        self, func: "ArrowMapIterFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        return self._map_partitions(func, schema, PythonEvalType.SQL_MAP_ARROW_ITER_UDF)

    mapInArrow.__doc__ = PySparkDataFrame.mapInArrow.__doc__

    def writeStream(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("writeStream() is not implemented.")

    def toJSON(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("toJSON() is not implemented.")

    def _repr_html_(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("_repr_html_() is not implemented.")

    def sameSemantics(self, other: "DataFrame") -> bool:
        assert self._plan is not None
        assert other._plan is not None
        return self._session.client.same_semantics(
            plan=self._plan.to_proto(self._session.client),
            other=other._plan.to_proto(other._session.client),
        )

    sameSemantics.__doc__ = PySparkDataFrame.sameSemantics.__doc__

    def semanticHash(self) -> int:
        assert self._plan is not None
        return self._session.client.semantic_hash(
            plan=self._plan.to_proto(self._session.client),
        )

    semanticHash.__doc__ = PySparkDataFrame.semanticHash.__doc__

    def writeTo(self, table: str) -> "DataFrameWriterV2":
        assert self._plan is not None
        return DataFrameWriterV2(self._plan, self._session, table)

    writeTo.__doc__ = PySparkDataFrame.writeTo.__doc__

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

    def cov(self, col1: str, col2: str) -> float:
        return self.df.cov(col1, col2)

    cov.__doc__ = DataFrame.cov.__doc__

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        return self.df.corr(col1, col2, method)

    corr.__doc__ = DataFrame.corr.__doc__

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        return self.df.approxQuantile(col, probabilities, relativeError)

    approxQuantile.__doc__ = DataFrame.approxQuantile.__doc__

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> DataFrame:
        return self.df.freqItems(cols, support)

    freqItems.__doc__ = DataFrame.freqItems.__doc__

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> DataFrame:
        return self.df.sampleBy(col, fractions, seed)

    sampleBy.__doc__ = DataFrame.sampleBy.__doc__


DataFrameStatFunctions.__doc__ = PySparkDataFrameStatFunctions.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.dataframe

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.connect.dataframe.__dict__.copy()
    # Spark Connect does not support RDD but the tests depend on them.
    del pyspark.sql.connect.dataframe.DataFrame.coalesce.__doc__
    del pyspark.sql.connect.dataframe.DataFrame.repartition.__doc__
    del pyspark.sql.connect.dataframe.DataFrame.repartitionByRange.__doc__

    # TODO(SPARK-41625): Support Structured Streaming
    del pyspark.sql.connect.dataframe.DataFrame.isStreaming.__doc__

    # TODO(SPARK-41888): Support StreamingQueryListener for DataFrame.observe
    del pyspark.sql.connect.dataframe.DataFrame.observe.__doc__

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.dataframe tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.dataframe,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
