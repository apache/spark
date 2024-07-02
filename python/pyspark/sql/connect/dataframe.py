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

# mypy: disable-error-code="override"
from pyspark.errors.exceptions.base import (
    SessionNotSameException,
    PySparkIndexError,
    PySparkAttributeError,
)
from pyspark.resource import ResourceProfile
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

import copy
import sys
import random
import pyarrow as pa
import json
import warnings
from collections.abc import Iterable
import functools

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.util import is_remote_only
from pyspark.sql.types import Row, StructType, _create_row
from pyspark.sql.dataframe import (
    DataFrame as ParentDataFrame,
    DataFrameNaFunctions as ParentDataFrameNaFunctions,
    DataFrameStatFunctions as ParentDataFrameStatFunctions,
)

from pyspark.errors import (
    PySparkTypeError,
    PySparkAttributeError,
    PySparkValueError,
    PySparkNotImplementedError,
    PySparkRuntimeError,
)
from pyspark.util import PythonEvalType
from pyspark.storagelevel import StorageLevel
import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.conversion import ArrowTableToRowsConversion
from pyspark.sql.connect.group import GroupedData
from pyspark.sql.connect.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.connect.streaming.readwriter import DataStreamWriter
from pyspark.sql.column import Column
from pyspark.sql.connect.expressions import (
    ColumnReference,
    UnresolvedRegex,
    UnresolvedStar,
)
from pyspark.sql.connect.functions import builtin as F
from pyspark.sql.pandas.types import from_arrow_schema, to_arrow_schema
from pyspark.sql.pandas.functions import _validate_pandas_udf  # type: ignore[attr-defined]


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        ColumnOrName,
        ColumnOrNameOrOrdinal,
        LiteralType,
        PrimitiveType,
        OptionalPrimitiveType,
        PandasMapIterFunction,
        ArrowMapIterFunction,
    )
    from pyspark.core.rdd import RDD
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike
    from pyspark.sql.connect.observation import Observation
    from pyspark.sql.connect.session import SparkSession
    from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
    from pyspark.sql.metrics import ExecutionInfo


class DataFrame(ParentDataFrame):
    def __new__(
        cls,
        plan: plan.LogicalPlan,
        session: "SparkSession",
    ) -> "DataFrame":
        self = object.__new__(cls)
        self.__init__(plan, session)  # type: ignore[misc]
        return self

    def __init__(
        self,
        plan: plan.LogicalPlan,
        session: "SparkSession",
    ):
        """Creates a new data frame"""
        self._plan = plan
        if self._plan is None:
            raise PySparkRuntimeError(
                error_class="MISSING_VALID_PLAN",
                message_parameters={"operator": "__init__"},
            )

        self._session: "SparkSession" = session  # type: ignore[assignment]
        if self._session is None:
            raise PySparkRuntimeError(
                error_class="NO_ACTIVE_SESSION",
                message_parameters={"operator": "__init__"},
            )

        # Check whether _repr_html is supported or not, we use it to avoid calling RPC twice
        # by __repr__ and _repr_html_ while eager evaluation opens.
        self._support_repr_html = False
        self._cached_schema: Optional[StructType] = None
        self._execution_info: Optional["ExecutionInfo"] = None

    def __reduce__(self) -> Tuple:
        """
        Custom method for serializing the DataFrame object using Pickle. Since the DataFrame
        overrides "__getattr__" method, the default serialization method does not work.

        Returns
        -------
        The tuple containing the information needed to reconstruct the object.

        """
        return (
            DataFrame,
            (
                self._plan,
                self._session,
            ),
            {
                "_support_repr_html": self._support_repr_html,
                "_cached_schema": self._cached_schema,
            },
        )

    def __repr__(self) -> str:
        if not self._support_repr_html:
            (
                repl_eager_eval_enabled,
                repl_eager_eval_max_num_rows,
                repl_eager_eval_truncate,
            ) = self._session._client.get_configs(
                "spark.sql.repl.eagerEval.enabled",
                "spark.sql.repl.eagerEval.maxNumRows",
                "spark.sql.repl.eagerEval.truncate",
            )
            if repl_eager_eval_enabled == "true":
                return self._show_string(
                    n=int(cast(str, repl_eager_eval_max_num_rows)),
                    truncate=int(cast(str, repl_eager_eval_truncate)),
                    vertical=False,
                )
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def _repr_html_(self) -> Optional[str]:
        if not self._support_repr_html:
            self._support_repr_html = True
        (
            repl_eager_eval_enabled,
            repl_eager_eval_max_num_rows,
            repl_eager_eval_truncate,
        ) = self._session._client.get_configs(
            "spark.sql.repl.eagerEval.enabled",
            "spark.sql.repl.eagerEval.maxNumRows",
            "spark.sql.repl.eagerEval.truncate",
        )
        if repl_eager_eval_enabled == "true":
            table, _ = DataFrame(
                plan.HtmlString(
                    child=self._plan,
                    num_rows=int(cast(str, repl_eager_eval_max_num_rows)),
                    truncate=int(cast(str, repl_eager_eval_truncate)),
                ),
                session=self._session,
            )._to_table()
            return table[0][0].as_py()
        else:
            return None

    @property
    def write(self) -> "DataFrameWriter":
        def cb(qe: "ExecutionInfo") -> None:
            self._execution_info = qe

        return DataFrameWriter(self._plan, self._session, cb)

    @functools.cache
    def isEmpty(self) -> bool:
        return len(self.select().take(1)) == 0

    @overload
    def select(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def select(self, __cols: Union[List[Column], List[str]]) -> ParentDataFrame:
        ...

    def select(self, *cols: "ColumnOrName") -> ParentDataFrame:  # type: ignore[misc]
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        if any(not isinstance(c, (str, Column)) for c in cols):
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_COLUMN_OR_STR",
                message_parameters={"arg_name": "columns"},
            )
        return DataFrame(
            plan.Project(self._plan, [F._to_col(c) for c in cols]),
            session=self._session,
        )

    def selectExpr(self, *expr: Union[str, List[str]]) -> ParentDataFrame:
        sql_expr = []
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]  # type: ignore[assignment]
        for element in expr:
            if isinstance(element, str):
                sql_expr.append(F.expr(element))
            else:
                sql_expr.extend([F.expr(e) for e in element])

        return DataFrame(plan.Project(self._plan, sql_expr), session=self._session)

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> ParentDataFrame:
        if not exprs:
            raise PySparkValueError(
                error_class="CANNOT_BE_EMPTY",
                message_parameters={"item": "exprs"},
            )

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            measures = [F._invoke_function(f, F.col(e)) for e, f in exprs[0].items()]
            return self.groupBy().agg(*measures)
        else:
            # other expressions
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Expression"
            exprs = cast(Tuple[Column, ...], exprs)
            return self.groupBy().agg(*exprs)

    def alias(self, alias: str) -> ParentDataFrame:
        res = DataFrame(plan.SubqueryAlias(self._plan, alias), session=self._session)
        res._cached_schema = self._cached_schema
        return res

    def colRegex(self, colName: str) -> Column:
        from pyspark.sql.connect.column import Column as ConnectColumn

        if not isinstance(colName, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "colName", "arg_type": type(colName).__name__},
            )
        return ConnectColumn(UnresolvedRegex(colName, self._plan._plan_id))

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self) -> List[str]:
        return self.schema.names

    @property
    def sparkSession(self) -> "SparkSession":
        return self._session

    def count(self) -> int:
        table, _ = self.agg(
            F._invoke_function("count", F.lit(1))
        )._to_table()  # type: ignore[operator]
        return table[0][0].as_py()

    def crossJoin(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        return DataFrame(
            plan.Join(
                left=self._plan, right=other._plan, on=None, how="cross"  # type: ignore[arg-type]
            ),
            session=self._session,
        )

    def _check_same_session(self, other: ParentDataFrame) -> None:
        if self._session.session_id != other._session.session_id:  # type: ignore[attr-defined]
            raise SessionNotSameException(
                error_class="SESSION_NOT_SAME",
                message_parameters={},
            )

    def coalesce(self, numPartitions: int) -> ParentDataFrame:
        if not numPartitions > 0:
            raise PySparkValueError(
                error_class="VALUE_NOT_POSITIVE",
                message_parameters={"arg_name": "numPartitions", "arg_value": str(numPartitions)},
            )
        res = DataFrame(
            plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=False),
            self._session,
        )
        res._cached_schema = self._cached_schema
        return res

    @overload
    def repartition(self, numPartitions: int, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def repartition(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    def repartition(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> ParentDataFrame:
        if isinstance(numPartitions, int):
            if not numPartitions > 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={
                        "arg_name": "numPartitions",
                        "arg_value": str(numPartitions),
                    },
                )
            if len(cols) == 0:
                res = DataFrame(
                    plan.Repartition(self._plan, numPartitions, shuffle=True),
                    self._session,
                )
            else:
                res = DataFrame(
                    plan.RepartitionByExpression(
                        self._plan, numPartitions, [F._to_col(c) for c in cols]
                    ),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            res = DataFrame(
                plan.RepartitionByExpression(self._plan, None, [F._to_col(c) for c in cols]),
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

        res._cached_schema = self._cached_schema
        return res

    @overload
    def repartitionByRange(self, numPartitions: int, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def repartitionByRange(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    def repartitionByRange(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> ParentDataFrame:
        if isinstance(numPartitions, int):
            if not numPartitions > 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={
                        "arg_name": "numPartitions",
                        "arg_value": str(numPartitions),
                    },
                )
            if len(cols) == 0:
                raise PySparkValueError(
                    error_class="CANNOT_BE_EMPTY",
                    message_parameters={"item": "cols"},
                )
            else:
                res = DataFrame(
                    plan.RepartitionByExpression(
                        self._plan, numPartitions, [F._sort_col(c) for c in cols]
                    ),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            res = DataFrame(
                plan.RepartitionByExpression(
                    self._plan, None, [F._sort_col(c) for c in [numPartitions] + list(cols)]
                ),
                self.sparkSession,
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_INT_OR_STR",
                message_parameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )

        res._cached_schema = self._cached_schema
        return res

    def dropDuplicates(self, *subset: Union[str, List[str]]) -> ParentDataFrame:
        # Acceptable args should be str, ... or a single List[str]
        # So if subset length is 1, it can be either single str, or a list of str
        # if subset length is greater than 1, it must be a sequence of str
        if len(subset) > 1:
            assert all(isinstance(c, str) for c in subset)

        if not subset:
            res = DataFrame(
                plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
            )
        elif len(subset) == 1 and isinstance(subset[0], list):
            res = DataFrame(
                plan.Deduplicate(child=self._plan, column_names=subset[0]),
                session=self._session,
            )
        else:
            res = DataFrame(
                plan.Deduplicate(child=self._plan, column_names=cast(List[str], subset)),
                session=self._session,
            )

        res._cached_schema = self._cached_schema
        return res

    drop_duplicates = dropDuplicates

    def dropDuplicatesWithinWatermark(self, *subset: Union[str, List[str]]) -> ParentDataFrame:
        # Acceptable args should be str, ... or a single List[str]
        # So if subset length is 1, it can be either single str, or a list of str
        # if subset length is greater than 1, it must be a sequence of str
        if len(subset) > 1:
            assert all(isinstance(c, str) for c in subset)

        if not subset:
            return DataFrame(
                plan.Deduplicate(child=self._plan, all_columns_as_keys=True, within_watermark=True),
                session=self._session,
            )
        elif len(subset) == 1 and isinstance(subset[0], list):
            return DataFrame(
                plan.Deduplicate(child=self._plan, column_names=subset[0], within_watermark=True),
                session=self._session,
            )
        else:
            return DataFrame(
                plan.Deduplicate(
                    child=self._plan,
                    column_names=cast(List[str], subset),
                    within_watermark=True,
                ),
                session=self._session,
            )

    def distinct(self) -> ParentDataFrame:
        res = DataFrame(
            plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
        )
        res._cached_schema = self._cached_schema
        return res

    @overload
    def drop(self, cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def drop(self, *cols: str) -> ParentDataFrame:
        ...

    def drop(self, *cols: "ColumnOrName") -> ParentDataFrame:  # type: ignore[misc]
        _cols = list(cols)
        if any(not isinstance(c, (str, Column)) for c in _cols):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "cols", "arg_type": type(cols).__name__},
            )

        return DataFrame(
            plan.Drop(
                child=self._plan,
                columns=_cols,
            ),
            session=self._session,
        )

    def filter(self, condition: Union[Column, str]) -> ParentDataFrame:
        if isinstance(condition, str):
            expr = F.expr(condition)
        else:
            expr = condition
        res = DataFrame(plan.Filter(child=self._plan, filter=expr), session=self._session)
        res._cached_schema = self._cached_schema
        return res

    def first(self) -> Optional[Row]:
        return self.head()

    @overload  # type: ignore[no-overload-impl]
    def groupby(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        ...

    @overload
    def groupby(self, __cols: Union[List[Column], List[str], List[int]]) -> "GroupedData":
        ...

    def groupBy(self, *cols: "ColumnOrNameOrOrdinal") -> GroupedData:
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            elif isinstance(c, int) and not isinstance(c, bool):
                if c < 1:
                    raise PySparkIndexError(
                        error_class="INDEX_NOT_POSITIVE", message_parameters={"index": str(c)}
                    )
                # ordinal is 1-based
                _cols.append(self[c - 1])
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                )

        return GroupedData(df=self, group_type="groupby", grouping_cols=_cols)

    groupby = groupBy  # type: ignore[assignment]

    @overload
    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def rollup(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            elif isinstance(c, int) and not isinstance(c, bool):
                if c < 1:
                    raise PySparkIndexError(
                        error_class="INDEX_NOT_POSITIVE", message_parameters={"index": str(c)}
                    )
                # ordinal is 1-based
                _cols.append(self[c - 1])
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                )

        return GroupedData(df=self, group_type="rollup", grouping_cols=_cols)

    @overload
    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def cube(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def cube(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                _cols.append(c)
            elif isinstance(c, str):
                _cols.append(self[c])
            elif isinstance(c, int) and not isinstance(c, bool):
                if c < 1:
                    raise PySparkIndexError(
                        error_class="INDEX_NOT_POSITIVE", message_parameters={"index": str(c)}
                    )
                # ordinal is 1-based
                _cols.append(self[c - 1])
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                )

        return GroupedData(df=self, group_type="cube", grouping_cols=_cols)

    def groupingSets(
        self, groupingSets: Sequence[Sequence["ColumnOrName"]], *cols: "ColumnOrName"
    ) -> "GroupedData":
        gsets: List[List[Column]] = []
        for grouping_set in groupingSets:
            gset: List[Column] = []
            for c in grouping_set:
                if isinstance(c, Column):
                    gset.append(c)
                elif isinstance(c, str):
                    gset.append(self[c])
                else:
                    raise PySparkTypeError(
                        error_class="NOT_COLUMN_OR_STR",
                        message_parameters={
                            "arg_name": "groupingSets",
                            "arg_type": type(c).__name__,
                        },
                    )
            gsets.append(gset)

        gcols: List[Column] = []
        for c in cols:
            if isinstance(c, Column):
                gcols.append(c)
            elif isinstance(c, str):
                gcols.append(self[c])
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                )

        return GroupedData(
            df=self, group_type="grouping_sets", grouping_cols=gcols, grouping_sets=gsets
        )

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

    def take(self, num: int) -> List[Row]:
        return self.limit(num).collect()

    def join(
        self,
        other: ParentDataFrame,
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> ParentDataFrame:
        self._check_same_session(other)
        if how is not None and isinstance(how, str):
            how = how.lower().replace("_", "")
        return DataFrame(
            plan.Join(left=self._plan, right=other._plan, on=on, how=how),  # type: ignore[arg-type]
            session=self._session,
        )

    def _joinAsOf(
        self,
        other: ParentDataFrame,
        leftAsOfColumn: Union[str, Column],
        rightAsOfColumn: Union[str, Column],
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
        *,
        tolerance: Optional[Column] = None,
        allowExactMatches: bool = True,
        direction: str = "backward",
    ) -> ParentDataFrame:
        self._check_same_session(other)
        if how is None:
            how = "inner"
        assert isinstance(how, str), "how should be a string"

        if tolerance is not None:
            assert isinstance(tolerance, Column), "tolerance should be Column"

        def _convert_col(df: ParentDataFrame, col: "ColumnOrName") -> Column:
            if isinstance(col, Column):
                return col
            else:
                return df._col(col)  # type: ignore[operator]

        return DataFrame(
            plan.AsOfJoin(
                left=self._plan,
                right=other._plan,  # type: ignore[arg-type]
                left_as_of=_convert_col(self, leftAsOfColumn),
                right_as_of=_convert_col(other, rightAsOfColumn),
                on=on,
                how=how,
                tolerance=tolerance,
                allow_exact_matches=allowExactMatches,
                direction=direction,
            ),
            session=self._session,
        )

    def limit(self, n: int) -> ParentDataFrame:
        res = DataFrame(plan.Limit(child=self._plan, limit=n), session=self._session)
        res._cached_schema = self._cached_schema
        return res

    def tail(self, num: int) -> List[Row]:
        return DataFrame(plan.Tail(child=self._plan, limit=num), session=self._session).collect()

    def _sort_cols(
        self,
        cols: Sequence[Union[int, str, Column, List[Union[int, str, Column]]]],
        kwargs: Dict[str, Any],
    ) -> List[Column]:
        """Return a JVM Seq of Columns that describes the sort order"""
        if cols is None:
            raise PySparkValueError(
                error_class="CANNOT_BE_EMPTY",
                message_parameters={"item": "cols"},
            )

        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        _cols: List[Column] = []
        for c in cols:
            if isinstance(c, int) and not isinstance(c, bool):
                # ordinal is 1-based
                if c > 0:
                    _c = self[c - 1]
                # negative ordinal means sort by desc
                elif c < 0:
                    _c = self[-c - 1].desc()
                else:
                    raise PySparkIndexError(
                        error_class="ZERO_INDEX",
                        message_parameters={},
                    )
            else:
                _c = c  # type: ignore[assignment]
            _cols.append(F._to_col(cast("ColumnOrName", _c)))

        ascending = kwargs.get("ascending", True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                _cols = [c.desc() for c in _cols]
        elif isinstance(ascending, list):
            _cols = [c if asc else c.desc() for asc, c in zip(ascending, _cols)]
        else:
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_LIST",
                message_parameters={"arg_name": "ascending", "arg_type": type(ascending).__name__},
            )

        return [F._sort_col(c) for c in _cols]

    def sort(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> ParentDataFrame:
        res = DataFrame(
            plan.Sort(
                self._plan,
                columns=self._sort_cols(cols, kwargs),
                is_global=True,
            ),
            session=self._session,
        )
        res._cached_schema = self._cached_schema
        return res

    orderBy = sort

    def sortWithinPartitions(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> ParentDataFrame:
        res = DataFrame(
            plan.Sort(
                self._plan,
                columns=self._sort_cols(cols, kwargs),
                is_global=False,
            ),
            session=self._session,
        )
        res._cached_schema = self._cached_schema
        return res

    def sample(
        self,
        withReplacement: Optional[Union[float, bool]] = None,
        fraction: Optional[Union[int, float]] = None,
        seed: Optional[int] = None,
    ) -> ParentDataFrame:
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
            argtypes = [type(arg).__name__ for arg in [withReplacement, fraction, seed]]
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_FLOAT_OR_INT",
                message_parameters={
                    "arg_name": "withReplacement (optional), "
                    + "fraction (required) and seed (optional)",
                    "arg_type": ", ".join(argtypes),
                },
            )

        if is_withReplacement_omitted_args:
            if fraction is not None:
                seed = cast(int, fraction)
            fraction = withReplacement
            withReplacement = None

        if withReplacement is None:
            withReplacement = False

        seed = int(seed) if seed is not None else random.randint(0, sys.maxsize)

        res = DataFrame(
            plan.Sample(
                child=self._plan,
                lower_bound=0.0,
                upper_bound=fraction,  # type: ignore[arg-type]
                with_replacement=withReplacement,  # type: ignore[arg-type]
                seed=seed,
            ),
            session=self._session,
        )
        res._cached_schema = self._cached_schema
        return res

    def withColumnRenamed(self, existing: str, new: str) -> ParentDataFrame:
        return self.withColumnsRenamed({existing: new})

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> ParentDataFrame:
        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "colsMap", "arg_type": type(colsMap).__name__},
            )

        return DataFrame(plan.WithColumnsRenamed(self._plan, colsMap), self._session)

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

        table, _ = DataFrame(
            plan.ShowString(
                child=self._plan,
                num_rows=n,
                truncate=_truncate,
                vertical=vertical,
            ),
            session=self._session,
        )._to_table()
        return table[0][0].as_py()

    def withColumns(self, colsMap: Dict[str, Column]) -> ParentDataFrame:
        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "colsMap", "arg_type": type(colsMap).__name__},
            )

        names: List[str] = []
        columns: List[Column] = []
        for columnName, column in colsMap.items():
            names.append(columnName)
            columns.append(column)

        return DataFrame(
            plan.WithColumns(
                self._plan,
                columnNames=names,
                columns=columns,
            ),
            session=self._session,
        )

    def withColumn(self, colName: str, col: Column) -> ParentDataFrame:
        if not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        return DataFrame(
            plan.WithColumns(
                self._plan,
                columnNames=[colName],
                columns=[col],
            ),
            session=self._session,
        )

    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> ParentDataFrame:
        if not isinstance(metadata, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "metadata", "arg_type": type(metadata).__name__},
            )

        return DataFrame(
            plan.WithColumns(
                self._plan,
                columnNames=[columnName],
                columns=[self[columnName]],
                metadata=[json.dumps(metadata)],
            ),
            session=self._session,
        )

    def unpivot(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> ParentDataFrame:
        assert ids is not None, "ids must not be None"

        def _convert_cols(
            cols: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]]
        ) -> List[Column]:
            if cols is None:
                return []
            elif isinstance(cols, (tuple, list)):
                return [F._to_col(c) for c in cols]
            else:
                return [F._to_col(cols)]

        return DataFrame(
            plan.Unpivot(
                self._plan,
                _convert_cols(ids),
                _convert_cols(values) if values is not None else None,
                variableColumnName,
                valueColumnName,
            ),
            self._session,
        )

    melt = unpivot

    def withWatermark(self, eventTime: str, delayThreshold: str) -> ParentDataFrame:
        # TODO: reuse error handling code in sql.DataFrame.withWatermark()
        if not eventTime or type(eventTime) is not str:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "eventTime", "arg_type": type(eventTime).__name__},
            )
        if not delayThreshold or type(delayThreshold) is not str:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={
                    "arg_name": "delayThreshold",
                    "arg_type": type(delayThreshold).__name__,
                },
            )

        return DataFrame(
            plan.WithWatermark(
                self._plan,
                event_time=eventTime,
                delay_threshold=delayThreshold,
            ),
            session=self._session,
        )

    def hint(
        self, name: str, *parameters: Union["PrimitiveType", "Column", List["PrimitiveType"]]
    ) -> ParentDataFrame:
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]  # type: ignore[assignment]

        if not isinstance(name, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "name", "arg_type": type(name).__name__},
            )

        allowed_types = (str, float, int, Column, list)
        allowed_primitive_types = (str, float, int)
        allowed_types_repr = ", ".join(
            [t.__name__ for t in allowed_types[:-1]]
            + ["list[" + t.__name__ + "]" for t in allowed_primitive_types]
        )
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise PySparkTypeError(
                    error_class="INVALID_ITEM_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "parameters",
                        "allowed_types": allowed_types_repr,
                        "item_type": type(p).__name__,
                    },
                )
            if isinstance(p, list):
                if not all(isinstance(e, allowed_primitive_types) for e in p):
                    raise PySparkTypeError(
                        error_class="INVALID_ITEM_FOR_CONTAINER",
                        message_parameters={
                            "arg_name": "parameters",
                            "allowed_types": allowed_types_repr,
                            "item_type": type(p).__name__ + "[" + type(p[0]).__name__ + "]",
                        },
                    )

        res = DataFrame(
            plan.Hint(self._plan, name, [F.lit(p) for p in list(parameters)]),
            session=self._session,
        )
        res._cached_schema = self._cached_schema
        return res

    def randomSplit(
        self,
        weights: List[float],
        seed: Optional[int] = None,
    ) -> List[ParentDataFrame]:
        for w in weights:
            if w < 0.0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "weights", "arg_value": str(w)},
                )
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        total = sum(weights)
        if total <= 0:
            raise PySparkValueError(
                error_class="VALUE_NOT_POSITIVE",
                message_parameters={"arg_name": "sum(weights)", "arg_value": str(total)},
            )
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
            samplePlan = DataFrame(
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
            samplePlan._cached_schema = self._cached_schema
            splits.append(samplePlan)
            j += 1

        return splits  # type: ignore[return-value]

    def observe(
        self,
        observation: Union["Observation", str],
        *exprs: Column,
    ) -> ParentDataFrame:
        from pyspark.sql.connect.observation import Observation

        if len(exprs) == 0:
            raise PySparkValueError(
                error_class="CANNOT_BE_EMPTY",
                message_parameters={"item": "exprs"},
            )
        if not all(isinstance(c, Column) for c in exprs):
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_COLUMN",
                message_parameters={"arg_name": "exprs"},
            )

        if isinstance(observation, Observation):
            res = observation._on(self, *exprs)
        elif isinstance(observation, str):
            res = DataFrame(
                plan.CollectMetrics(self._plan, observation, list(exprs)),
                self._session,
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_OBSERVATION_OR_STR",
                message_parameters={
                    "arg_name": "observation",
                    "arg_type": type(observation).__name__,
                },
            )

        res._cached_schema = self._cached_schema
        return res

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        print(self._show_string(n, truncate, vertical))

    def _merge_cached_schema(self, other: ParentDataFrame) -> Optional[StructType]:
        # to avoid type coercion, only propagate the schema
        # when the cached schemas are exactly the same
        if self._cached_schema is not None and self._cached_schema == other._cached_schema:
            return self.schema
        return None

    def union(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        return self.unionAll(other)

    def unionAll(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan, other._plan, "union", is_all=True  # type: ignore[arg-type]
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def unionByName(
        self, other: ParentDataFrame, allowMissingColumns: bool = False
    ) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan,
                other._plan,  # type: ignore[arg-type]
                "union",
                by_name=True,
                allow_missing_columns=allowMissingColumns,
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def subtract(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan, other._plan, "except", is_all=False  # type: ignore[arg-type]
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def exceptAll(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan, other._plan, "except", is_all=True  # type: ignore[arg-type]
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def intersect(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan, other._plan, "intersect", is_all=False  # type: ignore[arg-type]
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def intersectAll(self, other: ParentDataFrame) -> ParentDataFrame:
        self._check_same_session(other)
        res = DataFrame(
            plan.SetOperation(
                self._plan, other._plan, "intersect", is_all=True  # type: ignore[arg-type]
            ),
            session=self._session,
        )
        res._cached_schema = self._merge_cached_schema(other)
        return res

    def where(self, condition: Union[Column, str]) -> ParentDataFrame:
        if not isinstance(condition, (str, Column)):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )
        return self.filter(condition)

    @property
    def na(self) -> ParentDataFrameNaFunctions:
        return DataFrameNaFunctions(self)

    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        if not isinstance(value, (float, int, str, bool, dict)):
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR",
                message_parameters={"arg_name": "value", "arg_type": type(value).__name__},
            )
        if isinstance(value, dict):
            if len(value) == 0:
                raise PySparkValueError(
                    error_class="CANNOT_BE_EMPTY",
                    message_parameters={"item": "value"},
                )
            for c, v in value.items():
                if not isinstance(c, str):
                    raise PySparkTypeError(
                        error_class="NOT_STR",
                        message_parameters={
                            "arg_name": "key type of dict",
                            "arg_type": type(c).__name__,
                        },
                    )
                if not isinstance(v, (bool, int, float, str)):
                    raise PySparkTypeError(
                        error_class="NOT_BOOL_OR_FLOAT_OR_INT_OR_STR",
                        message_parameters={
                            "arg_name": "value type of dict",
                            "arg_type": type(v).__name__,
                        },
                    )

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise PySparkTypeError(
                            error_class="NOT_LIST_OR_STR_OR_TUPLE",
                            message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
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

        return DataFrame(
            plan.NAFill(child=self._plan, cols=_cols, values=_values),
            session=self._session,
        )

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        min_non_nulls: Optional[int] = None

        if how is not None:
            if not isinstance(how, str):
                raise PySparkTypeError(
                    error_class="NOT_STR",
                    message_parameters={"arg_name": "how", "arg_type": type(how).__name__},
                )
            if how == "all":
                min_non_nulls = 1
            elif how == "any":
                min_non_nulls = None
            else:
                raise PySparkValueError(
                    error_class="CANNOT_BE_EMPTY",
                    message_parameters={"arg_name": "how", "arg_value": str(how)},
                )

        if thresh is not None:
            if not isinstance(thresh, int):
                raise PySparkTypeError(
                    error_class="NOT_INT",
                    message_parameters={"arg_name": "thresh", "arg_type": type(thresh).__name__},
                )

            # 'thresh' overwrites 'how'
            min_non_nulls = thresh

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise PySparkTypeError(
                            error_class="NOT_LIST_OR_STR_OR_TUPLE",
                            message_parameters={"arg_name": "cols", "arg_type": type(c).__name__},
                        )
                _cols = list(subset)
            else:
                raise PySparkTypeError(
                    error_class="NOT_LIST_OR_STR_OR_TUPLE",
                    message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
                )

        return DataFrame(
            plan.NADrop(child=self._plan, cols=_cols, min_non_nulls=min_non_nulls),
            session=self._session,
        )

    def replace(
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> ParentDataFrame:
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
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "value", "arg_type": type(value).__name__},
            )

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise PySparkValueError(
                    error_class="LENGTH_SHOULD_BE_THE_SAME",
                    message_parameters={
                        "arg1": "to_replace",
                        "arg2": "value",
                        "arg1_length": str(len(to_replace)),
                        "arg2_length": str(len(value)),
                    },
                )

        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
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
            raise PySparkValueError(
                error_class="MIXED_TYPE_REPLACEMENT",
                message_parameters={},
            )

        def _convert_int_to_float(v: Any) -> Any:
            # a bool is also an int
            if v is not None and not isinstance(v, bool) and isinstance(v, int):
                return float(v)
            else:
                return v

        _replacements = []
        for k, v in rep_dict.items():
            _k = _convert_int_to_float(k)
            _v = _convert_int_to_float(v)
            _replacements.append((F.lit(_k), F.lit(_v)))

        return DataFrame(
            plan.NAReplace(
                child=self._plan,
                cols=subset,
                replacements=_replacements,
            ),
            session=self._session,
        )

    @property
    def stat(self) -> ParentDataFrameStatFunctions:
        return DataFrameStatFunctions(self)

    def summary(self, *statistics: str) -> ParentDataFrame:
        _statistics: List[str] = list(statistics)
        for s in _statistics:
            if not isinstance(s, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={"arg_name": "statistics", "arg_type": type(s).__name__},
                )
        return DataFrame(
            plan.StatSummary(child=self._plan, statistics=_statistics),
            session=self._session,
        )

    def describe(self, *cols: Union[str, List[str]]) -> ParentDataFrame:
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]  # type: ignore[assignment]

        _cols = []
        for column in cols:
            if isinstance(column, str):
                _cols.append(column)
            else:
                _cols.extend([s for s in column])
        return DataFrame(
            plan.StatDescribe(child=self._plan, cols=_cols),
            session=self._session,
        )

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
        table, _ = DataFrame(
            plan.StatCov(child=self._plan, col1=col1, col2=col2),
            session=self._session,
        )._to_table()
        return table[0][0].as_py()

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
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
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise PySparkValueError(
                error_class="VALUE_NOT_PEARSON",
                message_parameters={"arg_name": "method", "arg_value": method},
            )
        table, _ = DataFrame(
            plan.StatCorr(child=self._plan, col1=col1, col2=col2, method=method),
            session=self._session,
        )._to_table()
        return table[0][0].as_py()

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        if not isinstance(col, (str, list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [cast(str, col)]

        for c in col:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={"arg_name": "columns", "arg_type": type(c).__name__},
                )

        if not isinstance(probabilities, (list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_TUPLE",
                message_parameters={
                    "arg_name": "probabilities",
                    "arg_type": type(probabilities).__name__,
                },
            )
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_FLOAT_OR_INT",
                    message_parameters={
                        "arg_name": "probabilities",
                        "arg_type": type(p).__name__,
                    },
                )

        if not isinstance(relativeError, (float, int)):
            raise PySparkTypeError(
                error_class="NOT_FLOAT_OR_INT",
                message_parameters={
                    "arg_name": "relativeError",
                    "arg_type": type(relativeError).__name__,
                },
            )
        if relativeError < 0:
            raise PySparkValueError(
                error_class="NEGATIVE_VALUE",
                message_parameters={
                    "arg_name": "relativeError",
                    "arg_value": str(relativeError),
                },
            )
        relativeError = float(relativeError)
        table, _ = DataFrame(
            plan.StatApproxQuantile(
                child=self._plan,
                cols=list(col),
                probabilities=probabilities,
                relativeError=relativeError,
            ),
            session=self._session,
        )._to_table()
        jaq = [q.as_py() for q in table[0][0]]
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def crosstab(self, col1: str, col2: str) -> ParentDataFrame:
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
        return DataFrame(
            plan.StatCrosstab(child=self._plan, col1=col1, col2=col2),
            session=self._session,
        )

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> ParentDataFrame:
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_TUPLE",
                message_parameters={"arg_name": "cols", "arg_type": type(cols).__name__},
            )
        if not support:
            support = 0.01
        return DataFrame(
            plan.StatFreqItems(child=self._plan, cols=cols, support=support),
            session=self._session,
        )

    def sampleBy(
        self, col: "ColumnOrName", fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> ParentDataFrame:
        if not isinstance(col, (str, Column)):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        if not isinstance(fractions, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "fractions", "arg_type": type(fractions).__name__},
            )

        _fractions = []
        for k, v in fractions.items():
            if not isinstance(k, (float, int, str)):
                raise PySparkTypeError(
                    error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "fractions",
                        "arg_type": type(fractions).__name__,
                        "allowed_types": "float, int, str",
                        "item_type": type(k).__name__,
                    },
                )
            _fractions.append((F.lit(k), float(v)))

        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        return DataFrame(
            plan.StatSampleBy(
                child=self._plan,
                col=F._to_col(col),
                fractions=_fractions,
                seed=seed,
            ),
            session=self._session,
        )

    def _ipython_key_completions_(self) -> List[str]:
        """Returns the names of columns in this :class:`DataFrame`.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df._ipython_key_completions_()
        ['age', 'name']

        Would return illegal identifiers.
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age 1", "name?1"])
        >>> df._ipython_key_completions_()
        ['age 1', 'name?1']
        """
        return self.columns

    def __getattr__(self, name: str) -> "Column":
        if name in ["_jseq", "_jdf", "_jmap", "_jcols", "rdd", "toJSON"]:
            raise PySparkAttributeError(
                error_class="JVM_ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
            )

        if name not in self.columns:
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
            )

        return self._col(name)

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> ParentDataFrame:
        ...

    def __getitem__(
        self, item: Union[int, str, Column, List, Tuple]
    ) -> Union[Column, ParentDataFrame]:
        from pyspark.sql.connect.column import Column as ConnectColumn

        if isinstance(item, str):
            if item == "*":
                return ConnectColumn(
                    UnresolvedStar(
                        unparsed_target=None,
                        plan_id=self._plan._plan_id,
                    )
                )
            else:
                # TODO: revisit vanilla Spark's Dataset.col
                # if (sparkSession.sessionState.conf.supportQuotedRegexColumnName) {
                #   colRegex(colName)
                # } else {
                #   ConnectColumn(addDataFrameIdToCol(resolve(colName)))
                # }

                # validate the column name
                if not hasattr(self._session, "is_mock_session"):
                    from pyspark.sql.connect.types import verify_col_name

                    # Try best to verify the column name with cached schema
                    # If fails, fall back to the server side validation
                    if not verify_col_name(item, self.schema):
                        self.select(item).isLocal()

                return self._col(item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            return F.col(self.columns[item])
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "item", "arg_type": type(item).__name__},
            )

    def _col(self, name: str) -> Column:
        from pyspark.sql.connect.column import Column as ConnectColumn

        return ConnectColumn(
            ColumnReference(
                unparsed_identifier=name,
                plan_id=self._plan._plan_id,
            )
        )

    def __dir__(self) -> List[str]:
        attrs = set(dir(DataFrame))
        attrs.update(self.columns)
        return sorted(attrs)

    def collect(self) -> List[Row]:
        table, schema = self._to_table()

        # not all datatypes are supported in arrow based collect
        # here always verify the schema by from_arrow_schema
        schema2 = from_arrow_schema(table.schema, prefer_timestamp_ntz=True)
        schema = schema or schema2

        assert schema is not None and isinstance(schema, StructType)

        return ArrowTableToRowsConversion.convert(table, schema)

    def _to_table(self) -> Tuple["pa.Table", Optional[StructType]]:
        query = self._plan.to_proto(self._session.client)
        table, schema, self._execution_info = self._session.client.to_table(
            query, self._plan.observations
        )
        assert table is not None
        return (table, schema)

    def toArrow(self) -> "pa.Table":
        schema = to_arrow_schema(self.schema, error_on_duplicated_field_names_in_struct=True)
        table, _ = self._to_table()
        return table.cast(schema)

    def toPandas(self) -> "PandasDataFrameLike":
        query = self._plan.to_proto(self._session.client)
        pdf, ei = self._session.client.to_pandas(query, self._plan.observations)
        self._execution_info = ei
        return pdf

    @property
    def schema(self) -> StructType:
        # Schema caching is correct in most cases. Connect is lazy by nature. This means that
        # we only resolve the plan when it is submitted for execution or analysis. We do not
        # cache intermediate resolved plan. If the input (changes table, view redefinition,
        # etc...) of the plan changes between the schema() call, and a subsequent action, the
        # cached schema might be inconsistent with the end schema.
        if self._cached_schema is None:
            query = self._plan.to_proto(self._session.client)
            self._cached_schema = self._session.client.schema(query)
        return copy.deepcopy(self._cached_schema)

    @functools.cache
    def isLocal(self) -> bool:
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="is_local", plan=query).is_local
        assert result is not None
        return result

    @functools.cached_property
    def isStreaming(self) -> bool:
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="is_streaming", plan=query).is_streaming
        assert result is not None
        return result

    def printSchema(self, level: Optional[int] = None) -> None:
        if level:
            print(self.schema.treeString(level))
        else:
            print(self.schema.treeString())

    @functools.cache
    def inputFiles(self) -> List[str]:
        query = self._plan.to_proto(self._session.client)
        result = self._session.client._analyze(method="input_files", plan=query).input_files
        assert result is not None
        return result

    def to(self, schema: StructType) -> ParentDataFrame:
        assert schema is not None
        res = DataFrame(
            plan.ToSchema(child=self._plan, schema=schema),
            session=self._session,
        )
        res._cached_schema = schema
        return res

    def toDF(self, *cols: str) -> ParentDataFrame:
        for col_ in cols:
            if not isinstance(col_, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(col_).__name__},
                )
        return DataFrame(plan.ToDF(self._plan, list(cols)), self._session)

    def transform(
        self, func: Callable[..., ParentDataFrame], *args: Any, **kwargs: Any
    ) -> ParentDataFrame:
        result = func(self, *args, **kwargs)
        assert isinstance(
            result, DataFrame
        ), "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        return result

    def _explain_string(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> str:
        if extended is not None and mode is not None:
            raise PySparkValueError(
                error_class="CANNOT_SET_TOGETHER",
                message_parameters={"arg_list": "extended and mode"},
            )

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
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_STR",
                message_parameters={
                    "arg_name": "extended (optional) and mode (optional)",
                    "arg_type": ", ".join(argtypes),
                },
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

        query = self._plan.to_proto(self._session.client)
        return self._session.client.explain_string(query, explain_mode)

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        print(self._explain_string(extended=extended, mode=mode))

    def createTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=False
        ).command(session=self._session.client)
        _, _, ei = self._session.client.execute_command(command, self._plan.observations)
        self._execution_info = ei

    def createOrReplaceTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=True
        ).command(session=self._session.client)
        _, _, ei = self._session.client.execute_command(command, self._plan.observations)
        self._execution_info = ei

    def createGlobalTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=False
        ).command(session=self._session.client)
        _, _, ei = self._session.client.execute_command(command, self._plan.observations)
        self._execution_info = ei

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=True
        ).command(session=self._session.client)
        _, _, ei = self._session.client.execute_command(command, self._plan.observations)
        self._execution_info = ei

    def cache(self) -> ParentDataFrame:
        return self.persist()

    def persist(
        self,
        storageLevel: StorageLevel = (StorageLevel.MEMORY_AND_DISK_DESER),
    ) -> ParentDataFrame:
        relation = self._plan.plan(self._session.client)
        self._session.client._analyze(
            method="persist", relation=relation, storage_level=storageLevel
        )
        return self

    @property
    def storageLevel(self) -> StorageLevel:
        relation = self._plan.plan(self._session.client)
        storage_level = self._session.client._analyze(
            method="get_storage_level", relation=relation
        ).storage_level
        assert storage_level is not None
        return storage_level

    def unpersist(self, blocking: bool = False) -> ParentDataFrame:
        relation = self._plan.plan(self._session.client)
        self._session.client._analyze(method="unpersist", relation=relation, blocking=blocking)
        return self

    @property
    def is_cached(self) -> bool:
        return self.storageLevel != StorageLevel.NONE

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]:
        query = self._plan.to_proto(self._session.client)

        schema: Optional[StructType] = None
        for schema_or_table in self._session.client.to_table_as_iterator(
            query, self._plan.observations
        ):
            if isinstance(schema_or_table, StructType):
                assert schema is None
                schema = schema_or_table
            else:
                assert isinstance(schema_or_table, pa.Table)
                table = schema_or_table
                if schema is None:
                    schema = from_arrow_schema(table.schema, prefer_timestamp_ntz=True)
                yield from ArrowTableToRowsConversion.convert(table, schema)

    def pandas_api(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        from pyspark.pandas.namespace import _get_index_map
        from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
        from pyspark.pandas.internal import InternalFrame

        index_spark_columns, index_names = _get_index_map(self, index_col)
        internal = InternalFrame(
            spark_frame=self,
            index_spark_columns=index_spark_columns,
            index_names=index_names,  # type: ignore[arg-type]
        )
        return PandasOnSparkDataFrame(internal)

    def registerTempTable(self, name: str) -> None:
        warnings.warn("Deprecated in 2.0, use createOrReplaceTempView instead.", FutureWarning)
        self.createOrReplaceTempView(name)

    def _map_partitions(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        evalType: int,
        barrier: bool,
        profile: Optional[ResourceProfile],
    ) -> ParentDataFrame:
        from pyspark.sql.connect.udf import UserDefinedFunction

        _validate_pandas_udf(func, evalType)
        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=evalType,
        )

        res = DataFrame(
            plan.MapPartitions(
                child=self._plan,
                function=udf_obj,
                cols=self.columns,
                is_barrier=barrier,
                profile=profile,
            ),
            session=self._session,
        )
        if isinstance(schema, StructType):
            res._cached_schema = schema
        return res

    def mapInPandas(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> ParentDataFrame:
        return self._map_partitions(
            func, schema, PythonEvalType.SQL_MAP_PANDAS_ITER_UDF, barrier, profile
        )

    def mapInArrow(
        self,
        func: "ArrowMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> ParentDataFrame:
        return self._map_partitions(
            func, schema, PythonEvalType.SQL_MAP_ARROW_ITER_UDF, barrier, profile
        )

    def foreach(self, f: Callable[[Row], None]) -> None:
        def foreach_func(row: Any) -> None:
            f(row)

        self.select(F.struct(*self.schema.fieldNames()).alias("row")).select(
            F.udf(foreach_func, StructType())("row")  # type: ignore[arg-type]
        ).collect()

    def foreachPartition(self, f: Callable[[Iterator[Row]], None]) -> None:
        schema = self.schema
        field_converters = [
            ArrowTableToRowsConversion._create_converter(f.dataType) for f in schema.fields
        ]

        def foreach_partition_func(itr: Iterable[pa.RecordBatch]) -> Iterable[pa.RecordBatch]:
            def flatten() -> Iterator[Row]:
                for table in itr:
                    columnar_data = [column.to_pylist() for column in table.columns]
                    for i in range(0, table.num_rows):
                        values = [
                            field_converters[j](columnar_data[j][i])
                            for j in range(table.num_columns)
                        ]
                        yield _create_row(fields=schema.fieldNames(), values=values)

            f(flatten())
            return iter([])

        self.mapInArrow(foreach_partition_func, schema=StructType()).collect()

    @property
    def writeStream(self) -> DataStreamWriter:
        return DataStreamWriter(plan=self._plan, session=self._session)

    def sameSemantics(self, other: ParentDataFrame) -> bool:
        if not isinstance(other, DataFrame):
            raise PySparkTypeError(
                error_class="NOT_DATAFRAME",
                message_parameters={"arg_name": "other", "arg_type": type(other).__name__},
            )
        self._check_same_session(other)
        return self._session.client.same_semantics(
            plan=self._plan.to_proto(self._session.client),
            other=other._plan.to_proto(other._session.client),
        )

    @functools.cache
    def semanticHash(self) -> int:
        return self._session.client.semantic_hash(
            plan=self._plan.to_proto(self._session.client),
        )

    def writeTo(self, table: str) -> "DataFrameWriterV2":
        def cb(ei: "ExecutionInfo") -> None:
            self._execution_info = ei

        return DataFrameWriterV2(self._plan, self._session, table, cb)

    def offset(self, n: int) -> ParentDataFrame:
        return DataFrame(plan.Offset(child=self._plan, offset=n), session=self._session)

    def checkpoint(self, eager: bool = True) -> "DataFrame":
        cmd = plan.Checkpoint(child=self._plan, local=False, eager=eager)
        _, properties, self._execution_info = self._session.client.execute_command(
            cmd.command(self._session.client)
        )
        assert "checkpoint_command_result" in properties
        checkpointed = properties["checkpoint_command_result"]
        assert isinstance(checkpointed._plan, plan.CachedRemoteRelation)
        return checkpointed

    def localCheckpoint(self, eager: bool = True) -> "DataFrame":
        cmd = plan.Checkpoint(child=self._plan, local=True, eager=eager)
        _, properties, self._execution_info = self._session.client.execute_command(
            cmd.command(self._session.client)
        )
        assert "checkpoint_command_result" in properties
        checkpointed = properties["checkpoint_command_result"]
        assert isinstance(checkpointed._plan, plan.CachedRemoteRelation)
        return checkpointed

    if not is_remote_only():

        def toJSON(self, use_unicode: bool = True) -> "RDD[str]":
            raise PySparkNotImplementedError(
                error_class="NOT_IMPLEMENTED",
                message_parameters={"feature": "toJSON()"},
            )

        @property
        def rdd(self) -> "RDD[Row]":
            raise PySparkNotImplementedError(
                error_class="NOT_IMPLEMENTED",
                message_parameters={"feature": "rdd"},
            )

    @property
    def executionInfo(self) -> Optional["ExecutionInfo"]:
        return self._execution_info


class DataFrameNaFunctions(ParentDataFrameNaFunctions):
    def __init__(self, df: ParentDataFrame):
        self.df = df

    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        return self.df.fillna(value=value, subset=subset)  # type: ignore[arg-type]

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    def replace(
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> ParentDataFrame:
        return self.df.replace(to_replace, value, subset)  # type: ignore[arg-type]


class DataFrameStatFunctions(ParentDataFrameStatFunctions):
    def __init__(self, df: ParentDataFrame):
        self.df = df

    def cov(self, col1: str, col2: str) -> float:
        return self.df.cov(col1, col2)

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        return self.df.corr(col1, col2, method)

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        return self.df.approxQuantile(col, probabilities, relativeError)

    def crosstab(self, col1: str, col2: str) -> ParentDataFrame:
        return self.df.crosstab(col1, col2)

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> ParentDataFrame:
        return self.df.freqItems(cols, support)

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> ParentDataFrame:
        return self.df.sampleBy(col, fractions, seed)


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.util import is_remote_only
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.dataframe

    # It inherits docstrings but doctests cannot detect them so we run
    # the parent classe's doctests here directly.
    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.dataframe.__dict__.copy()

    if not is_remote_only():
        del pyspark.sql.dataframe.DataFrame.toJSON.__doc__
        del pyspark.sql.dataframe.DataFrame.rdd.__doc__

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.dataframe tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe,
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
