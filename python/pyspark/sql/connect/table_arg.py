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
    Iterable,
    TYPE_CHECKING,
    Union,
    Sequence,
    List,
    Tuple,
    cast,
)

import pyspark.sql.connect.proto as proto
from pyspark.sql.column import Column
from pyspark.sql.table_arg import TableArg as ParentTableArg
from pyspark.sql.connect.expressions import Expression, SubqueryExpression, SortOrder
from pyspark.sql.connect.functions import builtin as F

from pyspark.errors import IllegalArgumentException

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.connect.client import SparkConnectClient


def _to_cols(cols: Tuple[Union["ColumnOrName", Sequence["ColumnOrName"]], ...]) -> List[Column]:
    if len(cols) == 1 and isinstance(cols[0], list):
        cols = cols[0]  # type: ignore[assignment]
    return [F._to_col(c) for c in cast(Iterable["ColumnOrName"], cols)]


class TableArg(ParentTableArg):
    def __init__(self, subquery_expr: SubqueryExpression):
        self._subquery_expr = subquery_expr

    def _is_partitioned(self) -> bool:
        """Checks if partitioning is already applied."""
        return bool(self._subquery_expr._partition_spec) or bool(
            self._subquery_expr._with_single_partition
        )

    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg":
        if self._is_partitioned():
            raise IllegalArgumentException(
                "Cannot call partitionBy() after partitionBy() or "
                "withSinglePartition() has been called."
            )
        new_partition_spec = list(self._subquery_expr._partition_spec) + [
            cast(Expression, c._expr) for c in _to_cols(cols)
        ]
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr._subquery_type,
            partition_spec=new_partition_spec,
            order_spec=self._subquery_expr._order_spec,
            with_single_partition=self._subquery_expr._with_single_partition,
        )
        return TableArg(new_expr)

    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        if not self._is_partitioned():
            raise IllegalArgumentException(
                "Please call partitionBy() or withSinglePartition() before orderBy()."
            )
        new_order_spec = [cast(SortOrder, F._sort_col(c)._expr) for c in _to_cols(cols)]
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr._subquery_type,
            partition_spec=self._subquery_expr._partition_spec,
            order_spec=list(self._subquery_expr._order_spec) + new_order_spec,
            with_single_partition=self._subquery_expr._with_single_partition,
        )
        return TableArg(new_expr)

    def withSinglePartition(self) -> "TableArg":
        if self._is_partitioned():
            raise IllegalArgumentException(
                "Cannot call withSinglePartition() after partitionBy() "
                "or withSinglePartition() has been called."
            )
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr._subquery_type,
            partition_spec=self._subquery_expr._partition_spec,
            order_spec=self._subquery_expr._order_spec,
            with_single_partition=True,
        )
        return TableArg(new_expr)

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        return self._subquery_expr.to_plan(session)
