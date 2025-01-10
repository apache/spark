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

from typing import TYPE_CHECKING

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.expressions import SubqueryExpression

from pyspark.sql.table_arg import TableArg as ParentTableArg


if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.connect.client import SparkConnectClient


class TableArg(ParentTableArg):
    def __init__(self, subquery_expr: SubqueryExpression):
        self._subquery_expr = subquery_expr

    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg":
        # Create a new SubqueryExpression with updated partition_spec
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr._subquery_type,
            partition_spec=self._subquery_expr._partition_spec + list(cols),
            order_spec=self._subquery_expr._order_spec,
            with_single_partition=self._subquery_expr._with_single_partition,
        )
        return TableArg(new_expr)

    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        # Create a new SubqueryExpression with updated order_spec
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr._subquery_type,
            partition_spec=self._subquery_expr._partition_spec,
            order_spec=self._subquery_expr._order_spec + list(cols),
            with_single_partition=self._subquery_expr._with_single_partition,
        )
        return TableArg(new_expr)

    def withSinglePartition(self) -> "TableArg":
        # Create a new SubqueryExpression with updated with_single_partition
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
