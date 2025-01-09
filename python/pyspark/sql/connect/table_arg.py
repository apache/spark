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

from pyspark.sql.connect.expressions import SubqueryExpression

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


class TableArg:
    def __init__(self, subquery_expr: SubqueryExpression):
        self._subquery_expr = subquery_expr

    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg":
        partition_spec = self._subquery_expr.partition_spec + list(cols)
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr.subquery_type,
            partition_spec=partition_spec,
        )
        return TableArg(new_expr)

    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        order_spec = self._subquery_expr.order_spec + list(cols)
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr.subquery_type,
            order_spec=order_spec,
        )
        return TableArg(new_expr)

    def withSinglePartition(self) -> "TableArg":
        new_expr = SubqueryExpression(
            plan=self._subquery_expr._plan,
            subquery_type=self._subquery_expr.subquery_type,
            partition_spec=self._subquery_expr.partition_spec,
            order_spec=self._subquery_expr.order_spec,
            with_single_partition=True,
        )
        return TableArg(new_expr)

    def to_plan(self) -> SubqueryExpression:
        return self._subquery_expr
