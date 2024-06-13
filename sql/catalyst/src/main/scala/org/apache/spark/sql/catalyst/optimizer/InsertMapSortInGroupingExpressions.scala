/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, MapSort}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.MapType

/**
 * Adds MapSort to group expressions containing map columns by pushing down the projection,
 * as the key/value pairs need to be in the correct order before grouping:
 * SELECT COUNT(*) FROM TABLE GROUP BY map_column =>
 * SELECT COUNT(*) FROM (SELECT map_sort(map_column) as map_sorted) GROUP BY map_sorted
 *
 * SELECT map_column FROM TABLE GROUP BY map_column =>
 * SELECT map_sorted FROM (SELECT map_sort(map_column) as map_sorted) GROUP BY map_sorted
 *
 * We inject a new `Project` to ensure that we don't do recomputation, and so that we can reference
 * the newly sorted map column in the aggregate expressions list.
 */
object InsertMapSortInGroupingExpressions extends Rule[LogicalPlan] with AliasHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(AGGREGATE), ruleId) {
    case a @ Aggregate(groupingExpr, aggregateExpr, child) =>
      val toSort = groupingExpr.filter( expr =>
        !expr.isInstanceOf[MapSort] && expr.dataType.isInstanceOf[MapType]
      )
      val newChild = Project(toSort.map( expr =>
        Alias(MapSort(expr), "map_sorted")()
      ), child)
      val sortedAliasMap = getAliasMap(newChild)
      val newGroupingExpr = groupingExpr.map(expr => replaceAlias(expr, sortedAliasMap))
      val newAggregateExpr = aggregateExpr.map(expr =>
        replaceAliasButKeepName(expr, sortedAliasMap)
      )

      a.copy(groupingExpressions = newGroupingExpr,
        aggregateExpressions = newAggregateExpr, child = child)
  }
}
