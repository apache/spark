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

import org.apache.spark.sql.catalyst.expressions.MapSort
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.MapType

/**
 * Adds MapSort to group expressions containing map columns, as the key/value paris need to be
 * in the correct order before grouping:
 * SELECT COUNT(*) FROM TABLE GROUP BY map_column =>
 * SELECT COUNT(*) FROM TABLE GROUP BY map_sort(map_column)
 */
object InsertMapSortInGroupingExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(AGGREGATE), ruleId) {
    case a @ Aggregate(groupingExpr, _, _) =>
      val newGrouping = groupingExpr.map { expr =>
        if (!expr.isInstanceOf[MapSort] && expr.dataType.isInstanceOf[MapType]) {
          MapSort(expr)
        } else {
          expr
        }
      }
      a.copy(groupingExpressions = newGrouping)
  }
}
