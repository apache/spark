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

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * The rule is applied both normal and AQE Optimizer. It optimizes plan using max rows:
 *   - if the max rows of the child of sort is less than or equal to 1, remove the sort
 *   - if the max rows per partition of the child of local sort is less than or equal to 1,
 *     remove the local sort
 *   - if the max rows of the child of aggregate is less than or equal to 1 and its child and
 *     it's grouping only(include the rewritten distinct plan), convert aggregate to project
 *   - if the max rows of the child of aggregate is less than or equal to 1,
 *     set distinct to false in all aggregate expression
 */
object OptimizeOneRowPlan extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAnyPattern(SORT, AGGREGATE), ruleId) {
      case Sort(_, _, child) if child.maxRows.exists(_ <= 1L) => child
      case Sort(_, false, child) if child.maxRowsPerPartition.exists(_ <= 1L) => child
      case agg @ Aggregate(_, _, child) if agg.groupOnly && child.maxRows.exists(_ <= 1L) =>
        Project(agg.aggregateExpressions, child)
      case agg: Aggregate if agg.child.maxRows.exists(_ <= 1L) =>
        agg.transformExpressions {
          case aggExpr: AggregateExpression if aggExpr.isDistinct =>
            aggExpr.copy(isDistinct = false)
        }
    }
  }
}
