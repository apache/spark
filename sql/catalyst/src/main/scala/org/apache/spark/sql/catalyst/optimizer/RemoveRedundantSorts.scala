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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SORT

/**
 * Remove redundant local [[Sort]] from the logical plan if its child is already sorted, and also
 * rewrite global [[Sort]] under local [[Sort]] into [[RepartitionByExpression]].
 */
object RemoveRedundantSorts extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    recursiveRemoveSort(plan, optimizeGlobalSort = false)
  }

  private def recursiveRemoveSort(plan: LogicalPlan, optimizeGlobalSort: Boolean): LogicalPlan = {
    if (!plan.containsPattern(SORT)) {
      return plan
    }
    plan match {
      case s @ Sort(orders, false, child) =>
        if (SortOrder.orderingSatisfies(child.outputOrdering, orders)) {
          recursiveRemoveSort(child, optimizeGlobalSort = false)
        } else {
          s.withNewChildren(Seq(recursiveRemoveSort(child, optimizeGlobalSort = true)))
        }

      case s @ Sort(orders, true, child) =>
        val newChild = recursiveRemoveSort(child, optimizeGlobalSort = false)
        if (optimizeGlobalSort) {
          // For this case, the upper sort is local so the ordering of present sort is unnecessary,
          // so here we only preserve its output partitioning using `RepartitionByExpression`.
          // We should use `None` as the optNumPartitions so AQE can coalesce shuffle partitions.
          // This behavior is same with original global sort.
          RepartitionByExpression(orders, newChild, None)
        } else {
          s.withNewChildren(Seq(newChild))
        }

      case _ =>
        plan.withNewChildren(plan.children.map(recursiveRemoveSort(_, optimizeGlobalSort = false)))
    }
  }
}
