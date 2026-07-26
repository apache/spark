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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Replace hash-based aggregate with sort aggregate in the spark plan if the plan is a
 * [[HashAggregateExec]] or [[ObjectHashAggregateExec]], and the child satisfies the sort order
 * of corresponding [[SortAggregateExec]].
 *
 * Example:
 *
 * HashAggregate(t1.i, SUM, partial)         SortAggregate(t1.i, SUM, partial)
 *               |                     =>                  |
 *           Sort(t1.i)                                Sort(t1.i)
 *
 * Hash-based aggregate can be replaced when its child satisfies the sort order of
 * corresponding sort aggregate. Sort aggregate is faster in the sense that
 * it does not have hashing overhead of hash aggregate.
 *
 * Note that [[CombineAdjacentAggregation]] runs before this rule, so a pair of adjacent partial
 * and final aggregate has already been combined into a single `Complete` mode aggregate, which
 * this rule can further replace with a sort aggregate when the ordering is satisfied.
 */
object ReplaceHashWithSortAgg extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED)) {
      plan
    } else {
      replaceHashAgg(plan)
    }
  }

  /**
   * Replace [[HashAggregateExec]] and [[ObjectHashAggregateExec]] with [[SortAggregateExec]].
   */
  private def replaceHashAgg(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case hashAgg: BaseAggregateExec if isHashBasedAggWithKeys(hashAgg) =>
        val sortAgg = hashAgg.toSortAggregate
        if (SortOrder.orderingSatisfies(
            hashAgg.child.outputOrdering, sortAgg.requiredChildOrdering.head)) {
          sortAgg
        } else {
          hashAgg
        }
      case other => other
    }
  }

  /**
   * Check if `agg` is [[HashAggregateExec]] or [[ObjectHashAggregateExec]],
   * and has grouping keys.
   */
  private def isHashBasedAggWithKeys(agg: BaseAggregateExec): Boolean = {
    val isHashBasedAgg = agg match {
      case _: HashAggregateExec | _: ObjectHashAggregateExec => true
      case _ => false
    }
    isHashBasedAgg && agg.groupingExpressions.nonEmpty
  }
}
