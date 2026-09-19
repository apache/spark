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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf

/**
 * Remove redundant SortExec node from the spark plan. A sort node is redundant when either:
 *  - its child satisfies both its sort orders and its required child distribution. Note this
 *    rule differs from the Optimizer rule EliminateSorts in that this rule also checks if the
 *    child satisfies the required distribution so that it is safe to remove not only a local
 *    sort but also a global sort when its child already satisfies required sort orders; or
 *  - it is a local sort that is the direct child of a shuffle which neither requires its child
 *    to be ordered (empty `requiredChildOrdering`) nor exposes an ordering itself (empty
 *    `outputOrdering`). A regular shuffle does not preserve the child ordering, so such a local
 *    sort has no effect on the query result and is dead. The `outputOrdering.isEmpty` guard is
 *    what keeps this safe for a custom `ShuffleExchangeLike` that does preserve the child
 *    ordering: such a shuffle reports a non-empty `outputOrdering`, its local sort is not dead
 *    and must be kept. This commonly happens in AQE after `OptimizeSkewedJoin` inserts an extra
 *    shuffle between two joins: the local sort that used to feed the upper join is left dangling
 *    right below the newly added shuffle and ends up being computed in the wrong stage for
 *    nothing.
 */
object RemoveRedundantSorts extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED)) {
      plan
    } else {
      removeSorts(plan)
    }
  }

  private def removeSorts(plan: SparkPlan): SparkPlan = plan transform {
    case s @ SortExec(orders, _, child, _)
        if SortOrder.orderingSatisfies(child.outputOrdering, orders) &&
          child.outputPartitioning.satisfies(s.requiredChildDistribution.head) =>
      child

    case shuffle: ShuffleExchangeLike
        if shuffle.requiredChildOrdering.head.isEmpty && shuffle.outputOrdering.isEmpty =>
      shuffle.child match {
        case SortExec(_, false, sortChild, _) => shuffle.withNewChildren(Seq(sortChild))
        case _ => shuffle
      }
  }
}
