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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.window.{Final, Partial, WindowGroupLimitExec}

/**
 * Remove redundant partial WindowGroupLimitExec node from the spark plan. A partial
 * WindowGroupLimitExec node is redundant when the child left behind by removing it satisfies the
 * final node's required child distribution: the partial node only pre-filters, keeping the rows
 * whose rank within its own partition is within the limit, and a partition of the final node is a
 * union of partitions of the partial node, so it drops no row the final node would keep. A
 * [[GroupPartitionsExec]] between the two nodes, and a local sort, are looked through; the node is
 * left in place where removing it would take no sort with it and leave the sort above the grouping
 * with a bigger input.
 */
object RemoveRedundantWindowGroupLimits extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan transform {
    case outer @ WindowGroupLimitExec(_, _, _, _, Final, child) =>
      val newChild = removePartialLimit(child, hasUpperSort = false)
      if (newChild.outputPartitioning.satisfies(outer.requiredChildDistribution.head)) {
        outer.withNewChildren(Seq(newChild))
      } else {
        outer
      }
  }

  /**
   * Removes the partial [[WindowGroupLimitExec]] at the top of `plan`, together with the local sort
   * feeding it when `hasUpperSort` is set, or returns `plan` unchanged when it does not hold one.
   *
   * A [[GroupPartitionsExec]] and a local sort are looked through: `EnsureRequirements` adds the
   * grouping between the two nodes when the child needs its partitions coalesced or its partition
   * keys projected to satisfy the final node's distribution, and adds the sort between them to give
   * the final node its ordering. That sort orders the rows the final node ranks as a whole, which
   * is what makes the sort feeding the partial node dead; on its own that one is not redundant,
   * having been added to give the partial node its required ordering.
   *
   * A partial node holding no local sort of its own stays where `hasUpperSort` is set: it is then
   * the only cardinality reducer between its child and the sort above the grouping, and removing it
   * hands that sort the whole input.
   *
   * @param hasUpperSort whether a local sort between the final node and `plan` orders the rows the
   *                     final node ranks.
   */
  private def removePartialLimit(plan: SparkPlan, hasUpperSort: Boolean): SparkPlan = plan match {
    case WindowGroupLimitExec(_, _, _, _, Partial, child) =>
      if (hasUpperSort) {
        child match {
          case sort: SortExec if !sort.global => sort.child
          // Nothing goes with the partial node here: it is the only cardinality reducer between
          // its child and the sort above, which would then read the whole input -
          // `PushDownLocalSort.isOrderPreserving` declines the same trade by default.
          case _ => plan
        }
      } else {
        child
      }
    case group: GroupPartitionsExec =>
      group.withNewChildren(Seq(removePartialLimit(group.child, hasUpperSort)))
    case sort: SortExec if !sort.global =>
      sort.withNewChildren(Seq(removePartialLimit(sort.child, hasUpperSort = true)))
    case other => other
  }

}
