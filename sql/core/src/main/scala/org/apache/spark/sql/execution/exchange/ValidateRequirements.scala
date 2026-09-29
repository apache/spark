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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._

/**
 * Validates that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator, and so are the ordering requirements.
 */
object ValidateRequirements extends Logging {

  def validate(plan: SparkPlan, requiredDistribution: Distribution): Boolean = {
    validate(plan) && plan.outputPartitioning.satisfies(requiredDistribution)
  }

  def validate(plan: SparkPlan): Boolean = {
    plan.children.forall(validate) && validateInternal(plan)
  }

  private def validateInternal(plan: SparkPlan): Boolean = {
    val children: Seq[SparkPlan] = plan.children
    val requiredChildDistributions: Seq[Distribution] = plan.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = plan.requiredChildOrdering
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    val satisfied = children.zip(requiredChildDistributions.zip(requiredChildOrderings)).forall {
      case (child, (distribution, ordering))
          if !child.outputPartitioning.satisfies(distribution)
            || !SortOrder.orderingSatisfies(child.outputOrdering, ordering) =>
        logDebug(s"ValidateRequirements failed: $distribution, $ordering\n$plan")
        false
      case _ => true
    }

    // A `ClusteredDistribution` is the one distribution an operator can owe its children
    // together, so an operator whose children all owe one is judged on their mutual layout, and
    // every other child answers for itself. That is every such operator, not only a join: a
    // cogroup, for instance, zips corresponding partitions too.
    if (satisfied && children.length > 1 &&
      requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])) {
      // Check the co-partitioning requirement. A pair aligned without grouping is one each
      // side's layout answers for (`UngroupingOrigin`), so a pair no producer built is one whose
      // sides do not answer for themselves and never reaches here.
      if (satisfiesForPairing(children, requiredChildDistributions)) {
        true
      } else {
        logDebug(s"ValidateRequirements failed: children not co-partitioned in\n$plan")
        false
      }
    } else {
      satisfied
    }
  }

  /**
   * Whether the sides of a multi-child clustered operator line up: every side offers the layouts
   * it reports ([[PartitioningCollection.specsForPairing]]), and one member of the first side
   * pairs with every other side. A plan holds what its members report, so no key is deduped and
   * none is re-sorted to make a pair: a side is judged on the partitions it has, under the key
   * the operation clusters on.
   *
   * `EnsureRequirements` asks the same question of a pair it takes as it stands by a different
   * predicate: its `compatibleAsIs` reads two unprojected specs, while a member here may be
   * relabelled onto the operation's cluster key (`reportedSpecOf`), and its `agreeingPairs` /
   * `committed` path commits on the sides a reduce rebuilt rather than on the pair it picked. A
   * finished plan is asked the strict question alone, which the reduced pair answers
   * (`hasSameReducedKeys`). The coverage of every operation key
   * (`spark.sql.requireAllClusterKeysForCoPartition`) is a skew heuristic and part of neither.
   */
  private def satisfiesForPairing(
      children: Seq[SparkPlan],
      distributions: Seq[Distribution]): Boolean = {
    val specs = children.zip(distributions).map { case (child, distribution) =>
      PartitioningCollection.specsForPairing(
        child.outputPartitioning, distribution.asInstanceOf[ClusteredDistribution])
    }
    specs.headOption.exists { firstSide =>
      firstSide.exists(head => specs.tail.forall(side => side.exists(_.isCompatibleWith(head))))
    }
  }
}
