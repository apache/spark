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
import org.apache.spark.sql.execution.joins.ShuffledJoin
import org.apache.spark.sql.internal.SQLConf

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

    // A `ClusteredDistribution` is the one distribution an operator can owe its children together
    // rather than one by one, so an operator whose children all owe one is judged on their mutual
    // layout below, and every other child, an operator with a single clustered child included,
    // answers for itself. That is every such operator, not only a join: one that zips corresponding
    // partitions, a cogroup for instance, reads a layout both children have to hold together.
    val clusteredMultiChild = children.length > 1 &&
      requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])

    // The one member a finished plan may report without satisfying the distribution is the shape
    // partially clustered distribution spreads ungrouped, and one producer builds it:
    // `EnsureRequirements.checkKeyGroupCompatible`. Both halves of that admission are asked here,
    // and the answer is passed down to the pairing below so the waiver cannot be read one way here
    // and the other way there. Neither half is a second copy: the operator kinds come from the
    // producer itself. What is left to the member, its count and the permission for the collapse it
    // went through, is asked there.
    val mayBeUngrouped = clusteredMultiChild &&
      SQLConf.get.v2BucketingPartiallyClusteredDistributionEnabled &&
      ShuffledJoin.partiallyClusteredJoinType(plan).isDefined

    val satisfied = children.zip(requiredChildDistributions.zip(requiredChildOrderings)).forall {
      case (child, (distribution, ordering))
          if (!child.outputPartitioning.satisfies(distribution) &&
              !(mayBeUngrouped &&
                PartitioningCollection.representativeOf(child.outputPartitioning).isDefined))
            || !SortOrder.orderingSatisfies(child.outputOrdering, ordering) =>
        logDebug(s"ValidateRequirements failed: $distribution, $ordering\n$plan")
        false
      case _ => true
    }

    // What a multi-child clustered operator reads is the pairing: a pair aligned without grouping,
    // which partially clustered distribution builds on purpose, is one the sides agree on while
    // neither is grouped. The pairing cannot tell how the two sides hold a key's rows, since a
    // spread side and one that repeats the whole group report the same keys as two sides that
    // split the key, so that rests on the producer, which is why the ungrouped shape alone is
    // waived above.
    if (!satisfied) {
      false
    } else if (clusteredMultiChild) {
      val paired = satisfiesForPairing(children, requiredChildDistributions, mayBeUngrouped)
      if (!paired) {
        logDebug(s"ValidateRequirements failed: children not co-partitioned in\n$plan")
      }
      paired
    } else {
      true
    }
  }

  /**
   * Whether the sides of a multi-child clustered operator line up: every side offers the layouts it
   * reports ([[PartitioningCollection.specsForPairing]]), and one member of the first side pairs
   * with every other side. A plan holds what its members report, so no key is deduped and none is
   * re-sorted to make a pair: a side is judged on the partitions it has, under the key the
   * operation clusters on.
   *
   * This is the question `EnsureRequirements` asks of a pair it takes as it stands, the
   * `compatibleAsIs` path. Its other path commits on the sides it builds rather than on the pair it
   * picked: `agreeingPairs` admits a pair a reduce would reconcile (`areKeysCompatible` with the
   * reduce allowed), and `committed` then compares the declared layouts of the two sides that
   * reduce ran on. A finished plan is asked the strict question alone, which is sound because the
   * reduced pair answers it: `isExpressionCompatible` reads its two sides through
   * `hasSameReducedKeys`. The coverage of every operation key a member is additionally asked for
   * (`spark.sql.requireAllClusterKeysForCoPartition`) is a skew heuristic, and is not part of it.
   */
  private def satisfiesForPairing(
      children: Seq[SparkPlan],
      distributions: Seq[Distribution],
      mayBeUngrouped: Boolean): Boolean = {
    val specs = children.zip(distributions).map { case (child, distribution) =>
      PartitioningCollection.specsForPairing(
        child.outputPartitioning, distribution.asInstanceOf[ClusteredDistribution], mayBeUngrouped)
    }
    specs.headOption.exists { firstSide =>
      firstSide.exists(head => specs.tail.forall(side => side.exists(_.isCompatibleWith(head))))
    }
  }
}
