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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 *
 * @param optimizeOutRepartition A flag to indicate that if this rule should optimize out
 *                               user-specified repartition shuffles or not. This is mostly true,
 *                               but can be false in AQE when AQE optimization may change the plan
 *                               output partitioning and need to retain the user-specified
 *                               repartition shuffles in the plan.
 * @param requiredDistribution The root required distribution we should ensure. This value is used
 *                             in AQE in case we change final stage output partitioning.
 */
case class EnsureRequirements(
    optimizeOutRepartition: Boolean = true,
    requiredDistribution: Option[Distribution] = None)
  extends Rule[SparkPlan] {

  private def ensureDistributionAndOrdering(
      originalChildren: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution],
      requiredChildOrderings: Seq[Seq[SortOrder]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)
    // Ensure that the operator's children satisfy their output distribution requirements.
    var children = originalChildren.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(conf.numShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child, shuffleOrigin)
    }

    // Get the indexes of children which have specified distribution requirements and need to have
    // same number of partitions.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (UnspecifiedDistribution, _) => false
      case (_: BroadcastDistribution, _) => false
      case _ => true
    }.map(_._2)

    val childrenNumPartitions =
      childrenIndexes.map(children(_).outputPartitioning.numPartitions).toSet

    if (childrenNumPartitions.size > 1) {
      // Get the number of partitions which is explicitly required by the distributions.
      val requiredNumPartitions = {
        val numPartitionsSet = childrenIndexes.flatMap {
          index => requiredChildDistributions(index).requiredNumPartitions
        }.toSet
        assert(numPartitionsSet.size <= 1,
          s"$requiredChildDistributions have incompatible requirements of the number of partitions")
        numPartitionsSet.headOption
      }

      // If there are non-shuffle children that satisfy the required distribution, we have
      // some tradeoffs when picking the expected number of shuffle partitions:
      // 1. We should avoid shuffling these children.
      // 2. We should have a reasonable parallelism.
      val nonShuffleChildrenNumPartitions =
        childrenIndexes.map(children).filterNot(_.isInstanceOf[ShuffleExchangeExec])
          .map(_.outputPartitioning.numPartitions)
      val expectedChildrenNumPartitions = if (nonShuffleChildrenNumPartitions.nonEmpty) {
        if (nonShuffleChildrenNumPartitions.length == childrenIndexes.length) {
          // Here we pick the max number of partitions among these non-shuffle children.
          nonShuffleChildrenNumPartitions.max
        } else {
          // Here we pick the max number of partitions among these non-shuffle children as the
          // expected number of shuffle partitions. However, if it's smaller than
          // `conf.numShufflePartitions`, we pick `conf.numShufflePartitions` as the
          // expected number of shuffle partitions.
          math.max(nonShuffleChildrenNumPartitions.max, conf.defaultNumShufflePartitions)
        }
      } else {
        childrenNumPartitions.max
      }

      val targetNumPartitions = requiredNumPartitions.getOrElse(expectedChildrenNumPartitions)

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, distribution), index) if childrenIndexes.contains(index) =>
          if (child.outputPartitioning.numPartitions == targetNumPartitions) {
            child
          } else {
            val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
            child match {
              // If child is an exchange, we replace it with a new one having defaultPartitioning.
              case ShuffleExchangeExec(_, c, _) => ShuffleExchangeExec(defaultPartitioning, c)
              case _ => ShuffleExchangeExec(defaultPartitioning, child)
            }
          }

        case ((child, _), _) => child
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    children
  }

  private def reorder(
      leftKeys: IndexedSeq[Expression],
      rightKeys: IndexedSeq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): Option[(Seq[Expression], Seq[Expression])] = {
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return None
    }

    // Check if the current order already satisfies the expected order.
    if (expectedOrderOfKeys.zip(currentOrderOfKeys).forall(p => p._1.semanticEquals(p._2))) {
      return Some(leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          return None
      }
    }
    Some(leftKeysBuffer.toSeq, rightKeysBuffer.toSeq)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      reorderJoinKeysRecursively(
        leftKeys,
        rightKeys,
        Some(leftPartitioning),
        Some(rightPartitioning))
        .getOrElse((leftKeys, rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * Recursively reorders the join keys based on partitioning. It starts reordering the
   * join keys to match HashPartitioning on either side, followed by PartitioningCollection.
   */
  private def reorderJoinKeysRecursively(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Option[Partitioning],
      rightPartitioning: Option[Partitioning]): Option[(Seq[Expression], Seq[Expression])] = {
    (leftPartitioning, rightPartitioning) match {
      case (Some(HashPartitioning(leftExpressions, _)), _) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leftExpressions, leftKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(HashPartitioning(rightExpressions, _))) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, rightExpressions, rightKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, leftPartitioning, None))
      case (Some(PartitioningCollection(partitionings)), _) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, Some(p), rightPartitioning))
        }.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(PartitioningCollection(partitionings))) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, leftPartitioning, Some(p)))
        }.orElse(None)
      case _ =>
        None
    }
  }

  /**
   * When the physical operators are created for JOIN, the ordering of join keys is based on order
   * in which the join keys appear in the user query. That might not match with the output
   * partitioning of the join node's children (thus leading to extra sort / shuffle being
   * introduced). This rule will change the ordering of the join keys to match with the
   * partitioning of the join nodes' children.
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right, isSkew)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isSkew)

      case other => other
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transformUp {
      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin)
          if optimizeOutRepartition &&
            (shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM) =>
        def hasSemanticEqualPartitioning(partitioning: Partitioning): Boolean = {
          partitioning match {
            case lower: HashPartitioning if upper.semanticEquals(lower) => true
            case lower: PartitioningCollection =>
              lower.partitionings.exists(hasSemanticEqualPartitioning)
            case _ => false
          }
        }
        if (hasSemanticEqualPartitioning(child.outputPartitioning)) {
          child
        } else {
          operator
        }

      case operator: SparkPlan =>
        val reordered = reorderJoinPredicates(operator)
        val newChildren = ensureDistributionAndOrdering(
          reordered.children,
          reordered.requiredChildDistribution,
          reordered.requiredChildOrdering,
          ENSURE_REQUIREMENTS)
        reordered.withNewChildren(newChildren)
    }

    if (requiredDistribution.isDefined) {
      val shuffleOrigin = if (requiredDistribution.get.requiredNumPartitions.isDefined) {
        REPARTITION_BY_NUM
      } else {
        REPARTITION_BY_COL
      }
      val finalPlan = ensureDistributionAndOrdering(
        newPlan :: Nil,
        requiredDistribution.get :: Nil,
        Seq(Nil),
        shuffleOrigin)
      assert(finalPlan.size == 1)
      finalPlan.head
    } else {
      newPlan
    }
  }
}
