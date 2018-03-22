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
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec,
  SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 */
case class EnsureRequirements(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int =
    if (conf.adaptiveExecutionEnabled) {
      conf.maxNumPostShufflePartitions
    } else {
      conf.numShufflePartitions
    }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements.
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(defaultNumPreShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
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
          s"$operator have incompatible requirements of the number of partitions for its children")
        numPartitionsSet.headOption
      }

      val targetNumPartitions = requiredNumPartitions.getOrElse(childrenNumPartitions.max)

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, distribution), index) if childrenIndexes.contains(index) =>
          if (child.outputPartitioning.numPartitions == targetNumPartitions) {
            child
          } else {
            val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
            child match {
              // If child is an exchange, we replace it with a new one having defaultPartitioning.
              case ShuffleExchangeExec(_, c) => ShuffleExchangeExec(defaultPartitioning, c)
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

    operator.withNewChildren(children)
  }

  private def reorder(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    val leftKeysBuffer = ArrayBuffer[Expression]()
    val rightKeysBuffer = ArrayBuffer[Expression]()
    val pickedIndexes = mutable.Set[Int]()
    val keysAndIndexes = currentOrderOfKeys.zipWithIndex

    expectedOrderOfKeys.foreach(expression => {
      val index = keysAndIndexes.find { case (e, idx) =>
        // As we may have the same key used many times, we need to filter out its occurrence we
        // have already used.
        e.semanticEquals(expression) && !pickedIndexes.contains(idx)
      }.map(_._2).get
      pickedIndexes += index
      leftKeysBuffer.append(leftKeys(index))
      rightKeysBuffer.append(rightKeys(index))
    })
    (leftKeysBuffer, rightKeysBuffer)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      leftPartitioning match {
        case HashPartitioning(leftExpressions, _)
          if leftExpressions.length == leftKeys.length &&
            leftKeys.forall(x => leftExpressions.exists(_.semanticEquals(x))) =>
          reorder(leftKeys, rightKeys, leftExpressions, leftKeys)

        case _ => rightPartitioning match {
          case HashPartitioning(rightExpressions, _)
            if rightExpressions.length == rightKeys.length &&
              rightKeys.forall(x => rightExpressions.exists(_.semanticEquals(x))) =>
            reorder(leftKeys, rightKeys, rightExpressions, rightKeys)

          case _ => (leftKeys, rightKeys)
        }
      }
    } else {
      (leftKeys, rightKeys)
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
      case ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition, left, right)

      case other => other
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    // TODO: remove this after we create a physical operator for `RepartitionByExpression`.
    case operator @ ShuffleExchangeExec(upper: HashPartitioning, child) =>
      child.outputPartitioning match {
        case lower: HashPartitioning if upper.semanticEquals(lower) => child
        case _ => operator
      }
    case operator: SparkPlan =>
      ensureDistributionAndOrdering(reorderJoinPredicates(operator))
  }
}
