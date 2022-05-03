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


package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._

/**
 * A rule to propagate empty partitions, so that some unnecessary shuffle read can be skipped.
 *
 * The general idea is to utilize the shuffled join to skip some partitions.
 *
 * For example, assume the shuffled join has 4 partitions, and L2 and R3 are empty:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Suppose the join type is Inner. Then this rule will skip reading partitions: L2, R2, L3, R3.
 *
 * Suppose the join type is LeftOuter. Then this rule will skip reading partitions: L2, R2, R3.
 *
 * Suppose the join type is RightOuter. Then this rule will skip reading partitions: L2, L3, R3.
 *
 * Suppose the join type is FullOuter. Then this rule will skip reading partitions: L2, R3.
 */
object PropagateEmptyPartitions extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_NUM, REPARTITION_BY_COL,
      REBALANCE_PARTITIONS_BY_NONE, REBALANCE_PARTITIONS_BY_COL)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.propagateEmptyPartitionsEnabled) {
      return plan
    }

    // If there is no ShuffledJoin, no need to continue.
    if (plan.collectFirst { case j: ShuffledJoin => j }.isEmpty) {
      return plan
    }

    val stages = plan.collect { case s: ShuffleQueryStageExec => s }
    // currently, empty information is only extracted from and propagated to shuffle data.
    // TODO: support DataScan in the future.
    if (stages.size < 2 || !stages.forall(_.isMaterialized)) {
      return plan
    }

    val (_, emptyGroupInfos) = collectEmptyGroups(plan)

    // stageId -> propagated empty indices
    val emptyIndicesMap = mutable.Map.empty[Int, Set[Int]]
    emptyGroupInfos.foreach {
      case EmptyGroupInfo(stageIds, emptyIndices) =>
        if (stageIds.nonEmpty && emptyIndices.nonEmpty) {
          stageIds.foreach { stageId =>
            val emptySet = emptyIndicesMap.getOrElse(stageId, Set.empty)
            emptyIndicesMap.update(stageId, emptySet ++ emptyIndices)
          }
        }
    }

    if (emptyIndicesMap.nonEmpty) {
      updateShuffleReads(plan, emptyIndicesMap.toMap)
    } else {
      plan
    }
  }

  /**
   *  collect following information from a plan:
   *  1, group info at current operator
   *  2, collected group infos at AQEShuffleRead/ShuffleQueryStage/ShuffledJoin operators
   */
  private def collectEmptyGroups(plan: SparkPlan): (EmptyGroupInfo, Seq[EmptyGroupInfo]) = {
    plan match {
      case AQEShuffleReadExec(stage: ShuffleQueryStageExec, specs: Seq[ShufflePartitionSpec]) =>
        val emptyIndices = Iterator.range(0, specs.length).filter { i =>
          specs(i) match {
            case CoalescedPartitionSpec(_, _, Some(0L)) => true
            case PartialReducerPartitionSpec(_, _, _, 0L) => true
            case EmptyPartitionSpec => true
            case _ => false
          }
        }.toSet
        val currentGroupInfo = EmptyGroupInfo(Seq(stage.id), emptyIndices)
        (currentGroupInfo, Seq(currentGroupInfo))

      case stage: ShuffleQueryStageExec =>
        val sizes = stage.mapStats.get.bytesByPartitionId
        val emptyIndices = Iterator.range(0, sizes.length).filter(sizes(_) == 0).toSet
        val currentGroupInfo = EmptyGroupInfo(Seq(stage.id), emptyIndices)
        (currentGroupInfo, Seq(currentGroupInfo))

      case _: LeafExecNode => (EmptyGroupInfo(Nil, Set.empty), Nil)

      case unary: UnaryExecNode => collectEmptyGroups(unary.child)

      case join: ShuffledJoin =>
        val (EmptyGroupInfo(leftStageIds, leftEmptyIndices), leftGroupInfos) =
          collectEmptyGroups(join.left)
        val (EmptyGroupInfo(rightStageIds, rightEmptyIndices), rightGroupInfos) =
          collectEmptyGroups(join.right)
        val currentGroupInfo = join.joinType match {
          case Inner | Cross =>
            EmptyGroupInfo(leftStageIds ++ rightStageIds, leftEmptyIndices ++ rightEmptyIndices)

          case LeftOuter | LeftAnti | LeftSemi =>
            EmptyGroupInfo(leftStageIds ++ rightStageIds, leftEmptyIndices)

          case RightOuter =>
            EmptyGroupInfo(leftStageIds ++ rightStageIds, rightEmptyIndices)

          case FullOuter =>
            EmptyGroupInfo(leftStageIds ++ rightStageIds, leftEmptyIndices & rightEmptyIndices)

          case _ => EmptyGroupInfo(Nil, Set.empty)
        }
        (currentGroupInfo, leftGroupInfos ++ rightGroupInfos :+ currentGroupInfo)

      case _ =>
        val childGroupInfos = plan.children.flatMap(collectEmptyGroups(_)._2)
        (EmptyGroupInfo(Nil, Set.empty), childGroupInfos)
    }
  }

  private def updateShuffleReads(
      plan: SparkPlan,
      emptyIndicesMap: Map[Int, Set[Int]]): SparkPlan = plan match {
    case AQEShuffleReadExec(stage: ShuffleQueryStageExec, specs: Seq[ShufflePartitionSpec])
      if emptyIndicesMap.contains(stage.id) =>
      val indices = emptyIndicesMap(stage.id)
      val newSpecs = Seq.tabulate(specs.length) { i =>
        if (indices.contains(i)) {
          EmptyPartitionSpec
        } else {
          specs(i)
        }
      }
      AQEShuffleReadExec(stage, newSpecs)

    case stage: ShuffleQueryStageExec if emptyIndicesMap.contains(stage.id) =>
      val indices = emptyIndicesMap(stage.id)
      val sizes = stage.mapStats.get.bytesByPartitionId
      val newSpecs = Seq.tabulate(sizes.length) { i =>
        if (indices.contains(i)) {
          EmptyPartitionSpec
        } else {
          CoalescedPartitionSpec(i, i + 1, sizes(i))
        }
      }
      AQEShuffleReadExec(stage, newSpecs)

    case _ => plan.mapChildren(updateShuffleReads(_, emptyIndicesMap))
  }
}

/**
 * group info at one operator
 * @param stageIds indicate which AQEShuffleReadExec/ShuffleQueryStageExec
 *                 this emptyIndices can propagate to
 * @param emptyIndices indices of empty partitions
 */
private case class EmptyGroupInfo(
    stageIds: Seq[Int],
    emptyIndices: Set[Int])
