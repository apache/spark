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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to optimize skewed joins to avoid straggler tasks whose share of data are significantly
 * larger than those of the rest of the tasks.
 *
 * The general idea is to divide each skew partition into smaller partitions and replicate its
 * matching partition on the other side of the join so that they can run in parallel tasks.
 * Note that when matching partitions from the left side and the right side both have skew,
 * it will become a cartesian product of splits from left and right joining together.
 *
 * For example, assume the Sort-Merge join has 4 partitions:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Let's say L2, L4 and R3, R4 are skewed, and each of them get split into 2 sub-partitions. This
 * is scheduled to run 4 tasks at the beginning: (L1, R1), (L2, R2), (L3, R3), (L4, R4).
 * This rule expands it to 9 tasks to increase parallelism:
 * (L1, R1),
 * (L2-1, R2), (L2-2, R2),
 * (L3, R3-1), (L3, R3-2),
 * (L4-1, R4-1), (L4-2, R4-1), (L4-1, R4-2), (L4-2, R4-2)
 */
case class OptimizeSkewedJoin(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] {

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD. Thus we pick the larger one as the skew threshold.
   */
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      (medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }

  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin shuffled join (smj and shj).
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle read that loads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle read that loads partition0 3 times by
   *    3 tasks separately.
   */
  private def tryOptimizeJoinChildren(
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Option[(SparkPlan, SparkPlan)] = {
    // Splitting a side replicates the other side's partition to every split.
    val canSplitLeft = ShuffledJoin.canDuplicateRightSide(joinType)
    val canSplitRight = ShuffledJoin.canDuplicateLeftSide(joinType)
    if (!canSplitLeft && !canSplitRight) return None

    val leftSizes = left.mapStats.get.bytesByPartitionId
    val rightSizes = right.mapStats.get.bytesByPartitionId
    assert(leftSizes.length == rightSizes.length)
    val numPartitions = leftSizes.length
    // We use the median size of the original shuffle partitions to detect skewed partitions.
    val leftMedSize = Utils.median(leftSizes, false)
    val rightMedSize = Utils.median(rightSizes, false)
    logDebug(
      s"""
         |Optimizing skewed join.
         |Left side partitions size info:
         |${getSizeInfo(leftMedSize, leftSizes)}
         |Right side partitions size info:
         |${getSizeInfo(rightMedSize, rightSizes)}
      """.stripMargin)

    val leftSkewThreshold = getSkewThreshold(leftMedSize)
    val rightSkewThreshold = getSkewThreshold(rightMedSize)
    val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)
    val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)

    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    var numSkewedRight = 0
    for (partitionIndex <- 0 until numPartitions) {
      val leftSize = leftSizes(partitionIndex)
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
      val rightSize = rightSizes(partitionIndex)
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
      val leftNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))
      val rightNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))

      val leftParts = if (isLeftSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Left side partition $partitionIndex " +
            s"(${Utils.bytesToString(leftSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedLeft += 1
        }
        skewSpecs.getOrElse(leftNoSkewPartitionSpec)
      } else {
        leftNoSkewPartitionSpec
      }

      val rightParts = if (isRightSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Right side partition $partitionIndex " +
            s"(${Utils.bytesToString(rightSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedRight += 1
        }
        skewSpecs.getOrElse(rightNoSkewPartitionSpec)
      } else {
        rightNoSkewPartitionSpec
      }

      for {
        leftSidePartition <- leftParts
        rightSidePartition <- rightParts
      } {
        leftSidePartitions += leftSidePartition
        rightSidePartitions += rightSidePartition
      }
    }
    logDebug(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    if (numSkewedLeft > 0 || numSkewedRight > 0) {
      Some((
        SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq)),
        SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))
      ))
    } else {
      None
    }
  }

  /**
   * Split the skewed partition of the BroadcastHashJoin streamPlan.
   */
  private def tryOptimizeBroadcastHashJoinStreamedPlan(
      shuffle: ShuffleQueryStageExec): Option[SparkPlan] = {
    val mapStats = shuffle.mapStats.get
    val bytesByPartitionId = mapStats.bytesByPartitionId
    val medSize = Utils.median(bytesByPartitionId, false)
    val skewThreshold = getSkewThreshold(medSize)
    val streamedPlanTargetSize = targetSize(bytesByPartitionId, skewThreshold)

    val newPartitionsSpec = ShufflePartitionsUtil.optimizeSkewedPartitions(mapStats.shuffleId,
      bytesByPartitionId, skewThreshold, streamedPlanTargetSize)
    // return origin plan if we can not optimize partitions
    if (newPartitionsSpec.length == bytesByPartitionId.length) {
      None
    } else {
      Some(SkewJoinChildWrapper(AQEShuffleReadExec(shuffle, newPartitionsSpec)))
    }
  }

  /**
   * Returns the candidate for `plan` when it is a sort-merge, shuffled-hash or broadcast-hash join
   * with a skewed shuffled side this rule can split, and `None` otherwise.
   */
  private def splitSkewJoin(
      plan: SparkPlan,
      localShuffleReaderEnabled: Boolean): Option[SkewJoinCandidate] =
    plan match {
      case smj @ SortMergeJoinExec(_, _, joinType, _,
          s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
          s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
        tryOptimizeJoinChildren(left, right, joinType).map {
          case (newLeft, newRight) =>
            SkewJoinCandidate(smj, smj.copy(
              left = s1.copy(child = newLeft), right = s2.copy(child = newRight),
              isSkewJoin = true))
        }

      case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
          ShuffleStage(left: ShuffleQueryStageExec),
          ShuffleStage(right: ShuffleQueryStageExec), false) =>
        tryOptimizeJoinChildren(left, right, joinType).map {
          case (newLeft, newRight) =>
            SkewJoinCandidate(shj, shj.copy(left = newLeft, right = newRight, isSkewJoin = true))
        }

      // When localShuffleReader is disabled, the streamedPlan of BroadcastHashJoin may be skewed,
      // so we try to optimize it.
      case bhj @ BroadcastHashJoinExec(_, _, _, BuildRight, _,
          ShuffleStage(left: ShuffleQueryStageExec), _, _, false) if !localShuffleReaderEnabled =>
        tryOptimizeBroadcastHashJoinStreamedPlan(left).map {
          case newLeft => SkewJoinCandidate(bhj, bhj.copy(left = newLeft, isSkewJoin = true))
        }
      case bhj @ BroadcastHashJoinExec(_, _, _, BuildLeft, _, _,
          ShuffleStage(right: ShuffleQueryStageExec), _, false) if !localShuffleReaderEnabled =>
        tryOptimizeBroadcastHashJoinStreamedPlan(right).map {
          case newRight => SkewJoinCandidate(bhj, bhj.copy(right = newRight, isSkewJoin = true))
        }

      case _ => None
    }

  /**
   * Every join in `plan` this rule can split, in the bottom-up order `foreachUp` visits them.
   */
  private def collectSplitSkewJoins(plan: SparkPlan): Seq[SkewJoinCandidate] = {
    val localShuffleReaderEnabled = conf.getConf(SQLConf.LOCAL_SHUFFLE_READER_ENABLED)
    val candidates = mutable.ArrayBuffer.empty[SkewJoinCandidate]
    plan.foreachUp { operator =>
      splitSkewJoin(operator, localShuffleReaderEnabled).foreach(candidates += _)
    }
    candidates.toSeq
  }

  /**
   * `plan` with `candidates` replaced by their splits. Candidates are matched by node identity,
   * so structurally equal joins in different places are distinct candidates. A subset may be
   * passed, keeping the others out of the result.
   *
   * The walk is top-down: a shuffled-hash candidate has shuffle stages as children and a
   * sort-merge one has sorts over them, so neither contains another candidate; a broadcast-hash
   * join only constrains its streamed side, so another candidate can sit in its build side.
   * Replacing the outer join first keeps that inner candidate an instance `plan` holds, and the
   * walk then splits it inside the replacement.
   */
  private[adaptive] def applySkewJoins(
      plan: SparkPlan,
      candidates: Seq[SkewJoinCandidate]): SparkPlan = {
    if (candidates.isEmpty) {
      return plan
    }
    plan.transformDown { operator =>
      candidates.find(_.join eq operator).map(_.replacement).getOrElse(operator)
    }
  }

  private def requirementsSatisfied(plan: SparkPlan): Boolean = {
    ensureRequirements.requiredDistribution match {
      case Some(distribution) => ValidateRequirements.validate(plan, distribution)
      case None => ValidateRequirements.validate(plan)
    }
  }

  private def stripSkewJoinChildWrappers(plan: SparkPlan): SparkPlan = {
    plan.transform { case SkewJoinChildWrapper(child) => child }
  }

  /**
   * Applies the candidates one at a time, in the order a bottom-up pass visits them, keeping a
   * candidate when the plan holding the kept ones and this one still satisfies every requirement.
   * A split the plan cannot take therefore costs only its own join, where the whole-plan fast path
   * would have cost every join.
   *
   * Each trial is rebuilt from `plan` rather than from the previous trial, so acceptance never
   * depends on how a rebuild kept node instances; the handful of skewed joins keeps that cheap.
   */
  private def applySkewJoinsSatisfyingRequirements(
      plan: SparkPlan,
      candidates: Seq[SkewJoinCandidate]): SparkPlan = {
    var accepted = Seq.empty[SkewJoinCandidate]
    candidates.foreach { candidate =>
      val trial = applySkewJoins(plan, accepted :+ candidate)
      if (requirementsSatisfied(trial)) {
        accepted = accepted :+ candidate
      } else {
        logDebug(s"Skip splitting the skewed join " +
          s"${candidate.join.simpleString(conf.maxToStringFields)}: with it the plan fails " +
          "ValidateRequirements")
      }
    }
    logDebug(s"Optimized ${accepted.length} of ${candidates.length} skewed joins")
    stripSkewJoinChildWrappers(applySkewJoins(plan, accepted))
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    // Try every split at once first: one pass to find the candidates and one validation cover the
    // common case. When that breaks a requirement of the plan, either accept the extra shuffles if
    // the force-apply config is true, or keep only the joins whose splits the plan can take. The
    // candidates are collected once, so the fallback reuses the splits the fast path built.
    val candidates = collectSplitSkewJoins(plan)
    val optimized = applySkewJoins(plan, candidates)
    if (requirementsSatisfied(optimized)) {
      stripSkewJoinChildWrappers(optimized)
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      stripSkewJoinChildWrappers(ensureRequirements.apply(optimized))
    } else if (conf.getConf(SQLConf.SKEW_JOIN_PER_JOIN_FALLBACK_ENABLED)) {
      applySkewJoinsSatisfyingRequirements(plan, candidates)
    } else {
      plan
    }
  }

  object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
        Some(s)
      case _ => None
    }
  }
}

// A skewed join `OptimizeSkewedJoin` can split: the join node in the plan, its join type, and the
// join it is replaced by, with the skewed side split.
private case class SkewJoinCandidate(join: BaseJoinExec, replacement: SparkPlan)

// After optimizing skew joins, we need to run EnsureRequirements again to add necessary shuffles
// caused by skew join optimization. However, this shouldn't apply to the sub-plan under skew join,
// as it's guaranteed to satisfy distribution requirement.
case class SkewJoinChildWrapper(plan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
}
