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

import scala.annotation.tailrec

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, KeyedPartitioning, KeyReducer, Partitioning, PartitioningCollection, REPLICATED_FOR_JOIN, SPLIT_FOR_JOIN}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.execution.datasources.v2.{ExpectedKeySpec, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.util.Utils

/**
 * Splits the skewed key groups of an aligned storage-partitioned join. A stage retains its
 * original plan if the candidate loses the split's benefits or fails its requirements.
 *
 * The alignment `EnsureRequirements` plans coalesces every key's input partitions into one output
 * partition per side, leaving a key that holds disproportionately many partitions to a single join
 * task. This rule reads that finished alignment back: a pair of grouping nodes over the join's two
 * sides, one output partition per key. A key is skewed on a side when its input partition count
 * exceeds both the configured threshold and that side's median count times the configured factor.
 * The eligible skewed side spreads its splits over output partitions of about
 * `spark.sql.sources.v2.bucketing.advisoryInputPartitionsPerOutputPartition` each, and the other
 * side repeats its group into each of those.
 *
 * Each exchange input is optimized independently. Within a stage the rule builds a candidate,
 * then checks whether its skewed groupings retain their parallelism and its requirements remain
 * satisfied. A rejected candidate returns the original stage, preserving every existing grouping,
 * projection and aggregate. Rollback never reconstructs the original alignment.
 */
case class OptimizeSkewedKeyGroups(requiredDistribution: Option[Distribution] = None)
  extends Rule[SparkPlan] with SQLConfHelper with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.v2BucketingEnabled || !conf.v2BucketingSkewJoinEnabled) {
      return plan
    }
    val wrapped = plan.transformUp {
      case exchange: Exchange =>
        SkewKeyGroupExchangeWrapper(exchange.mapChildren(optimizeSkewWithinStage))
    }
    val optimized = optimizeSkewWithinStage(wrapped)
    def unwrap(p: SparkPlan): SparkPlan = p match {
      case SkewKeyGroupExchangeWrapper(exchange) => exchange.mapChildren(unwrap)
      case other => other.mapChildren(unwrap)
    }
    val result = unwrap(optimized)
    val satisfied = requiredDistribution match {
      case Some(distribution) => ValidateRequirements.validate(result, distribution)
      case None => ValidateRequirements.validate(result)
    }
    if (satisfied) result else plan
  }

  // Hide already optimized exchange inputs from stage-local rewrites and benefit checks.
  private case class SkewKeyGroupExchangeWrapper(exchange: SparkPlan) extends LeafExecNode {
    override def output: Seq[Attribute] = exchange.output
    override def outputPartitioning: Partitioning = exchange.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = exchange.outputOrdering
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Skew key group exchange wrapper must be removed")
  }

  private def optimizeSkewWithinStage(original: SparkPlan): SparkPlan = {
    val candidate = original.transformUp {
      case j @ SortMergeJoinExec(_, _, _, _,
          KeyedJoinSide(left), KeyedJoinSide(right), false) =>
        val optimized = splitKeyGroups(j, left, right)
        optimized.withNewChildren(optimized.children.zip(j.requiredChildOrdering).map {
          case (child, ordering) if !SortOrder.orderingSatisfies(child.outputOrdering, ordering) =>
            SortExec(ordering, global = false, child)
          case (child, _) => child
        })
      case j @ ShuffledHashJoinExec(_, _, _, _, _,
          KeyedJoinSide(left), KeyedJoinSide(right), false) =>
        splitKeyGroups(j, left, right)
    }
    if (candidate eq original) {
      return original
    }
    if (hasBenefits(candidate) && ValidateRequirements.validate(candidate)) {
      candidate
    } else {
      original
    }
  }

  /**
   * Check each stage-local input path, not the total grouping count. Split and replicated GPEs
   * on sibling join inputs are the required pair. On one path, any additional grouping conflicts
   * with a skewed GPE: plain grouping coalesces its slots; partial clustering has independent
   * slot reservations; another skewed grouping nests two decisions in one stage. Exchanges end
   * the path and allow their input stages to make independent decisions.
   */
  private def hasBenefits(plan: SparkPlan): Boolean = {
    def check(p: SparkPlan, groupedAbove: Boolean, skewedAbove: Boolean): Boolean = p match {
      case g: GroupPartitionsExec =>
        !skewedAbove && (!g.isSkew || !groupedAbove) &&
          check(g.child, groupedAbove = true, skewedAbove = g.isSkew)
      case other => other.children.forall(check(_, groupedAbove, skewedAbove))
    }
    check(plan, groupedAbove = false, skewedAbove = false)
  }

  /**
   * `join`, with the key groups its counts say are skewed spread over more output partitions. A
   * pair nothing qualifies for is returned as it stands, and so is one an earlier pass split: the
   * counts read here are the sources', so a second pass would decide the same way.
   */
  private def splitKeyGroups(
      join: ShuffledJoin,
      left: KeyedJoinSide,
      right: KeyedJoinSide): SparkPlan = {
    // ER has already aligned the pair. Without a grouping, neither side has splits to spread.
    if (left.gpe.isEmpty && right.gpe.isEmpty) {
      return join
    }
    if (Seq(left, right).exists(side => side.reducers.isDefined ||
        side.childPartitioning.mayContainUnknownPartitionKeys || side.gpe.exists(_.isSkew))) {
      return join
    }
    val canReplicateLeft = ShuffledJoin.canDuplicateLeftSide(join.joinType)
    val canReplicateRight = ShuffledJoin.canDuplicateRightSide(join.joinType)
    if (!canReplicateLeft && !canReplicateRight) {
      return join
    }

    // One entry a key: a pairing emits a key once per reserved run, and a repeated entry here
    // would reserve the key twice over.
    val expected = left.keys.distinct
    if (expected.isEmpty) {
      return join
    }
    val leftCounts = splitsPerKey(left)
    val rightCounts = splitsPerKey(right)
    def skewThreshold(counts: Map[InternalRowComparableWrapper, Int]): Long = {
      val median = Utils.median(expected.map(key => counts.getOrElse(key, 0).toLong).toArray,
        alreadySorted = false)
      conf.v2BucketingSkewJoinSkewedPartitionsPerGroupThreshold.toLong.max(
        (median * conf.v2BucketingSkewJoinSkewedGroupFactor).toLong)
    }
    val leftThreshold = skewThreshold(leftCounts)
    val rightThreshold = skewThreshold(rightCounts)
    val advisory = conf.v2BucketingAdvisoryInputPartitionsPerOutputPartition

    // Detect skew independently on each side, as OptimizeSkewedJoin does. If both sides qualify,
    // spread the side holding more splits; the join type must allow replicating the other side.
    val skewed = expected.flatMap { key =>
      val nL = leftCounts.getOrElse(key, 0)
      val nR = rightCounts.getOrElse(key, 0)
      val leftSkewed = canReplicateRight && nL > leftThreshold
      val rightSkewed = canReplicateLeft && nR > rightThreshold
      val candidate = (leftSkewed, rightSkewed) match {
        case (true, true) => Some(if (nL >= nR) (nL, true) else (nR, false))
        case (true, false) => Some((nL, true))
        case (false, true) => Some((nR, false))
        case _ => None
      }
      candidate.flatMap { case (count, leftDistributes) =>
        val numSplits = math.ceil(count.toDouble / advisory).toInt
        Option.when(numSplits > 1)(key -> (numSplits, leftDistributes))
      }
    }.toMap
    if (skewed.isEmpty) {
      return join
    }
    logInfo(s"Splitting ${skewed.size} skewed key groups of a storage-partitioned join")

    // A key the decision spread takes the origin of the side it spreads on. Every other key keeps
    // the origin the alignment gave it, and one the alignment claimed none for -- a key it
    // coalesced, with nothing spread to say -- takes the side-wide one: the pairing is judged
    // index by index over the origins a layout holds, and a partition holding none is one no
    // producer vouched for, which leaves the whole layout unclaimed and the pair unjudged.
    def specsOf(side: KeyedJoinSide, isLeft: Boolean): Seq[ExpectedKeySpec] = {
      // ER constructs both sides from the same merged keys, one specification per key.
      val specsByKey = side.keys.zip(side.specs).toMap
      expected.map { key =>
        val spec = specsByKey(key)
        skewed.get(key) match {
          case Some((numSplits, leftDistributes)) =>
            val spread = if (isLeft) leftDistributes else !leftDistributes
            val origin = if (spread) SPLIT_FOR_JOIN else REPLICATED_FOR_JOIN
            ExpectedKeySpec(numSplits, Some(origin), skewed = true)
          case None if spec.origin.isDefined => spec
          case None =>
            val origin = if (isLeft) SPLIT_FOR_JOIN else REPLICATED_FOR_JOIN
            spec.copy(origin = Some(origin))
        }
      }
    }

    def rebuilt(side: KeyedJoinSide, isLeft: Boolean): SparkPlan = {
      GroupPartitionsExec(
        side.child,
        side.joinKeyPositions,
        Some(expected.zip(specsOf(side, isLeft))),
        side.reducers,
        side.enableSortedMerge)
    }

    join.withNewChildren(Seq(rebuilt(left, isLeft = true), rebuilt(right, isLeft = false)))
  }

  private case class KeyedJoinSide(plan: SparkPlan) {
    def gpe: Option[GroupPartitionsExec] = plan match {
      case g: GroupPartitionsExec => Some(g)
      case _ => None
    }
    def child: SparkPlan = gpe.map(_.child).getOrElse(plan)
    def childPartitioning: KeyedPartitioning =
      PartitioningCollection.representativeOf(child.outputPartitioning).get
    def joinKeyPositions: Option[Seq[Int]] = gpe.flatMap(_.joinKeyPositions)
    def reducers: Option[Seq[Option[KeyReducer]]] = gpe.flatMap(_.reducers)
    def enableSortedMerge: Boolean = gpe.exists(_.enableSortedMerge)
    def keys: Seq[InternalRowComparableWrapper] =
      gpe.map(_.groupedPartitions.map(_._1).distinct).getOrElse(childPartitioning.partitionKeys)
    def specs: Seq[ExpectedKeySpec] = gpe.flatMap(_.expectedKeySpecs)
      .getOrElse(Seq.fill(keys.size)(ExpectedKeySpec(1, None)))
  }

  private object KeyedJoinSide {
    @tailrec
    def unapply(plan: SparkPlan): Option[KeyedJoinSide] = plan match {
      case s: SortExec if !s.global => unapply(s.child)
      case p => PartitioningCollection.representativeOf(p.outputPartitioning)
        .map(_ => KeyedJoinSide(p))
    }
  }

  /** The input split counts in the same projected key space used to rebuild the grouping. */
  private def splitsPerKey(side: KeyedJoinSide): Map[InternalRowComparableWrapper, Int] = {
    val input = side.childPartitioning
    side.joinKeyPositions.fold(input.partitionKeys)(input.projectKeys(_)._2)
      .groupBy(identity).view.mapValues(_.size).toMap
  }
}
