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

import org.apache.commons.io.FileUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec, MapInPandasExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowExecBase}
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
      medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR))
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

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Collect all ShuffledJoins/ShuffleQueryStages, and check valid operators;
   * 2. Collect all splittable ShuffleQueryStages by the semantics of internal nodes.
   *    A ShuffleQueryStages is splittable if it can be split into specs, each spec can be
   *    processed independently, and the original data result can be obtained by union all
   *    the outputs of specs.
   *    Splittable ShuffleQueryStages are collected in this way:
   *      1, at Join node, select the splittable paths according to its JoinType;
   *      2, at Agg/Window node, stop;
   *      3, all the reached leave are splittable;
   *    For example, in the following example stage, ShuffleQueryStages s0/s2/s4 are splittable.
   *
   *                     inner
   *                   /        \
   *                cross      right
   *               /     \    /     \
   *            inner    s2  s3    inner
   *           /     \            /     \
   *          s0     agg         s4     win
   *                  |                  |
   *                  s1                 s5
   *
   * 3. Precompute skewThreshold and targetSize for each splittable ShuffleQueryStageExec;
   * 4. For each splittable ShuffleQueryStageExec, check whether skew partitions exists, if true,
   *    split them into specs. This step also detects and handles Combinatorial Explosion: for
   *    each skew partition, check whether the combination number is too large, if so, re-split the
   *    ShuffleQueryStageExecs.
   *    For example, for partition 0, stage s0/s2/s4 are split into 100/100/100 specs,
   *    respectively. Then there are 1M combinations, which is too large, and will cause
   *    performance regression. Given a threshold (1k by default), the numbers of specs will
   *    be optimized to 10/10/10.
   * 5. Generate final specs. Suppose above splittable ShuffleQueryStages s0/s2/s4 are finally
   *    split into 2/2/3 specs, then there will be following 2X2X3=12 combinations:
   *      [s0_spec0, s1, s2_spec0, s3, s4_spec0, s5]
   *      [s0_spec0, s1, s2_spec0, s3, s4_spec1, s5]
   *      [s0_spec0, s1, s2_spec0, s3, s4_spec2, s5]
   *      [s0_spec0, s1, s2_spec1, s3, s4_spec0, s5]
   *      [s0_spec0, s1, s2_spec1, s3, s4_spec1, s5]
   *      [s0_spec0, s1, s2_spec1, s3, s4_spec2, s5]
   *      [s0_spec1, s1, s2_spec0, s3, s4_spec0, s5]
   *      [s0_spec1, s1, s2_spec0, s3, s4_spec1, s5]
   *      [s0_spec1, s1, s2_spec0, s3, s4_spec2, s5]
   *      [s0_spec1, s1, s2_spec1, s3, s4_spec0, s5]
   *      [s0_spec1, s1, s2_spec1, s3, s4_spec1, s5]
   *      [s0_spec1, s1, s2_spec1, s3, s4_spec2, s5]
   * 6. Generate optimized plan by attaching new specs to ShuffleQueryStageExecs;
   */
  private def optimizeShuffledJoin(join: ShuffledJoin): SparkPlan = {
    import OptimizeSkewedJoin._
    val logPrefix = s"Optimizing ${join.nodeName} #${join.id}"

    // Step 1: Collect all ShuffledJoins/ShuffleQueryStages, and validate operators.
    val joins = mutable.ArrayBuffer.empty[ShuffledJoin]
    val stages = mutable.ArrayBuffer.empty[ShuffleQueryStageExec]
    val invalids = mutable.ArrayBuffer.empty[SparkPlan]

    join.foreach {
      // All leave must be QueryStage for now
      // TODO: support Bucket Join with other types of leaves.
      case s: ShuffleQueryStageExec if s.isMaterialized => stages.append(s)
      case b: BroadcastQueryStageExec if b.isMaterialized =>

      case j: SortMergeJoinExec if !j.isSkewJoin => joins.append(j)
      case j: ShuffledHashJoinExec if !j.isSkewJoin => joins.append(j)
      case _: BroadcastHashJoinExec =>
      case _: BroadcastNestedLoopJoinExec =>
      case _: CartesianProductExec =>

      case a: ObjectHashAggregateExec if !a.isSkew =>
      case a: HashAggregateExec if !a.isSkew =>
      case a: SortAggregateExec if !a.isSkew =>

      case w: WindowExec if !w.isSkew =>

      case _: SortExec =>
      case _: FilterExec =>
      case _: ProjectExec =>
      case _: GenerateExec =>
      case _: CollectMetricsExec =>
      case _: WholeStageCodegenExec =>

      case _: ColumnarToRowExec =>
      case _: RowToColumnarExec =>

      case _: DeserializeToObjectExec =>
      case _: SerializeFromObjectExec =>

      case _: MapElementsExec =>
      case _: MapPartitionsExec =>
      case _: MapPartitionsInRWithArrowExec =>
      case _: MapInPandasExec =>
      case _: ArrowEvalPythonExec =>
      case _: BatchEvalPythonExec =>

      // There are more and more physical operators, this check is for data correctness
      // TODO: support more operators like AggregateInPandasExec/FlatMapCoGroupsInPandasExec/etc
      case invalid => invalids.append(invalid)
    }

    if (invalids.nonEmpty) {
      logDebug(s"$logPrefix: Do NOT support operators " +
        s"${invalids.map(_.nodeName).mkString("[", ", ", "]")}")
      return join
    }
    if (joins.isEmpty || joins.size != stages.size - 1) {
      return join
    }

    val stageStats = stages.flatMap {
      case ShuffleStage(stage: ShuffleQueryStageExec) =>
        stage.mapStats.filter(_.bytesByPartitionId.nonEmpty).map(stats => stage.id -> stats)
      case _ => None
    }.toMap
    if (stageStats.size != stages.size ||
      stageStats.values.map(_.bytesByPartitionId.length).toSet.size != 1) {
      return join
    }
    val stageIds = stageStats.keysIterator.toArray
    logDebug(s"$logPrefix: ShuffleQueryStages: ${stageIds.mkString("[", ", ", "]")}")
    val numPartitions = stageStats.head._2.bytesByPartitionId.length

    // Step 2: Collect all splittable ShuffleQueryStageExecs
    // How to determine splittable ShuffleQueryStageExecs:
    //  1, at Join node, select the splittable paths according to its JoinType;
    //  2, at Agg/Window node, stop;
    //  3, all the reached leave are splittable;
    def collectSplittableStageIds(plan: SparkPlan): Seq[Int] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage.id)

      case join: ShuffledJoin =>
        var splittableChildren = Seq.empty[SparkPlan]
        if (canSplitLeftSide(join.joinType)) splittableChildren :+= join.left
        if (canSplitRightSide(join.joinType)) splittableChildren :+= join.right
        splittableChildren.flatMap(collectSplittableStageIds)

      case _: BaseAggregateExec => Seq.empty

      case _: WindowExecBase => Seq.empty

      case _ => plan.children.flatMap(collectSplittableStageIds)
    }
    val splittableStageIds = collectSplittableStageIds(join)
    logDebug(s"$logPrefix: Splittable ShuffleQueryStages: " +
      s"${splittableStageIds.mkString("[", ", ", "]")}")
    if (splittableStageIds.isEmpty ||
      !splittableStageIds.forall(stageStats.contains)) {
      return join
    }

    // Step 3: Precompute skewThreshold and targetSize for each splittable ShuffleQueryStageExec
    val splittableStageInfos = splittableStageIds.map { stageId =>
      val sizes = stageStats(stageId).bytesByPartitionId
      val medSize = Utils.median(sizes)
      val threshold = getSkewThreshold(medSize)
      val target = targetSize(sizes, threshold)
      logDebug(s"$logPrefix: Analyzing ShuffleQueryStage #$stageId in " +
        s"skew join, size info: ${getSizeInfo(medSize, sizes)}")
      stageId -> (threshold, target)
    }.toMap

    // Step 4: Split skew partitions
    // within each partition, find and split the splittable skew ShuffleQueryStageExecs
    // (partitionIndex, stageId) -> skew splits
    val skewSpecs = mutable.OpenHashMap.empty[(Int, Int), Seq[PartialReducerPartitionSpec]]
    val partSpecs = mutable.OpenHashMap.empty[Int, Seq[PartialReducerPartitionSpec]]
    val maxCombinations = conf.getConf(SQLConf.SKEW_JOIN_MAX_SPLITS_PER_PARTITION)

    Range(0, numPartitions).foreach { partitionIndex =>
      partSpecs.clear()
      splittableStageInfos.foreach { case (stageId, (threshold, target)) =>
        val stats = stageStats(stageId)
        val size = stats.bytesByPartitionId(partitionIndex)
        if (size > threshold) {
          ShufflePartitionsUtil
            .createSkewPartitionSpecs(stats.shuffleId, partitionIndex, target)
            .foreach { splits =>
              logDebug(s"$logPrefix: Splitting ShuffleQueryStage #$stageId: " +
                s"partition $partitionIndex(${FileUtils.byteCountToDisplaySize(size)}) -> " +
                s"${splits.size} splits")
              partSpecs(stageId) = splits
            }
        }
      }

      // Handle Combinatorial Explosion.
      val numCombinations = safeProduct(partSpecs.valuesIterator.map(_.size))
      if (numCombinations > maxCombinations) {
        val (splitStageIds, numSplits) = partSpecs.mapValues(_.size).toArray.unzip
        val combinedNumSplits = combine(maxCombinations, numSplits)
        logDebug(s"$logPrefix: partition $partitionIndex: Combinatorial Explosion! " +
          s"Try to combine $numCombinations(${numSplits.mkString("[", ", ", "]")}) " +
          s"to ${safeProduct(combinedNumSplits)}(${combinedNumSplits.mkString("[", ", ", "]")})")

        partSpecs.clear()
        splitStageIds.zip(combinedNumSplits)
          .filter(_._2 > 1)
          .foreach { case (stageId, newNumSplits) =>
            val stats = stageStats(stageId)
            val size = stats.bytesByPartitionId(partitionIndex)
            // TODO: ShufflePartitionsUtil supports target number of specs
            // simply adjust the target size to control the number of splits for now
            val newTarget = (1.1 * size.toDouble / newNumSplits).toLong + 1L
            ShufflePartitionsUtil
              .createSkewPartitionSpecs(stats.shuffleId, partitionIndex, newTarget)
              .foreach { splits =>
                logDebug(s"$logPrefix: Re-splitting ShuffleQueryStage #$stageId: " +
                  s"partition $partitionIndex(${FileUtils.byteCountToDisplaySize(size)}) -> " +
                  s"${splits.size} splits")
                partSpecs(stageId) = splits
              }
          }
      }

      partSpecs.foreach { case (stageId, splits) => skewSpecs((partitionIndex, stageId)) = splits }
    }
    partSpecs.clear()
    logDebug(s"$logPrefix: Totally ${skewSpecs.size} skew partitions found")
    if (skewSpecs.isEmpty) return join

    // Step 5: Generate final specs
    // within a partition, split the skew ShuffleQueryStageExecs, and duplicate others
    def createNonSkewSpec(partitionIndex: Int, stageId: Int) = {
      val size = stageStats(stageId).bytesByPartitionId(partitionIndex)
      Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, size))
    }

    def traverseCombinations(seqs: Seq[Seq[ShufflePartitionSpec]]) = {
      require(seqs.nonEmpty)
      val iter = seqs.head.iterator.map(item => Seq(item))
      seqs.tail.foldLeft(iter)((iter, seq) => iter.flatMap(comb => seq.map(item => comb :+ item)))
    }

    val buffers = stageIds.map(_ => mutable.ArrayBuffer.empty[ShufflePartitionSpec])
    Range(0, numPartitions).foreach { partitionIndex =>
      val specs = stageIds.map { stageId =>
        skewSpecs.getOrElse((partitionIndex, stageId), createNonSkewSpec(partitionIndex, stageId))
      }
      traverseCombinations(specs).foreach { combination =>
        combination.indices.foreach(i => buffers(i) += combination(i))
      }
    }
    val newSpecs = stageIds.zip(buffers.map(_.toSeq)).toMap

    // Step 6: Generate final plan
    //  1, mark all Join/Agg/Window nodes skew;
    //  2, attach new specs to ShuffleQueryStageExecs;
    join transform {
      case smj: SortMergeJoinExec => smj.copy(isSkewJoin = true)
      case shj: ShuffledHashJoinExec => shj.copy(isSkewJoin = true)

      case obj: ObjectHashAggregateExec => obj.copy(isSkew = true)
      case hash: HashAggregateExec => hash.copy(isSkew = true)
      case sort: SortAggregateExec => sort.copy(isSkew = true)

      case win: WindowExec => win.copy(isSkew = true)

      case stage: ShuffleQueryStageExec =>
        SkewJoinChildWrapper(AQEShuffleReadExec(stage, newSpecs(stage.id)))
    }
  }

  private def optimize(plan: SparkPlan): SparkPlan = {
    plan transformDown {
      case join: ShuffledJoin if !join.isSkewJoin => optimizeShuffledJoin(join)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }
    if (plan.collectFirst { case s: ShuffledJoin if !s.isSkewJoin => s }.isEmpty) {
      return plan
    }

    val optimized = optimize(plan)
    if (optimized.collectFirst { case s: ShuffledJoin if s.isSkewJoin => s }.isEmpty) {
      return plan
    }
    val requirementSatisfied = if (ensureRequirements.requiredDistribution.isDefined) {
      ValidateRequirements.validate(optimized, ensureRequirements.requiredDistribution.get)
    } else {
      ValidateRequirements.validate(optimized)
    }
    if (requirementSatisfied) {
      optimized.transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      ensureRequirements.apply(optimized).transform {
        case SkewJoinChildWrapper(child) => child
      }
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


private[adaptive] object OptimizeSkewedJoin {

  /**
   * same as values.product, but make sure NO overflow:
   *    Iterator(10, 20, 30, 4, 10, 2, 1, 999, 88).product -> -751,912,960
   */
  def safeProduct(values: TraversableOnce[Int]): BigInt = values.foldLeft(BigInt(1))(_ * _)

  /**
   * Combine splits to make sure the total number of combinations no greater than given threshold.
   * This algorithm iteratively estimates an appropriate shrinkage factor for remaining combinable
   * stages (with more than 1 splits), and then perform split merge. Note that it tries to keep the
   * proportional relationship among input numbers of splits.
   */
  def combine(maxCombinations: Int, numSplits: Array[Int]): Array[Int] = {
    require(maxCombinations > 0)
    require(numSplits.nonEmpty && numSplits.forall(_ > 0))

    var numCombinations = safeProduct(numSplits)
    if (numCombinations <= maxCombinations) return numSplits

    val intNumSplits = numSplits.clone()
    val floatNumSplits = intNumSplits.map(_.toDouble)
    var numCombinables = intNumSplits.count(_ > 1)

    val maxShrinkage = 0.999
    val minShrinkage = 0.1
    val maxIterations = 1000 // 20 iterations should be enough in most cases, set 1000 for safety
    var iteration = 0
    while (numCombinations > maxCombinations && numCombinables > 0 && iteration < maxIterations) {
      var shrinkage = math.pow(
        (BigDecimal(maxCombinations) / BigDecimal(numCombinations)).doubleValue,
        1.0 / numCombinables
      )
      if (shrinkage.isNaN) {
        shrinkage = maxShrinkage
      } else {
        // clip shrinkage for numeric stability
        shrinkage = math.min(shrinkage, maxShrinkage)
        shrinkage = math.max(shrinkage, minShrinkage)
      }

      floatNumSplits.indices.foreach { i =>
        floatNumSplits(i) = math.max(1.0, floatNumSplits(i) * shrinkage)
      }

      Iterator.tabulate(floatNumSplits.length) { i =>
        val prevIntNumSplits = intNumSplits(i)
        val currIntNumSplits = floatNumSplits(i).round.toInt
        (i, prevIntNumSplits, currIntNumSplits)
      }.filter { case (_, prevIntNumSplits, currIntNumSplits) =>
        currIntNumSplits < prevIntNumSplits
      }.toArray.sortBy { case (i, prevIntNumSplits, currIntNumSplits) =>
        // first try small updates to numCombinations
        (1.0 - currIntNumSplits.toDouble / prevIntNumSplits, i)
      }.foreach { case (i, prevIntNumSplits, currIntNumSplits) =>
        if (numCombinations > maxCombinations) {
          intNumSplits(i) = currIntNumSplits
          numCombinations /= prevIntNumSplits
          numCombinations *= currIntNumSplits
        }
      }

      numCombinables = intNumSplits.count(_ > 1)
      iteration += 1
    }

    if (numCombinations <= maxCombinations) {
      intNumSplits
    } else {
      Array.fill(numSplits.length)(1)
    }
  }
}

// After optimizing skew joins, we need to run EnsureRequirements again to add necessary shuffles
// caused by skew join optimization. However, this shouldn't apply to the sub-plan under skew join,
// as it's guaranteed to satisfy distribution requirement.
case class SkewJoinChildWrapper(plan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
}
