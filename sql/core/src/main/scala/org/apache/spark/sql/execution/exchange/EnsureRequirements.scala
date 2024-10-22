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

import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

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
      parent: Option[SparkPlan],
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
        distribution match {
          case _: StatefulOpClusteredDistribution =>
            ShuffleExchangeExec(
              distribution.createPartitioning(numPartitions), child,
              REQUIRED_BY_STATEFUL_OPERATOR)

          case _ =>
            ShuffleExchangeExec(
              distribution.createPartitioning(numPartitions), child, shuffleOrigin)
        }
    }

    // Get the indexes of children which have specified distribution requirements and need to be
    // co-partitioned.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (_: ClusteredDistribution, _) => true
      case _ => false
    }.map(_._2)

    // Special case: if all sides of the join are single partition and it's physical size less than
    // or equal spark.sql.maxSinglePartitionBytes.
    val preferSinglePartition = childrenIndexes.forall { i =>
      children(i).outputPartitioning == SinglePartition &&
        children(i).logicalLink
          .forall(_.stats.sizeInBytes <= conf.getConf(SQLConf.MAX_SINGLE_PARTITION_BYTES))
    }

    // If there are more than one children, we'll need to check partitioning & distribution of them
    // and see if extra shuffles are necessary.
    if (childrenIndexes.length > 1 && !preferSinglePartition) {
      val specs = childrenIndexes.map(i => {
        val requiredDist = requiredChildDistributions(i)
        assert(requiredDist.isInstanceOf[ClusteredDistribution],
          s"Expected ClusteredDistribution but found ${requiredDist.getClass.getSimpleName}")
        i -> children(i).outputPartitioning.createShuffleSpec(
          requiredDist.asInstanceOf[ClusteredDistribution])
      }).toMap

      // Find out the shuffle spec that gives better parallelism. Currently this is done by
      // picking the spec with the largest number of partitions.
      //
      // NOTE: this is not optimal for the case when there are more than 2 children. Consider:
      //   (10, 10, 11)
      // where the number represent the number of partitions for each child, it's better to pick 10
      // here since we only need to shuffle one side - we'd need to shuffle two sides if we pick 11.
      //
      // However this should be sufficient for now since in Spark nodes with multiple children
      // always have exactly 2 children.

      // Whether we should consider `spark.sql.shuffle.partitions` and ensure enough parallelism
      // during shuffle. To achieve a good trade-off between parallelism and shuffle cost, we only
      // consider the minimum parallelism iff ALL children need to be re-shuffled.
      //
      // A child needs to be re-shuffled iff either one of below is true:
      //   1. It can't create partitioning by itself, i.e., `canCreatePartitioning` returns false
      //      (as for the case of `RangePartitioning`), therefore it needs to be re-shuffled
      //      according to other shuffle spec.
      //   2. It already has `ShuffleExchangeLike`, so we can re-use existing shuffle without
      //      introducing extra shuffle.
      //
      // On the other hand, in scenarios such as:
      //   HashPartitioning(5) <-> HashPartitioning(6)
      // while `spark.sql.shuffle.partitions` is 10, we'll only re-shuffle the left side and make it
      // HashPartitioning(6).
      val shouldConsiderMinParallelism = specs.forall(p =>
        !p._2.canCreatePartitioning || children(p._1).isInstanceOf[ShuffleExchangeLike]
      )
      // Choose all the specs that can be used to shuffle other children
      val candidateSpecs = specs
          .filter(_._2.canCreatePartitioning)
          .filter(p => !shouldConsiderMinParallelism ||
              children(p._1).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)
      val bestSpecOpt = if (candidateSpecs.isEmpty) {
        None
      } else {
        // When choosing specs, we should consider those children with no `ShuffleExchangeLike` node
        // first. For instance, if we have:
        //   A: (No_Exchange, 100) <---> B: (Exchange, 120)
        // it's better to pick A and change B to (Exchange, 100) instead of picking B and insert a
        // new shuffle for A.
        val candidateSpecsWithoutShuffle = candidateSpecs.filter { case (k, _) =>
          !children(k).isInstanceOf[ShuffleExchangeLike]
        }
        val finalCandidateSpecs = if (candidateSpecsWithoutShuffle.nonEmpty) {
          candidateSpecsWithoutShuffle
        } else {
          candidateSpecs
        }
        // Pick the spec with the best parallelism
        Some(finalCandidateSpecs.values.maxBy(_.numPartitions))
      }

      // Check if the following conditions are satisfied:
      //   1. There are exactly two children (e.g., join). Note that Spark doesn't support
      //      multi-way join at the moment, so this check should be sufficient.
      //   2. All children are of `KeyGroupedPartitioning`, and they are compatible with each other
      // If both are true, skip shuffle.
      val isKeyGroupCompatible = parent.isDefined &&
          children.length == 2 && childrenIndexes.length == 2 && {
        val left = children.head
        val right = children(1)
        val newChildren = checkKeyGroupCompatible(
          parent.get, left, right, requiredChildDistributions)
        if (newChildren.isDefined) {
          children = newChildren.get
        }
        newChildren.isDefined
      }

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx) if isKeyGroupCompatible || !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            bestSpecOpt match {
              // If keyGroupCompatible = false, we can still perform SPJ
              // by shuffling the other side based on join keys (see the else case below).
              // Hence we need to ensure that after this call, the outputPartitioning of the
              // partitioned side's BatchScanExec is grouped by join keys to match,
              // and we do that by pushing down the join keys
              case Some(KeyGroupedShuffleSpec(_, _, Some(joinKeyPositions))) =>
                populateJoinKeyPositions(child, Some(joinKeyPositions))
              case _ => child
            }
          } else {
            val newPartitioning = bestSpecOpt.map { bestSpec =>
              // Use the best spec to create a new partitioning to re-shuffle this child
              val clustering = dist.asInstanceOf[ClusteredDistribution].clustering
              bestSpec.createPartitioning(clustering)
            }.getOrElse {
              // No best spec available, so we create default partitioning from the required
              // distribution
              val numPartitions = dist.requiredNumPartitions
                  .getOrElse(conf.numShufflePartitions)
              dist.createPartitioning(numPartitions)
            }

            child match {
              case ShuffleExchangeExec(_, c, so, ps) =>
                ShuffleExchangeExec(newPartitioning, c, so, ps)
              case _ => ShuffleExchangeExec(newPartitioning, child)
            }
          }
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
      case (Some(KeyGroupedPartitioning(clustering, _, _, _)), _) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(KeyGroupedPartitioning(clustering, _, _, _))) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, rightKeys)
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

  /**
   * Checks whether two children, `left` and `right`, of a join operator have compatible
   * `KeyGroupedPartitioning`, and can benefit from storage-partitioned join.
   *
   * Returns the updated new children if the check is successful, otherwise `None`.
   */
  private def checkKeyGroupCompatible(
      parent: SparkPlan,
      left: SparkPlan,
      right: SparkPlan,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    parent match {
      case smj: SortMergeJoinExec =>
        checkKeyGroupCompatible(left, right, smj.joinType, requiredChildDistribution)
      case sj: ShuffledHashJoinExec =>
        checkKeyGroupCompatible(left, right, sj.joinType, requiredChildDistribution)
      case _ =>
        None
    }
  }

  private def checkKeyGroupCompatible(
      left: SparkPlan,
      right: SparkPlan,
      joinType: JoinType,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    assert(requiredChildDistribution.length == 2)

    var newLeft = left
    var newRight = right

    val specs = Seq(left, right).zip(requiredChildDistribution).map { case (p, d) =>
      if (!d.isInstanceOf[ClusteredDistribution]) return None
      val cd = d.asInstanceOf[ClusteredDistribution]
      val specOpt = createKeyGroupedShuffleSpec(p.outputPartitioning, cd)
      if (specOpt.isEmpty) return None
      specOpt.get
    }

    val leftSpec = specs.head
    val rightSpec = specs(1)

    var isCompatible = false
    if (!conf.v2BucketingPushPartValuesEnabled &&
        !conf.v2BucketingAllowJoinKeysSubsetOfPartitionKeys) {
      isCompatible = leftSpec.isCompatibleWith(rightSpec)
    } else {
      logInfo("Pushing common partition values for storage-partitioned join")
      isCompatible = leftSpec.areKeysCompatible(rightSpec)

      // Partition expressions are compatible. Regardless of whether partition values
      // match from both sides of children, we can calculate a superset of partition values and
      // push-down to respective data sources so they can adjust their output partitioning by
      // filling missing partition keys with empty partitions. As result, we can still avoid
      // shuffle.
      //
      // For instance, if two sides of a join have partition expressions
      // `day(a)` and `day(b)` respectively
      // (the join query could be `SELECT ... FROM t1 JOIN t2 on t1.a = t2.b`), but
      // with different partition values:
      //   `day(a)`: [0, 1]
      //   `day(b)`: [1, 2, 3]
      // Following the case 2 above, we don't have to shuffle both sides, but instead can
      // just push the common set of partition values: `[0, 1, 2, 3]` down to the two data
      // sources.
      if (isCompatible) {
        val leftPartValues = leftSpec.partitioning.partitionValues
        val rightPartValues = rightSpec.partitioning.partitionValues

        val numLeftPartValues = MDC(LogKeys.NUM_LEFT_PARTITION_VALUES, leftPartValues.size)
        val numRightPartValues = MDC(LogKeys.NUM_RIGHT_PARTITION_VALUES, rightPartValues.size)
        logInfo(
          log"""
              |Left side # of partitions: $numLeftPartValues
              |Right side # of partitions: $numRightPartValues
              |""".stripMargin)

        // As partition keys are compatible, we can pick either left or right as partition
        // expressions
        val partitionExprs = leftSpec.partitioning.expressions

        // in case of compatible but not identical partition expressions, we apply 'reduce'
        // transforms to group one side's partitions as well as the common partition values
        val leftReducers = leftSpec.reducers(rightSpec)
        val leftParts = reducePartValues(leftSpec.partitioning.partitionValues,
          partitionExprs,
          leftReducers)
        val rightReducers = rightSpec.reducers(leftSpec)
        val rightParts = reducePartValues(rightSpec.partitioning.partitionValues,
          partitionExprs,
          rightReducers)

        // merge values on both sides
        var mergedPartValues = mergePartitions(leftParts, rightParts, partitionExprs, joinType)
            .map(v => (v, 1))

        logInfo(log"After merging, there are " +
          log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartValues.size)} partitions")

        var replicateLeftSide = false
        var replicateRightSide = false
        var applyPartialClustering = false

        // This means we allow partitions that are not clustered on their values,
        // that is, multiple partitions with the same partition value. In the
        // following, we calculate how many partitions that each distinct partition
        // value has, and pushdown the information to scans, so they can adjust their
        // final input partitions respectively.
        if (conf.v2BucketingPartiallyClusteredDistributionEnabled) {
          logInfo("Calculating partially clustered distribution for " +
              "storage-partitioned join")

          // Similar to `OptimizeSkewedJoin`, we need to check join type and decide
          // whether partially clustered distribution can be applied. For instance, the
          // optimization cannot be applied to a left outer join, where the left hand
          // side is chosen as the side to replicate partitions according to stats.
          // Otherwise, query result could be incorrect.
          val canReplicateLeft = canReplicateLeftSide(joinType)
          val canReplicateRight = canReplicateRightSide(joinType)

          if (!canReplicateLeft && !canReplicateRight) {
            logInfo(log"Skipping partially clustered distribution as it cannot be applied for " +
              log"join type '${MDC(LogKeys.JOIN_TYPE, joinType)}'")
          } else {
            val leftLink = left.logicalLink
            val rightLink = right.logicalLink

            replicateLeftSide = if (
              leftLink.isDefined && rightLink.isDefined &&
                  leftLink.get.stats.sizeInBytes > 1 &&
                  rightLink.get.stats.sizeInBytes > 1) {
              val leftLinkStatsSizeInBytes = MDC(LogKeys.LEFT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES,
                leftLink.get.stats.sizeInBytes)
              val rightLinkStatsSizeInBytes = MDC(LogKeys.RIGHT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES,
                rightLink.get.stats.sizeInBytes)
              logInfo(
                log"""
                   |Using plan statistics to determine which side of join to fully
                   |cluster partition values:
                   |Left side size (in bytes): $leftLinkStatsSizeInBytes
                   |Right side size (in bytes): $rightLinkStatsSizeInBytes
                   |""".stripMargin)
              leftLink.get.stats.sizeInBytes < rightLink.get.stats.sizeInBytes
            } else {
              // As a simple heuristic, we pick the side with fewer number of partitions
              // to apply the grouping & replication of partitions
              logInfo("Using number of partitions to determine which side of join " +
                  "to fully cluster partition values")
              leftPartValues.size < rightPartValues.size
            }

            replicateRightSide = !replicateLeftSide

            // Similar to skewed join, we need to check the join type to see whether replication
            // of partitions can be applied. For instance, replication should not be allowed for
            // the left-hand side of a right outer join.
            if (replicateLeftSide && !canReplicateLeft) {
              logInfo(log"Left-hand side is picked but cannot be applied to join type " +
                log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
                log"distribution.")
              replicateLeftSide = false
            } else if (replicateRightSide && !canReplicateRight) {
              logInfo(log"Right-hand side is picked but cannot be applied to join type " +
                log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
                log"distribution.")
              replicateRightSide = false
            } else {
              // In partially clustered distribution, we should use un-grouped partition values
              val spec = if (replicateLeftSide) rightSpec else leftSpec
              val partValues = spec.partitioning.originalPartitionValues

              val numExpectedPartitions = partValues
                .map(InternalRowComparableWrapper(_, partitionExprs))
                .groupBy(identity)
                .transform((_, v) => v.size)

              mergedPartValues = mergedPartValues.map { case (partVal, numParts) =>
                (partVal, numExpectedPartitions.getOrElse(
                  InternalRowComparableWrapper(partVal, partitionExprs), numParts))
              }

              logInfo(log"After applying partially clustered distribution, there are " +
                log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartValues.map(_._2).sum)} partitions.")
              applyPartialClustering = true
            }
          }
        }

        // Now we need to push-down the common partition information to the scan in each child
        newLeft = populateCommonPartitionInfo(left, mergedPartValues, leftSpec.joinKeyPositions,
          leftReducers, applyPartialClustering, replicateLeftSide)
        newRight = populateCommonPartitionInfo(right, mergedPartValues, rightSpec.joinKeyPositions,
          rightReducers, applyPartialClustering, replicateRightSide)
      }
    }

    if (isCompatible) Some(Seq(newLeft, newRight)) else None
  }

  // Similar to `OptimizeSkewedJoin.canSplitRightSide`
  private def canReplicateLeftSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  // Similar to `OptimizeSkewedJoin.canSplitLeftSide`
  private def canReplicateRightSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
        joinType == LeftAnti || joinType == LeftOuter
  }

  // Populate the common partition information down to the scan nodes
  private def populateCommonPartitionInfo(
      plan: SparkPlan,
      values: Seq[(InternalRow, Int)],
      joinKeyPositions: Option[Seq[Int]],
      reducers: Option[Seq[Option[Reducer[_, _]]]],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean): SparkPlan = plan match {
    case scan: BatchScanExec =>
      scan.copy(
        spjParams = scan.spjParams.copy(
          commonPartitionValues = Some(values),
          joinKeyPositions = joinKeyPositions,
          reducers = reducers,
          applyPartialClustering = applyPartialClustering,
          replicatePartitions = replicatePartitions
        )
      )
    case node =>
      node.mapChildren(child => populateCommonPartitionInfo(
        child, values, joinKeyPositions, reducers, applyPartialClustering, replicatePartitions))
  }


  private def populateJoinKeyPositions(
      plan: SparkPlan,
      joinKeyPositions: Option[Seq[Int]]): SparkPlan = plan match {
    case scan: BatchScanExec =>
      scan.copy(
        spjParams = scan.spjParams.copy(
          joinKeyPositions = joinKeyPositions
        )
      )
    case node =>
      node.mapChildren(child => populateJoinKeyPositions(
        child, joinKeyPositions))
  }

  private def reducePartValues(
      partValues: Seq[InternalRow],
      expressions: Seq[Expression],
      reducers: Option[Seq[Option[Reducer[_, _]]]]) = {
    reducers match {
      case Some(reducers) => partValues.map { row =>
        KeyGroupedShuffleSpec.reducePartitionValue(row, expressions, reducers)
      }.distinct.map(_.row)
      case _ => partValues
    }
  }

  /**
   * Tries to create a [[KeyGroupedShuffleSpec]] from the input partitioning and distribution, if
   * the partitioning is a [[KeyGroupedPartitioning]] (either directly or indirectly), and
   * satisfies the given distribution.
   */
  private def createKeyGroupedShuffleSpec(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Option[KeyGroupedShuffleSpec] = {
    def tryCreate(partitioning: KeyGroupedPartitioning): Option[KeyGroupedShuffleSpec] = {
      val attributes = partitioning.expressions.flatMap(_.collectLeaves())
      val clustering = distribution.clustering

      val satisfies = if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
        attributes.length == clustering.length && attributes.zip(clustering).forall {
          case (l, r) => l.semanticEquals(r)
        }
      } else {
        partitioning.satisfies(distribution)
      }

      if (satisfies) {
        Some(partitioning.createShuffleSpec(distribution).asInstanceOf[KeyGroupedShuffleSpec])
      } else {
        None
      }
    }

    partitioning match {
      case p: KeyGroupedPartitioning => tryCreate(p)
      case PartitioningCollection(partitionings) =>
        val specs = partitionings.map(p => createKeyGroupedShuffleSpec(p, distribution))
        specs.filter(_.isDefined).map(_.get).headOption
      case _ => None
    }
  }

  /**
   * Merge and sort partitions values for SPJ and optionally enable partition filtering.
   * Both sides must have
   * matching partition expressions.
   * @param leftPartitioning left side partition values
   * @param rightPartitioning right side partition values
   * @param partitionExpression partition expressions
   * @param joinType join type for optional partition filtering
   * @return merged and sorted partition values
   */
  private def mergePartitions(
      leftPartitioning: Seq[InternalRow],
      rightPartitioning: Seq[InternalRow],
      partitionExpression: Seq[Expression],
      joinType: JoinType): Seq[InternalRow] = {

    val merged = if (SQLConf.get.getConf(SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED)) {
      joinType match {
        case Inner => InternalRowComparableWrapper.mergePartitions(
          leftPartitioning, rightPartitioning, partitionExpression, intersect = true)
        case LeftOuter => leftPartitioning.map(
          InternalRowComparableWrapper(_, partitionExpression))
        case RightOuter => rightPartitioning.map(
          InternalRowComparableWrapper(_, partitionExpression))
        case _ => InternalRowComparableWrapper.mergePartitions(leftPartitioning,
          rightPartitioning, partitionExpression)
      }
    } else {
      InternalRowComparableWrapper.mergePartitions(leftPartitioning, rightPartitioning,
        partitionExpression)
    }

    // SPARK-41471: We keep to order of partitions to make sure the order of
    // partitions is deterministic in different case.
    val partitionOrdering: Ordering[InternalRow] = {
      RowOrdering.createNaturalAscendingOrdering(partitionExpression.map(_.dataType))
    }
    merged.map(_.row).sorted(partitionOrdering)
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transformUp {
      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin, _)
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
          Some(reordered),
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
        None,
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
