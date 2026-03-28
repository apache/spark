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

import org.apache.spark.internal.{LogKeys}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
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
      case (child, distribution) =>
        // Split child's partitioning into categories
        val (other, grouped, nonGrouped) = splitKeyedPartitionings(child.outputPartitioning)

        // If non-KeyedPartitioning already satisfies, no changes needed
        if (other.exists(_.satisfies(distribution))) {
          child
        } else {
          // Check KeyedPartitioning satisfaction conditions
          val groupedSatisfies = grouped.find(_.satisfies(distribution))
          val nonGroupedSatisfiesAsIs = nonGrouped.exists(_.nonGroupedSatisfies(distribution))
          val nonGroupedSatisfiesWhenGrouped = nonGrouped.find(_.groupedSatisfies(distribution))

          // Check if any KeyedPartitioning satisfies the distribution
          if (groupedSatisfies.isDefined || nonGroupedSatisfiesAsIs
              || nonGroupedSatisfiesWhenGrouped.isDefined) {
            distribution match {
              case o: OrderedDistribution =>
                // OrderedDistribution requires grouped KeyedPartitioning with sorted keys
                // according to the distribution's ordering.
                // Find any KeyedPartitioning that satisfies via groupedSatisfies.
                val satisfyingKeyedPartitioning =
                  groupedSatisfies.orElse(nonGroupedSatisfiesWhenGrouped).get
                val attrs = satisfyingKeyedPartitioning.expressions.flatMap(_.collectLeaves())
                  .map(_.asInstanceOf[Attribute])
                val keyRowOrdering = RowOrdering.create(o.ordering, attrs)
                val keyOrdering = keyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)
                if (satisfyingKeyedPartitioning.partitionKeys.sliding(2).forall {
                  case Seq(k1, k2) => keyOrdering.lteq(k1, k2)
                }) {
                  child
                } else {
                  // Use distributePartitions to spread splits across expected partitions
                  val sortedGroupedKeys = satisfyingKeyedPartitioning.partitionKeys
                    .groupBy(identity).view.mapValues(_.size)
                    .toSeq.sortBy(_._1)(keyOrdering)
                  GroupPartitionsExec(child,
                    expectedPartitionKeys = Some(sortedGroupedKeys),
                    distributePartitions = true
                  )
                }

              case _ if groupedSatisfies.isDefined =>
                // Grouped KeyedPartitioning already satisfies
                child

              case _ if nonGroupedSatisfiesAsIs =>
                // Non-grouped KeyedPartitioning satisfies without grouping
                child

              case _ =>
                // Non-grouped KeyedPartitioning satisfies only after grouping
                GroupPartitionsExec(child)
            }
          } else {
            // No partitioning satisfies - need broadcast or shuffle
            val numPartitions = distribution.requiredNumPartitions
              .getOrElse(conf.numShufflePartitions)
            distribution match {
              case BroadcastDistribution(mode) =>
                BroadcastExchangeExec(mode, child)
              case _: StatefulOpClusteredDistribution =>
                ShuffleExchangeExec(
                  distribution.createPartitioning(numPartitions), child,
                  REQUIRED_BY_STATEFUL_OPERATOR)
              case _ =>
                ShuffleExchangeExec(
                  distribution.createPartitioning(numPartitions), child, shuffleOrigin)
            }
          }
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
      val candidateSpecs = specs.filter { case (index, spec) =>
        spec.canCreatePartitioning &&
          (!shouldConsiderMinParallelism ||
            children(index).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)
      }
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
      //   2. All children are of the compatible key group partitioning or
      //      compatible shuffle partition id pass through partitioning
      // If both are true, skip shuffle.
      val areChildrenCompatible = parent.isDefined &&
          children.length == 2 && childrenIndexes.length == 2 && {
        val left = children.head
        val right = children(1)

        // key group compatibility check
        val newChildren = checkKeyGroupCompatible(
          parent.get, left, right, requiredChildDistributions)
        if (newChildren.isDefined) {
          children = newChildren.get
          true
        } else {
          // If key group check fails, check ShufflePartitionIdPassThrough compatibility
          checkShufflePartitionIdPassThroughCompatible(
            left, right, requiredChildDistributions)
        }
      }

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx) if areChildrenCompatible ||
            !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            bestSpecOpt match {
              // If `areChildrenCompatible` is false, we can still perform SPJ
              // by shuffling the other side based on join keys (see the else case below).
              // Hence we need to ensure that after this call, the outputPartitioning of the
              // partitioned side's BatchScanExec is grouped by join keys to match,
              // and we do that by pushing down the join keys
              case Some(KeyedShuffleSpec(_, _, Some(joinKeyPositions))) =>
                withJoinKeyPositions(child, joinKeyPositions)
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
              case GroupPartitionsExec(c, _, _, _, _) => ShuffleExchangeExec(newPartitioning, c)
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
      case (Some(KeyedPartitioning(clustering, _, _)), _) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(KeyedPartitioning(clustering, _, _))) =>
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
   * `KeyedPartitioning`, and can benefit from storage-partitioned join.
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
      val specOpt = createKeyedShuffleSpec(p.outputPartitioning, cd)
      if (specOpt.isEmpty) return None
      specOpt.get
    }

    val leftSpec = specs.head
    val rightSpec = specs(1)
    val leftPartitioning = leftSpec.partitioning
    val rightPartitioning = rightSpec.partitioning

    // We don't need to alter the existing or add new `GroupPartitionsExec` when the child
    // partitionings are not modified (projected) in specs and left and right side partitionings are
    // compatible with each other.
    // Left and right `outputPartitioning` is a `PartitioningCollection` or a `KeyedPartitioning`
    // otherwise `createKeyedShuffleSpec()` would have returned `None`.
    var isCompatible =
      left.outputPartitioning.asInstanceOf[Expression].exists(_ == leftPartitioning) &&
      right.outputPartitioning.asInstanceOf[Expression].exists(_ == rightPartitioning) &&
      leftSpec.isCompatibleWith(rightSpec)
    if ((!isCompatible || conf.v2BucketingPartiallyClusteredDistributionEnabled) &&
        (conf.v2BucketingPushPartValuesEnabled ||
          conf.v2BucketingAllowJoinKeysSubsetOfPartitionKeys)) {
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
        val leftPartKeys = leftPartitioning.partitionKeys
        val rightPartKeys = rightPartitioning.partitionKeys

        val numLeftPartKeys = MDC(LogKeys.NUM_LEFT_PARTITION_VALUES, leftPartKeys.size)
        val numRightPartKeys = MDC(LogKeys.NUM_RIGHT_PARTITION_VALUES, rightPartKeys.size)
        logInfo(
          log"""
              |Left side # of partitions: $numLeftPartKeys
              |Right side # of partitions: $numRightPartKeys
              |""".stripMargin)

        // in case of compatible but not identical partition expressions, we apply 'reduce'
        // transforms to group one side's partitions as well as the common partition values
        val leftReducers = leftSpec.reducers(rightSpec)
        val rightReducers = rightSpec.reducers(leftSpec)
        val (leftReducedDataTypes, leftReducedKeys) = leftReducers.fold(
          (leftPartitioning.expressionDataTypes, leftPartitioning.partitionKeys)
        )(leftPartitioning.reduceKeys)
        val (rightReducedDataTypes, rightReducedKeys) = rightReducers.fold(
          (rightPartitioning.expressionDataTypes, rightPartitioning.partitionKeys)
        )(rightPartitioning.reduceKeys)
        val reducedDataTypes = if (leftReducedDataTypes == rightReducedDataTypes) {
          leftReducedDataTypes
        } else {
          throw QueryExecutionErrors.storagePartitionJoinIncompatibleReducedTypesError(
            leftReducers = leftReducers,
            leftReducedDataTypes = leftReducedDataTypes,
            rightReducers = rightReducers,
            rightReducedDataTypes = rightReducedDataTypes)
        }

        val reducedKeyRowOrdering = RowOrdering.createNaturalAscendingOrdering(reducedDataTypes)
        val reducedKeyOrdering =
          reducedKeyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)

        // merge values on both sides
        var mergedPartitionKeys =
          mergeAndDedupPartitions(leftReducedKeys, rightReducedKeys, joinType, reducedKeyOrdering)
            .map((_, 1))

        logInfo(log"After merging, there are " +
          log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.size)} partitions")

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
            val unwrappedLeft = unwrapGroupPartitions(left)
            val unwrappedRight = unwrapGroupPartitions(right)

            val leftLink = unwrappedLeft.logicalLink
            val rightLink = unwrappedRight.logicalLink

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
              leftPartKeys.size < rightPartKeys.size
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
              val (partiallyClusteredChild, partiallyClusteredSpec) = if (replicateLeftSide) {
                (unwrappedRight, rightSpec)
              } else {
                (unwrappedLeft, leftSpec)
              }
              // Original `KeyedPartitioning` can be obtained from the child directly if the child
              // satisfied the distribution requirement; or from the child's child if it didn't as
              // the child must be a `GroupPartitionsExec` inserted by `EnsureRequirement`
              // to satisfy the distribution requirement.
              val originalPartitioning =
                partiallyClusteredChild.outputPartitioning.asInstanceOf[Expression]
              // `outputPartitioning` is either a `PartitioningCollection` or a `KeyedPartitioning`
              // otherwise `createKeyedShuffleSpec()` would have returned `None`.
              val originalKeyedPartitioning =
                originalPartitioning.collectFirst { case k: KeyedPartitioning => k }.get
              val projectedOriginalPartitionKeys = partiallyClusteredSpec.joinKeyPositions
                .fold(originalKeyedPartitioning.partitionKeys)(
                  originalKeyedPartitioning.projectKeys(_)._2)

              val numExpectedPartitions =
                projectedOriginalPartitionKeys.groupBy(identity).view.mapValues(_.size)

              mergedPartitionKeys = mergedPartitionKeys.map { case (key, numParts) =>
                (key, numExpectedPartitions.getOrElse(key, numParts))
              }

              logInfo(log"After applying partially clustered distribution, there are " +
                log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.map(_._2).sum)} partitions.")
              applyPartialClustering = true
            }
          }
        }

        // Now we need to push-down the common partition information to the `GroupPartitionsExec`s.
        newLeft = applyGroupPartitions(left, leftSpec.joinKeyPositions, mergedPartitionKeys,
          leftReducers, distributePartitions = applyPartialClustering && !replicateLeftSide)
        newRight = applyGroupPartitions(right, rightSpec.joinKeyPositions, mergedPartitionKeys,
          rightReducers, distributePartitions = applyPartialClustering && !replicateRightSide)
      }
    }

    if (isCompatible) Some(Seq(newLeft, newRight)) else None
  }

  private def checkShufflePartitionIdPassThroughCompatible(
      left: SparkPlan,
      right: SparkPlan,
      requiredChildDistribution: Seq[Distribution]): Boolean = {
    (left.outputPartitioning, right.outputPartitioning) match {
      case (p1: ShufflePartitionIdPassThrough, p2: ShufflePartitionIdPassThrough) =>
        assert(requiredChildDistribution.length == 2)
        val leftSpec = p1.createShuffleSpec(
          requiredChildDistribution.head.asInstanceOf[ClusteredDistribution])
        val rightSpec = p2.createShuffleSpec(
          requiredChildDistribution(1).asInstanceOf[ClusteredDistribution])
        leftSpec.isCompatibleWith(rightSpec)
      case _ =>
        false
    }
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

  /**
   * Unwraps a GroupPartitionsExec to get the underlying child plan.
   */
  private def unwrapGroupPartitions(plan: SparkPlan): SparkPlan = plan match {
    case g: GroupPartitionsExec => g.child
    case other => other
  }

  /**
   * Applies or updates `GroupPartitionsExec` with the given parameters.
   *
   * `GroupPartitionsExec` can be either the given plan node (child of the join inserted by
   * `EnsureRequirement`) if the original child didn't satisfy the distribution requirement; or we
   * can create a new one specifically for this join.
   */
  private def applyGroupPartitions(
      plan: SparkPlan,
      joinKeyPositions: Option[Seq[Int]],
      mergedPartitionKeys: Seq[(InternalRowComparableWrapper, Int)],
      reducers: Option[Seq[Option[Reducer[_, _]]]],
      distributePartitions: Boolean): SparkPlan = {
    plan match {
      case g: GroupPartitionsExec =>
        val newGroupPartitions = g.copy(
          joinKeyPositions = joinKeyPositions,
          expectedPartitionKeys = Some(mergedPartitionKeys),
          reducers = reducers,
          distributePartitions = distributePartitions)
        newGroupPartitions.copyTagsFrom(g)
        newGroupPartitions
      case _ =>
        GroupPartitionsExec(plan, joinKeyPositions, Some(mergedPartitionKeys), reducers,
          distributePartitions)
    }
  }

  /**
   * Applies join key positions to a plan by wrapping or updating GroupPartitionsExec.
   */
  private def withJoinKeyPositions(plan: SparkPlan, positions: Seq[Int]): SparkPlan = {
    plan match {
      case g: GroupPartitionsExec =>
        val newGroupPartitions = g.copy(joinKeyPositions = Some(positions))
        newGroupPartitions.copyTagsFrom(g)
        newGroupPartitions
      case _ => GroupPartitionsExec(plan, joinKeyPositions = Some(positions))
    }
  }

  /**
   * Tries to create a [[KeyedShuffleSpec]] from the input partitioning and distribution, if the
   * partitioning is a [[KeyedPartitioning]] (either directly or indirectly), and satisfies the
   * given distribution.
   */
  private def createKeyedShuffleSpec(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Option[KeyedShuffleSpec] = {
    def tryCreate(partitioning: KeyedPartitioning): Option[KeyedShuffleSpec] = {
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
        Some(partitioning.createShuffleSpec(distribution).asInstanceOf[KeyedShuffleSpec])
      } else {
        None
      }
    }

    partitioning match {
      case p: KeyedPartitioning => tryCreate(p)
      case PartitioningCollection(partitionings) =>
        partitionings.collectFirst(Function.unlift(createKeyedShuffleSpec(_, distribution)))
      case _ => None
    }
  }

  /**
   * Merge, dedup and sort partitions keys for SPJ and optionally enable partition filtering.
   * Both sides must have matching partition expressions.
   * @param leftPartitionKeys left side partition keys
   * @param rightPartitionKeys right side partition keys
   * @param joinType join type for optional partition filtering
   * @param keyOrdering ordering to sort partition keys
   * @return merged and sorted partition values
   */
  def mergeAndDedupPartitions(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      joinType: JoinType,
      keyOrdering: Ordering[InternalRowComparableWrapper]): Seq[InternalRowComparableWrapper] = {
    val merged = if (SQLConf.get.getConf(SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED)) {
      joinType match {
        case Inner =>
          mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys, intersect = true)
        case LeftOuter => leftPartitionKeys.distinct
        case RightOuter => rightPartitionKeys.distinct
        case _ => mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
      }
    } else {
      mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
    }

    // SPARK-41471: We keep to order of partitions to make sure the order of
    // partitions is deterministic in different case.
    merged.sorted(keyOrdering)
  }

  private def mergeAndDedupPartitionKeys(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      intersect: Boolean = false) = {
    val leftKeySet = mutable.HashSet.from(leftPartitionKeys)
    val rightKeySet = mutable.HashSet.from(rightPartitionKeys)
    val result = if (intersect) {
      leftKeySet.intersect(rightKeySet)
    } else {
      leftKeySet.union(rightKeySet)
    }
    result.toSeq
  }

  /**
   * Splits a partitioning into three categories:
   * 1. Non-KeyedPartitioning (HashPartitioning, RangePartitioning, etc.)
   * 2. Grouped KeyedPartitioning (isGrouped = true)
   * 3. Non-grouped KeyedPartitioning (isGrouped = false)
   *
   * @param partitioning The partitioning to split
   * @return A tuple of (other, grouped, nonGrouped) where:
   *         - other: Option containing non-KeyedPartitioning(s)
   *         - grouped: Seq of grouped KeyedPartitionings
   *         - nonGrouped: Seq of non-grouped KeyedPartitionings
   */
  private def splitKeyedPartitionings(partitioning: Partitioning) = {
    val otherPartitionings = ArrayBuffer.empty[Partitioning]
    val groupedKeyedPartitionings = ArrayBuffer.empty[KeyedPartitioning]
    val nonGroupedKeyedPartitionings = ArrayBuffer.empty[KeyedPartitioning]

    def split(p: Partitioning): Unit = p match {
      case c: PartitioningCollection => c.partitionings.foreach(split)
      case k: KeyedPartitioning =>
        if (k.isGrouped) {
          groupedKeyedPartitionings += k
        } else {
          nonGroupedKeyedPartitionings += k
        }
      case o => otherPartitionings += o
    }

    split(partitioning)

    val other = otherPartitionings.length match {
      case 0 => None
      case 1 => Some(otherPartitionings.head)
      case _ => Some(PartitioningCollection(otherPartitionings.toSeq))
    }

    (other, groupedKeyedPartitionings.toSeq, nonGroupedKeyedPartitionings.toSeq)
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
