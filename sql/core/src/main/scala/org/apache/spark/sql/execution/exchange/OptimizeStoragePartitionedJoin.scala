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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.Utils

/**
 * This rule detects whether for a join plan, both children are of [[KeyGroupedPartitioning]],
 * and with required distribution [[ClusteredDistribution]] from the join. If the conditions are
 * met, it tries to apply Storage-Partitioned Join and remove shuffle if possible.
 *
 * Note: this must be run after [[ReorderJoinKeys]] and before [[EnsureRequirements]].
 */
object OptimizeStoragePartitionedJoin extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case operator: SparkPlan if isKeyGroupPartitioned(operator) =>
        val reordered = ReorderJoinKeys.reorderJoinPredicates(operator)
         reordered match {
          case smj @ SortMergeJoinExec(_, _, joinType, _, left, right, _, _) =>
            val newChildren = tryOptimize(left, right, joinType, smj.requiredChildDistribution)
            newChildren.map { case (newLeft, newRight) =>
              smj.copy(left = newLeft, right = newRight, isStoragePartitionedJoin = true)
            }.getOrElse(operator)
          case sj @ ShuffledHashJoinExec(_, _, joinType, _, _, left, right, _, _) =>
            val newChildren = tryOptimize(left, right, joinType, sj.requiredChildDistribution)
            newChildren.map { case (newLeft, newRight) =>
              sj.copy(left = newLeft, right = newRight, isStoragePartitionedJoin = true)
            }.getOrElse(sj)
          case op => op
        }
    }
  }

  private def tryOptimize(
      left: SparkPlan,
      right: SparkPlan,
      joinType: JoinType,
      requiredChildDistribution: Seq[Distribution]
  ): Option[(SparkPlan, SparkPlan)] = {
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
    if (!conf.v2BucketingPushPartValuesEnabled) {
      isCompatible = leftSpec.isCompatibleWith(rightSpec)
    } else {
      logInfo("Pushing common partition values for storage-partitioned join")
      isCompatible = leftSpec.areKeysCompatible(rightSpec)

      // Partition expressions are compatible. Regardless of whether partition values
      // match from both sides of children, we can we can calculate a superset of
      // partition values and push-down to respective data sources so they can adjust
      // their output partitioning by filling missing partition keys with empty
      // partitions. As result, we can still avoid shuffle.
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

        logInfo(
          s"""
             |Left side # of partitions: ${leftPartValues.size}
             |Right side # of partitions: ${rightPartValues.size}
             |""".stripMargin)

        var mergedPartValues = Utils.mergeOrdered(
          Seq(leftPartValues, rightPartValues))(leftSpec.ordering)
            .toSeq
            .distinct
            .map(v => (v, 1))

        logInfo(s"After merging, there are ${mergedPartValues.size} partitions")

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

          val canReplicateLeft = canReplicateLeftSide(joinType)
          val canReplicateRight = canReplicateRightSide(joinType)

          if (!canReplicateLeft && !canReplicateRight) {
            logInfo("Skipping partially clustered distribution as it cannot be applied for " +
                s"join type '$joinType'")
          } else {
            val leftLink = left.logicalLink
            val rightLink = right.logicalLink

            replicateLeftSide = if (
              leftLink.isDefined && rightLink.isDefined &&
                  leftLink.get.stats.sizeInBytes > 1 &&
                  rightLink.get.stats.sizeInBytes > 1) {
              logInfo(
                s"""
                   |Using plan statistics to determine which side of join to fully
                   |cluster partition values:
                   |Left side size (in bytes): ${leftLink.get.stats.sizeInBytes}
                   |Right side size (in bytes): ${rightLink.get.stats.sizeInBytes}
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
              logInfo("Left-hand side is picked but cannot be applied to join type " +
                  s"'$joinType'. Skipping partially clustered distribution.")
              replicateLeftSide = false
            } else if (replicateRightSide && !canReplicateRight) {
              logInfo("Right-hand side is picked but cannot be applied to join type " +
                  s"'$joinType'. Skipping partially clustered distribution.")
              replicateRightSide = false
            } else {
              val partValues = if (replicateLeftSide) rightPartValues else leftPartValues
              val numExpectedPartitions = partValues.groupBy(identity).mapValues(_.size)

              mergedPartValues = mergedPartValues.map { case (partVal, numParts) =>
                (partVal, numExpectedPartitions.getOrElse(partVal, numParts))
              }

              logInfo("After applying partially clustered distribution, there are " +
                  s"${mergedPartValues.map(_._2).sum} partitions.")
              applyPartialClustering = true
            }
          }
        }

        // Now we need to push-down the common partition key to the scan in each child
        newLeft = populatePartitionValues(
          left, mergedPartValues, applyPartialClustering, replicateLeftSide)
        newRight = populatePartitionValues(
          right, mergedPartValues, applyPartialClustering, replicateRightSide)
      }
    }

    if (isCompatible) Some((newLeft, newRight)) else None
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

  // Populate the common partition values down to the scan nodes
  private def populatePartitionValues(
      plan: SparkPlan,
      values: Seq[(InternalRow, Int)],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean): SparkPlan =
    plan match {
      case scan: BatchScanExec =>
        scan.copy(
          commonPartitionValues = Some(values),
          applyPartialClustering = applyPartialClustering,
          replicatePartitions = replicatePartitions
        )
      case node =>
        node.mapChildren(child => populatePartitionValues(
          child, values, applyPartialClustering, replicatePartitions))
    }

  private def isKeyGroupPartitioned(operator: SparkPlan): Boolean = {
    if (operator.children.isEmpty) {
      operator.outputPartitioning.isInstanceOf[KeyGroupedPartitioning]
    } else {
      operator.children.forall(isKeyGroupPartitioned)
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
    def check(partitioning: KeyGroupedPartitioning): Option[KeyGroupedShuffleSpec] = {
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
      case p: KeyGroupedPartitioning => check(p)
      case PartitioningCollection(partitionings) =>
        val specs = partitionings.map(p => createKeyGroupedShuffleSpec(p, distribution))
        specs.head
      case _ => None
    }
  }
}
