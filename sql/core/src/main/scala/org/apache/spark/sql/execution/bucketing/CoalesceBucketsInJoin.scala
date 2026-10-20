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

package org.apache.spark.sql.execution.bucketing

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}

/**
 * This rule coalesces one side of the `SortMergeJoin` and `ShuffledHashJoin`
 * if the following conditions are met:
 *   - Two bucketed tables are joined.
 *   - Join keys match with output partition expressions on their respective sides.
 *   - The larger bucket number is divisible by the smaller bucket number.
 *   - COALESCE_BUCKETS_IN_JOIN_ENABLED is set to true.
 *   - The ratio of the number of buckets is less than the value set in
 *     COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO.
 */
object CoalesceBucketsInJoin extends Rule[SparkPlan] {
  private def updateNumCoalescedBucketsInScan(
      plan: SparkPlan,
      targetScan: FileSourceScanExec,
      numCoalescedBuckets: Int): SparkPlan = {
    // Only the scan whose partitioning reaches this join. A broadcast build
    // scan in the same subtree keeps its own bucket count.
    plan transformUp {
      case f: FileSourceScanExec if f eq targetScan =>
        f.copy(optionalNumCoalescedBuckets = Some(numCoalescedBuckets))
    }
  }

  private def updateNumCoalescedBuckets(
      join: ShuffledJoin,
      leftScan: FileSourceScanExec,
      rightScan: FileSourceScanExec,
      numLeftBuckets: Int,
      numCoalescedBuckets: Int): ShuffledJoin = {
    if (numCoalescedBuckets != numLeftBuckets) {
      val leftCoalescedChild =
        updateNumCoalescedBucketsInScan(join.left, leftScan, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(left = leftCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(left = leftCoalescedChild)
      }
    } else {
      val rightCoalescedChild =
        updateNumCoalescedBucketsInScan(join.right, rightScan, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(right = rightCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(right = rightCoalescedChild)
      }
    }
  }

  private def isCoalesceSHJStreamSide(
      join: ShuffledHashJoinExec,
      numLeftBuckets: Int,
      numCoalescedBuckets: Int): Boolean = {
    if (numCoalescedBuckets == numLeftBuckets) {
      join.buildSide != BuildRight
    } else {
      join.buildSide != BuildLeft
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceBucketsInJoinEnabled) {
      return plan
    }

    plan transform {
      case ExtractJoinWithBuckets(join, leftScan, rightScan, numLeftBuckets, numRightBuckets)
        if math.max(numLeftBuckets, numRightBuckets) / math.min(numLeftBuckets, numRightBuckets) <=
          conf.coalesceBucketsInJoinMaxBucketRatio =>
        val numCoalescedBuckets = math.min(numLeftBuckets, numRightBuckets)
        join match {
          case j: SortMergeJoinExec =>
            updateNumCoalescedBuckets(j, leftScan, rightScan, numLeftBuckets, numCoalescedBuckets)
          case j: ShuffledHashJoinExec
            // Only coalesce the buckets for shuffled hash join stream side,
            // to avoid OOM for build side.
            if isCoalesceSHJStreamSide(j, numLeftBuckets, numCoalescedBuckets) =>
            updateNumCoalescedBuckets(j, leftScan, rightScan, numLeftBuckets, numCoalescedBuckets)
          case other => other
        }
      case other => other
    }
  }
}

/**
 * An extractor that extracts `SortMergeJoinExec` and `ShuffledHashJoin`,
 * where both sides of the join have the bucketed tables,
 * are consisted of only the scan operation,
 * and numbers of buckets are not equal but divisible.
 */
object ExtractJoinWithBuckets {
  private def streamedSide(
      buildSide: BuildSide, left: SparkPlan, right: SparkPlan): SparkPlan = {
    if (buildSide == BuildLeft) right else left
  }

  /**
   * Bucketed scan whose partitioning this side exposes. Broadcast joins
   * expose the streamed child, so a bucketed build scan is not the count
   * the parent sort-merge or shuffled-hash join coalesces against.
   */
  @tailrec
  private def findBucketedScan(plan: SparkPlan): Option[FileSourceScanExec] = {
    plan match {
      case f: FilterExec => findBucketedScan(f.child)
      case p: ProjectExec => findBucketedScan(p.child)
      case j: BroadcastHashJoinExec =>
        findBucketedScan(streamedSide(j.buildSide, j.left, j.right))
      case j: BroadcastNestedLoopJoinExec =>
        findBucketedScan(streamedSide(j.buildSide, j.left, j.right))
      case f: FileSourceScanExec if f.relation.bucketSpec.nonEmpty => Some(f)
      case _ => None
    }
  }

  private def getBucketSpec(scan: FileSourceScanExec): Option[BucketSpec] = {
    if (scan.optionalNumCoalescedBuckets.isEmpty) scan.relation.bucketSpec else None
  }

  /**
   * The join keys should match with expressions for output partitioning. Note that
   * the ordering does not matter because it will be handled in `EnsureRequirements`.
   */
  private def satisfiesOutputPartitioning(
      keys: Seq[Expression],
      partitioning: Partitioning): Boolean = {
    partitioning match {
      case HashPartitioning(exprs, _) if exprs.length == keys.length =>
        exprs.forall(e => keys.exists(_.semanticEquals(e)))
      case PartitioningCollection(partitionings) =>
        partitionings.exists(satisfiesOutputPartitioning(keys, _))
      case _ => false
    }
  }

  private def isDivisible(numBuckets1: Int, numBuckets2: Int): Boolean = {
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    numBuckets1 != numBuckets2 && large % small == 0
  }

  def unapply(
      plan: SparkPlan): Option[(ShuffledJoin, FileSourceScanExec, FileSourceScanExec, Int, Int)] = {
    plan match {
      case j: ShuffledJoin =>
        for {
          leftScan <- findBucketedScan(j.left)
          rightScan <- findBucketedScan(j.right)
          if satisfiesOutputPartitioning(j.leftKeys, j.left.outputPartitioning) &&
            satisfiesOutputPartitioning(j.rightKeys, j.right.outputPartitioning)
          leftBucket <- getBucketSpec(leftScan)
          rightBucket <- getBucketSpec(rightScan)
          if isDivisible(leftBucket.numBuckets, rightBucket.numBuckets)
        } yield (j, leftScan, rightScan, leftBucket.numBuckets, rightBucket.numBuckets)
      case _ => None
    }
  }
}
