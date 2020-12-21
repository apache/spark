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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}

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
      numCoalescedBuckets: Int): SparkPlan = {
    plan transformUp {
      case f: FileSourceScanExec =>
        f.copy(optionalNumCoalescedBuckets = Some(numCoalescedBuckets))
    }
  }

  private def updateNumCoalescedBuckets(
      join: BaseJoinExec,
      numLeftBuckets: Int,
      numRightBucket: Int,
      numCoalescedBuckets: Int): BaseJoinExec = {
    if (numCoalescedBuckets != numLeftBuckets) {
      val leftCoalescedChild =
        updateNumCoalescedBucketsInScan(join.left, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(left = leftCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(left = leftCoalescedChild)
      }
    } else {
      val rightCoalescedChild =
        updateNumCoalescedBucketsInScan(join.right, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(right = rightCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(right = rightCoalescedChild)
      }
    }
  }

  private def isCoalesceSHJStreamSide(
      join: ShuffledHashJoinExec,
      numLeftBuckets: Int,
      numRightBucket: Int,
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
      case ExtractJoinWithBuckets(join, numLeftBuckets, numRightBuckets)
        if math.max(numLeftBuckets, numRightBuckets) / math.min(numLeftBuckets, numRightBuckets) <=
          conf.coalesceBucketsInJoinMaxBucketRatio =>
        val numCoalescedBuckets = math.min(numLeftBuckets, numRightBuckets)
        join match {
          case j: SortMergeJoinExec =>
            updateNumCoalescedBuckets(j, numLeftBuckets, numRightBuckets, numCoalescedBuckets)
          case j: ShuffledHashJoinExec
            // Only coalesce the buckets for shuffled hash join stream side,
            // to avoid OOM for build side.
            if isCoalesceSHJStreamSide(j, numLeftBuckets, numRightBuckets, numCoalescedBuckets) =>
            updateNumCoalescedBuckets(j, numLeftBuckets, numRightBuckets, numCoalescedBuckets)
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
  @tailrec
  private def hasScanOperation(plan: SparkPlan): Boolean = plan match {
    case f: FilterExec => hasScanOperation(f.child)
    case p: ProjectExec => hasScanOperation(p.child)
    case _: FileSourceScanExec => true
    case _ => false
  }

  private def getBucketSpec(plan: SparkPlan): Option[BucketSpec] = {
    plan.collectFirst {
      case f: FileSourceScanExec if f.relation.bucketSpec.nonEmpty &&
          f.optionalNumCoalescedBuckets.isEmpty =>
        f.relation.bucketSpec.get
    }
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
      case _ => false
    }
  }

  private def isApplicable(j: BaseJoinExec): Boolean = {
    (j.isInstanceOf[SortMergeJoinExec] ||
      j.isInstanceOf[ShuffledHashJoinExec]) &&
      hasScanOperation(j.left) &&
      hasScanOperation(j.right) &&
      satisfiesOutputPartitioning(j.leftKeys, j.left.outputPartitioning) &&
      satisfiesOutputPartitioning(j.rightKeys, j.right.outputPartitioning)
  }

  private def isDivisible(numBuckets1: Int, numBuckets2: Int): Boolean = {
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    numBuckets1 != numBuckets2 && large % small == 0
  }

  def unapply(plan: SparkPlan): Option[(BaseJoinExec, Int, Int)] = {
    plan match {
      case j: BaseJoinExec if isApplicable(j) =>
        val leftBucket = getBucketSpec(j.left)
        val rightBucket = getBucketSpec(j.right)
        if (leftBucket.isDefined && rightBucket.isDefined &&
            isDivisible(leftBucket.get.numBuckets, rightBucket.get.numBuckets)) {
          Some(j, leftBucket.get.numBuckets, rightBucket.get.numBuckets)
        } else {
          None
        }
      case _ => None
    }
  }
}
