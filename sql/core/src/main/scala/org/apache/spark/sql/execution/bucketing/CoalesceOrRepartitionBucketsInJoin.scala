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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.BucketReadStrategyInJoin

/**
 * This rule coalesces or repartitions one side of the `SortMergeJoin` and `ShuffledHashJoin`
 * if the following conditions are met:
 *   - Two bucketed tables are joined.
 *   - Join keys match with output partition expressions on their respective sides.
 *   - The larger bucket number is divisible by the smaller bucket number.
 *   - The ratio of the number of buckets is less than the value set in
 *     COALESCE_OR_REPARTITION_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO.
 *
 * The bucketed table with a larger number of buckets is coalesced if BUCKET_READ_STRATEGY_IN_JOIN
 * is set to BucketReadStrategyInJoin.COALESCE, whereas the bucketed table with a smaller number of
 * buckets is repartitioned if BUCKET_READ_STRATEGY_IN_JOIN is set to
 * BucketReadStrategyInJoin.REPARTITION.
 */
case class CoalesceOrRepartitionBucketsInJoin(conf: SQLConf) extends Rule[SparkPlan] {
  private def updateNumBucketsInScan(
      plan: SparkPlan,
      newNumBuckets: Int): SparkPlan = {
    plan transformUp {
      case f: FileSourceScanExec =>
        f.copy(optionalNewNumBuckets = Some(newNumBuckets))
    }
  }

  private def updateNumBuckets(
      join: BaseJoinExec,
      numLeftBuckets: Int,
      numRightBucket: Int,
      newNumBuckets: Int): BaseJoinExec = {
    if (newNumBuckets != numLeftBuckets) {
      val leftChild = updateNumBucketsInScan(join.left, newNumBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(left = leftChild)
        case j: ShuffledHashJoinExec => j.copy(left = leftChild)
      }
    } else {
      val rightChild = updateNumBucketsInScan(join.right, newNumBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(right = rightChild)
        case j: ShuffledHashJoinExec => j.copy(right = rightChild)
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
    val bucketReadStrategy = conf.bucketReadStrategyInJoin
    if (bucketReadStrategy == BucketReadStrategyInJoin.OFF) {
      return plan
    }

    plan transform {
      case ExtractJoinWithBuckets(join, numLeftBuckets, numRightBuckets)
        if math.max(numLeftBuckets, numRightBuckets) / math.min(numLeftBuckets, numRightBuckets) <=
          conf.bucketReadStrategyInJoinMaxBucketRatio =>
        val newNumBuckets = if (bucketReadStrategy == BucketReadStrategyInJoin.COALESCE) {
          math.min(numLeftBuckets, numRightBuckets)
        } else {
          math.max(numLeftBuckets, numRightBuckets)
        }
        join match {
          case j: SortMergeJoinExec =>
            updateNumBuckets(j, numLeftBuckets, numRightBuckets, newNumBuckets)
          case j: ShuffledHashJoinExec =>
            bucketReadStrategy match {
              case BucketReadStrategyInJoin.REPARTITION =>
                updateNumBuckets(j, numLeftBuckets, numRightBuckets, newNumBuckets)
              case BucketReadStrategyInJoin.COALESCE
                if isCoalesceSHJStreamSide(j, numLeftBuckets, numRightBuckets, newNumBuckets) =>
                // Only coalesce the buckets for shuffled hash join stream side,
                // to avoid OOM for build side.
                updateNumBuckets(j, numLeftBuckets, numRightBuckets, newNumBuckets)
              case _ => j
            }
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
          f.optionalNewNumBuckets.isEmpty =>
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
    // A bucket can be coalesced or repartitioned only if the bigger number of buckets is divisible
    // by the smaller number of buckets because bucket id is calculated by modding the total number
    // of buckets.
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
