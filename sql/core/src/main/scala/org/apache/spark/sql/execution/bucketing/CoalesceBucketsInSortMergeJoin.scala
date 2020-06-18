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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule coalesces one side of the `SortMergeJoin` if the following conditions are met:
 *   - Two bucketed tables are joined.
 *   - Join keys match with output partition expressions on their respective sides.
 *   - The larger bucket number is divisible by the smaller bucket number.
 *   - COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED is set to true.
 *   - The ratio of the number of buckets is less than the value set in
 *     COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_MAX_BUCKET_RATIO.
 */
case class CoalesceBucketsInSortMergeJoin(conf: SQLConf) extends Rule[SparkPlan] {
  private def mayCoalesce(numBuckets1: Int, numBuckets2: Int, conf: SQLConf): Option[Int] = {
    assert(numBuckets1 != numBuckets2)
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    if (large % small == 0 &&
      large / small <= conf.getConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_MAX_BUCKET_RATIO)) {
      Some(small)
    } else {
      None
    }
  }

  private def updateNumCoalescedBuckets(plan: SparkPlan, numCoalescedBuckets: Int): SparkPlan = {
    plan.transformUp {
      case f: FileSourceScanExec =>
        f.copy(optionalNumCoalescedBuckets = Some(numCoalescedBuckets))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED)) {
      return plan
    }

    plan transform {
      case ExtractSortMergeJoinWithBuckets(smj, numLeftBuckets, numRightBuckets)
        if numLeftBuckets != numRightBuckets =>
        mayCoalesce(numLeftBuckets, numRightBuckets, conf).map { numCoalescedBuckets =>
          if (numCoalescedBuckets != numLeftBuckets) {
            smj.copy(left = updateNumCoalescedBuckets(smj.left, numCoalescedBuckets))
          } else {
            smj.copy(right = updateNumCoalescedBuckets(smj.right, numCoalescedBuckets))
          }
        }.getOrElse(smj)
      case other => other
    }
  }
}

/**
 * An extractor that extracts `SortMergeJoinExec` where both sides of the join have the bucketed
 * tables and are consisted of only the scan operation.
 */
object ExtractSortMergeJoinWithBuckets {
  private def isScanOperation(plan: SparkPlan): Boolean = plan match {
    case f: FilterExec => isScanOperation(f.child)
    case p: ProjectExec => isScanOperation(p.child)
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

  private def isApplicable(s: SortMergeJoinExec): Boolean = {
    isScanOperation(s.left) &&
      isScanOperation(s.right) &&
      satisfiesOutputPartitioning(s.leftKeys, s.left.outputPartitioning) &&
      satisfiesOutputPartitioning(s.rightKeys, s.right.outputPartitioning)
  }

  def unapply(plan: SparkPlan): Option[(SortMergeJoinExec, Int, Int)] = {
    plan match {
      case s: SortMergeJoinExec if isApplicable(s) =>
        val leftBucket = getBucketSpec(s.left)
        val rightBucket = getBucketSpec(s.right)
        if (leftBucket.isDefined && rightBucket.isDefined) {
          Some(s, leftBucket.get.numBuckets, rightBucket.get.numBuckets)
        } else {
          None
        }
      case _ => None
    }
  }
}
