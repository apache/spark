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
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule injects a hint if one side of two bucketed tables can be coalesced
 * when the two bucketed tables are inner-joined and they differ in the number of buckets.
 */
object CoalesceBucketInJoin extends Rule[LogicalPlan]  {
  val JOIN_HINT_COALESCED_NUM_BUCKETS: String = "JoinHintCoalescedNumBuckets"

  private val sqlConf = SQLConf.get

  private def isPlanEligible(plan: LogicalPlan): Boolean = {
    def forall(plan: LogicalPlan)(p: LogicalPlan => Boolean): Boolean = {
      p(plan) && plan.children.forall(forall(_)(p))
    }

    forall(plan) {
      case _: Filter | _: Project | _: LogicalRelation => true
      case _ => false
    }
  }

  private def getBucketSpec(plan: LogicalPlan): Option[BucketSpec] = {
    if (isPlanEligible(plan)) {
      plan.collectFirst {
        case _ @ LogicalRelation(r: HadoopFsRelation, _, _, _)
          if r.bucketSpec.nonEmpty && !r.options.contains(JOIN_HINT_COALESCED_NUM_BUCKETS) =>
          r.bucketSpec.get
      }
    } else {
      None
    }
  }

  private def mayCoalesce(numBuckets1: Int, numBuckets2: Int): Option[Int] = {
    assert(numBuckets1 != numBuckets2)
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    if ((large % small == 0) &&
      (large - small) <= sqlConf.getConf(SQLConf.COALESCE_BUCKET_IN_JOIN_MAX_NUM_BUCKETS_DIFF)) {
      Some(small)
    } else {
      None
    }
  }

  private def addBucketHint(plan: LogicalPlan, hint: (String, String)): LogicalPlan = {
    plan.transformUp {
      case l @ LogicalRelation(r: HadoopFsRelation, _, _, _) =>
        l.copy(relation = r.copy(options = r.options + hint)(r.sparkSession))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sqlConf.getConf(SQLConf.COALESCE_BUCKET_IN_JOIN_ENABLED)) {
      return plan
    }

    plan transform {
      case join: Join if join.joinType == Inner =>
        val leftBucket = getBucketSpec(join.left)
        val rightBucket = getBucketSpec(join.right)
        if (leftBucket.isEmpty || rightBucket.isEmpty) {
          return plan
        }

        val leftBucketNumber = leftBucket.get.numBuckets
        val rightBucketNumber = rightBucket.get.numBuckets
        if (leftBucketNumber == rightBucketNumber) {
          return plan
        }

        mayCoalesce(leftBucketNumber, rightBucketNumber).map { coalescedNumBuckets =>
          val hint = JOIN_HINT_COALESCED_NUM_BUCKETS -> coalescedNumBuckets.toString
          if (coalescedNumBuckets != leftBucketNumber) {
            join.copy(left = addBucketHint(join.left, hint))
          } else {
            join.copy(right = addBucketHint(join.right, hint))
          }
        }.getOrElse(join)

      case other => other
    }
  }
}
