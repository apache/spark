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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Wraps `LogicalRelation` to provide the number of buckets for coalescing.
 */
case class CoalesceBuckets(
    numCoalescedBuckets: Int,
    child: LogicalRelation) extends UnaryNode {
  require(numCoalescedBuckets > 0,
    s"Number of coalesced buckets ($numCoalescedBuckets) must be positive.")

  override def output: Seq[Attribute] = child.output
}

/**
 * This rule adds a `CoalesceBuckets` logical plan if the following conditions are met:
 *   - Two bucketed tables are joined.
 *   - Join is the equi-join.
 *   - The larger bucket number is divisible by the smaller bucket number.
 *   - "spark.sql.bucketing.coalesceBucketsInJoin.enabled" is set to true.
 *   - The difference in the number of buckets is less than the value set in
 *     "spark.sql.bucketing.coalesceBucketsInJoin.maxNumBucketsDiff".
 */
object CoalesceBucketsInEquiJoin extends Rule[LogicalPlan]  {
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
        case _ @ LogicalRelation(r: HadoopFsRelation, _, _, _) if r.bucketSpec.nonEmpty =>
          r.bucketSpec.get
      }
    } else {
      None
    }
  }

  private def mayCoalesce(numBuckets1: Int, numBuckets2: Int, conf: SQLConf): Option[Int] = {
    assert(numBuckets1 != numBuckets2)
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    if ((large % small == 0) && ((large - small) <= conf.coalesceBucketsInJoinMaxNumBucketsDiff)) {
      Some(small)
    } else {
      None
    }
  }

  private def addCoalesceBuckets(plan: LogicalPlan, numCoalescedBuckets: Int): LogicalPlan = {
    plan.transformUp {
      case l @ LogicalRelation(_: HadoopFsRelation, _, _, _) =>
        CoalesceBuckets(numCoalescedBuckets, l)
    }
  }

  object ExtractJoinWithBuckets {
    def unapply(plan: LogicalPlan): Option[(Join, Int, Int)] = {
      plan match {
        case join @ ExtractEquiJoinKeys(_, _, _, _, left, right, _) =>
          val leftBucket = getBucketSpec(left)
          val rightBucket = getBucketSpec(right)
          if (leftBucket.isDefined && rightBucket.isDefined) {
            Some(join, leftBucket.get.numBuckets, rightBucket.get.numBuckets)
          } else {
            None
          }
        case _ => None
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val sqlConf = SQLConf.get
    if (sqlConf.coalesceBucketsInJoinEnabled) {
      plan transform {
        case ExtractJoinWithBuckets(join, numLeftBuckets, numRightBuckets)
            if numLeftBuckets != numRightBuckets =>
          mayCoalesce(numLeftBuckets, numRightBuckets, sqlConf).map { numCoalescedBuckets =>
            if (numCoalescedBuckets != numLeftBuckets) {
              join.copy(left = addCoalesceBuckets(join.left, numCoalescedBuckets))
            } else {
              join.copy(right = addCoalesceBuckets(join.right, numCoalescedBuckets))
            }
          }.getOrElse(join)

        case other => other
      }
    } else {
      plan
    }
  }
}
