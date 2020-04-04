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

/**
 * This rule injects the bucket related hints and supports the following scenarios:
 * - If the two bucketed tables are inner-joined and they differ in the number of buckets,
 *   the number of buckets for the other table of join is injected as a hint.
 */
object InjectBucketHint extends Rule[LogicalPlan]  {
  val JOIN_HINT_NUM_BUCKETS: String = "JoinHintNumBuckets"

  private def isPlanEligible(plan: LogicalPlan): Boolean = {
    plan.forall {
      case _: Filter | _: Project | _: LogicalRelation => true
      case _ => false
    }
  }

  private def getBucketSpec(plan: LogicalPlan): Option[BucketSpec] = {
    if (isPlanEligible(plan)) {
      plan.collectFirst {
        case _ @ LogicalRelation(r: HadoopFsRelation, _, _, _)
          if r.bucketSpec.nonEmpty && !r.options.contains(JOIN_HINT_NUM_BUCKETS) =>
          r.bucketSpec.get
      }
    } else {
      None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case join : Join if join.joinType == Inner =>
        val leftBucket = getBucketSpec(join.left)
        val rightBucket = getBucketSpec(join.right)
        if (leftBucket.isEmpty ||
          rightBucket.isEmpty ||
          leftBucket.get.numBuckets == rightBucket.get.numBuckets) {
          return plan
        }

        def addBucketHint(subPlan: LogicalPlan, numBuckets: Int): LogicalPlan = {
          subPlan.transformUp {
            case l @ LogicalRelation(r: HadoopFsRelation, _, _, _) =>
              val newOption = JOIN_HINT_NUM_BUCKETS -> numBuckets.toString
              l.copy(relation = r.copy(options = r.options + newOption)(r.sparkSession))
          }
        }

        join.copy(
          left = addBucketHint(join.left, rightBucket.get.numBuckets),
          right = addBucketHint(join.right, leftBucket.get.numBuckets))
      case other => other
    }
  }
}
