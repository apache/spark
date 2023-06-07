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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.ShuffledJoin

/**
 * A simple implementation of [[Cost]], which takes a number of [[Long]] as the cost value.
 */
case class SimpleCost(value: Long) extends Cost {

  override def compare(that: Cost): Int = that match {
    case SimpleCost(thatValue) =>
      if (value < thatValue) -1 else if (value > thatValue) 1 else 0
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }
}

/**
 * A skew aware implementation of [[CostEvaluator]], which counts the number of
 * skew join nodes and skew [[AQEShuffleReadExec]] nodes in the plan.
 */
case class SimpleCostEvaluator(
    forceOptimizeSkewedJoin: Boolean,
    forceOptimizeSkewsInRebalancePartitions: Boolean)
    extends CostEvaluator {
  override def evaluateCost(plan: SparkPlan): Cost = {
    val numShuffles = plan.collect {
      case s: ShuffleExchangeLike => s
    }.size

    if (forceOptimizeSkewedJoin || forceOptimizeSkewsInRebalancePartitions) {
      val numSkewJoins = plan.collect {
        case j: ShuffledJoin if j.isSkewJoin => j
      }.size

      // When numSkewJoins > 0, it means that OptimizeSkewedJoin takes effect,
      // otherwise we also need to calculate the number of AQEShuffleReadExec
      // for OptimizeSkewedShuffleRead if needed.
      val numSkews = if (numSkewJoins > 0 && forceOptimizeSkewedJoin) {
        numSkewJoins
      } else if (numSkewJoins == 0 && forceOptimizeSkewsInRebalancePartitions) {
        plan.collect {
          case r: AQEShuffleReadExec if r.hasSkewedPartition => r
        }.size
      } else 0

      // We put `-numSkews` in the first 32 bits of the long value, so that it's compared first
      // when comparing the cost, and larger `numSkews` means lower cost.
      SimpleCost(-numSkews.toLong << 32 | numShuffles)
    } else {
      SimpleCost(numShuffles)
    }
  }
}
