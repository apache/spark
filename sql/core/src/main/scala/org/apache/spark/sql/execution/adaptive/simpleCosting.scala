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
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledJoin}

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
 * A skew join aware implementation of [[CostEvaluator]], which counts the number of
 * [[ShuffleExchangeLike]] nodes, skew join nodes and (optionally) local [[SortExec]] nodes in the
 * plan.
 *
 * The cost is packed into a single [[Long]] so that the components are compared in priority order.
 * From the most significant bits to the least significant:
 *   - `-numSkewJoins` (only when `forceOptimizeSkewedJoin` is true), so that more skew joins means
 *     lower cost and is compared first;
 *   - `numShuffles`, so that fewer shuffles means lower cost;
 *   - `numLocalSorts` (only when `countLocalSort` is true), the lowest-priority tiebreaker, so that
 *     among plans with the same number of skew joins and shuffles the one with fewer local sorts is
 *     preferred (e.g. a shuffled hash join over a sort merge join when the conversion does not push
 *     extra sorts elsewhere).
 */
case class SimpleCostEvaluator(forceOptimizeSkewedJoin: Boolean, countLocalSort: Boolean)
  extends CostEvaluator {

  import SimpleCostEvaluator._

  override def evaluateCost(plan: SparkPlan): Cost = {
    var numShuffles = 0
    var numLocalSorts = 0
    plan.foreach {
      case _: ShuffleExchangeLike => numShuffles += 1
      case s: SortExec if !s.global => numLocalSorts += 1
      case _ =>
    }
    val sortCost = if (countLocalSort) numLocalSorts.toLong else 0L
    val shuffleAndSortCost = (numShuffles.toLong << SHUFFLE_SHIFT) | sortCost

    if (forceOptimizeSkewedJoin) {
      val numSkewJoins = plan.collect {
        case j: ShuffledJoin if j.isSkewJoin => j
        case j: BroadcastHashJoinExec if j.isSkewJoin => j
      }.size
      SimpleCost((-numSkewJoins.toLong << SKEW_SHIFT) | shuffleAndSortCost)
    } else {
      SimpleCost(shuffleAndSortCost)
    }
  }
}

object SimpleCostEvaluator {
  // Bit shifts that place each cost component in its own band of the packed Long: the local-sort
  // count occupies the low 32 bits, the number of shuffles the next band, and the skew-join term
  // above both. 32 bits is far more than the number of sorts or shuffles a single query plan can
  // contain.
  private val SHUFFLE_SHIFT = 32
  private val SKEW_SHIFT = 48
}
