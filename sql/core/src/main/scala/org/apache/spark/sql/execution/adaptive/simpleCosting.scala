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
 * A simple implementation of [[Cost]] produced by [[SimpleCostEvaluator]]. Its three components are
 * compared lexicographically in priority order:
 *   1. `numSkewJoins`: more skew joins means lower cost, so it is compared descending and first;
 *   2. `numShuffles`: fewer shuffles means lower cost;
 *   3. `numLocalSorts`: the lowest-priority tiebreaker, so among plans with the same number of skew
 *      joins and shuffles the one with fewer local sorts is preferred (e.g. a shuffled hash join
 *      over a sort merge join when the conversion does not push extra sorts elsewhere).
 *
 * `numSkewJoins` and `numLocalSorts` are `0` when the corresponding feature is disabled in the
 * evaluator, so they do not affect the comparison in that case.
 */
case class SimpleCost(numSkewJoins: Int, numShuffles: Int, numLocalSorts: Int) extends Cost {

  override def compare(that: Cost): Int = that match {
    case SimpleCost(thatSkewJoins, thatShuffles, thatLocalSorts) =>
      val bySkewJoins = Integer.compare(thatSkewJoins, numSkewJoins)
      if (bySkewJoins != 0) {
        bySkewJoins
      } else {
        val byShuffles = Integer.compare(numShuffles, thatShuffles)
        if (byShuffles != 0) byShuffles else Integer.compare(numLocalSorts, thatLocalSorts)
      }
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }
}

/**
 * A skew join aware implementation of [[CostEvaluator]], which counts the number of
 * [[ShuffleExchangeLike]] nodes, skew join nodes and (optionally) local [[SortExec]] nodes in the
 * plan. See [[SimpleCost]] for how the components are compared.
 */
case class SimpleCostEvaluator(forceOptimizeSkewedJoin: Boolean, countLocalSort: Boolean)
  extends CostEvaluator {

  override def evaluateCost(plan: SparkPlan): Cost = {
    var numSkewJoins = 0
    var numShuffles = 0
    var numLocalSorts = 0
    plan.foreach {
      case j: ShuffledJoin if forceOptimizeSkewedJoin && j.isSkewJoin => numSkewJoins += 1
      case j: BroadcastHashJoinExec if forceOptimizeSkewedJoin && j.isSkewJoin => numSkewJoins += 1
      case _: ShuffleExchangeLike => numShuffles += 1
      case s: SortExec if countLocalSort && !s.global => numLocalSorts += 1
      case _ =>
    }
    SimpleCost(numSkewJoins, numShuffles, numLocalSorts)
  }
}
