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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}

/**
 * Divide the spark plan into multiple QueryStages. For each Exchange in the plan, it wraps it with
 * a [[QueryStage]]. At the end it adds an [[AdaptiveSparkPlan]] at the top, which will drive the
 * execution of query stages.
 */
case class PlanQueryStage(session: SparkSession) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    var id = 0
    val exchangeToQueryStage = new java.util.IdentityHashMap[Exchange, QueryStage]
    val planWithStages = plan.transformUp {
      case e: ShuffleExchangeExec =>
        val queryStage = ShuffleQueryStage(id, e)
        id += 1
        exchangeToQueryStage.put(e, queryStage)
        queryStage
      case e: BroadcastExchangeExec =>
        val queryStage = BroadcastQueryStage(id, e)
        id += 1
        exchangeToQueryStage.put(e, queryStage)
        queryStage
      // The `ReusedExchangeExec` was added in the rule `ReuseExchange`, via transforming up the
      // query plan. This rule also transform up the query plan, so when we hit `ReusedExchangeExec`
      // here, the exchange being reused must already be hit before and there should be an entry
      // for it in `exchangeToQueryStage`.
      case e: ReusedExchangeExec =>
        val existingQueryStage = exchangeToQueryStage.get(e.child)
        assert(existingQueryStage != null, "The exchange being reused should be hit before.")
        ReusedQueryStage(existingQueryStage, e.output)
    }
    AdaptiveSparkPlan(ResultQueryStage(id, planWithStages), session)
  }
}
