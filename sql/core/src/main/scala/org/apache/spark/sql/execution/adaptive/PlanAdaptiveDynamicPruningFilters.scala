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

import scala.collection.concurrent.TrieMap

import org.apache.spark.sql.catalyst.expressions.{BindReferences, DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptiveDynamicPruningFilters(
    stageCache: TrieMap[SparkPlan, QueryStageExec]) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningExpression(InSubqueryExec(
          value, SubqueryAdaptiveBroadcastExec(name, index, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId, _)) =>
        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), adaptivePlan.executedPlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = BroadcastExchangeExec(mode, adaptivePlan.executedPlan)
        val existingStage = stageCache.get(exchange.canonicalized)
        if (existingStage.nonEmpty && conf.exchangeReuseEnabled) {
          val name = s"dynamicpruning#${exprId.id}"
          val reuseQueryStage = existingStage.get.newReuseInstance(0, exchange.output)
          val broadcastValues =
            SubqueryBroadcastExec(name, index, buildKeys, reuseQueryStage)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else {
          DynamicPruningExpression(Literal.TrueLiteral)
        }
    }
  }
}
