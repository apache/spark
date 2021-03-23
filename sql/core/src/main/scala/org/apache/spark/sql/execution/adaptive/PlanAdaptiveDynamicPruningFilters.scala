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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptiveDynamicPruningFilters(fullPlan: SparkPlan,
    stageCache: TrieMap[SparkPlan, QueryStageExec]) extends Rule[SparkPlan] {

  @transient lazy val allBroadcastHashJoins =
    fullPlan.collect { case b: BroadcastHashJoinExec => b }

  private def hasBroadcastHashJoinExec(currentPhysicalPlan: SparkPlan): Boolean = {
    val found = allBroadcastHashJoins.exists {
      case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
        left.sameResult(currentPhysicalPlan)
      case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
        right.sameResult(currentPhysicalPlan)
    }
    logDebug(s"PlanAdaptiveDynamicPruningFilters: hasBroadcastHashJoinExec => $found")
    found
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningExpression(InSubqueryExec(
          value, SubqueryAdaptiveBroadcastExec(name, index, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId, _)) =>
        val currentPhysicalPlan = adaptivePlan.executedPlan
        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), currentPhysicalPlan.output)
        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          hasBroadcastHashJoinExec(currentPhysicalPlan)

        logDebug(s"PlanAdaptiveDynamicPruningFilters: canReuseExchange => $canReuseExchange " +
          s"currentPhysicalPlan => ${currentPhysicalPlan}  " +
          s"plan => $plan")

        var dynamicPruningExpression = DynamicPruningExpression(Literal.TrueLiteral)
        if (canReuseExchange) {
          val mode = HashedRelationBroadcastMode(packedKeys)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, currentPhysicalPlan)
          val existingStage = stageCache.get(exchange.canonicalized)
          val broadcastQueryStage = if (existingStage.isDefined) {
            val reuseQueryStage = adaptivePlan.reuseQueryStage(existingStage.get, exchange)
            logDebug(s"PlanAdaptiveDynamicPruningFilters: reuseQueryStage => $reuseQueryStage")
            Option(reuseQueryStage)
          } else if (conf.dynamicPartitionPruningCreateBroadcastEnabled) {
            var newStage = adaptivePlan.newQueryStage(exchange)
            val queryStage = stageCache.getOrElseUpdate(exchange.canonicalized, newStage)
            if (queryStage.ne(newStage)) {
              logDebug(s"PlanAdaptiveDynamicPruningFilters: the $exchange already exists in " +
                s"the stageCache.")
              newStage = adaptivePlan.reuseQueryStage(queryStage, exchange)
            }
            logDebug(s"PlanAdaptiveDynamicPruningFilters: newStage => $newStage ")
            Option(newStage)
          } else {
            None
          }
          if (broadcastQueryStage.isDefined) {
            val broadcastValues = SubqueryBroadcastExec(
              name, index, buildKeys, broadcastQueryStage.get)
            dynamicPruningExpression =
              DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
          }
        } else {
          // TODO: we need to apply an aggregate on the buildPlan in order to be column
          // pruned.
          logInfo(s"DPP cannot reuse broadcast in AQE and the adaptivePlan is $adaptivePlan.")
        }
        dynamicPruningExpression
    }
  }
}
