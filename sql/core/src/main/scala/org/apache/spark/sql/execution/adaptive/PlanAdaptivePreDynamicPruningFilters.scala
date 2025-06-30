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

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptivePreDynamicPruningFilters(
    context: AdaptiveExecutionContext) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      case expressions.DynamicPruningSubquery(value, buildPlan,
      buildKeys, indices, onlyInBroadcast, exprId, _) =>
        val name = s"dynamicpruning#${exprId.id}"
        val sparkPlan = QueryExecution.prepareExecutedPlan(
          buildPlan, context)
        assert(sparkPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptivePlan: AdaptiveSparkPlanExec = sparkPlan.asInstanceOf[AdaptiveSparkPlanExec]

        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), adaptivePlan.executedPlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = BroadcastExchangeExec(mode, adaptivePlan.executedPlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.exists {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(exchange)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(exchange)
            case _ => false
          }

        if (canReuseExchange) {
          exchange.setLogicalLink(adaptivePlan.executedPlan.logicalLink.get)
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)

          val broadcastValues = SubqueryBroadcastExec(
            name, indices, buildKeys, newAdaptivePlan)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val aliases = indices.map(idx => Alias(buildKeys(idx), buildKeys(idx).toString)())
          val aggregate = Aggregate(aliases, aliases, buildPlan)

          val sparkPlan = QueryExecution.prepareExecutedPlan(aggregate, adaptivePlan.context)
          assert(sparkPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val newAdaptivePlan = sparkPlan.asInstanceOf[AdaptiveSparkPlanExec]
          val values = SubqueryExec(name, newAdaptivePlan)
          DynamicPruningExpression(InSubqueryExec(value, values, exprId))
        }
    }
  }
}
