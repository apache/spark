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

import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, BroadcastValueProjection, DynamicPruningBroadcastValueMetadata, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptiveDynamicPruningFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  override def conf: SQLConf = rootPlan.context.session.sessionState.conf

  private def reusableBroadcast(
      name: String,
      indices: Seq[Int],
      buildKeys: Seq[Expression],
      valueExpression: Option[Expression],
      adaptivePlan: AdaptiveSparkPlanExec): Option[BaseSubqueryExec] = {
    if (!conf.exchangeReuseEnabled || buildKeys.isEmpty) {
      return None
    }

    val packedKeys = BindReferences.bindReferences(
      HashJoin.rewriteKeyExpr(buildKeys), adaptivePlan.executedPlan.output)
    val exchange = BroadcastExchangeExec(
      HashedRelationBroadcastMode(packedKeys, isNullAware = false),
      adaptivePlan.executedPlan)
    val canReuseExchange = find(rootPlan) {
      case join: BroadcastHashJoinExec if !join.isNullAwareAntiJoin =>
        val candidatePlan = join.buildSide match {
          case BuildLeft => join.left
          case BuildRight => join.right
        }
        candidatePlan.sameResult(exchange)
      case _ => false
    }.isDefined

    if (canReuseExchange) {
      adaptivePlan.executedPlan.logicalLink.foreach(exchange.setLogicalLink)
      val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)
      Some(valueExpression match {
        case Some(value) =>
          ProjectedBroadcastValueSubqueryExec(name, value, newAdaptivePlan)
        case None =>
          SubqueryBroadcastExec(name, indices, buildKeys, newAdaptivePlan)
      })
    } else {
      None
    }
  }

  private def projectedBroadcast(
      name: String,
      projection: BroadcastValueProjection,
      context: AdaptiveExecutionContext): Option[BaseSubqueryExec] = {
    QueryExecution.prepareExecutedPlan(projection.sourcePlan, context) match {
      case adaptive: AdaptiveSparkPlanExec =>
        reusableBroadcast(
          name,
          Seq(0),
          projection.sourceHashKeys,
          Some(projection.valueExpression),
          adaptive)
      case _ => None
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(DYNAMIC_PRUNING_EXPRESSION, IN_SUBQUERY_EXEC)) {
      case DynamicPruningExpression(InSubqueryExec(
          value, subquery @ SubqueryAdaptiveBroadcastExec(
            name, indices, onlyInBroadcast, buildPlan, buildKeys,
            adaptivePlan: AdaptiveSparkPlanExec), exprId, _, _, _)) =>
        val directBroadcast = reusableBroadcast(
          name, indices, buildKeys, None, adaptivePlan)
        val reusedBroadcast = directBroadcast.orElse {
          if (onlyInBroadcast) {
            DynamicPruningBroadcastValueMetadata.get(subquery)
              .flatMap(projectedBroadcast(name, _, adaptivePlan.context))
          } else {
            None
          }
        }

        reusedBroadcast match {
          case Some(broadcastValues) =>
            DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
          case None if onlyInBroadcast =>
            DynamicPruningExpression(Literal.TrueLiteral)
          case None =>
            val aliases = indices.map(idx =>
              Alias(buildKeys(idx), buildKeys(idx).toString)())
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
