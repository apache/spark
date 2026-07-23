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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSeq, BindReferences, BroadcastValueProjection, DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{BaseSubqueryExec, InSubqueryExec, ProjectedBroadcastValueSubqueryExec, QueryExecution, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
case class PlanDynamicPruningFilters(sparkSession: SparkSession) extends Rule[SparkPlan] {

  override def conf: SQLConf = sparkSession.sessionState.conf

  /**
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], output: AttributeSeq): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), output)
    HashedRelationBroadcastMode(packedKeys)
  }

  private def reusableBroadcast(
      plan: SparkPlan,
      name: String,
      indices: Seq[Int],
      buildPlan: LogicalPlan,
      buildKeys: Seq[Expression],
      projection: Option[BroadcastValueProjection]): Option[BaseSubqueryExec] = {
    if (!conf.exchangeReuseEnabled || buildKeys.isEmpty) {
      return None
    }

    val sparkPlan = QueryExecution.createSparkPlan(
      sparkSession.sessionState.planner, buildPlan)
    val requiredMode = broadcastMode(buildKeys, sparkPlan.output)
    val canReuseExchange = plan.exists {
      case join: BroadcastHashJoinExec =>
        val (candidateKeys, candidatePlan) = join.buildSide match {
          case BuildLeft => (join.leftKeys, join.left)
          case BuildRight => (join.rightKeys, join.right)
        }
        // Preserve existing direct reuse; only a projected domain requires the exact hash mode.
        candidatePlan.sameResult(sparkPlan) &&
          (projection.isEmpty ||
            (!join.isNullAwareAntiJoin &&
              broadcastMode(candidateKeys, candidatePlan.output) == requiredMode))
      case _ => false
    }

    if (canReuseExchange) {
      val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
      val exchange = BroadcastExchangeExec(
        broadcastMode(buildKeys, executedPlan.output), executedPlan)
      Some(projection match {
        case Some(valueProjection) =>
          ProjectedBroadcastValueSubqueryExec(
            name, valueProjection.valueExpression, exchange)
        case None =>
          SubqueryBroadcastExec(name, indices, buildKeys, exchange)
      })
    } else {
      None
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      case pruning @ DynamicPruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndices, onlyInBroadcast, exprId, _) =>
        val name = s"dynamicpruning#${exprId.id}"
        val directBroadcast = reusableBroadcast(
          plan, name, broadcastKeyIndices, buildPlan, buildKeys, None)
        val reusedBroadcast = directBroadcast.orElse {
          if (onlyInBroadcast) {
            pruning.broadcastValueProjection.flatMap { projection =>
              reusableBroadcast(
                plan,
                name,
                Seq(0),
                projection.sourcePlan,
                projection.sourceHashKeys,
                Some(projection))
            }
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
            val aliases = broadcastKeyIndices.map(idx =>
              Alias(buildKeys(idx), buildKeys(idx).toString)())
            val aggregate = Aggregate(aliases, aliases, buildPlan)
            val sparkPlan = QueryExecution.prepareExecutedPlan(sparkSession, aggregate)
            val values = SubqueryExec(name, sparkPlan)
            DynamicPruningExpression(InSubqueryExec(value, values, exprId))
        }
    }
  }
}
