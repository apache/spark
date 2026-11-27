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

import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, BindReferences, BroadcastValueProjection, DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
case class PlanDynamicPruningFilters(
    sparkSession: SparkSession,
    dppCache: DynamicPruningSubqueryCache) extends Rule[SparkPlan] {

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

    val planned = dppCache.planQuery(buildPlan) {
      QueryExecution.createSparkPlan(sparkSession.sessionState.planner, buildPlan.clone())
    }
    val keys = planned.bindKeys(buildPlan, buildKeys)
    val requiredMode = broadcastMode(keys, planned.output)
    val canReuseExchange = plan.exists {
      case join: BroadcastHashJoinExec =>
        val (candidateKeys, candidatePlan) = join.buildSide match {
          case BuildLeft => (join.leftKeys, join.left)
          case BuildRight => (join.rightKeys, join.right)
        }
        // Preserve existing direct reuse; only a projected domain requires the exact hash mode.
        candidatePlan.canonicalized == planned.comparisonPlan &&
          (projection.isEmpty ||
            (!join.isNullAwareAntiJoin &&
              broadcastMode(candidateKeys, candidatePlan.output) == requiredMode))
      case _ => false
    }

    if (canReuseExchange) {
      val values = dppCache.prepareBroadcast(
          buildPlan, buildKeys, indices, projection.map(_.valueExpression)) {
        val sparkPlan = planned.takeInput().getOrElse {
          QueryExecution.createSparkPlan(sparkSession.sessionState.planner, planned.query.clone())
        }
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan, dppCache)
        val exchange = BroadcastExchangeExec(broadcastMode(keys, executedPlan.output), executedPlan)
        projection match {
          case Some(valueProjection) =>
            val value = planned.bindKeys(buildPlan, Seq(valueProjection.valueExpression)).head
            ProjectedBroadcastValueSubqueryExec(name, value, exchange)
          case None =>
            SubqueryBroadcastExec(name, indices, keys, exchange)
        }
      }
      Some(values)
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
            pruning.usableBroadcastValueProjection.flatMap { projection =>
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
            val aggregate =
              DynamicPruningSubquery.planDistinctBuildKeys(buildPlan, buildKeys,
                broadcastKeyIndices)
            val values = dppCache.prepareIndependentQuery(aggregate, adaptive = false) {
              val sparkPlan = QueryExecution.prepareExecutedPlan(sparkSession, aggregate, dppCache)
              SubqueryExec(name, sparkPlan)
            }
            DynamicPruningExpression(InSubqueryExec(value, values, exprId))
        }
    }
  }
}

object PlanDynamicPruningFilters {
  def apply(sparkSession: SparkSession): PlanDynamicPruningFilters = {
    new PlanDynamicPruningFilters(sparkSession, new DynamicPruningSubqueryCache(sparkSession))
  }
}
