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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSeq, BindReferences, BloomFilterMightContain, DynamicPruningExpression, DynamicPruningSubquery, Expression, ListQuery, Literal, XxHash64}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.execution.{InSubqueryExec, QueryExecution, ScalarSubquery => ScalarSubqueryExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
case class PlanDynamicPruningFilters(sparkSession: SparkSession)
  extends Rule[SparkPlan] with JoinSelectionHelper with DynamicPruningHelper  {

  /**
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], output: AttributeSeq): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), output)
    HashedRelationBroadcastMode(packedKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled && !conf.runtimeRowLevelOperationGroupFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      case DynamicPruningSubquery(value, buildPlan, buildKeys, index, onlyInBroadcast, exprId) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.exists {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(sparkPlan)
            case _ => false
          }

        if (canReuseExchange) {
          val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
          val mode = broadcastMode(buildKeys, executedPlan.output)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          val name = s"dynamicpruning#${exprId.id}"
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues = SubqueryBroadcastExec(name, index, buildKeys, exchange)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          // it is not worthwhile to execute the query, so we fall-back to a true literal
          DynamicPruningExpression(Literal.TrueLiteral)
        } else if (canBroadcastBySize(buildPlan, conf)) {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val alias = Alias(buildKeys(index), buildKeys(index).toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
          DynamicPruningExpression(expressions.InSubquery(
            Seq(value), ListQuery(aggregate, childOutputs = aggregate.output)))
        } else if (!conf.exchangeReuseEnabled) {
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          val reusedShuffleExchange = plan.collectFirst {
            case j: ShuffledJoin if j.left.sameResult(sparkPlan) || j.right.sameResult(sparkPlan) =>
              EnsureRequirements()(j).collectFirst {
                case s: ShuffleExchangeExec if s.child.sameResult(sparkPlan) => s
              }
          }.flatten

          val bfLogicalPlan = planBloomFilterLogicalPlan(buildPlan, buildKeys, index)
          val bfPhysicalPlan =
            planBloomFilterPhysicalPlan(bfLogicalPlan, reusedShuffleExchange).map { plan =>
              val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, plan)
              val scalarSubquery = ScalarSubqueryExec(SubqueryExec.createForScalarSubquery(
                s"scalar-subquery#${exprId.id}", executedPlan), exprId)
            BloomFilterMightContain(scalarSubquery, new XxHash64(value))
          }.getOrElse(Literal.TrueLiteral)
          DynamicPruningExpression(bfPhysicalPlan)
        }
    }
  }
}
