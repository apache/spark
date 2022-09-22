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

package org.apache.spark.sql.execution

import scala.annotation.tailrec

import org.apache.spark.sql.{execution, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

/**
 * A rule to add re-usuable Exchange in bloom creation plan.
 * This is only used in Non-AQE execution flow.
 */
case class PlanRuntimeBloomFilters(sparkSession: SparkSession) extends Rule[SparkPlan] {

  @tailrec
  private def getFirstExchangeBelowSort(plan: SparkPlan): Option[ShuffleExchangeExec] = {
    plan match {
      case se: SortExec => getFirstExchangeBelowSort(se.child)
      case see: ShuffleExchangeExec => Some(see)
      case _ => None
    }
  }

  private def getTargetExchangePartitioning(
      rootPlan: SparkPlan,
      buildPlan: SparkPlan): Option[Partitioning] = {
    val nonOHA = buildPlan.find(!_.isInstanceOf[ObjectHashAggregateExec]).get
    var matchingSide: BuildSide = null
    val prunedJoin = rootPlan.find {
      case SortMergeJoinExec(_, _, _, _, left, _, _) if left.sameResult(nonOHA) =>
        matchingSide = BuildLeft
        true
      case SortMergeJoinExec(_, _, _, _, _, right, _) if right.sameResult(nonOHA) =>
        matchingSide = BuildRight
        true
      case _ => false
    }.get

    val ensurePlan = EnsureRequirements().apply(prunedJoin).asInstanceOf[SortMergeJoinExec]
    val targetExchange = matchingSide match {
      case BuildLeft => getFirstExchangeBelowSort(ensurePlan.left)
      case BuildRight => getFirstExchangeBelowSort(ensurePlan.right)
    }

    targetExchange.map(_.outputPartitioning)
  }

  private def getBloomChildPlan(plan: SparkPlan): SparkPlan = {
    plan.find(!_.isInstanceOf[ObjectHashAggregateExec]).get
  }

  private def planBloom(
      rootPlan: SparkPlan,
      scalarSubquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery)
      : Option[execution.ScalarSubquery] = {
    // ScalarSubquery
    //  SubqueryExec
    //    OHA
    //      Ex
    //        OHA
    val buildSparkPlan = QueryExecution.createSparkPlan(
      sparkSession,
      sparkSession.sessionState.planner,
      scalarSubquery.plan)
    val bloomChildPlan = getBloomChildPlan(buildSparkPlan)

    val canReuseExchange = conf.exchangeReuseEnabled && rootPlan.exists {
      case SortMergeJoinExec(_, _, _, _, left, right, _) =>
        left.sameResult(bloomChildPlan) || right.sameResult(bloomChildPlan)
      case _ => false
    } &&
      // As this rule runs before EnsureRequirements we need to check
      // weather the pruned Join will introduced exchange or not which can be reused by bloom
      getTargetExchangePartitioning(rootPlan, buildSparkPlan).isDefined

    if (canReuseExchange) {
      val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, buildSparkPlan)
      val distribution = getTargetExchangePartitioning(rootPlan, buildSparkPlan)

      // TODO:: Do this in unprepared plan as it can cause issue once codegen for OHA is introduced
      val localAgg = executedPlan
        .asInstanceOf[ObjectHashAggregateExec]
        .child
        .asInstanceOf[ShuffleExchangeExec]
        .child
        .asInstanceOf[ObjectHashAggregateExec]
      val newPlanWithExchange = executedPlan transformUp {
        case targetAgg: ObjectHashAggregateExec if targetAgg.eq(localAgg) =>
          targetAgg.copy(child = ShuffleExchangeExec(distribution.get, targetAgg.child))
      }

      Some(
        execution.ScalarSubquery(
          SubqueryExec.createForScalarSubquery(
            s"scalar-subquery#${scalarSubquery.exprId.id}",
            newPlanWithExchange),
          scalarSubquery.exprId))
    } else None
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.cboEnabled || !conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressions {
      case original @ BloomFilterMightContain(
            scalarSubquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery,
            _,
            fromBroadcast) =>
        planBloom(plan, scalarSubquery) match {
          case Some(newSubquery) => original.copy(bloomFilterExpression = newSubquery)
          case None if fromBroadcast => original
          case None => Literal.TrueLiteral
        }
    }
  }
}
