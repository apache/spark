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

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, NamedExpression, RuntimeFilterExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, ScalarSubquery, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryExec, SubqueryWrapper}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExecProxy, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

/**
 * A rule to insert runtime filter in order to reuse exchange.
 */
case class PlanAdaptiveRuntimeFilterFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER)) {
      case RuntimeFilterExpression(SubqueryWrapper(
          SubqueryAdaptiveBroadcastExec(_, _, true, _, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId)) =>
        val filterCreationSidePlan = getFilterCreationSidePlan(adaptivePlan.executedPlan)

        var exchange = filterCreationSidePlan
        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          find(rootPlan) {
            case e: ShuffleExchangeExec =>
              val newPlan = adaptPlan(filterCreationSidePlan, e.child)
              if (newPlan.isDefined && e.child.sameResult(newPlan.get)) {
                exchange = ShuffleExchangeExec(e.outputPartitioning,
                  newPlan.get, e.shuffleOrigin, e.advisoryPartitionSize)
                true
              } else {
                false
              }
            case _ => false
          }.isDefined

        val bloomFilterSubquery = if (canReuseExchange) {
          exchange.setLogicalLink(filterCreationSidePlan.logicalLink.get)

          val newProject = ProjectExec(buildKeys.asInstanceOf[Seq[NamedExpression]], exchange)
          val broadcastProxy =
            BroadcastExchangeExecProxy(newProject, filterCreationSidePlan.output)

          val newExecutedPlan = adaptivePlan.executedPlan transformUp {
            case hashAggregateExec: ObjectHashAggregateExec
              if hashAggregateExec.child.eq(filterCreationSidePlan) =>
              hashAggregateExec.copy(child = broadcastProxy)
          }
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = newExecutedPlan)

          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              newAdaptivePlan), exprId)
        } else {
          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              adaptivePlan), exprId)
        }

        RuntimeFilterExpression(bloomFilterSubquery)
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        getFilterCreationSidePlan(objectHashAggregate.child)
      case shuffleExchange: ShuffleExchangeExec =>
        getFilterCreationSidePlan(shuffleExchange.child)
      case queryStageExec: ShuffleQueryStageExec =>
        getFilterCreationSidePlan(queryStageExec.plan)
      case other => other
    }
  }

  private def adaptPlan(current: SparkPlan, target: SparkPlan): Option[SparkPlan] = {
    (current, target) match {
      case (cp: ProjectExec, tp: ProjectExec) =>
        val newChild = adaptPlan(cp.child, tp.child)
        if (newChild.isDefined) {
          val cpSet = AttributeSet(cp.projectList.flatMap(_.references))
          val tpSet = AttributeSet(tp.projectList.flatMap(_.references))
          if (cpSet.subsetOf(tpSet)) {
            return Some(tp.withNewChildren(Seq(newChild.get)))
          }
        }
        None
      case (cj: BroadcastHashJoinExec, tj: BroadcastHashJoinExec)
        if cj.buildSide == tj.buildSide && cj.leftKeys.length == tj.leftKeys.length &&
          cj.rightKeys.length == tj.rightKeys.length =>

        val keysEquals = cj.leftKeys.zip(tj.leftKeys).forall { case (l, r) =>
          l.semanticEquals(r)
        } && cj.rightKeys.zip(tj.rightKeys).forall { case (l, r) =>
          l.semanticEquals(r)
        }
        if (keysEquals) {
          val newLeft = adaptPlan(cj.left, tj.left)
          val newRight = adaptPlan(cj.right, tj.right)
          if (newLeft.isDefined && newRight.isDefined) {
            return Some(cj.withNewChildren(Seq(newLeft.get, newRight.get)))
          }
        }

        None
      case (ProjectExec(_, cf: FilterExec), tf: FilterExec) =>
        adaptPlan(cf, tf)
      case (c, t) if c.canonicalized == t.canonicalized =>
        Some(t)
      case _ =>
        None
    }
  }
}
