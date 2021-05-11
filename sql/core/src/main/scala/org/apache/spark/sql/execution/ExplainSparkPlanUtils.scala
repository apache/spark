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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.BaseExplainUtils
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

object ExplainSparkPlanUtils extends BaseExplainUtils[SparkPlan] {

  override protected def processSubquries(
    append: String => Unit,
    startOperatorID: Int,
    subqueries: Seq[(SparkPlan, Expression, SparkPlan)]): Unit = {
    var i = 0
    var currentOperatorID = startOperatorID
    for (sub <- subqueries) {
      if (i == 0) {
        append("\n===== Subqueries =====\n\n")
      }
      i = i + 1
      append(s"Subquery:$i Hosting operator id = " +
        s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

      // For each subquery expression in the parent plan, process its child plan to compute
      // the explain output. In case of subquery reuse, we don't print subquery plan more
      // than once. So we skip [[ReusedSubqueryExec]] here.
      if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
        currentOperatorID = processPlanSkippingSubqueries(
          sub._3.children.head,
          append,
          currentOperatorID)
      }
      append("\n")
    }
  }

  override protected def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      operatorIDs: mutable.ArrayBuffer[(Int, QueryPlan[_])]): Int = {
    var currentOperationID = startOperatorID
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }
    plan.foreachUp {
      case p: WholeStageCodegenExec =>
      case p: InputAdapter =>
      case other: QueryPlan[_] =>

        def setOpId(): Unit = if (other.getTagValue(QueryPlan.OP_ID_TAG).isEmpty) {
          currentOperationID += 1
          other.setTagValue(QueryPlan.OP_ID_TAG, currentOperationID)
          operatorIDs += ((currentOperationID, other))
        }

        other match {
          case p: AdaptiveSparkPlanExec =>
            currentOperationID =
              generateOperatorIDs(p.executedPlan, currentOperationID, operatorIDs)
            setOpId()
          case p: QueryStageExec =>
            currentOperationID = generateOperatorIDs(p.plan, currentOperationID, operatorIDs)
            setOpId()
          case _ =>
            setOpId()
            other.innerChildren.foldLeft(currentOperationID) {
              (curId, plan) => generateOperatorIDs(plan, curId, operatorIDs)
            }
        }
    }

    generateWholeStageCodegenIds(plan)
    currentOperationID
  }

  /**
   * Traverses the supplied input plan in a top-down fashion and records the
   * whole stage code gen id in the plan via setting a tag.
   */
  private def generateWholeStageCodegenIds(plan: QueryPlan[_]): Unit = {
    var currentCodegenId = -1

    def setCodegenId(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      if (currentCodegenId != -1) {
        p.setTagValue(QueryPlan.CODEGEN_ID_TAG, currentCodegenId)
      }
      children.foreach(generateWholeStageCodegenIds)
    }

    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return
    }
    plan.foreach {
      case p: WholeStageCodegenExec => currentCodegenId = p.codegenStageId
      case _: InputAdapter => currentCodegenId = -1
      case p: AdaptiveSparkPlanExec => setCodegenId(p, Seq(p.executedPlan))
      case p: QueryStageExec => setCodegenId(p, Seq(p.plan))
      case other: QueryPlan[_] => setCodegenId(other, other.innerChildren)
    }
  }

  override def getSubqueries(
      plan: => SparkPlan,
      subqueries: ArrayBuffer[(SparkPlan, Expression, SparkPlan)]): Unit = {
    plan.foreach {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries)
      case p: SparkPlan =>
        p.expressions.foreach (_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                getSubqueries(s, subqueries)
              case _ =>
            }
        })
    }
  }

  override def removeTags(plan: QueryPlan[_]): Unit = {
    plan foreach {
      case p: AdaptiveSparkPlanExec => removeIdTags(p, Seq(p.executedPlan))
      case p: QueryStageExec => removeIdTags(p, Seq(p.plan))
      case plan: QueryPlan[_] => removeIdTags(plan, plan.innerChildren)
    }
  }
}
