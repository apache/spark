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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

object ExplainUtils {
  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generate the plan -> operator id map.
   *   2. Generate the plan -> codegen id map
   *   3. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier.
   *      2. Second part explans each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit,
      startOperatorID: Int,
      planToOperatorIDArray: ArrayBuffer[mutable.LinkedHashMap[TreeNode[_], Int]]): Int = {

    // ReusedSubqueryExecs are skipped over
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return startOperatorID
    }

    val planToOperationID = new mutable.LinkedHashMap[TreeNode[_], Int]()
    val operationIDToPlan = new mutable.LinkedHashMap[Int, TreeNode[_]]()
    val planToWholeStageID = new mutable.LinkedHashMap[TreeNode[_], Int]()
    var currentOperatorID = startOperatorID
    try {
      currentOperatorID = generateOperatorIDs(plan,
        currentOperatorID,
        planToOperationID,
        operationIDToPlan)
      generateWholeStageCodegenIdMap(plan, planToWholeStageID)

      QueryPlan.append(
        plan,
        append,
        verbose = false,
        addSuffix = false,
        planToOpID = planToOperationID)

      append("\n")
      var i: Integer = 0
      for ((opId, curPlan) <- operationIDToPlan) {
        append(curPlan.asInstanceOf[QueryPlan[_]].verboseString(planToOperationID,
          planToWholeStageID.get(curPlan)))
      }
    } catch {
      case e: AnalysisException => append(e.toString)
    }
    planToOperatorIDArray += planToOperationID
    currentOperatorID
  }

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans.
   *   2. Generates the explain output for each subquery referenced in the plan.
   */
  def processPlan[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit): Unit = {

    val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, SparkPlan)]
    val planToOperatorIDArray = ArrayBuffer.empty[mutable.LinkedHashMap[TreeNode[_], Int]]
    var currentOperatorID = 0
    currentOperatorID =
      processPlanSkippingSubqueries(plan, append, currentOperatorID, planToOperatorIDArray)
    getSubqueries(plan, subqueries)
    var i = 0

    // Process all the subqueries in the plan.
    for (sub <- subqueries) {
      if (i == 0) {
        append("\n===== Subqueries =====\n\n")
      }
      i = i + 1
      append(s"Subquery:$i Hosting operator id = " +
        s"${getOperatorId(planToOperatorIDArray, sub._1)} Hosting Expression = ${sub._2}\n")

      // For each subquery expression in the parent plan, process its child plan to compute
      // the explain output.
      currentOperatorID = processPlanSkippingSubqueries(
        sub._3,
        append,
        currentOperatorID,
        planToOperatorIDArray)
      append("\n")
    }
  }

  /**
   * Traverses the supplied input plan in a bottem-up fashion to produce the following two maps :
   *    1. operator -> operator identifier
   *    2. operator identifier -> operator
   * Note :
   *    1. Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't
   *       appear in the explain output.
   *    2. operator identifier starts at startIdx + 1
   */
  private def generateOperatorIDs(
      plan: TreeNode[_],
      startOperatorID: Int,
      planToOperationID: mutable.LinkedHashMap[TreeNode[_], Int],
      operationIDToPlan: mutable.LinkedHashMap[Int, TreeNode[_]]): Int = {

    var currentOperationID = startOperatorID
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }
    plan.foreachUp {
      case p: WholeStageCodegenExec =>
      case p: InputAdapter =>
      case other: TreeNode[_] =>
        if (!planToOperationID.get(other).isDefined) {
          currentOperationID += 1
          planToOperationID.put(other, currentOperationID)
          operationIDToPlan.put(currentOperationID, other)
        }
        other.innerChildren.foreach { plan =>
          currentOperationID = generateOperatorIDs(plan,
            currentOperationID,
            planToOperationID,
            operationIDToPlan)
        }
    }
    currentOperationID
  }
  /**
   * Traverses the supplied input plan in a top-down fashion to produce the following map:
   *    1. operator -> whole stage codegen id
   */
  private def generateWholeStageCodegenIdMap(
      plan: TreeNode[_],
      planToWholeStageID: mutable.LinkedHashMap[TreeNode[_], Int]): Unit = {
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return
    }
    var currentCodegenId = -1
    plan.foreach {
      case p: WholeStageCodegenExec => currentCodegenId = p.codegenStageId
      case p: InputAdapter => currentCodegenId = -1
      case other: TreeNode[_] =>
        if (currentCodegenId != -1) {
          planToWholeStageID.put(other, currentCodegenId)
        }
        other.innerChildren.foreach { plan =>
          generateWholeStageCodegenIdMap(plan, planToWholeStageID)
        }
    }
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *  1. Hosting opeator id.
   *  2. Hosting expression
   *  3. Subquery plan
   */
  private def getSubqueries[T <: QueryPlan[T]](
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, SparkPlan)]): Unit = {
    plan.foreach {
      case p: SparkPlan =>
        p.expressions.flatMap(_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s : BaseSubqueryExec =>
                subqueries += ((p, e, s.child))
                getSubqueries(e.plan, subqueries)
            }
          case other =>
        })
    }
  }

  /**
   * Returns the operator identifier for the supplied plan by performing a lookup
   * against an list of plan to operator identifier maps.
   */
  private def getOperatorId(
      planToIdMaps: ArrayBuffer[mutable.LinkedHashMap[TreeNode[_], Int]],
      planToLookup: QueryPlan[_]): String = {
    val plan = planToIdMaps.find(_.get(planToLookup).isDefined)
    if (plan.isDefined) {
      s"${plan.get(planToLookup)}"
    } else {
      "unknown"
    }
  }
}
