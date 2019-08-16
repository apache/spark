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
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

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
      startOperatorID: Int): Int = {

    // ReusedSubqueryExecs are skipped over
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return startOperatorID
    }

    val operationIDs = new mutable.ArrayBuffer[(Int, QueryPlan[_])]()
    var currentOperatorID = startOperatorID
    try {
      currentOperatorID = generateOperatorIDs(plan, currentOperatorID, operationIDs)
      generateWholeStageCodegenIdMap(plan)

      QueryPlan.append(
        plan,
        append,
        verbose = false,
        addSuffix = false,
        printOperatorId = true)

      append("\n")
      var i: Integer = 0
      for ((opId, curPlan) <- operationIDs) {
        append(curPlan.verboseStringWithOperatorId())
      }
    } catch {
      case e: AnalysisException => append(e.toString)
    }
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
    try {
      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, SparkPlan)]
      var currentOperatorID = 0
      currentOperatorID = processPlanSkippingSubqueries(plan, append, currentOperatorID)
      getSubqueries(plan, subqueries)
      var i = 0

      // Process all the subqueries in the plan.
      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(s"Subquery:$i Hosting operator id = " +
          s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

        // For each subquery expression in the parent plan, process its child plan to compute
        // the explain output.
        currentOperatorID = processPlanSkippingSubqueries(
          sub._3,
          append,
          currentOperatorID)
        append("\n")
      }
    } finally {
      removeTags(plan)
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
        if (!other.getTagValue(QueryPlan.opidTag).isDefined) {
          currentOperationID += 1
          other.setTagValue(QueryPlan.opidTag, currentOperationID)
          operatorIDs += ((currentOperationID, other))
        }
        other.innerChildren.foreach { plan =>
          currentOperationID = generateOperatorIDs(plan,
            currentOperationID,
            operatorIDs)
        }
    }
    currentOperationID
  }
  /**
   * Traverses the supplied input plan in a top-down fashion to produce the following map:
   *    1. operator -> whole stage codegen id
   */
  private def generateWholeStageCodegenIdMap(plan: QueryPlan[_]): Unit = {
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return
    }
    var currentCodegenId = -1
    plan.foreach {
      case p: WholeStageCodegenExec => currentCodegenId = p.codegenStageId
      case p: InputAdapter => currentCodegenId = -1
      case other: QueryPlan[_] =>
        if (currentCodegenId != -1) {
          other.setTagValue(QueryPlan.codegenTag, currentCodegenId)
        }
        other.innerChildren.foreach { plan =>
          generateWholeStageCodegenIdMap(plan)
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
  def getOpId(plan: QueryPlan[_]): String = {
    plan.getTagValue(QueryPlan.opidTag).map(v => s"$v").getOrElse("unknown")
  }

  def getCodegenId(plan: QueryPlan[_]): String = {
    plan.getTagValue(QueryPlan.codegenTag).map(v => s"[codegen id : $v]").getOrElse("")
  }

  def removeTags(plan: QueryPlan[_]): Unit = {
    plan foreach {
      case plan: QueryPlan[_] =>
        plan.removeTag(QueryPlan.opidTag)
        plan.removeTag(QueryPlan.codegenTag)
        plan.innerChildren.foreach { p =>
          removeTags(p)
        }
    }
  }
}
