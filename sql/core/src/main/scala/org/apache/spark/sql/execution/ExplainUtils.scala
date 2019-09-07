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
   *   1. Computes the operator id for current operator and records it in the operaror
   *      by setting a tag.
   *   2. Computes the whole stage codegen id for current operator and records it in the
   *      operator by setting a tag.
   *   3. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier.
   *      2. Second part explans each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan Input query plan to process
   * @param append function used to append the explain output
   * @param startOperationID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   *
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
   *
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit,
      startOperatorID: Int): Int = {

    val operationIDs = new mutable.ArrayBuffer[(Int, QueryPlan[_])]()
    var currentOperatorID = startOperatorID
    try {
      currentOperatorID = generateOperatorIDs(plan, currentOperatorID, operationIDs)
      generateWholeStageCodegenIds(plan)

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
      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
      var currentOperatorID = 0
      currentOperatorID = processPlanSkippingSubqueries(plan, append, currentOperatorID)
      getSubqueries(plan, subqueries)
      var i = 0

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
            sub._3.child,
            append,
            currentOperatorID)
        }
        append("\n")
      }
    } finally {
      removeTags(plan)
    }
  }

  /**
   * Traverses the supplied input plan in a bottem-up fashion does the following :
   *    1. produces a map : operator identifier -> operator
   *    2. Records the operator id via setting a tag in the operator.
   * Note :
   *    1. Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't
   *       appear in the explain output.
   *    2. operator identifier starts at startOperatorID + 1
   * @param plan Input query plan to process
   * @param startOperationID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   * @param operatorIDs A output parameter that contains a map of operator id and query plan. This
   *                    is used by caller to print the detail portion of the plan.
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
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
        if (!other.getTagValue(QueryPlan.OP_ID_TAG).isDefined) {
          currentOperationID += 1
          other.setTagValue(QueryPlan.OP_ID_TAG, currentOperationID)
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
   * Traverses the supplied input plan in a top-down fashion and records the
   * whole stage code gen id in the plan via setting a tag.
   */
  private def generateWholeStageCodegenIds(plan: QueryPlan[_]): Unit = {
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
          other.setTagValue(QueryPlan.CODEGEN_ID_TAG, currentCodegenId)
        }
        other.innerChildren.foreach { plan =>
          generateWholeStageCodegenIds(plan)
        }
    }
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *  1. Hosting opeator id.
   *  2. Hosting expression
   *  3. Subquery plan
   */
  private def getSubqueries(
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]): Unit = {
    plan.foreach {
      case p: SparkPlan =>
        p.expressions.flatMap(_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                getSubqueries(s, subqueries)
            }
          case other =>
        })
    }
  }

  /**
   * Returns the operator identifier for the supplied plan by retrieving the
   * `operationId` tag value.`
   */
  def getOpId(plan: QueryPlan[_]): String = {
    plan.getTagValue(QueryPlan.OP_ID_TAG).map(v => s"$v").getOrElse("unknown")
  }

  /**
   * Returns the operator identifier for the supplied plan by retrieving the
   * `codegenId` tag value.`
   */
  def getCodegenId(plan: QueryPlan[_]): String = {
    plan.getTagValue(QueryPlan.CODEGEN_ID_TAG).map(v => s"[codegen id : $v]").getOrElse("")
  }

  def removeTags(plan: QueryPlan[_]): Unit = {
    plan foreach {
      case plan: QueryPlan[_] =>
        plan.unsetTagValue(QueryPlan.OP_ID_TAG)
        plan.unsetTagValue(QueryPlan.CODEGEN_ID_TAG)
        plan.innerChildren.foreach { p =>
          removeTags(p)
        }
    }
  }
}
