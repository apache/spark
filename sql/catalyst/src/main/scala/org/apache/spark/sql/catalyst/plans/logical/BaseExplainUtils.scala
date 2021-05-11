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

package org.apache.spark.sql.catalyst.plans.logical

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.internal.SQLConf

abstract class BaseExplainUtils[PlanType <: QueryPlan[PlanType]] {
  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Computes the operator id for current operator and records it in the operator
   *      by setting a tag.
   *   2. Computes the whole stage codegen id for current operator and records it in the
   *      operator by setting a tag.
   *   3. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier.
   *      2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan Input query plan to process
   * @param append function used to append the explain output
   * @param startOperatorID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   *
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
   *
   */
  protected def processPlanSkippingSubqueries(
      plan: => PlanType,
      append: String => Unit,
      startOperatorID: Int): Int = {

    val operationIDs = new mutable.ArrayBuffer[(Int, QueryPlan[_])]()
    var currentOperatorID = startOperatorID
    try {
      currentOperatorID = generateOperatorIDs(plan, currentOperatorID, operationIDs)

      plan.treeString(
        append,
        verbose = false,
        addSuffix = false,
        maxFields = SQLConf.get.maxToStringFields,
        printOperatorId = true)

      append("\n")
      for ((_, curPlan) <- operationIDs) {
        append(curPlan.verboseStringWithOperatorId())
      }
    } catch {
      case e: AnalysisException => append(e.toString)
    }
    currentOperatorID
  }

  protected def processSubquries(
    append: String => Unit,
    startOperatorID: Int,
    subqueries: Seq[(PlanType, Expression, PlanType)]): Unit = {
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
      // the explain output.
      currentOperatorID = processPlanSkippingSubqueries(sub._3, append, currentOperatorID)
      append("\n")
    }
  }

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans.
   *   2. Generates the explain output for each subquery referenced in the plan.
   */
  def processPlan(
      plan: => PlanType,
      append: String => Unit): Unit = {
    try {
      val subqueries = ArrayBuffer.empty[(PlanType, Expression, PlanType)]
      var currentOperatorID = 0
      currentOperatorID = processPlanSkippingSubqueries(plan, append, currentOperatorID)
      getSubqueries(plan, subqueries)
      processSubquries(append, currentOperatorID, subqueries)
    } finally {
      removeTags(plan)
    }
  }

  /**
   * Traverses the supplied input plan in a bottom-up fashion does the following :
   *    1. produces a map : operator identifier -> operator
   *    2. Records the operator id via setting a tag in the operator.
   * Note :
   *    1. Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't
   *       appear in the explain output.
   *    2. operator identifier starts at startOperatorID + 1
   * @param plan Input query plan to process
   * @param startOperatorID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   * @param operatorIDs A output parameter that contains a map of operator id and query plan. This
   *                    is used by caller to print the detail portion of the plan.
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
   */
  protected def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      operatorIDs: mutable.ArrayBuffer[(Int, QueryPlan[_])]): Int

  /**
   * Generate detailed field string with different format based on type of input value
   */
  def generateFieldString(fieldName: String, values: Any): String = values match {
    case iter: Iterable[_] if (iter.size == 0) => s"${fieldName}: []"
    case iter: Iterable[_] => s"${fieldName} [${iter.size}]: ${iter.mkString("[", ", ", "]")}"
    case str: String if (str == null || str.isEmpty) => s"${fieldName}: None"
    case str: String => s"${fieldName}: ${str}"
    case _ => throw new IllegalArgumentException(s"Unsupported type for argument values: $values")
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *  1. Hosting operator id.
   *  2. Hosting expression
   *  3. Subquery plan
   */
  protected def getSubqueries(
      plan: => PlanType,
      subqueries: ArrayBuffer[(PlanType, Expression, PlanType)]): Unit

  /**
   * Returns the operator identifier for the supplied plan by retrieving the
   * `operationId` tag value.
   */
  def getOpId(plan: PlanType): String = {
    plan.getTagValue(QueryPlan.OP_ID_TAG).map(v => s"$v").getOrElse("unknown")
  }

  protected def removeIdTags(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
    p.unsetTagValue(QueryPlan.OP_ID_TAG)
    p.unsetTagValue(QueryPlan.CODEGEN_ID_TAG)
    children.foreach(removeTags)
  }

  protected def removeTags(plan: QueryPlan[_]): Unit = {
    plan foreach {
      case p: QueryPlan[_] => removeIdTags(p, p.innerChildren)
    }
  }
}
