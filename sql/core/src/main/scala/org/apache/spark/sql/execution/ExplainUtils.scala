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

import java.util.IdentityHashMap

import scala.collection.mutable.{ArrayBuffer, BitSet}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

object ExplainUtils extends AdaptiveSparkPlanHelper {
  def localIdMap: ThreadLocal[java.util.Map[QueryPlan[_], Int]] = QueryPlan.localIdMap

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Computes the whole stage codegen id for current operator and records it in the
   *      operator by setting a tag.
   *   2. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier.
   *      2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan Input query plan to process
   * @param append function used to append the explain output
   * @param collectedOperators The IDs of the operators that are already collected and we shouldn't
   *                           collect again.
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectedOperators: BitSet): Unit = {
    try {
      generateWholeStageCodegenIds(plan)

      QueryPlan.append(
        plan,
        append,
        verbose = false,
        addSuffix = false,
        printOperatorId = true)

      append("\n")

      val operationsWithID = ArrayBuffer.empty[QueryPlan[_]]
      collectOperatorsWithID(plan, operationsWithID, collectedOperators)
      operationsWithID.foreach(p => append(p.verboseStringWithOperatorId()))

    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans.
   *   2. Generates the explain output for each subquery referenced in the plan.
   *
   * Note that, ideally this is a no-op as different explain actions operate on different plan,
   * instances but cached plan is an exception. The `InMemoryRelation#innerChildren` use a shared
   * plan instance across multi-queries. Add lock for this method to avoid tag race condition.
   */
  def processPlan[T <: QueryPlan[T]](plan: T, append: String => Unit): Unit = {
    val prevIdMap = localIdMap.get()
    try {
      // Initialize a reference-unique id map to store generated ids, which also avoid accidental
      // overwrites and to allow intentional overwriting of IDs generated in previous AQE iteration
      val idMap = new IdentityHashMap[QueryPlan[_], Int]()
      localIdMap.set(idMap)
      // Initialize an array of ReusedExchanges to help find Adaptively Optimized Out
      // Exchanges as part of SPARK-42753
      val reusedExchanges = ArrayBuffer.empty[ReusedExchangeExec]

      var currentOperatorID = 0
      currentOperatorID = generateOperatorIDs(plan, currentOperatorID, idMap, reusedExchanges,
        true)

      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
      getSubqueries(plan, subqueries)

      currentOperatorID = subqueries.foldLeft(currentOperatorID) {
        (curId, plan) => generateOperatorIDs(plan._3.child, curId, idMap, reusedExchanges,
          true)
      }

      // SPARK-42753: Process subtree for a ReusedExchange with unknown child
      val optimizedOutExchanges = ArrayBuffer.empty[Exchange]
      reusedExchanges.foreach{ reused =>
        val child = reused.child
        if (!idMap.containsKey(child)) {
          optimizedOutExchanges.append(child)
          currentOperatorID = generateOperatorIDs(child, currentOperatorID, idMap,
            reusedExchanges, false)
        }
      }

      val collectedOperators = BitSet.empty
      processPlanSkippingSubqueries(plan, append, collectedOperators)

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
          processPlanSkippingSubqueries(sub._3.child, append, collectedOperators)
        }
        append("\n")
      }

      i = 0
      optimizedOutExchanges.foreach{ exchange =>
        if (i == 0) {
          append("\n===== Adaptively Optimized Out Exchanges =====\n\n")
        }
        i = i + 1
        append(s"Subplan:$i\n")
        processPlanSkippingSubqueries[SparkPlan](exchange, append, collectedOperators)
        append("\n")
      }
    } finally {
      localIdMap.set(prevIdMap)
    }
  }

  /**
   * Traverses the supplied input plan in a bottom-up fashion and records the operator id via
   * setting a tag in the operator.
   * Note :
   * - Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't
   *     appear in the explain output.
   * - Operator identifier starts at startOperatorID + 1
   *
   * @param plan Input query plan to process
   * @param startOperatorID The start value of operation id. The subsequent operations will be
   *                        assigned higher value.
   * @param idMap   A reference-unique map store operators visited by generateOperatorIds and its
   *                id. This Map is scoped at the callsite function processPlan. It serves three
   *                purpose:
   *                Firstly, it stores the QueryPlan - generated ID mapping. Secondly, it is used to
   *                avoid accidentally overwriting existing IDs that were generated in the same
   *                processPlan call. Thirdly, it is used to allow for intentional ID overwriting as
   *                part of SPARK-42753 where an Adaptively Optimized Out Exchange and its subtree
   *                may contain IDs that were generated in a previous AQE iteration's processPlan
   *                call which would result in incorrect IDs.
   * @param reusedExchanges A unique set of ReusedExchange nodes visited which will be used to
   *                        idenitfy adaptively optimized out exchanges in SPARK-42753.
   * @param addReusedExchanges Whether to add ReusedExchange nodes to reusedExchanges set. We set it
   *                           to false to avoid processing more nested ReusedExchanges nodes in the
   *                           subtree of an Adpatively Optimized Out Exchange.
   * @return The last generated operation id for this input plan. This is to ensure we always
   *         assign incrementing unique id to each operator.
   */
  private def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      idMap: java.util.Map[QueryPlan[_], Int],
      reusedExchanges: ArrayBuffer[ReusedExchangeExec],
      addReusedExchanges: Boolean): Int = {
    var currentOperationID = startOperatorID
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }

    def setOpId(plan: QueryPlan[_]): Unit = idMap.computeIfAbsent(plan, plan => {
      plan match {
        case r: ReusedExchangeExec if addReusedExchanges =>
          reusedExchanges.append(r)
        case _ =>
      }
      currentOperationID += 1
      currentOperationID
    })

    plan.foreachUp {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        currentOperationID = generateOperatorIDs(p.executedPlan, currentOperationID, idMap,
          reusedExchanges, addReusedExchanges)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          currentOperationID = generateOperatorIDs(p.initialPlan, currentOperationID, idMap,
            reusedExchanges, addReusedExchanges)
        }
        setOpId(p)
      case p: QueryStageExec =>
        currentOperationID = generateOperatorIDs(p.plan, currentOperationID, idMap,
          reusedExchanges, addReusedExchanges)
        setOpId(p)
      case other: QueryPlan[_] =>
        setOpId(other)
        currentOperationID = other.innerChildren.foldLeft(currentOperationID) {
          (curId, plan) => generateOperatorIDs(plan, curId, idMap, reusedExchanges,
            addReusedExchanges)
        }
    }
    currentOperationID
  }

  /**
   * Traverses the supplied input plan in a bottom-up fashion and collects operators with assigned
   * ids.
   *
   * @param plan Input query plan to process
   * @param operators An output parameter that contains the operators.
   * @param collectedOperators The IDs of the operators that are already collected and we shouldn't
   *                           collect again.
   */
  private def collectOperatorsWithID(
      plan: QueryPlan[_],
      operators: ArrayBuffer[QueryPlan[_]],
      collectedOperators: BitSet): Unit = {
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return
    }

    def collectOperatorWithID(plan: QueryPlan[_]): Unit = {
      Option(ExplainUtils.localIdMap.get().get(plan)).foreach { id =>
        if (collectedOperators.add(id)) operators += plan
      }
    }

    plan.foreachUp {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        collectOperatorsWithID(p.executedPlan, operators, collectedOperators)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          collectOperatorsWithID(p.initialPlan, operators, collectedOperators)
        }
        collectOperatorWithID(p)
      case p: QueryStageExec =>
        collectOperatorsWithID(p.plan, operators, collectedOperators)
        collectOperatorWithID(p)
      case other: QueryPlan[_] =>
        collectOperatorWithID(other)
        other.innerChildren.foreach(collectOperatorsWithID(_, operators, collectedOperators))
    }
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

  /**
   * Generate detailed field string with different format based on type of input value
   */
  def generateFieldString(fieldName: String, values: Any): String =
    QueryPlan.generateFieldString(fieldName, values)

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *  1. Hosting operator id.
   *  2. Hosting expression
   *  3. Subquery plan
   */
  private def getSubqueries(
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]): Unit = {
    plan.foreach {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries)
      case q: QueryStageExec =>
        getSubqueries(q.plan, subqueries)
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

  /**
   * Returns the operator identifier for the supplied plan by retrieving the
   * `operationId` tag value.
   */
  def getOpId(plan: QueryPlan[_]): String = {
    Option(ExplainUtils.localIdMap.get().get(plan)).map(v => s"$v").getOrElse("unknown")
  }
}
