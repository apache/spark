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

  private final class IdentityPlanSet {
    private val plans = new IdentityHashMap[QueryPlan[_], java.lang.Boolean]()

    def add(plan: QueryPlan[_]): Boolean = plans.put(plan, java.lang.Boolean.TRUE) == null
    def size: Int = plans.size()
  }

  private final class ExplainTraversalContext {
    val operatorIds = new IdentityPlanSet
    val subqueries = new IdentityPlanSet
    val collectedPlans = new IdentityPlanSet
    val printedSubqueries = new IdentityHashMap[SparkPlan, java.lang.Integer]()
    val collectedOperatorIds = BitSet.empty
    var hostReferences = 0

    def snapshot: ExplainTraversalStats = ExplainTraversalStats(
      operatorIds.size, subqueries.size, collectedPlans.size,
      printedSubqueries.size(), hostReferences)
  }

  private[sql] final case class ExplainTraversalStats(
      operatorIdNodes: Int,
      subquerySearchNodes: Int,
      detailedOperatorNodes: Int,
      renderedSubqueryPlans: Int,
      hostReferences: Int)

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Computes the whole stage codegen id for the current operator.
   *   2. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with a unique identifier for each operator.
   *      2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan Input query plan to process
   * @param append function used to append the explain output
   * @param context The traversal state for this explain output.
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      context: ExplainTraversalContext): Unit = {
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
      collectOperatorsWithID(plan, operationsWithID, context)
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
   * Plan instances can be shared across queries, for example through cached relations. Operator
   * and codegen IDs are scoped to each rendering.
   */
  def processPlan[T <: QueryPlan[T]](plan: T, append: String => Unit): Unit = {
    processPlan(plan, append, new ExplainTraversalContext)
  }

  private[sql] def processPlanWithStats[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit): ExplainTraversalStats = {
    val context = new ExplainTraversalContext
    processPlan(plan, append, context)
    context.snapshot
  }

  private def processPlan[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      context: ExplainTraversalContext): Unit = {
    val prevIdMap = localIdMap.get()
    val prevCodegenIdMap = QueryPlan.localCodegenIdMap.get()
    try {
      // Initialize a reference-unique id map to store generated ids, which also avoid accidental
      // overwrites and to allow intentional overwriting of IDs generated in previous AQE iteration
      val idMap = new IdentityHashMap[QueryPlan[_], Int]()
      localIdMap.set(idMap)
      QueryPlan.localCodegenIdMap.set(new IdentityHashMap[QueryPlan[_], Int]())
      val (subqueries, optimizedOutExchanges) = assignOperatorIds(plan, idMap, context)

      processPlanSkippingSubqueries(plan, append, context)

      var i = 0
      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(s"Subquery:$i Hosting operator id = " +
          s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

        // Reused subqueries keep their existing heading-only output. Other subquery plans
        // are rendered once per object, including transparent roots without an operator id.
        if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
          val child = sub._3.child
          val firstShown = context.printedSubqueries.get(child)
          if (firstShown == null) {
            context.printedSubqueries.put(child, java.lang.Integer.valueOf(i))
            processPlanSkippingSubqueries(child, append, context)
          } else {
            append(s"Shared subquery plan, first shown in Subquery:$firstShown\n")
          }
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
        processPlanSkippingSubqueries[SparkPlan](exchange, append, context)
        append("\n")
      }
    } finally {
      localIdMap.set(prevIdMap)
      if (prevCodegenIdMap == null) {
        QueryPlan.localCodegenIdMap.remove()
      } else {
        QueryPlan.localCodegenIdMap.set(prevCodegenIdMap)
      }
    }
  }

  /**
   * Assigns operator IDs to all operators across the full plan tree -- the main plan,
   * any subqueries, and any adaptively-optimized-out exchanges (SPARK-42753) -- by populating
   * the supplied idMap. Returns the discovered subqueries and optimized-out exchanges so
   * the caller ([[processPlan]]) can perform post-assignment work (text output) without
   * rediscovering them.
   */
  private def assignOperatorIds(
      plan: QueryPlan[_],
      idMap: java.util.Map[QueryPlan[_], Int],
      context: ExplainTraversalContext)
      : (ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)], ArrayBuffer[Exchange]) = {
    // Initialize an array of ReusedExchanges to help find Adaptively Optimized Out
    // Exchanges as part of SPARK-42753
    val reusedExchanges = ArrayBuffer.empty[ReusedExchangeExec]
    var currentOperatorID = generateOperatorIDs(plan, 0, idMap, reusedExchanges, true, context)

    val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
    getSubqueries(plan, subqueries, context)
    currentOperatorID = subqueries.foldLeft(currentOperatorID) {
      (curId, sub) =>
        generateOperatorIDs(sub._3.child, curId, idMap, reusedExchanges, true, context)
    }

    // SPARK-42753: Process subtree for a ReusedExchange with unknown child
    val optimizedOutExchanges = ArrayBuffer.empty[Exchange]
    reusedExchanges.foreach { reused =>
      val child = reused.child
      if (!idMap.containsKey(child)) {
        optimizedOutExchanges.append(child)
        currentOperatorID = generateOperatorIDs(child, currentOperatorID, idMap,
          reusedExchanges, false, context)
      }
    }
    (subqueries, optimizedOutExchanges)
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
      addReusedExchanges: Boolean,
      context: ExplainTraversalContext): Int = {
    if (!context.operatorIds.add(plan)) {
      return startOperatorID
    }
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

    plan.children.foreach {
      case child: QueryPlan[_] =>
        currentOperationID = generateOperatorIDs(child, currentOperationID, idMap,
          reusedExchanges, addReusedExchanges, context)
    }
    plan match {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        currentOperationID = generateOperatorIDs(p.executedPlan, currentOperationID, idMap,
          reusedExchanges, addReusedExchanges, context)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          currentOperationID = generateOperatorIDs(p.initialPlan, currentOperationID, idMap,
            reusedExchanges, addReusedExchanges, context)
        }
        setOpId(p)
      case p: QueryStageExec =>
        currentOperationID = generateOperatorIDs(p.plan, currentOperationID, idMap,
          reusedExchanges, addReusedExchanges, context)
        setOpId(p)
      case other: QueryPlan[_] =>
        setOpId(other)
        currentOperationID = other.innerChildren.foldLeft(currentOperationID) {
          (curId, plan) => generateOperatorIDs(plan, curId, idMap, reusedExchanges,
            addReusedExchanges, context)
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
   * @param context The traversal state for this explain output.
   */
  private def collectOperatorsWithID(
      plan: QueryPlan[_],
      operators: ArrayBuffer[QueryPlan[_]],
      context: ExplainTraversalContext): Unit = {
    if (!context.collectedPlans.add(plan)) {
      return
    }
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return
    }

    def collectOperatorWithID(plan: QueryPlan[_]): Unit = {
      Option(ExplainUtils.localIdMap.get().get(plan)).foreach { id =>
        if (context.collectedOperatorIds.add(id)) operators += plan
      }
    }

    plan.children.foreach {
      case child: QueryPlan[_] => collectOperatorsWithID(child, operators, context)
    }
    plan match {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        collectOperatorsWithID(p.executedPlan, operators, context)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          collectOperatorsWithID(p.initialPlan, operators, context)
        }
        collectOperatorWithID(p)
      case p: QueryStageExec =>
        collectOperatorsWithID(p.plan, operators, context)
        collectOperatorWithID(p)
      case other: QueryPlan[_] =>
        collectOperatorWithID(other)
        other.innerChildren.foreach(collectOperatorsWithID(_, operators, context))
    }
  }

  /**
   * Traverses the supplied input plan in a top-down fashion and records the
   * whole stage code gen id for this explain output.
   */
  private def generateWholeStageCodegenIds(plan: QueryPlan[_]): Unit = {
    var currentCodegenId = -1

    def setCodegenId(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      if (currentCodegenId != -1) {
        QueryPlan.localCodegenIdMap.get().put(p, currentCodegenId)
      }
      children.foreach(generateWholeStageCodegenIds)
    }

    // Subquery bodies are processed separately; do not descend through their wrappers here.
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
      plan: QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)],
      context: ExplainTraversalContext): Unit = {
    if (!context.subqueries.add(plan)) {
      return
    }
    plan match {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries, context)
      case q: QueryStageExec =>
        getSubqueries(q.plan, subqueries, context)
      case p: SparkPlan =>
        p.expressions.foreach (_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                context.hostReferences += 1
                getSubqueries(s, subqueries, context)
              case _ =>
            }
        })
      case _ =>
    }
    plan.children.foreach {
      case child: QueryPlan[_] => getSubqueries(child, subqueries, context)
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
