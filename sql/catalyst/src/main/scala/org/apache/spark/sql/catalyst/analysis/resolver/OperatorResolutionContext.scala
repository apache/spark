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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{ArrayDeque, ArrayList, LinkedHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedHaving
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LimitAll, LogicalPlan, SubqueryAlias}

/**
 * Operator resolution context used to store information required for resolution of currently
 * resolved operator and all expressions within.
 *
 * @param unresolvedPlan The unresolved logical plan being currently resolved.
 * @param ordinalReplacementExpressions Expressions that are candidates to be resolved by ordinal
 *     in current operator ([[Sort]] or [[Aggregate]]).
 * @param baseOperator Optional base operator set only in case we place a [[Project]] on top of the
 *     operator being resolved. Used to validate the resolution of the original operator.
 *     NOTE: It's not set in case we are resolving a [[Project]] with LCAs since the type of the
 *     operator don't change as we put another [[Project]] on top of the original one.
 * @param isResolvingTreeUnderHaving flag indicates that we are currently resolving the operator
 *     tree below [[UnresolvedHaving]] operator. Resets to false when entering new subquery scope.
 * @param parameterNamesToValues Optional map of parameter names to values. Used to resolve
 *     [[NamedParameter]]s in the context of [[NameParameterizedQuery]]. Lazily created only when
 *     parameters are present to avoid overhead for non-parameterized queries.
 * @param resolvingLimitAll flag that represents whether a LIMIT ALL should be considered in the
 *     current context. This is used for defining infinite recursive CTEs.
 * @param hasGroupingAnalytics Boolean flag indicating whether grouping analytics (i.e., ROLLUP,
 *     CUBE, GROUPING SETS) are present. Set to true when detected during resolution in
 *     [[GroupingAnalyticsResolver]].
 */
class OperatorResolutionContext(
    var unresolvedPlan: Option[LogicalPlan] = None,
    var ordinalReplacementExpressions: Option[OrdinalReplacementExpressions] = None,
    var baseOperator: Option[LogicalPlan] = None,
    val isResolvingTreeUnderHaving: Boolean = false,
    var parameterNamesToValues: Option[LinkedHashMap[String, Expression]] = None,
    val resolvingLimitAll: Boolean = false,
    var hasGroupingAnalytics: Boolean = false) {

  /**
   * @param subqueryExpressionsToValidate List of [[SubqueryExpression]]s in the context of current
   *     operator. Used to validate the resolution of the operator using
   *     [[ValidateSubqueryExpression]].
   */
  lazy val subqueryExpressionsToValidate: ArrayList[SubqueryExpression] =
    new ArrayList[SubqueryExpression]

  /**
   * Map of aggregate expressions from subqueries in the current operator, that should be pushed
   * down to the nearest outer [[Aggregate]].
   */
  private lazy val subqueryOuterAggregateExpressionsAliased: LinkedHashMap[Expression, Alias] =
    new LinkedHashMap[Expression, Alias]

  /**
   * Adds an aggregate expression from a subquery to be pushed down to the nearest outer
   * [[Aggregate]].
   */
  def addSubqueryAggregateExpression(alias: Alias): Unit = {
    subqueryOuterAggregateExpressionsAliased.putIfAbsent(alias.child.canonicalized, alias)
  }

  /**
   * Gets an aliased aggregate expression from a subquery that has already been extracted, if such
   * exists.
   */
  def getSubqueryAggregateExpression(expression: Expression): Option[Alias] = {
    if (subqueryOuterAggregateExpressionsAliased.containsKey(expression.canonicalized)) {
      Some(subqueryOuterAggregateExpressionsAliased.get(expression.canonicalized))
    } else {
      None
    }
  }

  /**
   * Returns aliases of all aggregate expressions from subqueries that have been extracted.
   */
  def getAllSubqueryAggregateExpressions: Seq[Alias] =
    subqueryOuterAggregateExpressionsAliased.values().asScala.toSeq
}

/**
 * Stack of [[OperatorResolutionContext]]s managed by [[Resolver]], with each entry in the stack
 * corresponding to an operator being resolved.
 */
class OperatorResolutionContextStack {
  private val stack = new ArrayDeque[OperatorResolutionContext]
  stack.push(new OperatorResolutionContext)

  /**
   * Returns whether Limit All should be applied to the current context according to the following
   * rules:
   * - [[LimitAll]] sets the flag to true
   * - Allow-listed operators (from [[ApplyLimitAll.propagatesLimitAll]]) propagate the flag from
   * the parent context
   * - All other operators reset the flag to false
   */
  private def currentContextIsResolvingLimitAll(plan: LogicalPlan): Boolean = plan match {
    case _: LimitAll =>
      true
    case _ if propagatesLimitAll(plan) =>
      current.resolvingLimitAll
    case _ =>
      false
  }

  /**
   * Returns the current resolution context from the stack.
   */
  def current: OperatorResolutionContext = {
    if (stack.isEmpty) {
      throw SparkException.internalError("No current operator resolution context")
    }
    stack.peek()
  }

  /**
   * Pushes a new resolution context onto the stack. The new context shares the
   * `parameterNamesToValues` map instance and `isResolvingTreeUnderHaving` from the current
   * (parent) context. `isResolvingTreeUnderHaving` should be reset to false when entering a
   * new subquery scope.
   */
  def push(unresolvedPlan: LogicalPlan, isSubqueryRoot: Boolean = false): Unit = {
    val isResolvingTreeUnderHaving = unresolvedPlan match {
      case _ if isSubqueryRoot => false
      case _: SubqueryAlias => false
      case _: UnresolvedHaving => true
      case _ => current.isResolvingTreeUnderHaving
    }
    stack.push(
      new OperatorResolutionContext(
        unresolvedPlan = Some(unresolvedPlan),
        isResolvingTreeUnderHaving = isResolvingTreeUnderHaving,
        parameterNamesToValues = current.parameterNamesToValues,
        resolvingLimitAll = currentContextIsResolvingLimitAll(unresolvedPlan)
      )
    )
  }

  /**
   * Pops the top resolution context from the stack. Before popping, propagates the
   * `hasGroupingAnalytics` from the child context to the parent context if it was set.
   */
  def pop(): Unit = {
    val childContext = current
    stack.pop()
    if (childContext.hasGroupingAnalytics) {
      current.hasGroupingAnalytics = childContext.hasGroupingAnalytics
    }
  }
}
