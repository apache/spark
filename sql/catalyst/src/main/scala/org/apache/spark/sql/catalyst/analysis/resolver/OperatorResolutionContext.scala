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

import java.util.{ArrayDeque, ArrayList, HashMap, LinkedHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{UnresolvedHaving, UnresolvedRelation}
import org.apache.spark.sql.catalyst.analysis.ApplyLimitAll.propagatesLimitAll
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Expression,
  ExprId,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  LimitAll,
  LogicalPlan,
  ResolvedHint,
  SetVariable,
  SubqueryAlias,
  UnresolvedWith,
  View
}

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
 * @param isResolvingSetVariableSource Whether this operator is within a [[SetVariable]] assignment,
 *     including nested subqueries.
 * @param parameterNamesToValues Optional map of parameter names to values. Used to resolve
 *     [[NamedParameter]]s in the context of [[NameParameterizedQuery]]. Lazily created only when
 *     parameters are present to avoid overhead for non-parameterized queries.
 * @param resolvingLimitAll flag that represents whether a LIMIT ALL should be considered in the
 *     current context. This is used for defining infinite recursive CTEs.
 * @param hasGroupingAnalytics Boolean flag indicating whether grouping analytics (i.e., ROLLUP,
 *     CUBE, GROUPING SETS) are present. Set to true when detected during resolution in
 *     [[GroupingAnalyticsResolver]].
 * @param hintApplicationContext The enclosing [[HintResolver]] installs at most one
 *     [[HintApplicationContext]] on the context that corresponds to its [[UnresolvedHint]]
 *     operator, before resolving the hint's child subtree.
 * @param enclosingHintApplicationContexts Hint applications installed by enclosing
 *     [[UnresolvedHint]] operators that are visible to this operator, ordered innermost first.
 *     Precomputed when the context is pushed (see [[OperatorResolutionContextStack.push]]) and
 *     read by [[HintResolver.maybeApplyHint]] when a [[SubqueryAlias]] is resolved. The chain
 *     resets at [[SubqueryAlias]], [[View]], [[UnresolvedWith]], and subquery-expression
 *     boundaries so that hints do not leak into nested alias, view, CTE, or subquery
 *     (scalar/IN/EXISTS) scopes.
 * @param isSubqueryRoot Whether this context is the root of a subquery expression.
 */
class OperatorResolutionContext(
    var unresolvedPlan: Option[LogicalPlan] = None,
    var ordinalReplacementExpressions: Option[OrdinalReplacementExpressions] = None,
    var baseOperator: Option[LogicalPlan] = None,
    val isResolvingTreeUnderHaving: Boolean = false,
    val isResolvingSetVariableSource: Boolean = false,
    var parameterNamesToValues: Option[LinkedHashMap[String, Expression]] = None,
    val resolvingLimitAll: Boolean = false,
    var hasGroupingAnalytics: Boolean = false,
    var hintApplicationContext: Option[HintApplicationContext] = None,
    val enclosingHintApplicationContexts: Seq[HintApplicationContext] = Nil,
    val isSubqueryRoot: Boolean = false) {

  /**
   * Metadata columns from [[NameScope.hiddenOutput]] that are referenced by this operator's
   * expressions.
   */
  val referencedMetadataColumns: HashMap[ExprId, Attribute] = new HashMap[ExprId, Attribute]

  /**
   * Expressions that are not allowed in the current operator, collected across all of its
   * expression trees. For example, both window functions in
   * `ORDER BY max(col1) OVER (), min(col1) OVER ()` are collected, even though each order key is
   * resolved in a separate expression tree traversal.
   */
  private lazy val invalidExpressionsBuffer: ArrayList[Expression] = new ArrayList[Expression]

  /**
   * Records `expression` as not allowed in the current operator.
   */
  def addInvalidExpression(expression: Expression): Unit = {
    invalidExpressionsBuffer.add(expression)
  }

  /**
   * Returns the expressions that are not allowed in the current operator, in resolution order.
   */
  def invalidExpressions: Seq[Expression] = invalidExpressionsBuffer.asScala.toSeq

  /**
   * Subquery expressions owned by the current operator resolution context. The collection has the
   * same lifetime as its [[OperatorResolutionContext]] and is discarded when that context is
   * popped. Entries normally use the current resolved operator as their validation target unless
   * [[subqueryValidationTargetOverrides]] assigns a synthesized source operator.
   */
  lazy val subqueryExpressionsToValidate: ArrayList[SubqueryExpression] =
    new ArrayList[SubqueryExpression]

  /**
   * Map of target operator overrides for subqueries. Used in validation to make sure that a
   * subquery is validated against its hosting operator in [[ValidateSubqueryExpression]].
   */
  lazy val subqueryValidationTargetOverrides: HashMap[ExprId, LogicalPlan] =
    new HashMap[ExprId, LogicalPlan]

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

  /** Whether the operator ancestry contains an enclosing SET VARIABLE assignment. */
  def isResolvingSetVariableSource: Boolean =
    current.isResolvingSetVariableSource

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
    val isResolvingSetVariableSource = unresolvedPlan match {
      case _: SetVariable => true
      case _ => current.isResolvingSetVariableSource
    }
    stack.push(
      new OperatorResolutionContext(
        unresolvedPlan = Some(unresolvedPlan),
        isResolvingTreeUnderHaving = isResolvingTreeUnderHaving,
        isResolvingSetVariableSource = isResolvingSetVariableSource,
        parameterNamesToValues = current.parameterNamesToValues,
        resolvingLimitAll = currentContextIsResolvingLimitAll(unresolvedPlan),
        enclosingHintApplicationContexts = enclosingHintApplicationContextsForChild(isSubqueryRoot),
        isSubqueryRoot = isSubqueryRoot
      )
    )
  }

  /**
   * Computes the [[HintApplicationContext]]s visible to a child operator about to be pushed, based
   * on the current (parent) context. Accumulates the hint installed on each enclosing
   * [[UnresolvedHint]] context (innermost first) and resets at [[SubqueryAlias]]/[[View]]/
   * [[UnresolvedWith]] boundaries so that hints do not leak into nested alias, view, or CTE
   * scopes. The chain is also reset at subquery-expression roots (`isSubqueryRoot`) so that an
   * outer hint does not leak into the plan of a scalar/IN/EXISTS subquery. This matches the
   * fixed-point [[ResolveHints.ResolveJoinStrategyHints]] rule, which only traverses logical plan
   * children and therefore never descends into subquery expressions.
   *
   * The chain also resets below an existing [[ResolvedHint]], because the fixed-point rule does
   * not traverse past it either. The exception is a [[ResolvedHint]] directly over an
   * [[UnresolvedRelation]] or [[SubqueryAlias]]: the fixed-point rule merges a matching hint
   * into it, so the chain stays visible to that child and [[HintResolver.handleResolvedHint]]
   * performs the merge.
   */
  private def enclosingHintApplicationContextsForChild(
      isSubqueryRoot: Boolean): Seq[HintApplicationContext] = {
    val parentContext = current
    parentContext.unresolvedPlan match {
      case _ if isSubqueryRoot =>
        Nil
      case Some(_: SubqueryAlias) | Some(_: View) | Some(_: UnresolvedWith) =>
        Nil
      case Some(ResolvedHint(_: UnresolvedRelation | _: SubqueryAlias, _)) =>
        parentContext.enclosingHintApplicationContexts
      case Some(_: ResolvedHint) =>
        Nil
      case _ =>
        parentContext.hintApplicationContext match {
          case Some(hintApplicationContext) =>
            hintApplicationContext +: parentContext.enclosingHintApplicationContexts
          case None =>
            parentContext.enclosingHintApplicationContexts
        }
    }
  }

  /**
   * Pops the top resolution context from the stack. Before popping, propagates the
   * `hasGroupingAnalytics` from the child context to the parent context if it was set.
   * The flag is NOT propagated across subquery boundaries (isSubqueryRoot) or derived-table
   * boundaries (SubqueryAlias), because grouping analytics inside a subquery or derived table
   * are unrelated to operators in the outer query.
   */
  def pop(): Unit = {
    val childContext = current
    stack.pop()
    val isDerivedTableBoundary = childContext.unresolvedPlan match {
      case Some(_: SubqueryAlias) => true
      case _ => false
    }
    if (childContext.hasGroupingAnalytics &&
        !childContext.isSubqueryRoot &&
        !isDerivedTableBoundary) {
      current.hasGroupingAnalytics = childContext.hasGroupingAnalytics
    }
  }
}
