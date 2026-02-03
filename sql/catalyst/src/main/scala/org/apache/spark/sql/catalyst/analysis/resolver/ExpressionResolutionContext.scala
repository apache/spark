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

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/**
 * The [[ExpressionResolutionContext]] is a state that is propagated between the nodes of the
 * expression tree during the bottom-up expression resolution process. This way we pass the results
 * of [[ExpressionResolver.resolve]] call, which are not the resolved child itself, from children
 * to parents.
 *
 * @param parentContext The parent context of the current context or None if current context is the
 *   root.
 * @param isRoot A flag indicating that we are resolving the root of the expression tree. It's
 *   used by the [[ExpressionResolver]] to correctly propagate top-level information like
 *   [[shouldPreserveAlias]]. It's going to be set to `true` for the top-level context when we are
 *   entering expression resolution from a specific operator (this can be either a top-level query
 *   or a subquery).
 * @param hasLocalReferences A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has local [[AttributeReferences]] in its subtree.
 * @param hasOuterReferences A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has outer [[AttributeReferences]] in its subtree.
 * @param hasAggregateExpressionsOutsideWindow A flag that highlights that a specific node
 *   corresponding to [[ExpressionResolutionContext]] has aggregate expressions in its subtree.
 * @param hasAttributeOutsideOfAggregateExpressions A flag that highlights that a specific node
 *   corresponding to [[ExpressionResolutionContext]] has non-LCA attributes in its subtree which
 *   are not under an [[AggregateExpression]].
 * @param hasLateralColumnAlias A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has LCA in its subtree.
 * @param hasWindowExpressions A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has [[WindowExpression]]s in its subtree.
 * @param shouldPreserveAlias A flag indicating whether we preserve the [[Alias]] e.g. if it is on
 *   top of a [[Project.projectList]]. If it is `false`, extra [[Alias]]es have to be stripped
 *   away.
 * @param resolvingGroupingExpressions A flag indicating whether an expression we are resolving is
 *   one of [[Aggregate.groupingExpressions]].
 * @param resolvingWindowExpression Optional window expression that is the parent of the subtree
 *   currently being resolved.
 * @param resolvingWindowFunction A flag indicating whether an expression we are resolving is
 *   part of [[WindowExpression.windowFunction]] expression tree.
 * @param windowFunctionNestednessLevel A flag indicating expression's function nestedness level
 *   in its lowest [[WindowExpression.windowFunction]] ancestor. Consider this example:
 *
 *   {{{ SELECT SUM(1 + RANK()) OVER (ORDER BY col1), MAX(RAND()) FROM VALUES 1, 2; }}}
 *
 *   The value of `windowFunctionNestednessLevel` during resolution of each function will be:
 *     - `SUM(1 + RANK())`: Function is followed by an `OVER` clause, thus it is considered
 *       its own lowest [[WindowExpression.windowFunction]] ancestor. In such cases
 *       windowFunctionNestednessLevel` is `1`.
 *     - `RANK()`: Function is nested one level deeper than `SUM(1 + RANK())`, therefore
 *       windowFunctionNestednessLevel` is `2`.
 *     - `MAX(RAND())`: Function is not followed by an `OVER` clause, nor is it nested inside
 *       another [[WindowExpression.windowFunction]], therefore `windowFunctionNestednessLevel`
 *       is `0`.
 *     - `RAND()`: Function is not followed by an `OVER` clause, nor is it nested inside
 *       another [[WindowExpression.windowFunction]], therefore `windowFunctionNestednessLevel`
 *       remains unchanged and is `0`.
 * @param resolvingWindowSpec A flag indicating whether an expression we are resolving is part
 *   of [[WindowExpression.windowSpec]].
 * @param hasCorrelatedScalarSubqueryExpressions A flag that highlights that a specific node
 *   corresponding to [[ExpressionResolutionContext]] has correlated [[ScalarSubquery]] expressions
 *   in its subtree.
 * @param resolvingTreeUnderAggregateExpression A flag indicating whether an expression we are
 *   resolving a tree under [[AggregateExpression]].
 * @param resolvingCreateNamedStruct A flag indicating whether we are resolving a tree under an
 *   [[CreateNamedStruct]]. This is needed in order to prevent alias collapsing before the name of
 *   [[UnresolvedAlias]] above this [[CreateNamedStruct]] is computed.
 * @param resolvingUnresolvedAlias A flag indicating whether we are resolving a tree under an
 *   [[UnresolvedAlias]]. This is needed in order to prevent alias collapsing before the name of
 *   [[UnresolvedAlias]] above is computed.
 * @param resolvingPivotAggregates A flag indicating whether we are resolving a tree under the
 *   [[Pivot.aggregates]]. This need for validation of those expressions.
 * @param extractValueExtractionKey Extraction key for [[UnresolvedExtractValue]] if we are
 *  currently resolving one, None otherwise.
 * @param lambdaVariableMap A map of lambda variable names to their corresponding
 *   [[NamedExpression]]s used to resolve [[UnresolvedLambdaVariable]]s inside lambda functions.
 */
class ExpressionResolutionContext(
    val parentContext: Option[ExpressionResolutionContext] = None,
    val isRoot: Boolean = false,
    var hasLocalReferences: Boolean = false,
    var hasOuterReferences: Boolean = false,
    var hasAggregateExpressionsOutsideWindow: Boolean = false,
    var hasAttributeOutsideOfAggregateExpressions: Boolean = false,
    var hasLateralColumnAlias: Boolean = false,
    var hasWindowExpressions: Boolean = false,
    var shouldPreserveAlias: Boolean = false,
    var resolvingGroupingExpressions: Boolean = false,
    var resolvingWindowExpression: Option[Expression] = None,
    var resolvingWindowFunction: Boolean = false,
    var windowFunctionNestednessLevel: Int = 0,
    var resolvingWindowSpec: Boolean = false,
    var hasCorrelatedScalarSubqueryExpressions: Boolean = false,
    var resolvingTreeUnderAggregateExpression: Boolean = false,
    var resolvingCreateNamedStruct: Boolean = false,
    var resolvingUnresolvedAlias: Boolean = false,
    var resolvingPivotAggregates: Boolean = false,
    var extractValueExtractionKey: Option[Expression] = None,
    var lambdaVariableMap: Option[IdentifierMap[NamedExpression]] = None) {

  /**
   * Propagate generic information that is valid across the whole expression tree from the
   * [[child]] context.
   */
  def mergeChild(child: ExpressionResolutionContext): Unit = {
    hasLocalReferences |= child.hasLocalReferences
    hasOuterReferences |= child.hasOuterReferences
    hasAggregateExpressionsOutsideWindow |= child.hasAggregateExpressionsOutsideWindow
    hasAttributeOutsideOfAggregateExpressions |= child.hasAttributeOutsideOfAggregateExpressions
    hasLateralColumnAlias |= child.hasLateralColumnAlias
    hasWindowExpressions |= child.hasWindowExpressions
    hasCorrelatedScalarSubqueryExpressions |= child.hasCorrelatedScalarSubqueryExpressions
  }
}

object ExpressionResolutionContext {

  /**
   * Create a new child [[ExpressionResolutionContext]]. Propagates the relevant information
   * top-down.
   */
  def createChild(parent: ExpressionResolutionContext): ExpressionResolutionContext = {
    if (parent.isRoot) {
      new ExpressionResolutionContext(
        shouldPreserveAlias = parent.shouldPreserveAlias,
        resolvingGroupingExpressions = parent.resolvingGroupingExpressions,
        resolvingWindowExpression = parent.resolvingWindowExpression,
        resolvingWindowFunction = parent.resolvingWindowFunction,
        windowFunctionNestednessLevel = parent.windowFunctionNestednessLevel,
        resolvingWindowSpec = parent.resolvingWindowSpec,
        resolvingUnresolvedAlias = parent.resolvingUnresolvedAlias,
        resolvingPivotAggregates = parent.resolvingPivotAggregates,
        lambdaVariableMap = parent.lambdaVariableMap
      )
    } else {
      new ExpressionResolutionContext(
        parentContext = Some(parent),
        resolvingGroupingExpressions = parent.resolvingGroupingExpressions,
        resolvingTreeUnderAggregateExpression = parent.resolvingTreeUnderAggregateExpression,
        resolvingWindowExpression = parent.resolvingWindowExpression,
        resolvingWindowFunction = parent.resolvingWindowFunction,
        windowFunctionNestednessLevel = parent.windowFunctionNestednessLevel,
        resolvingWindowSpec = parent.resolvingWindowSpec,
        resolvingCreateNamedStruct = parent.resolvingCreateNamedStruct,
        resolvingUnresolvedAlias = parent.resolvingUnresolvedAlias,
        resolvingPivotAggregates = parent.resolvingPivotAggregates,
        lambdaVariableMap = parent.lambdaVariableMap
      )
    }
  }
}
