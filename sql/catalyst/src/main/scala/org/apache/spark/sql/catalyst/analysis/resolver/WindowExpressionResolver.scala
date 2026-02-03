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

import org.apache.spark.sql.catalyst.analysis.WindowResolution
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  WindowExpression,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

/**
 * Resolver for [[WindowExpression]]. It handles the resolution and validation of
 * [[WindowExpression]].
 */
class WindowExpressionResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[WindowExpression, Expression]
    with ResolvesExpressionChildren
    with CoercesExpressionTypes
    with CollectsWindowSourceExpressions {
  protected val windowResolutionContextStack = expressionResolver.getWindowResolutionContextStack
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack

  /**
   * Handles resolution and validation of [[WindowExpression]]. The children resolution process must
   * follow a specific order to correctly handle both the window function and the window
   * specification:
   *
   *  1. Capture the current value of `resolvingWindowSpec` to determine if the window expression
   *     should be handled by the source operator or by the window operator:
   *      - Window expression nesting can only occur inside window specification. In such cases we
   *        replace the nested expression by an attribute and handle the original expression by the
   *        source operator in [[WindowResolution.buildWindow]]. This process is repeated until
   *        there are no more nested window expressions.
   *
   *        Consider the following query:
   *
   *        {{{ SELECT SUM(col1) OVER (ORDER BY SUM(col1) OVER ()) FROM VALUES 1; }}}
   *
   *        Parsed plan:
   *
   *        'Project [unresolvedalias('SUM('col1) windowspec('SUM('col1) windowspec...))]
   *
   *        Plan after the first [[WindowResolution.buildWindow]] call:
   *
   *        Window [sum(col1#0) windowspec... AS _w0#1, col1#0]
   *        +- Project [col1#0]
   *
   *        Final analyzed plan:
   *
   *        Window[sum(col1#0) windowspec(_w1#1 ...)...#2], [_w1#1 ASC NULLS FIRST]
   *        +- Window [col1#0, sum(col1#0) windowspec... AS _w1#1]
   *           +- Project [col1#0]
   *  2. Set `hasWindowExpression` to `true` to indicate that there is a window in the current
   *     expression tree.
   *  3. Set `resolvingWindowFunction` to `true`, which will allow manipulation of
   *     `windowFunctionNestednessLevel` in [[FunctionResolver.resolve]]
   *     to differentiate between [[AggregateExpression]]s and window aggregate functions.
   *     Consider the following query:
   *
   *     {{{
   *     SELECT SUM() OVER() FROM VALUES 1, 2;
   *     }}}
   *
   *     In this case `SUM()` should be treated as a [[WindowFunction]], not as an
   *     [[AggregateExpression]]. Refer to [[FunctionResolver.resolve]] and
   *     [[AggregateExpressionResolver.handleAggregateExpressionWithChildrenResolved]] for
   *     additional context.
   *  4. Resolve [[WindowExpression.windowFunction]] using [[ExpressionResolver]].
   *  5. Set `resolvingWindowFunction` back to `false` because we may have [[WindowFunction]]
   *     without an over clause as part of [[WindowExpression.windowSpec]], in such cases
   *     we must throw an error in the [[FunctionResolver]]. For example:
   *
   *     {{{ SELECT RANK() OVER (ORDER BY RANK()) FROM t; }}}
   *
   *  6. Set `resolvingWindowSpec` to `true` so that we can check for [[Window]] nesting from
   *     step 1 in the future [[WindowExpressionResolver.resolve]] calls
   *  7. Resolve window frame using [[WindowResolution.resolveFrame]] helper
   *  8. Resolve the remainder of the [[WindowExpression.windowSpec]] using [[ExpressionResolver]]
   *  9. Assign order to [[RankLike]] functions and perform ordering validations in
   *     [[WindowResolution.resolveOrder]]
   *  10. Perform a final set of validations on fully resolved [[WindowExpression]] via
   *      [[validateResolvedWindowExpression]]
   *  11. If the expression is nested in a window specification, collect it for window resolution.
   */
  override def resolve(windowExpression: WindowExpression): Expression = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()

    expressionResolutionContext.resolvingWindowExpression = Some(windowExpression)

    val isNestedInWindowSpec = expressionResolutionContext.resolvingWindowSpec

    expressionResolutionContext.hasWindowExpressions = true

    expressionResolutionContext.windowFunctionNestednessLevel = 0

    expressionResolutionContext.resolvingWindowFunction = true
    val resolvedWindowFunction = expressionResolver.resolve(windowExpression.windowFunction)
    expressionResolutionContext.resolvingWindowFunction = false

    val windowExpressionWithResolvedFunction = windowExpression.copy(
      resolvedWindowFunction
    )

    expressionResolutionContext.resolvingWindowSpec = true

    val windowExpressionWithResolvedFrame = WindowResolution
      .resolveFrame(windowExpressionWithResolvedFunction)

    val windowExpressionWithResolvedSpec = windowExpressionWithResolvedFrame.copy(
      windowSpec = expressionResolver
        .resolve(windowExpressionWithResolvedFrame.windowSpec)
        .asInstanceOf[WindowSpecDefinition]
    )

    val resolvedWindowExpression = WindowResolution
      .resolveOrder(windowExpressionWithResolvedSpec)

    WindowResolution.validateResolvedWindowExpression(resolvedWindowExpression)

    if (isNestedInWindowSpec) {
      collectWindowSourceExpression(
        expression = resolvedWindowExpression,
        parentOperator = traversals.current.parentOperator
      )
    }

    resolvedWindowExpression
  }
}
