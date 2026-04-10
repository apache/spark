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

import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, NamedExpression}

/**
 * Resolver for [[LambdaFunction]] expressions.
 */
class LambdaFunctionResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[LambdaFunction, Expression]
    with ResolvesExpressionChildren {
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack

  /**
   * Resolves the [[LambdaFunction]] based on the type of the lambda function. There are three
   * cases:
   *  1. If the lambda function is not hidden, its arguments are added to the current
   *     [[ExpressionResolutionContext.lambdaVariableMap]] so that they can be used for resolution
   *     of the [[UnresolvedNamedLambdaVariables]] later in the [[ExpressionResolver]]. Hidden
   *     functions are the ones that are created internally when the user specifies non-lambda
   *     argument (instead provide an actual lambda function because it's semantically more right).
   *     In the following example we would replace each array element with the constant `0`,
   *     resulting in `array(0)`. Spark internally creates a hidden lambda since the user didn't
   *     provide one:
   *
   *     {{{
   *       SELECT transform(array(1), 0);
   *     }}}
   *
   *     Here we would have `lambdafunction(0, lambda col0#0, true)` where the original `0`
   *     would be the `function` and `NamedLambdaVariable(col0#0)` would be the `argument`.
   *
   *  2. If the lambda function is hidden, it is resolved by resolving its children without adding
   *     its arguments to the current [[ExpressionResolutionContext.lambdaVariableMap]].
   *
   * In case there is a query:
   *
   * {{{
   *   SELECT filter(array('a'), x -> x = 'a');
   * }}}
   *
   * Lambda function would be `x -> x = 'a'`, which is not hidden. So, `x` (left side one) would be
   * added to the current [[ExpressionResolutionContext.lambdaVariableMap]] and used later when
   * resolving the `x = 'a'` expression (`x` is an [[UnresolvedNamedLambdaVariable]] at that
   * point).
   *
   * In case of nested lambdas:
   *
   * {{{
   *   SELECT
   *   transform(
   *     nested_arrays,
   *     inner_array -> aggregate(
   *       inner_array,
   *       1,
   *       (product, element) -> product * element * size(inner_array)
   *     )
   *   )
   * FROM VALUES (
   *   array(array(1, 2), array(3, 4))
   * ) AS t(nested_arrays);
   * }}}
   *
   * We need to pass the `inner_array` lambda variable to the inner lambda function resolver
   * because it is used inside the inner lambda function body. So, while resolving the `aggregate`
   * lambda function the `lambdaVariableMap` would look like:
   *  - 'product' -> NamedLambdaVariable(product)
   *  - 'element' -> NamedLambdaVariable(element)
   *  - 'inner_array' -> NamedLambdaVariable(inner_array)
   */
  override def resolve(lambdaFunction: LambdaFunction): Expression = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()
    val previousLambdaVariableMap = expressionResolutionContext.lambdaVariableMap.map {
      existingMap =>
        existingMap.copyTo(new IdentifierMap[NamedExpression])
    }

    try {
      if (!lambdaFunction.hidden) {
        resolveUnhiddenLambdaFunction(
          lambdaFunction = lambdaFunction,
          expressionResolutionContext = expressionResolutionContext
        )
      } else {
        withResolvedChildren(
          unresolvedExpression = lambdaFunction,
          resolveChild = expressionResolver.resolve _
        )
      }
    } finally {
      expressionResolutionContext.lambdaVariableMap = previousLambdaVariableMap
    }
  }

  private def resolveUnhiddenLambdaFunction(
      lambdaFunction: LambdaFunction,
      expressionResolutionContext: ExpressionResolutionContext): Expression = {
    val lambdaMap = expressionResolutionContext.lambdaVariableMap
      .map(_.copyTo(new IdentifierMap[NamedExpression]))
      .getOrElse(new IdentifierMap[NamedExpression])
    lambdaFunction.arguments.foreach(argument => lambdaMap += (argument.name, argument))
    expressionResolutionContext.lambdaVariableMap = Some(lambdaMap)

    withResolvedChildren(
      unresolvedExpression = lambdaFunction,
      resolveChild = expressionResolver.resolve _
    )
  }
}
