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

import org.apache.spark.sql.catalyst.analysis.{
  AnalysisErrorAt,
  FunctionResolution,
  LambdaBinder,
  SubqueryExpressionInLambdaOrHigherOrderFunctionValidator,
  UnresolvedFunction
}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.MapZipWithTypeCoercion
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A resolver specifically for higher-order functions (functions that accept lambda expressions).
 */
class HigherOrderFunctionResolver(
    protected val expressionResolver: ExpressionResolver,
    functionResolution: FunctionResolution)
    extends TreeNodeResolver[UnresolvedFunction, Expression]
    with ProducesUnresolvedSubtree
    with CoercesExpressionTypes
    with FunctionResolverUtils {

  private val traversals = expressionResolver.getExpressionTreeTraversals

  /**
   * Resolves an [[UnresolvedFunction]] representing a higher-order function into a fully resolved
   * [[HigherOrderFunction]] expression. This is done in the following steps:
   *  1. Handle any star (`*`) arguments in the function call. See more in the
   *     [[FunctionResolverUtils.handleStarInArguments]] scala doc.
   *  2. Resolve the function to a built-in or temporary function.
   *  3. Validate that the resolved function is indeed a higher-order function. Otherwise, throw an
   *     exception.
   *  4. Recursively resolve all arguments of the higher-order function.
   *  5. Apply the [[MapZipWithTypeCoercion]] type coercion transformation to the higher-order
   *     function. It must be done before binding the higher-order function so that the
   *     [[MapZipWith]] arguments have the correct types.
   *  6. Validate that there are no subquery expressions within the higher-order function. If there
   *     are, throw an exception.
   *  7. Bind the higher-order function using the [[LambdaBinder]]. See more in the [[LambdaBinder]]
   *     scala doc.
   *  8. Recursively resolve all lambda functions within the higher-order function.
   *  9. Coerce the higher-order function.
   *  10. Apply a time zone if needed.
   *
   * In case of the following query:
   *
   * {{{
   *   SELECT filter(array('a'), x -> x = 'a');
   * }}}
   *
   * The parsed plan would be:
   *
   * {{{
   *   'Project [unresolvedalias('filter('array(a), lambdafunction((lambda 'x = a), lambda 'x)))]
   *   +- OneRowRelation
   * }}}
   *
   * Whereas the analyzed plan is:
   *
   * {{{
   *   Project [filter(array(a), lambdafunction((lambda x#0 = a), lambda x#0)) AS ...]
   *   +- OneRowRelation
   * }}}
   *
   * For a query with star (`*`) expansion:
   *
   * {{{
   *   SELECT transform(array(*), x -> x + 1) FROM VALUES (1);
   * }}}
   *
   * The parsed plan would be:
   *
   * {{{
   *   'Project [unresolvedalias('transform('array(*), lambdafunction((lambda 'x + 1), lambda 'x)))]
   *   +- SubqueryAlias t
   *      +- LocalRelation [col1#0]
   * }}}
   *
   * Whereas the analyzed plan (after star expansion) is:
   *
   * {{{
   *   Project [transform(array(col1#0), lambdafunction((lambda x#3 + 1), lambda x#3)) AS ...]
   *   +- SubqueryAlias t
   *      +- LocalRelation [col1#0]
   * }}}
   */
  override def resolve(unresolvedFunction: UnresolvedFunction): Expression = {
    val unresolvedFunctionWithExpandedStarArgs = handleStarInArguments(unresolvedFunction)

    val partiallyResolvedFunction = functionResolution.resolveBuiltinOrTempFunction(
      name = unresolvedFunctionWithExpandedStarArgs.nameParts,
      arguments = unresolvedFunctionWithExpandedStarArgs.arguments,
      u = unresolvedFunctionWithExpandedStarArgs
    )

    val partiallyResolvedHigherOrderFunction =
      validateHigherOrderFunction(partiallyResolvedFunction, unresolvedFunctionWithExpandedStarArgs)

    val resolvedArguments = partiallyResolvedHigherOrderFunction.arguments.map { arg =>
      expressionResolver.resolve(arg)
    }

    val higherOrderFunctionWithResolvedArguments =
      partiallyResolvedHigherOrderFunction
        .withNewChildren(resolvedArguments ++ partiallyResolvedHigherOrderFunction.functions)

    val functionWithCoercedArguments =
      MapZipWithTypeCoercion(higherOrderFunctionWithResolvedArguments)
        .asInstanceOf[HigherOrderFunction]

    SubqueryExpressionInLambdaOrHigherOrderFunctionValidator(functionWithCoercedArguments)

    val boundHigherOrderFunction = functionWithCoercedArguments.bind(
      (expression, argumentsInfo) => LambdaBinder(expression, argumentsInfo)
    )

    val resolvedFunctions = boundHigherOrderFunction.functions.map { func =>
      expressionResolver.resolve(func)
    }

    val higherOrderFunctionWithResolvedChildren =
      boundHigherOrderFunction.withNewChildren(resolvedArguments ++ resolvedFunctions)

    val resolvedHigherOrderFunction = coerceExpressionTypes(
      expression = higherOrderFunctionWithResolvedChildren,
      expressionTreeTraversal = traversals.current
    )

    TimezoneAwareExpressionResolver.resolveTimezone(
      expression = resolvedHigherOrderFunction,
      timeZoneId = traversals.current.sessionLocalTimeZone
    )
  }

  private def validateHigherOrderFunction(
      partiallyResolvedFunction: Option[Expression],
      unresolvedFunction: UnresolvedFunction): HigherOrderFunction = {
    partiallyResolvedFunction match {
      case Some(higherOrderFunction: HigherOrderFunction) => higherOrderFunction
      case Some(other) =>
        other.failAnalysis(
          errorClass = "INVALID_LAMBDA_FUNCTION_CALL.NON_HIGHER_ORDER_FUNCTION",
          messageParameters = Map("class" -> other.getClass.getCanonicalName)
        )
      case None =>
        throw QueryCompilationErrors.unresolvedRoutineError(
          unresolvedFunction.nameParts,
          Seq("system.builtin", "system.session"),
          unresolvedFunction.origin
        )
    }
  }
}
