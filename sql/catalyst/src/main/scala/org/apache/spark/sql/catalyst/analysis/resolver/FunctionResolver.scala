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

import java.util.Locale

import scala.util.Random

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{
  FunctionResolution,
  ResolvedStar,
  UnresolvedFunction,
  UnresolvedSeed,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  BinaryArithmetic,
  Expression,
  ExpressionWithRandomSeed,
  InheritAnalysisRules,
  Literal,
  TryEval
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

/**
 * A resolver for [[UnresolvedFunction]]s that resolves functions to concrete [[Expression]]s.
 * It resolves the children of the function first by calling [[ExpressionResolver.resolve]] on them
 * if they are not [[UnresolvedStar]]s. If the children are [[UnresolvedStar]]s, it resolves them
 * using [[ExpressionResolver.resolveStar]]. Examples are following:
 *
 *  - Function doesn't contain any [[UnresolvedStar]]:
 *  {{{ SELECT ARRAY(col1) FROM VALUES (1); }}}
 *  it is resolved only using [[ExpressionResolver.resolve]].
 *  - Function contains [[UnresolvedStar]]:
 *  {{{ SELECT ARRAY(*) FROM VALUES (1); }}}
 *  it is resolved using [[ExpressionResolver.resolveStar]].
 *
 * After resolving the function with [[FunctionResolution.resolveFunction]] specific expression
 * nodes require further resolution. See [[resolve]] for more details.
 *
 * Finally apply type coercion to the result of previous step and in case that the resulting
 * expression is [[TimeZoneAwareExpression]], apply timezone.
 */
class FunctionResolver(
    expressionResolver: ExpressionResolver,
    functionResolution: FunctionResolution,
    aggregateExpressionResolver: AggregateExpressionResolver,
    binaryArithmeticResolver: BinaryArithmeticResolver)
    extends TreeNodeResolver[UnresolvedFunction, Expression]
    with ProducesUnresolvedSubtree
    with CoercesExpressionTypes {

  private val random = new Random()
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack

  /**
   * Main method used to resolve an [[UnresolvedFunction]]. It resolves it in the following steps:
   *  - Check if the `unresolvedFunction` is an aggregate expression. Set
   *    `resolvingTreeUnderAggregateExpression` to `true` in that case so we can properly resolve
   *    attributes in ORDER BY and HAVING.
   *  - If the function is `count(*)` it is replaced with `count(1)` (please check
   *    [[normalizeCountExpression]] documentation for more details). Otherwise, we resolve the
   *    children of it.
   *  - Resolve the function using [[FunctionResolution.resolveFunction]].
   *  - Perform further resolution on specific nodes:
   *    - Initialize the seed of [[InheritAnalysisRules]];
   *    - Resolve the replacement expression of [[InheritAnalysisRules]];
   *    - Type coerce [[TryEval]]'s child. Copying tags is specifically important for
   *      [[FunctionRegistry.FUNC_ALIAS]];
   *    - Use [[AggregateExpressionResolver]] to resolve [[AggregateExpression]];
   *    - Use [[BinaryArithmeticResolver]] to resolve [[BinaryArithmetic]]. Specifically important
   *      for cases like:
   *      - {{{ SELECT `+`(1,2); }}}
   *      - df.select(1+2)
   *  - Apply [[TypeCoercion]] rules to the result of previous step. In case that the resulting
   *    expression of the previous step is [[BinaryArithmetic]], skip this one as type coercion is
   *    already applied.
   *  - Apply timezone, if the resulting expression is [[TimeZoneAwareExpression]].
   */
  override def resolve(unresolvedFunction: UnresolvedFunction): Expression = {
    val expressionInfo = functionResolution.lookupBuiltinOrTempFunction(
      unresolvedFunction.nameParts,
      Some(unresolvedFunction)
    )
    if (expressionInfo.exists(_.getGroup == "agg_funcs")) {
      expressionResolutionContextStack.peek().resolvingTreeUnderAggregateExpression = true
    }

    val functionWithResolvedChildren =
      if (isCountStarExpansionAllowed(unresolvedFunction)) {
        normalizeCountExpression(unresolvedFunction)
      } else {
        withResolvedChildren(unresolvedFunction, expressionResolver.resolve _)
          .asInstanceOf[UnresolvedFunction]
      }

    var resolvedFunction = functionResolution.resolveFunction(functionWithResolvedChildren)

    resolvedFunction = resolvedFunction match {
      case expressionWithRandomSeed: ExpressionWithRandomSeed
          if expressionWithRandomSeed.seedExpression == UnresolvedSeed =>
        val withNewSeed: ExpressionWithRandomSeed = expressionWithRandomSeed
          .withNewSeed(random.nextLong())
          .asInstanceOf[ExpressionWithRandomSeed]
        if (!withNewSeed.seedExpression.foldable) {
          throwSeedExpressionIsUnfoldable(expressionWithRandomSeed)
        }
        coerceExpressionTypes(
          expression = withNewSeed,
          expressionTreeTraversal = traversals.current
        )
      case inheritAnalysisRules: InheritAnalysisRules =>
        val resolvedInheritAnalysisRules =
          withResolvedChildren(inheritAnalysisRules, expressionResolver.resolve _)
        coerceExpressionTypes(
          expression = resolvedInheritAnalysisRules,
          expressionTreeTraversal = traversals.current
        )
      case tryEval: TryEval =>
        val coercedTryEval = tryEval.copy(
          child = coerceExpressionTypes(
            expression = tryEval.child,
            expressionTreeTraversal = traversals.current
          )
        )
        coercedTryEval.copyTagsFrom(tryEval)
        coercedTryEval
      case aggregateExpression: AggregateExpression =>
        val resolvedAggregateExpression =
          aggregateExpressionResolver.resolveWithoutRecursingIntoChildren(aggregateExpression)
        coerceExpressionTypes(
          expression = resolvedAggregateExpression,
          expressionTreeTraversal = traversals.current
        )
      case binaryArithmetic: BinaryArithmetic =>
        binaryArithmeticResolver.resolve(binaryArithmetic)
      case other =>
        coerceExpressionTypes(expression = other, expressionTreeTraversal = traversals.current)
    }

    TimezoneAwareExpressionResolver.resolveTimezone(
      resolvedFunction,
      traversals.current.sessionLocalTimeZone
    )
  }

  private def isCountStarExpansionAllowed(unresolvedFunction: UnresolvedFunction): Boolean =
    unresolvedFunction.arguments match {
      case Seq(UnresolvedStar(None)) => isCount(unresolvedFunction)
      case Seq(_: ResolvedStar) => isCount(unresolvedFunction)
      case _ => false
    }

  /**
   * Method used to determine whether the given function should be replaced with another one.
   */
  private def isCount(unresolvedFunction: UnresolvedFunction): Boolean = {
    !unresolvedFunction.isDistinct &&
    unresolvedFunction.nameParts.length == 1 &&
    unresolvedFunction.nameParts.head.toLowerCase(Locale.ROOT) == "count"
  }

  /**
   * Method used to replace the `count(*)` function with `count(1)` function. Resolution of the
   * `count(*)` is done in the following way:
   *  - SQL: It is done during the construction of the AST (in [[AstBuilder]]).
   *  - Dataframes: It is done during the analysis phase and that's why we need to do it here.
   */
  private def normalizeCountExpression(
      unresolvedFunction: UnresolvedFunction): UnresolvedFunction = {
    unresolvedFunction.copy(
      nameParts = Seq("count"),
      arguments = Seq(Literal(1)),
      filter = unresolvedFunction.filter
    )
  }

  private def throwSeedExpressionIsUnfoldable(expressionWithRandomSeed: ExpressionWithRandomSeed) =
    throw new AnalysisException(
      errorClass = "SEED_EXPRESSION_IS_UNFOLDABLE",
      messageParameters = Map(
        "seedExpr" -> toSQLExpr(expressionWithRandomSeed.seedExpression),
        "exprWithSeed" -> toSQLExpr(expressionWithRandomSeed)
      )
    )
}
