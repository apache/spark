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
  Literal
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
 * After resolving the function with [[FunctionResolution.resolveFunction]], performs further
 * resolution in following cases:
 *  - result of [[FunctionResolution.resolveFunction]] is [[ExpressionWithRandomSeed]] in which
 *  case it is necessary to initialize the random seed.
 *  - result of [[FunctionResolution.resolveFunction]] is [[InheritAnalysisRules]], in which case
 *    it is necessary to resolve its replacement expression.
 *  - result of [[FunctionResolution.resolveFunction]] is [[AggregateExpression]] or
 *  [[BinaryArithmetic]], in which case it is necessary to perform further resolution and checks on
 *  its children.
 *
 * Finally apply type coercion to the result of previous step and in case that the resulting
 * expression is [[TimeZoneAwareExpression]], apply timezone.
 */
class FunctionResolver(
    expressionResolver: ExpressionResolver,
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver,
    functionResolution: FunctionResolution,
    aggregateExpressionResolver: AggregateExpressionResolver,
    binaryArithmeticResolver: BinaryArithmeticResolver)
    extends TreeNodeResolver[UnresolvedFunction, Expression]
    with ProducesUnresolvedSubtree {

  private val random = new Random()

  private val typeCoercionResolver: TypeCoercionResolver =
    new TypeCoercionResolver(timezoneAwareExpressionResolver)
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
   *  - If the result from previous step is an [[AggregateExpression]], [[BinaryArithmetic]] or
   *    [[InheritAnalysisRules]], perform further resolution on its children. If the result is an
   *    [[ExpressionWithRandomSeed]], initialize the seed.
   *  - Apply [[TypeCoercion]] rules to the result of previous step. In case that the resulting
   *    expression of the previous step is [[BinaryArithmetic]], skip this one as type coercion is
   *    already applied.
   *  - Apply timezone, if the resulting expression is [[TimeZoneAwareExpression]].
   */
  override def resolve(unresolvedFunction: UnresolvedFunction): Expression = {
    val expressionInfo = functionResolution.lookupBuiltinOrTempFunction(
      unresolvedFunction.nameParts, Some(unresolvedFunction)
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
        withNewSeed
      case inheritAnalysisRules: InheritAnalysisRules =>
        // Since this [[InheritAnalysisRules]] node is created by
        // [[FunctionResolution.resolveFunction]], we need to re-resolve its replacement
        // expression.
        val resolvedInheritAnalysisRules =
          withResolvedChildren(inheritAnalysisRules, expressionResolver.resolve _)
        typeCoercionResolver.resolve(resolvedInheritAnalysisRules)
      case aggregateExpression: AggregateExpression =>
        // In case `functionResolution.resolveFunction` produces a `AggregateExpression` we
        // need to apply further resolution which is done in the
        // `AggregateExpressionResolver`.
        val resolvedAggregateExpression = aggregateExpressionResolver.resolve(aggregateExpression)
        typeCoercionResolver.resolve(resolvedAggregateExpression)
      case binaryArithmetic: BinaryArithmetic =>
        // In case `functionResolution.resolveFunction` produces a `BinaryArithmetic` we
        // need to apply further resolution which is done in the `BinaryArithmeticResolver`.
        //
        // Examples for this case are following (SQL and Dataframe):
        //  - {{{ SELECT `+`(1,2); }}}
        //  - df.select(1+2)
        binaryArithmeticResolver.resolve(binaryArithmetic)
      case other =>
        typeCoercionResolver.resolve(other)
    }

    timezoneAwareExpressionResolver.withResolvedTimezone(
      resolvedFunction,
      conf.sessionLocalTimeZone
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
