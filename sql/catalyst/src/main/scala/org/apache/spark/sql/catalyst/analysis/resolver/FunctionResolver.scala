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

import scala.util.Random

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{
  FunctionResolution,
  SQLFunctionExpression,
  UnresolvedFunction,
  UnresolvedSeed
}
import org.apache.spark.sql.catalyst.expressions.{
  BinaryArithmetic,
  Collate,
  CreateNamedStruct,
  Expression,
  ExpressionInfo,
  ExpressionWithRandomSeed,
  Grouping,
  GroupingID,
  InheritAnalysisRules,
  ResolvedCollation,
  TryEval,
  UnresolvedCollation,
  WindowFunction
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.util.CollationFactory

/**
 * A resolver for [[UnresolvedFunction]]s that resolves functions to concrete [[Expression]]s.
 * In case the function is a higher order function, it delegates the resolution to
 * [[HigherOrderFunctionResolver]]. Otherwise, the resolution is done in the [[resolveFunction]]
 * method.
 * It resolves the children of the function first by calling [[ExpressionResolver.resolve]] on them
 * if they are not [[UnresolvedStar]]s. If the children are [[Star]]s, it expands them using
 * [[ExpressionResolver.expandStarExpressions]]. Examples are following:
 *
 *  - Function doesn't contain any [[Star]]:
 *  {{{ SELECT ARRAY(col1) FROM VALUES (1); }}}
 *  it is resolved only using [[ExpressionResolver.resolve]].
 *  - Function contains [[Star]]:
 *  {{{ SELECT ARRAY(*) FROM VALUES (1); }}}
 *  it first expands all stars using [[ExpressionResolver.expandStarExpressions]] and only after
 *  that calls [[ExpressionResolver.resolve]].
 *
 * After resolving the function with [[FunctionResolution.resolveFunction]] specific expression
 * nodes require further resolution. See [[resolve]] for more details.
 *
 * Finally apply type coercion to the result of previous step and in case that the resulting
 * expression is [[TimeZoneAwareExpression]], apply timezone.
 */
class FunctionResolver(
    protected val expressionResolver: ExpressionResolver,
    protected val functionResolution: FunctionResolution,
    aggregateExpressionResolver: AggregateExpressionResolver,
    binaryArithmeticResolver: BinaryArithmeticResolver)
    extends TreeNodeResolver[UnresolvedFunction, Expression]
    with ProducesUnresolvedSubtree
    with CoercesExpressionTypes
    with FunctionResolverUtils {

  private val random = new Random()
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack
  private val higherOrderFunctionResolver =
    new HigherOrderFunctionResolver(expressionResolver, functionResolution)

  /**
   * In case the function is a higher order function, it delegates the resolution to
   * [[HigherOrderFunctionResolver]]. Otherwise, the resolution is done in the [[resolveFunction]]
   * method.
   * Examples of higher order functions are (resolved by [[HigherOrderFunctionResolver]]):
   *  - `ARRAY_SORT`
   *  - `MAP_FILTER`
   *  - `MAP_ZIP_WITH`
   * Higher order functions take lambda functions as arguments (which are handled in the
   * [[LambdaFunctionResolver]]).
   * Examples of non-higher order functions are (resolved by [[resolveFunction]]):
   *  - `LOWER`
   *  - `ROUND`
   *  - `COALESCE`
   */
  override def resolve(unresolvedFunction: UnresolvedFunction): Expression = {
    val expressionInfo = functionResolution.lookupBuiltinOrTempFunction(
      nameParts = unresolvedFunction.nameParts,
      unresolvedFunc = Some(unresolvedFunction)
    )

    if (expressionInfo.exists(_.getGroup == "lambda_funcs")) {
      higherOrderFunctionResolver.resolve(unresolvedFunction)
    } else {
      resolveFunction(unresolvedFunction, expressionInfo)
    }
  }

  /**
   * Main method used to resolve an [[UnresolvedFunction]]. It's used only for non-lambda functions
   * (for lambda ones, see [[HigherOrderFunctionResolver]]). It resolves it in the following steps:
   *  - Check if the `unresolvedFunction` is an aggregate expression. Set
   *    `resolvingTreeUnderAggregateExpression` to `true` in that case so we can properly resolve
   *    attributes in ORDER BY and HAVING.
   *  - Increment `windowFunctionNestednessLevel` if `resolvingWindowFunction` is `true`. This
   *    is necessary so that in the case of aggregate expressions in window aggregate functions, the
   *    child aggregate expressions are not treated as [[WindowExpression.windowFunction]]. For
   *    additional context refer to
   *    [[AggregateExpressionResolver.handleAggregateExpressionWithChildrenResolved]]. This is also
   *    necessary for handling `WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE` exception.
   *  - Expand all star `*` arguments in the function (see [[handleStarInArguments]] documentation
   *    for more details).
   *  - Resolve children of the function.
   *  - Resolve the function using [[FunctionResolution.resolveFunction]] and reject the function
   *    types the single-pass Analyzer does not support yet (see [[checkSupportedFunctionType]]).
   *  - Perform further resolution and validation on specific nodes:
   *    - For [[Collate]], resolve its [[UnresolvedCollation]] to [[ResolvedCollation]]
   *      and perform type coercion.
   *    - In case [[ExpressionWithRandomSeed.seedExpression]] is not foldable, throw
   *      `SEED_EXPRESSION_IS_UNFOLDABLE` exception.
   *    - Initialize the seed of [[ExpressionWithRandomSeed]];
   *    - Resolve the replacement expression of [[InheritAnalysisRules]];
   *    - Type coerce [[TryEval]]'s child. Copying tags is specifically important for
   *      [[FunctionRegistry.FUNC_ALIAS]];
   *    - Use [[AggregateExpressionResolver]] to resolve [[AggregateExpression]];
   *    - Use [[BinaryArithmeticResolver]] to resolve [[BinaryArithmetic]]. Specifically important
   *      for cases like:
   *      - {{{ SELECT `+`(1,2); }}}
   *      - df.select(1+2)
   *    - Throw `WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE` exception in case the `resolvedFunction`
   *      is [[WindowFunction]] but is not a direct child of [[WindowExpression]]. This check is
   *      performed by assessing `windowFunctionNestednessLevel` value, for example:
   *      - {{{ SELECT LAG() FROM VALUES 1, 2; }}}
   *        When resolving `LAG()` `windowFunctionNestednessLevel` is `0`, signaling we are not
   *        resolving an expression in [[WindowExpression]] expression tree.
   *      - {{{ SELECT SUM(RANK()) OVER (ORDER BY col1) FROM VALUES 1, 2; }}}
   *        When resolving `RANK()` `windowFunctionNestednessLevel` is `2`, signaling we are
   *        resolving a function nested in [[WindowExpression.windowFunction]], indicating a missing
   *        over clause.
   *      - Consequently, if `windowFunctionNestednessLevel != 1` we are resolving something
   *        other than [[WindowExpression]]'s function child which implies a missing over clause.
   *    - In case the `resolvedFunction` is [[Grouping]] or [[GroupingID]], set
   *      `hasGroupingAnalyticsExpression` to `true` in the current [[ExpressionResolutionContext]].
   *    - For [[CreateNamedStruct]], apply type coercion to each child individually, since
   *      [[DefaultCollationTypeCoercion]] applied to [[CreateNamedStruct]] doesn't recurse into
   *      children. This is needed so that default collation is applied to name literals created
   *      by [[CreateStruct.apply]].
   *  - Apply [[TypeCoercion]] rules to the result of previous step. In case that the resulting
   *    expression of the previous step is [[BinaryArithmetic]], skip this one as type coercion is
   *    already applied.
   *  - Apply timezone, if the resulting expression is [[TimeZoneAwareExpression]].
   */
  private def resolveFunction(
      unresolvedFunction: UnresolvedFunction,
      expressionInfo: Option[ExpressionInfo]): Expression = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()

    if (isAggregateFunction(expressionInfo)) {
      expressionResolutionContext.resolvingTreeUnderAggregateExpression = true
    }

    if (expressionResolutionContext.resolvingWindowFunction) {
      expressionResolutionContext.windowFunctionNestednessLevel += 1
    }

    val unresolvedFunctionWithExpandedStarArgs = handleStarInArguments(unresolvedFunction)

    val functionWithResolvedChildren =
      withResolvedChildren(
        unresolvedExpression = unresolvedFunctionWithExpandedStarArgs,
        resolveChild = expressionResolver.resolve _
      ).asInstanceOf[UnresolvedFunction]

    withResolvedSubtreeWithManualTagCleanup(
      functionWithResolvedChildren,
      handlePartiallyResolvedFunction _
    ) {
      val partiallyResolved = functionResolution.resolveFunction(functionWithResolvedChildren)
      checkSupportedFunctionType(expressionInfo, partiallyResolved)
      partiallyResolved
    }
  }

  /**
   * Rejects a resolved function single-pass does not support, throwing
   * [[ExplicitlyUnsupportedResolverFeature]]: a builtin/session function ([[Some]]) whose source is
   * neither `sql_udf`, `built-in` nor empty (e.g. a temp Scala or Python UDF), or a catalog
   * function ([[None]]) that did not resolve to a [[SQLFunctionExpression]], since a catalog
   * function can resolve to an arbitrary expression via a custom builder.
   * [[SQLFunctionExpression]]s are rejected later in [[handlePartiallyResolvedFunction]].
   *
   * An empty source is accepted because it is not a UDF: functions injected through
   * [[org.apache.spark.sql.SparkSessionExtensions]] and builtin expressions without an
   * [[ExpressionDescription]] are registered with an empty source.
   *
   * [[ResolverGuard]] also screens non-SQL session functions before resolution, so the [[Some]]
   * branch is redundant in the full pipeline; it stays as the resolver's own fail-safe so a direct
   * resolution without the guard still rejects an unsupported function rather than resolving it.
   */
  private def checkSupportedFunctionType(
      expressionInfo: Option[ExpressionInfo],
      partiallyResolved: Expression): Unit = expressionInfo match {
    case Some(info)
        if info.getSource == "sql_udf" || info.getSource == "built-in" ||
          info.getSource.isEmpty =>
    case Some(info) =>
      throw new ExplicitlyUnsupportedResolverFeature(
        ResolutionGap.NonSQLFunction,
        s"non-SQL function (source: '${info.getSource}')"
      )
    case None =>
      partiallyResolved match {
        case _: SQLFunctionExpression =>
        case _ =>
          throw new ExplicitlyUnsupportedResolverFeature(
            ResolutionGap.NonSQLCatalogFunction(partiallyResolved.getClass)
          )
      }
  }

  private def handlePartiallyResolvedFunction(partiallyResolvedFunction: Expression): Expression = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()
    val resolvedFunction = partiallyResolvedFunction match {
      case _: SQLFunctionExpression =>
        throw new ExplicitlyUnsupportedResolverFeature(ResolutionGap.SQLFunctions)
      case collate @ Collate(_, collation: UnresolvedCollation) =>
        val withResolvedCollation = collate.copy(
          collation = ResolvedCollation(
            CollationFactory.resolveFullyQualifiedName(collation.collationName.toArray)
          )
        )
        withResolvedCollation.copyTagsFrom(collate)
        coerceExpressionTypes(
          expression = withResolvedCollation,
          expressionTreeTraversal = traversals.current
        )
      case expressionWithRandomSeed: ExpressionWithRandomSeed
          if !expressionWithRandomSeed.seedExpression.foldable &&
          expressionWithRandomSeed.seedExpression != UnresolvedSeed =>
        throwSeedExpressionIsUnfoldable(expressionWithRandomSeed)
      case expressionWithRandomSeed: ExpressionWithRandomSeed
          if expressionWithRandomSeed.seedExpression == UnresolvedSeed =>
        val withNewSeed: ExpressionWithRandomSeed = expressionWithRandomSeed
          .withNewSeed(random.nextLong())
          .asInstanceOf[ExpressionWithRandomSeed]
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
      case windowFunction: WindowFunction
          if (expressionResolutionContext.windowFunctionNestednessLevel != 1) =>
        throwWindowFunctionWithoutOverClause(windowFunction)
      case grouping @ (_: Grouping | _: GroupingID) =>
        expressionResolutionContextStack.peek().hasGroupingAnalyticsExpression = true
        coerceExpressionTypes(expression = grouping, expressionTreeTraversal = traversals.current)
      case createNamedStruct: CreateNamedStruct =>
        val coercedChildren = createNamedStruct.children.map(
          coerceExpressionTypes(_, traversals.current))
        createNamedStruct.withNewChildren(coercedChildren)
      case other =>
        coerceExpressionTypes(expression = other, expressionTreeTraversal = traversals.current)
    }

    TimezoneAwareExpressionResolver.resolveTimezone(
      resolvedFunction,
      traversals.current.sessionLocalTimeZone
    )
  }

  /**
   * Returns true if the function is a true aggregate function whose children should be resolved
   * with aggregate-expression context. Grouping and GroupingID are classified under "agg_funcs"
   * for documentation purposes but are not real aggregates - they are Unevaluable placeholders
   * rewritten into spark_grouping_id bit-extraction expressions.
   */
  private def isAggregateFunction(expressionInfo: Option[ExpressionInfo]): Boolean = {
    expressionInfo.exists { info =>
      info.getGroup == "agg_funcs" &&
        info.getClassName != classOf[Grouping].getName &&
        info.getClassName != classOf[GroupingID].getName
    }
  }

  private def throwWindowFunctionWithoutOverClause(windowFunction: WindowFunction) =
    throw new AnalysisException(
      errorClass = "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE",
      messageParameters = Map("funcName" -> toSQLExpr(windowFunction))
    )

  private def throwSeedExpressionIsUnfoldable(expressionWithRandomSeed: ExpressionWithRandomSeed) =
    throw new AnalysisException(
      errorClass = "SEED_EXPRESSION_IS_UNFOLDABLE",
      messageParameters = Map(
        "seedExpr" -> toSQLExpr(expressionWithRandomSeed.seedExpression),
        "exprWithSeed" -> toSQLExpr(expressionWithRandomSeed)
      )
    )
}
