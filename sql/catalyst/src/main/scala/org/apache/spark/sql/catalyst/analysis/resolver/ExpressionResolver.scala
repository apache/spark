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

import java.util.{ArrayDeque, HashMap, HashSet}

import scala.annotation.nowarn
import scala.collection.compat.immutable.LazyList
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.databricks.spark.util.BehaviorChangeLogging
import com.databricks.sql.BehaviorChangeConf.{
  SC116075_NAME_COLLISION_ON_OUTER_REFERENCE_AND_LCA,
  SC128052_RELYING_ON_DERIVED_COLUMN_ALIAS
}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  FunctionResolution,
  GetViewColumnByNameAndOrdinal,
  NamedParameter,
  Parameter,
  Star,
  TypeCoercionValidation,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedExtractValue,
  UnresolvedFunction,
  UnresolvedHaving,
  UnresolvedOrdinal,
  UpCastResolution
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Filter,
  LogicalPlan,
  Pivot,
  Project,
  RepartitionByExpression,
  Sort,
  Unpivot
}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.{CollationFactory, GeneratedColumn}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.Utils

/**
 * The [[ExpressionResolver]] is used by the [[Resolver]] during the analysis to resolve
 * expressions.
 *
 * The functions here generally traverse unresolved [[Expression]] nodes recursively,
 * constructing and returning the resolved [[Expression]] nodes bottom-up.
 * This is the primary entry point for implementing expression analysis,
 * wherein the [[resolve]] method accepts a fully unresolved [[Expression]] and returns
 * a fully resolved [[Expression]] in response with all data types and attribute
 * reference ID assigned for valid requests. This resolver also takes responsibility
 * to detect any errors in the initial SQL query or DataFrame and return appropriate
 * error messages including precise parse locations wherever possible.
 *
 * @param resolver [[Resolver]] is passed from the parent to resolve other
 *   operators which are nested in expressions.
 * @param scopes [[NameScopeStack]] to resolve the expression tree in the correct scope.
 * @param functionResolution [[FunctionResolution]] to resolve function expressions.
 * @param planLogger [[PlanLogger]] to log expression tree resolution events.
 */
class ExpressionResolver(
    resolver: Resolver,
    functionResolution: FunctionResolution,
    planLogger: PlanLogger)
    extends TreeNodeResolver[Expression, Expression]
    with ProducesUnresolvedSubtree
    with ResolvesExpressionChildren
    with BehaviorChangeLogging // EDGE
    with CoercesExpressionTypes
    with CollectsWindowSourceExpressions {

  /**
   * This field stores referenced attributes from the most recently resolved expression tree. It is
   * populated in [[resolveExpressionTreeInOperatorImpl]] when [[ExpressionTreeTraversal]] is
   * popped from the stack. It is the responsibility of the parent operator resolver to collect
   * referenced attributes, before this field is overwritten.
   */
  private var lastReferencedAttributes: Option[HashMap[ExprId, Attribute]] = None

  /**
   * This field stores invalid expressions in the context of the parent operator from the most
   * recently resolved expression tree. It is populated in [[resolveExpressionTreeInOperatorImpl]]
   * when [[ExpressionTreeTraversal]] is popped from the stack. It is the responsibility of the
   * parent operator resolver to collect invalid expressions, before this field is overwritten.
   */
  private var lastInvalidExpressionsInTheContextOfOperator: Option[Seq[Expression]] = None

  /**
   * This is a flag indicating that we are re-analyzing a resolved [[OuterReference]] subtree. It's
   * managed by [[handleResolvedOuterReference]].
   */
  private var inOuterReferenceSubtree: Boolean = false

  /**
   * The stack of parent operators which were encountered during the resolution of a certain
   * expression tree. This is filled by the [[resolveExpressionTreeInOperatorImpl]], and will
   * usually have size 1 (the parent for this expression tree). However, in case of subquery
   * expressions we would call [[resolveExpressionTreeInOperatorImpl]] several times recursively
   * for each expression tree in the operator tree -> expression tree -> operator tree ->
   * expression tree -> ... chain. Consider this example:
   *
   * {{{ SELECT (SELECT col1 FROM values(1) LIMIT 1) FROM VALUES(1); }}}
   *
   * Would have the following analyzed tree:
   *
   * Project [...]
   * :  +- GlobalLimit 1
   * :     +- LocalLimit 1
   * :        +- Project [col1]
   * :           +- LocalRelation [col1]
   * +- LocalRelation [col1]
   *
   * The stack would contain the following operators during the resolution of the nested
   * operator/expression trees:
   *
   * Project -> Project
   */
  private val expressionIdAssigner = new ExpressionIdAssigner
  private val autoGeneratedAliasProvider = new AutoGeneratedAliasProvider(expressionIdAssigner)
  private val traversals = new ExpressionTreeTraversalStack
  private val expressionResolutionContextStack = new ArrayDeque[ExpressionResolutionContext]
  private val scopes = resolver.getNameScopes
  private val subqueryRegistry = resolver.getSubqueryRegistry
  protected val windowResolutionContextStack = new WindowResolutionContextStack

  private val aliasResolver = new AliasResolver(this)
  private val timezoneAwareExpressionResolver = new TimezoneAwareExpressionResolver(this)
  private val binaryArithmeticResolver = new BinaryArithmeticResolver(this)
  private val limitLikeExpressionValidator = new LimitLikeExpressionValidator
  private val aggregateExpressionResolver = new AggregateExpressionResolver(resolver, this)
  private val functionResolver = new FunctionResolver(
    this,
    functionResolution,
    aggregateExpressionResolver,
    binaryArithmeticResolver
  )
  private val subqueryExpressionResolver = new SubqueryExpressionResolver(this, resolver)
  private val ordinalResolver = new OrdinalResolver(resolver)
  private val lcaResolver = new LateralColumnAliasResolver(this, resolver)
  private val semiStructuredExtractResolver = new SemiStructuredExtractResolver(this)
  private val windowExpressionResolver = new WindowExpressionResolver(this)
  private val extractValueResolver = new ExtractValueResolver(this)
  private val lambdaFunctionResolver = new LambdaFunctionResolver(this)
  private val operatorResolutionContextStack = resolver.getOperatorResolutionContextStack

  /**
   * Get the expression tree traversal stack.
   */
  def getExpressionTreeTraversals: ExpressionTreeTraversalStack = traversals

  /**
   * Get the expression resolution context stack.
   */
  def getExpressionResolutionContextStack: ArrayDeque[ExpressionResolutionContext] =
    expressionResolutionContextStack

  /**
   * Get the window resolution context stack.
   */
  def getWindowResolutionContextStack: WindowResolutionContextStack =
    windowResolutionContextStack

  def getExpressionIdAssigner: ExpressionIdAssigner = expressionIdAssigner

  /**
   * Get [[NameScopeStack]] bound to the used [[Resolver]].
   */
  def getNameScopes: NameScopeStack = scopes

  /**
   * Get the [[TimezoneAwareExpressionResolver]] to resolve timezone-aware expressions.
   */
  def getTimezoneAwareExpressionResolver: TimezoneAwareExpressionResolver =
    timezoneAwareExpressionResolver

  /**
   * Get the [[LateralColumnAliasResolver]] to resolve lateral column references.
   */
  def getLcaResolver: LateralColumnAliasResolver = lcaResolver

  /**
   * Returns all attributes that have been referenced during the most recent expression tree
   * resolution.
   */
  def getLastReferencedAttributes: HashMap[ExprId, Attribute] =
    lastReferencedAttributes.getOrElse(new HashMap[ExprId, Attribute])

  /**
   * Returns all invalid expressions in the context of the parent operator from the most recent
   * expression tree resolution.
   */
  def getLastInvalidExpressionsInTheContextOfOperator: Seq[Expression] =
    lastInvalidExpressionsInTheContextOfOperator.getOrElse(Seq.empty)

  /**
   * Get the [[AutoGeneratedAliasProvider]] which is used to generate implicit aliases.
   */
  def getAutoGeneratedAliasProvider: AutoGeneratedAliasProvider = autoGeneratedAliasProvider

  /**
   * Get the [[OrdinalResolver]] to resolve ordinals in `ORDER BY` and `GROUP BY` clauses.
   */
  def getOrdinalResolver: OrdinalResolver = ordinalResolver

  /**
   * Resolve `unresolvedExpression` which is a child of `parentOperator`. This is the main entry
   * point into the [[ExpressionResolver]] for operators.
   */
  def resolveExpressionTreeInOperator(
      unresolvedExpression: Expression,
      parentOperator: LogicalPlan): Expression = {
    val (resolvedExpression, _) =
      resolveExpressionTreeInOperatorImpl(unresolvedExpression, parentOperator)
    resolvedExpression
  }

  /**
   * This method is an expression analysis entry point. The method first checks if the expression
   * has already been resolved (necessary because of partially-unresolved subtrees, see
   * [[ProducesUnresolvedSubtree]]). If not already resolved, method takes an unresolved
   * [[Expression]] and chooses the right `resolve*` method using pattern matching on the
   * `unresolvedExpression` type. This pattern matching enumerates all the expression node types
   * that are supported by the single-pass analysis.
   * When developers introduce a new [[Expression]] type to the Catalyst, they should implement
   * a corresponding `resolve*` method in the [[ExpressionResolver]] and add it to this pattern
   * match list.
   *
   * [[resolve]] will be called recursively during the expression tree traversal eventually
   * producing a fully resolved expression subtree or a descriptive error message.
   *
   * [[resolve]] can recursively call `resolver` to resolve nested operators (e.g. scalar
   * subqueries):
   *
   * {{{ SELECT * FROM VALUES (1), (2) WHERE col1 IN (SELECT 1); }}}
   *
   * In this case `IN` is an expression and `SELECT 1` is a nested operator tree for which
   * the [[ExpressionResolver]] would invoke the [[Resolver]].
   *
   * This function avoids wrappers like [[CurrentOrigin.withOrigin]] to avoid deep recursion stacks,
   * because expression trees may be quite deep.
   */
  override def resolve(unresolvedExpression: Expression): Expression = {
    val previousOrigin = CurrentOrigin.get
    CurrentOrigin.set(unresolvedExpression.origin)

    try {
      planLogger.logExpressionTreeResolutionEvent(
        unresolvedExpression,
        "Unresolved expression tree"
      )

      if (tryPopSinglePassSubtreeBoundary(unresolvedExpression)) {
        unresolvedExpression
      } else {
        pushResolutionContext()
        validateStarsInsideExpression(unresolvedExpression)

        val resolvedExpression = unresolvedExpression match {
          case unresolvedAggregateExpression: AggregateExpression =>
            aggregateExpressionResolver.resolve(unresolvedAggregateExpression)
          case unresolvedAggregateFunction: AggregateFunction =>
            resolveExpressionGenericallyWithTypeCoercion(unresolvedAggregateFunction)
          case unresolvedBinaryArithmetic: BinaryArithmetic =>
            binaryArithmeticResolver.resolve(unresolvedBinaryArithmetic)
          case unresolvedDateAddYMInterval: DateAddYMInterval =>
            resolveExpressionGenerically(unresolvedDateAddYMInterval)
          case extractIntervalPart: ExtractIntervalPart[_] =>
            resolveExpressionGenerically(extractIntervalPart)
          case unresolvedNamedExpression: NamedExpression =>
            resolveNamedExpression(unresolvedNamedExpression)
          case unresolvedFunction: UnresolvedFunction =>
            functionResolver.resolve(unresolvedFunction)
          case unresolvedWindowExpression: WindowExpression =>
            windowExpressionResolver.resolve(unresolvedWindowExpression)
          case unresolvedLiteral: Literal =>
            resolveLiteral(unresolvedLiteral)
          case unresolvedOrdinal: UnresolvedOrdinal =>
            ordinalResolver.resolve(unresolvedOrdinal)
          case unresolvedPredicate: Predicate =>
            resolvePredicate(unresolvedPredicate)
          case unresolvedScalarSubquery: ScalarSubquery =>
            subqueryExpressionResolver.resolveScalarSubquery(unresolvedScalarSubquery)
          case unresolvedListQuery: ListQuery =>
            subqueryExpressionResolver.resolveListQuery(unresolvedListQuery)
          case unresolvedTimestampAdd: TimestampAddInterval =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(unresolvedTimestampAdd)
          case unresolvedUnaryMinus: UnaryMinus =>
            resolveExpressionGenericallyWithTypeCoercion(unresolvedUnaryMinus)
          case createNamedStruct: CreateNamedStruct =>
            resolveCreateNamedStruct(createNamedStruct)
          case sortOrder: SortOrder =>
            resolveExpressionGenerically(sortOrder)
          case unresolvedConditionalExpression: ConditionalExpression =>
            resolveExpressionGenericallyWithTypeCoercion(unresolvedConditionalExpression)
          case getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal =>
            resolveGetViewColumnByNameAndOrdinal(getViewColumnByNameAndOrdinal)
          case getTimeField: GetTimeField =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(getTimeField)
          case makeTimestamp: MakeTimestamp =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(makeTimestamp)
          case makeTimestamp: MakeTimestampFromDateTime =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(makeTimestamp)
          case namedParameter: NamedParameter =>
            resolveNamedParameter(namedParameter)
          case unresolvedRuntimeReplaceable: RuntimeReplaceable =>
            resolveExpressionGenericallyWithTypeCoercion(unresolvedRuntimeReplaceable)
          case unresolvedTimezoneExpression: TimeZoneAwareExpression =>
            timezoneAwareExpressionResolver.resolve(unresolvedTimezoneExpression)
          case unresolvedUpCast: UpCast =>
            resolveUpCast(unresolvedUpCast)
          case unresolvedCollation: UnresolvedCollation =>
            resolveCollation(unresolvedCollation)
          case semiStructuredExtract: SemiStructuredExtract =>
            semiStructuredExtractResolver.resolve(semiStructuredExtract)
          case unresolvedExtractValue: UnresolvedExtractValue =>
            extractValueResolver.resolve(unresolvedExtractValue)
          case lambdaFunction: LambdaFunction =>
            lambdaFunctionResolver.resolve(lambdaFunction)
          case expression: Expression =>
            resolveExpressionGenericallyWithTypeCoercion(expression)
        }

        preserveTags(unresolvedExpression, resolvedExpression)
        popResolutionContext()

        withPosition(unresolvedExpression) {
          validateResolvedExpressionGenerically(resolvedExpression)
        }

        planLogger.logExpressionTreeResolution(unresolvedExpression, resolvedExpression)
        resolvedExpression
      }
    } finally {
      CurrentOrigin.set(previousOrigin)
    }
  }

  /**
   * Resolve and validate the limit like expressions from either [[LocalLimit]], [[GlobalLimit]],
   * [[Offset]] or [[Tail]] operator.
   */
  def resolveLimitLikeExpression(
      unresolvedLimitLikeExpr: Expression,
      partiallyResolvedLimitLike: LogicalPlan): Expression = {
    val resolvedLimitLikeExpr = resolveExpressionTreeInOperator(
      unresolvedLimitLikeExpr,
      partiallyResolvedLimitLike
    )
    limitLikeExpressionValidator.validateLimitLikeExpr(
      resolvedLimitLikeExpr,
      partiallyResolvedLimitLike
    )
  }

  /**
   * The [[Project]] list can contain different unresolved expressions before the resolution, which
   * will be resolved using generic [[resolve]]. However, [[Star]] is a special case, because it is
   * expanded into a sequence of [[NamedExpression]]s. Because of that this method returns a
   * sequence and doesn't conform to generic [[resolve]] interface - it's called directly from the
   * [[Resolver]] during [[Project]] resolution.
   *
   * The output sequence can be larger or smaller than the input sequence due to [[Star]] expansion.
   * For example, in the following query the project list will shrink:
   *
   * {{{ SELECT 42, col1.* FROM (SELECT struct() as col1) }}}
   *
   * In the example, the project list `[unresolvedalias(42), col1.*]` will be resolved into
   * `[42 AS 42#0]`
   *
   * @return The list of resolved expressions along with flags indicating whether the resolved
   * project list contains aggregate expressions or attributes (encapsulated in
   * [[ResolvedProjectList]]) which are used during the further resolution of the tree.
   *
   * The following query:
   *
   * {{{ SELECT COUNT(col1), 2 FROM VALUES(1); }}}
   *
   * would have a project list with two expressions: `COUNT(col1)` and `2`. After the resolution it
   * would return the following result:
   * ResolvedProjectList(
   *   expressions = [count(col1) as count(col1), 2 AS 2],
   *   hasAggregateExpressionsOutsideWindow = true, // because it contains `count(col1)` in the
   *                                                // project list
   *   hasLateralColumnAlias = false // because there are no lateral column aliases
   *   hasWindowExpressions = false // because there are no window expressions
   *   hasCorrelatedScalarSubqueryExpressions = false // because there are no correlated scalar
   *                                                  // subquery expressions
   *   has
   * )
   */
  def resolveProjectList(
      sourceUnresolvedProjectList: Seq[NamedExpression],
      operator: LogicalPlan): ResolvedProjectList = {
    val unresolvedProjectList = tryDrainLazySequences(sourceUnresolvedProjectList)

    var hasAggregateExpressionsOutsideWindow = false
    var hasLateralColumnAlias = false
    var hasWindowExpressions = false
    var hasCorrelatedScalarSubqueryExpressions = false

    val unresolvedProjectListWithStarsExpanded = traversals.withNewTraversal(operator) {
      expandStarExpressions(unresolvedProjectList)
    }

    val resolvedProjectList = unresolvedProjectListWithStarsExpanded.flatMap { expression =>
      val (resolvedElement, resolvedElementContext) = {
        resolveExpressionTreeInOperatorImpl(
          expression,
          operator,
          shouldPreserveAlias = true
        )
      }

      hasAggregateExpressionsOutsideWindow |=
      resolvedElementContext.hasAggregateExpressionsOutsideWindow
      hasLateralColumnAlias |= resolvedElementContext.hasLateralColumnAlias
      hasWindowExpressions |= resolvedElementContext.hasWindowExpressions
      hasCorrelatedScalarSubqueryExpressions |=
      resolvedElementContext.hasCorrelatedScalarSubqueryExpressions

      Seq(resolvedElement.asInstanceOf[NamedExpression])
    }

    ResolvedProjectList(
      expressions = resolvedProjectList,
      hasAggregateExpressionsOutsideWindow = hasAggregateExpressionsOutsideWindow,
      hasLateralColumnAlias = hasLateralColumnAlias,
      hasWindowExpressions = hasWindowExpressions,
      hasCorrelatedScalarSubqueryExpressions = hasCorrelatedScalarSubqueryExpressions,
      aggregateListAliases = Seq.empty
    )
  }

  /**
   * Used to resolve [[Pivot.aggregates]] expressions. Uses `resolveExpressionTreeInOperatorImpl`
   * with the `resolvingPivotAggregates` flag set to true.
   */
  def resolvePivotAggregates(pivot: Pivot): Seq[Expression] = {
    pivot.aggregates.map { expression =>
      val (resolved, _) = resolveExpressionTreeInOperatorImpl(
        unresolvedExpression = expression,
        parentOperator = pivot,
        resolvingPivotAggregates = true
      )
      resolved
    }
  }

  /**
   * Resolve [[Unpivot.values]] or [[Unpivot.ids]] expressions. This method first expands [[Star]]
   * expressions, then resolves each expression using [[resolveExpressionTreeInOperatorImpl]].
   * We set the `shouldPreserveAlias` flag to true since both [[Unpivot.values]] and
   * [[Unpivot.ids]] are sequences of [[NamedExpression]]s.
   */
  def resolveUnpivotArguments(
      arguments: Seq[Expression],
      unpivot: LogicalPlan): Seq[NamedExpression] = {
    val argumentsWithStarsExpanded = traversals.withNewTraversal(unpivot) {
      expandStarExpressions(arguments)
    }

    argumentsWithStarsExpanded.map { argument =>
      val (resolvedExpression, _) = resolveExpressionTreeInOperatorImpl(
        parentOperator = unpivot,
        unresolvedExpression = argument,
        shouldPreserveAlias = true
      )
      resolvedExpression.asInstanceOf[NamedExpression]
    }
  }

  /**
   * Resolve aggregate expressions in [[Aggregate]] operator.
   *
   * The [[Aggregate]] list can contain different unresolved expressions before the resolution,
   * which will be resolved using generic [[resolve]]. However, [[Star]] is a special case, because
   * it is expanded into a sequence of [[NamedExpression]]s. Because of that this method returns a
   * sequence and doesn't conform to generic [[resolve]] interface - it's called directly from the
   * [[AggregateResolver]] during [[Aggregate]] resolution.
   *
   * Besides resolution, we do the following:
   *   - If there is a [[Star]] in the list we set `hasStar` to true in order to throw
   *     if there are any ordinals in grouping expressions.
   *     Example of an invalid query:
   *     {{{ SELECT * FROM VALUES(1) GROUP BY 1; }}}
   *
   *   - If there is an expression which has aggregate function in its subtree, we add it to the
   *     `expressionsWithAggregateFunctions` list in order to throw if there is any ordinal in
   *     grouping expressions which references that aggregate expression.
   *     Example of an invalid query:
   *     {{{ SELECT count(col1) FROM VALUES(1) GROUP BY 1; }}}
   *
   *   - If the resolved expression is an [[Alias]], add it to
   *     `scopes.current.topAggregateExpressionsByAliasName` so it can be used for grouping
   *     expressions resolution, if needed.
   *     Example of a query with an [[Alias]]:
   *       1. Implicit alias:
   *          {{{ SELECT col1 + col2 FROM VALUES(1, 2) GROUP BY `(col1 + col2)`; }}}
   *       2. Explicit alias:
   *          {{{ SELECT 1 AS column GROUP BY column; }}}
   *
   * While resolving the list, we have to keep track of all the expressions that don't have
   * [[AggregateExpression]]s in their subtrees (`expressionsWithoutAggregates`) and whether any of
   * aggregate expressions (that are not `expressionsWithoutAggregates`) has attributes in the
   * subtree outside of [[AggregateExpressions]]s (`hasAttributeOutsideOfAggregateExpressions`).
   * This is used when resolving `GROUP BY ALL` in the [[AggregateResolver.resolveGroupByAll]].
   *
   * @returns List of resolved expressions, list of expressions that don't have
   *          [[AggregateExpression]] in their subtrees, if any of resolved expressions have
   *          attributes in the subtree that are not under an [[AggregateExpression]], if any of
   *          expressions is a star (`*`) and list of indices of expressions that have aggregate
   *          functions in the subtree encapsulated in [[ResolvedAggregateExpressions]].
   */
  def resolveAggregateExpressions(
      sourceUnresolvedAggregateExpressions: Seq[NamedExpression],
      unresolvedAggregate: Aggregate): ResolvedAggregateExpressions = {
    val unresolvedAggregateExpressions = tryDrainLazySequences(sourceUnresolvedAggregateExpressions)

    val expressionsWithoutAggregates = new mutable.ArrayBuffer[NamedExpression]
    val expressionIndexesWithAggregateFunctions = new HashSet[Int]
    var hasAttributeOutsideOfAggregateExpressions = false
    var hasStar = false
    var hasLateralColumnAlias = false
    var hasWindowExpressions = false
    var hasCorrelatedScalarSubqueryExpressions = false

    val unresolvedAggregateExpressionsWithStarsExpanded =
      traversals.withNewTraversal(unresolvedAggregate) {
        unresolvedAggregateExpressions.flatMap {
          case star: Star =>
            hasStar = true
            expandStar(star)
          case other => Seq(other)
        }
      }

    val resolvedAggregateExpressions =
      unresolvedAggregateExpressionsWithStarsExpanded.zipWithIndex.flatMap {
        case (expression, index) =>
          val (resolvedElement, resolvedElementContext) = resolveExpressionTreeInOperatorImpl(
            expression,
            unresolvedAggregate,
            shouldPreserveAlias = true
          )

          hasWindowExpressions |= resolvedElementContext.hasWindowExpressions
          hasLateralColumnAlias |= resolvedElementContext.hasLateralColumnAlias
          hasCorrelatedScalarSubqueryExpressions =
            resolvedElementContext.hasCorrelatedScalarSubqueryExpressions

          resolvedElement match {
            case alias: Alias =>
              scopes.current.addTopAggregateExpression(alias)
            case other =>
          }

          if (resolvedElementContext.hasAggregateExpressionsOutsideWindow) {
            expressionIndexesWithAggregateFunctions.add(index)
            hasAttributeOutsideOfAggregateExpressions |=
            resolvedElementContext.hasAttributeOutsideOfAggregateExpressions
          } else {
            expressionsWithoutAggregates += resolvedElement.asInstanceOf[NamedExpression]
          }

          Seq(resolvedElement.asInstanceOf[NamedExpression])
      }

    ResolvedAggregateExpressions(
      expressions = resolvedAggregateExpressions,
      resolvedExpressionsWithoutAggregates = expressionsWithoutAggregates.toSeq,
      hasAttributeOutsideOfAggregateExpressions = hasAttributeOutsideOfAggregateExpressions,
      hasStar = hasStar,
      expressionIndexesWithAggregateFunctions = expressionIndexesWithAggregateFunctions,
      hasLateralColumnAlias = hasLateralColumnAlias,
      hasWindowExpressions = hasWindowExpressions,
      hasCorrelatedScalarSubqueryExpressions = hasCorrelatedScalarSubqueryExpressions
    )
  }

  /**
   * Resolve grouping expressions in [[Aggregate]] operator.
   *
   * It's done using `resolveExpressionTreeInOperatorImpl` for every expression besides
   * [[BaseGroupingSets]] for which we call it on its children. See
   * `resolveGroupingExpressionsElement` doc for more info.
   */
  def resolveGroupingExpressions(
      sourceUnresolvedGroupingExpressions: Seq[Expression],
      unresolvedAggregate: Aggregate): Seq[Expression] = {
    val unresolvedGroupingExpressions = tryDrainLazySequences(sourceUnresolvedGroupingExpressions)

    unresolvedGroupingExpressions.map {
      case baseGroupingSets: BaseGroupingSets =>
        val newChildren = baseGroupingSets.children.map { expression =>
          resolveGroupingExpressionsElement(expression, unresolvedAggregate)
        }
        baseGroupingSets.withNewChildren(newChildren)
      case other =>
        resolveGroupingExpressionsElement(other, unresolvedAggregate)
    }
  }

  /**
   * Resolves a single grouping expression element in [[Aggregate]] operator:
   *  - If the expression can be resolved using the child's output (`scopes.current.output`),
   *    resolve it that way.
   *    Example:
   *    {{{ SELECT col1 FROM VALUES(1) GROUP BY `col1`; }}}
   *
   *  - If not, try to resolve it as a top level [[Alias]] (which was populated during the
   *    resolution of the aggregate expressions).
   *    Example:
   *    1. Group by implicit alias
   *    {{{ SELECT concat_ws(' ', 'a', 'b') GROUP BY `concat_ws( , a, b)`; }}}
   *    2. Group by explicit alias
   *    {{{ SELECT col1 AS column_1 FROM VALUES(1) GROUP BY column_1; }}}
   *
   * After resolving the expression, remove the top level [[Alias]] if it exists.
   */
  private def resolveGroupingExpressionsElement(
      expression: Expression,
      aggregate: Aggregate): Expression = {
    val (resolvedExpression, _) = resolveExpressionTreeInOperatorImpl(
      unresolvedExpression = expression,
      parentOperator = aggregate,
      resolvingGroupingExpressions = true
    )

    resolvedExpression match {
      case alias: Alias =>
        alias.child
      case other => other
    }
  }

  /**
   * Validate if `expression` is under supported operator or not. In case it's not, add `expression`
   * to the [[ExpressionTreeTraversal.invalidExpressionsInTheContextOfOperator]] list to throw
   * error later, when [[getLastInvalidExpressionsInTheContextOfOperator]] is called by the
   * [[Resolver]]. Here, we avoid adding [[AggregateExpressions]] when they are under a [[Sort]] or
   * [[Filter]] on top of [[Aggregate]] as they are not transformed to attributes at the moment.
   * Please see [[UnsupportedExpressionInOperatorValidation.isExpressionInUnsupportedOperator]] for
   * more info.
   */
  def validateExpressionUnderSupportedOperator(expression: Expression): Unit = {
    if (UnsupportedExpressionInOperatorValidation.isExpressionInUnsupportedOperator(
        expression = expression,
        operator = traversals.current.parentOperator,
        isFilterOnTopOfAggregate = traversals.current.isFilterOnTopOfAggregate,
        isSortOnTopOfAggregate = traversals.current.isSortOnTopOfAggregate
      )) {
      traversals.current.invalidExpressionsInTheContextOfOperator.add(expression)
    }
  }

  /**
   * Returns new sequence of expressions with all [[Star]]s expanded.
   */
  def expandStarExpressions(expressions: Seq[Expression]): Seq[Expression] = expressions.flatMap {
    case star: Star => expandStar(star)
    case other => Seq(other)
  }

  private def resolveExpressionTreeInOperatorImpl(
      unresolvedExpression: Expression,
      parentOperator: LogicalPlan,
      shouldPreserveAlias: Boolean = false,
      resolvingGroupingExpressions: Boolean = false,
      resolvingPivotAggregates: Boolean = false
  ): (Expression, ExpressionResolutionContext) = {
    traversals.withNewTraversal(
      parentOperator = parentOperator,
      defaultCollation = resolver.getViewResolver.getViewResolutionContext.flatMap(_.collation),
      isFilterOnTopOfAggregate = isFilterOnTopOfAggregate(parentOperator),
      isSortOnTopOfAggregate = isSortOnTopOfAggregate(parentOperator)
    ) {
      expressionResolutionContextStack.push(
        new ExpressionResolutionContext(
          isRoot = true,
          shouldPreserveAlias = shouldPreserveAlias,
          resolvingGroupingExpressions = resolvingGroupingExpressions,
          resolvingPivotAggregates = resolvingPivotAggregates
        )
      )

      try {
        val resolvedExpression = resolve(unresolvedExpression)

        lastReferencedAttributes = Some(traversals.current.referencedAttributes)
        lastInvalidExpressionsInTheContextOfOperator =
          Some(traversals.current.invalidExpressionsInTheContextOfOperator.asScala.toSeq)

        (resolvedExpression, expressionResolutionContextStack.peek())
      } finally {
        expressionResolutionContextStack.pop()
      }
    }
  }

  private def resolveNamedExpression(unresolvedNamedExpression: Expression): Expression =
    unresolvedNamedExpression match {
      case alias: Alias =>
        aliasResolver.handleResolvedAlias(alias)
      case unresolvedAlias: UnresolvedAlias =>
        aliasResolver.resolve(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        resolveAttribute(unresolvedAttribute)
      case attributeReference: AttributeReference =>
        handleResolvedAttributeReference(attributeReference)
      case outerReference: OuterReference =>
        handleResolvedOuterReference(outerReference)
      case unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable =>
        resolveUnresolvedNamedLambdaVariable(unresolvedNamedLambdaVariable)
      case namedLambdaVariable: NamedLambdaVariable =>
        resolveNamedLambdaVariable(namedLambdaVariable)
      case _ =>
        withPosition(unresolvedNamedExpression) {
          throwUnsupportedSinglePassAnalyzerFeature(unresolvedNamedExpression)
        }
    }

  /**
   * Expand [[Star]] expressions into a sequence of [[NamedExpression]]s. Note that they could be
   * unresolved.
   *
   * [[Star]] resolution relies on the [[NameScope]]'s ability to get the attributes by a multipart
   * name ([[Star]]'s `target` field):
   *
   * - Star target is defined:
   *
   * -- Target is a table:
   * {{{
   * SELECT t.* FROM VALUES (1) AS t;
   * ->
   * Project [col1#0]
   * }}}
   *
   * -- Target is a struct:
   * {{{
   * SELECT s.* FROM (SELECT named_struct('a', 1, 'b', 2) AS s);
   * ->
   * Project [1 AS a#0, 2 AS b#1]
   * }}}
   *
   * - Star target is not defined:
   *
   * {{{
   * SELECT * FROM (SELECT 1 as col1), (SELECT 2 as col2);
   * ->
   * Project [col1#0, col2#1]
   * }}}
   */
  private def expandStar(star: Star): Seq[NamedExpression] =
    withPosition(star) {
      validateStarParentOperator(star)
      scopes.current.expandStar(star)
    }

  /**
   * Throws an exception if the [[Star]] is used under an operator which does not allow it.
   *
   * [[UnresolvedHaving]] is a special case, because it is replaced by [[Filter]] during the
   * analysis. Fixed-point analyzer does not allow `HAVING` with stars, so we block it here
   * explicitly.
   */
  private def validateStarParentOperator(star: Star): Unit = {
    val currentOperator = traversals.current.parentOperator
    val isHaving = operatorResolutionContextStack.current.unresolvedPlan match {
      case Some(_: UnresolvedHaving) => true
      case _ => false
    }
    currentOperator match {
      case _ if isHaving =>
        throw QueryCompilationErrors.invalidStarUsageError(
          prettyName = Utils.getSimpleName(classOf[UnresolvedHaving]),
          stars = Seq(star)
        )
      case _: Project | _: Aggregate | _: Filter | _: Unpivot =>
      case _ =>
        throw QueryCompilationErrors.invalidStarUsageError(
          prettyName = currentOperator.nodeName,
          stars = Seq(star)
        )
    }
  }

  /**
   * Throws an exception if the `expression` contains [[Star]]s in its children,
   * but it is not allowed to have them.
   */
  private def validateStarsInsideExpression(expression: Expression): Unit = expression match {
    case _: UnresolvedFunction | _: In =>
    case createNamedStruct: CreateNamedStruct if allowStarInCreateNamedStruct(createNamedStruct) =>
    case _ if containsStar(expression.children) =>
      throw QueryCompilationErrors.invalidStarUsageError(
        prettyName = s"expression `${expression.prettyName}`",
        stars = expression.children.collect { case star: Star => star }
      )
    case _ =>
  }

  /**
   * [[UnresolvedAttribute]] resolution relies on [[NameScope]] to lookup the attribute by its
   * multipart name. The resolution can result in three different outcomes which are handled in the
   * [[NameTarget.pickCandidate]]:
   *
   * - No results from the [[NameScope]] mean that the attribute lookup failed as in:
   *   {{{ SELECT col1 FROM (SELECT 1 as col2); }}}
   *
   * - Several results from the [[NameScope]] mean that the reference is ambiguous as in:
   *   {{{ SELECT col1 FROM (SELECT 1 as col1), (SELECT 2 as col1); }}}
   *
   * - Single result from the [[NameScope]] means that the attribute was found as in:
   *   {{{ SELECT col1 FROM VALUES (1); }}}
   *
   * If [[NameTarget.lateralAttributeReference]] is defined, it means that we are resolving an
   * attribute that is a lateral column alias reference. In that case we mark the referenced
   * attribute as referenced and tag the LCA attribute for further [[Alias]] resolution. We don't
   * count LCA attributes for `UNRESOLVED_GROUP_BY_ALL` validation because in the fixed-point
   * implementation the `ALL` keyword resolution is done before the LCA. For example in the query:
   * {{{ SELECT COUNT(1) as a, SUM(1) + a GROUP BY ALL; }}}
   * Aggregate expressions list before `ALL` resolution looks like:
   * [count(1) as `a`, sum(1) + lateralAliasReference(a) as `sum(1) + lateralAliasReference(a)`]
   * `lateralAliasReference` node is not an attribute and thus we successfully resolve the grouping
   * expression.
   *
   * Check whether the result of the previous step is [[Attribute]] and if so, throw if we are
   * resolving [[Pivot.aggregates]] and the attribute is not below an [[AggregateExpression]] (see
   * `checkAggregateExpressionRequiredForPivotError` doc for more details).
   *
   * In case that attribute is resolved as a literal function (i.e. result is [[CurrentDate]]),
   * perform additional resolution on it.
   *
   * In case result of the previous step is a recursive data type, we coerce it to stay compatible
   * with the fixed-point analyzer.
   *
   * If the attribute is at the top of the project list (which is indicated by
   * [[ExpressionResolutionContext.shouldPreserveAlias]]), we preserve the [[Alias]] and add it to
   * the current [[NameScope.lcaRegistry]] or remove it otherwise.
   */
  private def resolveAttribute(unresolvedAttribute: UnresolvedAttribute): Expression = {
    withPosition(unresolvedAttribute) {
      val expressionResolutionContext = expressionResolutionContextStack.peek()

      val nameTarget: NameTarget = resolveMultipartName(unresolvedAttribute.nameParts)

      logSC128052RelyingOnDerivedColumnAlias(nameTarget) // EDGE

      val candidate = nameTarget.pickCandidate(unresolvedAttribute)

      if (nameTarget.lateralAttributeReference.isEmpty) {
        expressionResolutionContext.hasAttributeOutsideOfAggregateExpressions = true
      }

      if (nameTarget.isOuterReference) {
        logSC116075NameCollisionOnOuterReferenceAndLca(candidate)
        expressionResolutionContext.hasOuterReferences = true
      } else {
        expressionResolutionContext.hasLocalReferences = true
      }

      if (traversals.current.lcaEnabled && nameTarget.lateralAttributeReference.isDefined) {
        checkLateralColumnAliasInWindowExpression(unresolvedAttribute)

        handleLateralColumnAliasReferenceFromNameTarget(
          lateralAttributeReference = nameTarget.lateralAttributeReference.get,
          candidate = candidate
        )
      }

      tryAddReferencedAttribute(candidate)

      checkAggregateExpressionRequiredForPivotError(candidate, expressionResolutionContext)

      val processedCandidate = processCandidateFromNameTarget(candidate)

      if (expressionResolutionContext.shouldPreserveAlias && nameTarget.aliasName.isDefined) {
        generateAliasForCandidateFromNameTarget(
          candidate = processedCandidate,
          nameTarget = nameTarget
        )
      } else {
        processedCandidate
      }
    }
  }

  /**
   * A helper function for the [[NameScope.resolveMultipartName]].
   *
   * See [[NameScope.resolveMultipartName]] for more details.
   */
  private def resolveMultipartName(multipartName: Seq[String]): NameTarget = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()
    val viewResolutionContext = resolver.getViewResolver.getViewResolutionContext
    val extractValueExtractionKey =
      expressionResolutionContext.parentContext.flatMap(_.extractValueExtractionKey)

    scopes.resolveMultipartName(
      multipartName = multipartName,
      NameResolutionParameters(
        canLaterallyReferenceColumn = canLaterallyReferenceColumn,
        canReferenceAggregateExpressionAliases =
          expressionResolutionContext.resolvingGroupingExpressions &&
          traversals.current.groupByAliases,
        canResolveNameByHiddenOutput = canResolveNameByHiddenOutput,
        canResolveNameByHiddenOutputInSubquery =
          subqueryRegistry.currentScope.aggregateExpressionsExtractor.isDefined,
        shouldPreferHiddenOutput = traversals.current.isFilterOnTopOfAggregate,
        canReferenceAggregatedAccessOnlyAttributes =
          expressionResolutionContext.resolvingTreeUnderAggregateExpression,
        resolvingView = viewResolutionContext.isDefined,
        resolvingExecuteImmediate = false,
        referredTempVariableNames =
          viewResolutionContext.map(_.referredTempVariableNames).getOrElse(Seq.empty),
        extractValueExtractionKey = extractValueExtractionKey
      )
    )
  }

  /**
   * Throw an exception if we encounter a lateral column alias reference inside a window
   * expression.
   */
  private def checkLateralColumnAliasInWindowExpression(attribute: UnresolvedAttribute): Unit = {
    if (expressionResolutionContextStack.peek().resolvingWindowExpression.isDefined) {
      throw QueryCompilationErrors.lateralColumnAliasInWindowUnsupportedError(
        lcaNameParts = attribute.nameParts,
        windowExpr = expressionResolutionContextStack.peek().resolvingWindowExpression.get
      )
    }
  }

  /**
   * Given that the candidate is laterally referencing another expression in the project/aggregate
   * list, register it in the [[LateralColumnAliasRegistry]], tag it with
   * [[ResolverTag.SINGLE_PASS_IS_LCA]] and mark current [[ExpressionResolutionContext]] as having
   * lateral column alias.
   */
  private def handleLateralColumnAliasReferenceFromNameTarget(
      lateralAttributeReference: Attribute,
      candidate: Expression): Unit = {
    scopes.current.lcaRegistry
      .markAttributeLaterallyReferenced(lateralAttributeReference)
    candidate.setTagValue(ResolverTag.SINGLE_PASS_IS_LCA, ())
    expressionResolutionContextStack.peek().hasLateralColumnAlias = true
  }

  /**
   * Process the candidate expression from the [[NameTarget]] resolution.
   *
   * - If the candidate is a [[CurrentDate]], we complete the resolution it using the
   *   [[TimezoneAwareExpressionResolver]] to ensure that it is timezone-aware.
   * - If the candidate is an [[Attribute]], we pre-emptively collect it for window source
   *   expressions.
   * - If the candidate is an [[ExtractValue]], we handle it to ensure that recursive types are
   *   coerced correctly.
   */
  private def processCandidateFromNameTarget(candidate: Expression): Expression = {
    candidate match {
      case currentDate: CurrentDate =>
        timezoneAwareExpressionResolver.resolve(currentDate)
      case attribute: Attribute =>
        collectWindowSourceExpression(
          expression = attribute,
          parentOperator = traversals.current.parentOperator
        )
        attribute
      case extractValue: ExtractValue =>
        extractValueResolver.handleResolvedExtractValue(extractValue)
      case other =>
        other
    }
  }

  /**
   * Generate an [[Alias]] for the candidate expression from the [[NameTarget]]. This is
   * necessary because the candidate expression is not always an [[Attribute]] and this needs to
   * be aliased if it's at the top of the project/aggregate list.
   */
  private def generateAliasForCandidateFromNameTarget(
      candidate: Expression,
      nameTarget: NameTarget): Alias = {
    scopes.current.lcaRegistry.withNewLcaScope() {
      val alias = autoGeneratedAliasProvider.newAlias(
        child = candidate,
        name = nameTarget.aliasName
      )
      scopes.current.registerAlias(alias)
      alias
    }
  }

  private def isFilterOnTopOfAggregate(parentOperator: LogicalPlan): Boolean = {
    parentOperator match {
      case _: Filter if scopes.current.baseAggregate.isDefined => true
      case _ => false
    }
  }

  private def isSortOnTopOfAggregate(parentOperator: LogicalPlan): Boolean = {
    parentOperator match {
      case _: Sort if scopes.current.baseAggregate.isDefined => true
      case _ => false
    }
  }

  private def canResolveNameByHiddenOutput = traversals.current.parentOperator match {
    case operator @ (_: Filter | _: Sort | _: RepartitionByExpression) => true
    case other => false
  }

  /**
   * [[AttributeReference]] is already resolved if it's passed to us from DataFrame `col(...)`
   * function, for example.
   *
   * After mapping the [[AttributeReference]] to a correct [[ExprId]], we need to assert that the
   * attribute exists in current [[NameScope]]. If the attribute in the current scope is nullable,
   * we need to preserve this nullability in the [[AttributeReference]] as well. This is necessary
   * because of the following case:
   *
   * {{{
   * val df1 = Seq((1, 1)).toDF("a", "b")
   * val df2 = Seq((2, 2)).toDF("a", "b")
   * df1.join(df2, df1("a") === df2("a"), "outer")
   *   .select(coalesce(df1("a"), df1("b")), coalesce(df2("a"), df2("b"))),
   * }}}
   *
   * Because of the outer join, after df1.join(df2), the output of the join will have nullable
   * attributes "a" and "b" that come from df2. When these attributes are referenced in
   * `select(coalesce(df2("a"), df2("b"))`, they need to inherit nullability of join's output, or
   * retain nullability of [[AttributeReference]], if it was true.
   *
   * Without this, a nullable column's nullable field can be actually set as non-nullable, which
   * can cause illegal optimization (e.g., NULL propagation) and wrong answers. See SPARK-13484 and
   * SPARK-13801 for the concrete queries of this case.
   */
  private def handleResolvedAttributeReference(attributeReference: AttributeReference) = {
    val expressionResolutionContext = expressionResolutionContextStack.peek()

    expressionResolutionContext.hasAttributeOutsideOfAggregateExpressions = true

    val resultAttribute = if (!inOuterReferenceSubtree) {
      expressionResolutionContext.hasLocalReferences = true

      expressionIdAssigner.mapExpression(attributeReference)
    } else {
      expressionResolutionContext.hasOuterReferences = true

      expressionIdAssigner.mapOuterReference(attributeReference)
    }

    val existingAttributeWithId = scopes.current.getAttributeById(resultAttribute.exprId)
    val resultAttributeWithNullability = if (existingAttributeWithId.isEmpty) {
      resultAttribute
    } else {
      val nullability = existingAttributeWithId.get.nullable || resultAttribute.nullable
      resultAttribute.withNullability(nullability)
    }

    tryAddReferencedAttribute(resultAttributeWithNullability)

    collectWindowSourceExpression(
      expression = resultAttributeWithNullability,
      parentOperator = traversals.current.parentOperator
    )

    resultAttributeWithNullability
  }

  /**
   * While handling the resolved [[OuterReference]] we need to set [[inOuterReferenceSubtree]] to
   * `true` to correctly remap [[AttributeReference]] expression IDs using [[ExpressionIdAssigner]]
   * using outer expression ID mapping.
   */
  private def handleResolvedOuterReference(outerReference: OuterReference): Expression = {
    inOuterReferenceSubtree = true
    try {
      OuterReference(e = resolve(outerReference.e).asInstanceOf[NamedExpression])
    } finally {
      inOuterReferenceSubtree = false
    }
  }

  /**
   * [[Literal]]s inside a View's body with an explicitly set default collation
   * should be resolved by modifying their [[Literal.dataType]], replacing all occurrences of
   * the companion object [[StringType]] with a [[StringType]] with default collation.
   *
   * In other cases, no specific resolution logic is required.
   */
  private def resolveLiteral(literal: Literal): Expression = {
    traversals.current.defaultCollation match {
      case Some(defaultCollation) =>
        DefaultCollationTypeCoercion(literal, defaultCollation)
      case None =>
        literal
    }
  }

  /**
   * [[SubqueryExpression]]s are special case of [[Predicate]] and require special resolution logic.
   * [[In]] is a special case because it can contain [[Star]] expressions (e.g., `col IN (*)`).
   * Otherwise, default to generic resolution with type coercion.
   */
  private def resolvePredicate(unresolvedPredicate: Predicate): Expression = {
    unresolvedPredicate match {
      case unresolvedInSubquery: InSubquery =>
        subqueryExpressionResolver.resolveInSubquery(unresolvedInSubquery)
      case unresolvedExists: Exists =>
        subqueryExpressionResolver.resolveExists(unresolvedExists)
      case in: In if containsStar(in.list) =>
        val inWithExpandedStars = in.copy(
          list = expandStarExpressions(in.list)
        )
        resolveExpressionGenericallyWithTypeCoercion(inWithExpandedStars)
      case _ =>
        resolveExpressionGenericallyWithTypeCoercion(unresolvedPredicate)
    }
  }

  /**
   * Resolves [[CreateNamedStruct]]. Before resolution, we need to set
   * `resolvingCreateNamedStruct` to `true` in order to prevent alias collapsing in
   * [[AliasResolver]]. This is necessary because we can't collapse aliases inside
   * [[CreateNamedStruct]] before the name of [[UnresolvedAlias]], that is above this
   * [[CreateNamedStruct]], is resolved.
   *
   * Group children in pairs (name, value) and:
   *  - if it is (NamePlaceholder, Star), expand the [[Star]] and use [[CreateStruct]] to unpack
   *    children. This is done in order to stay compatible with fixed-point analyzer.
   *  - [[Star]] is not supported in other cases and should be filtered out by
   *    [[validateStarsInsideExpression]].
   *  - for all other cases, resolve children generically.
   */
  private def resolveCreateNamedStruct(createNamedStruct: CreateNamedStruct): Expression = {
    expressionResolutionContextStack.peek().resolvingCreateNamedStruct = true

    val resolvedChildren = createNamedStruct.children
      .grouped(2)
      .flatMap {
        case Seq(NamePlaceholder, star: Star) =>
          CreateStruct(expandStar(star)).children
        case Seq(name, value) =>
          Seq(resolve(name), resolve(value))
      }
      .toSeq

    expressionResolutionContextStack.peek().resolvingCreateNamedStruct = false

    CreateNamedStruct(resolvedChildren)
  }

  /**
   * The [[GetViewColumnByNameAndOrdinal]] is a special internal expression that is placed by the
   * [[SessionCatalog]] in the top [[Project]] operator of the freshly reconstructed unresolved
   * view plan. Since the view schema is fixed and persisted in the catalog, we have to extract
   * the right attributes from the view plan regardless of the underlying table schema changes.
   * [[GetViewColumnByNameAndOrdinal]] contains attribute name and it's ordinal to perform the
   * necessary matching. If the matching was not successful, or the number of matched candidates
   * differs from the recorded one, we throw an error.
   *
   * Example of the correct name matching:
   *
   * {{{
   * CREATE TABLE underlying (col1 INT, col2 STRING);
   * CREATE VIEW all_columns AS SELECT * FROM underlying;
   *
   * -- View plan for the SELECT below will contain a Project node on top with the following
   * -- expressions:
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col1, 0, 1)
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col2, 0, 1)
   * SELECT * FROM all_columns;
   *
   * ALTER TABLE underlying DROP COLUMN col2;
   * ALTER TABLE underlying ADD COLUMN col3 STRING;
   * ALTER TABLE underlying ADD COLUMN col2 STRING;
   *
   * -- The output schema for the SELECT below is [col1, col3, col2]
   * SELECT * FROM underlying;
   *
   * -- The output schema for the SELECT below is [col1, col2], because the view schema is fixed.
   * -- GetViewColumnByNameAndOrdinal allows us to perform this operation by matching attribute
   * -- names.
   * SELECT * FROM all_columns;
   * }}}
   *
   * Example of the correct ordinal matching:
   *
   * {{{
   * CREATE TABLE underlying1 (col1 INT, col2 STRING);
   * CREATE TABLE underlying2 (col1 INT, col2 STRING);
   *
   * CREATE VIEW all_columns (c1, c2, c3, c4) AS SELECT * FROM underlying1, underlying2;
   *
   * ALTER TABLE underlying1 ADD COLUMN col3 STRING;
   *
   * -- The output schema for this query has changed to [col1, col2, col3, col1, col2].
   * -- Now we need GetViewColumnByNameAndOrdinal to glue it to the fixed view schema.
   * SELECT * FROM underlying1, underlying2;
   *
   * -- GetViewColumnByNameAndOrdinal helps us to disambiguate the column names from different
   * -- tables by matching the same attribute names from those tables by their ordinal in the
   * -- Project list, which is dependant on the order of tables in the inner join operator from
   * -- the view plan:
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col1, 0, 2)
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col2, 0, 2)
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col1, 1, 2)
   * -- getviewcolumnbynameandordinal(`spark_catalog`.`default`.`all_columns`, col2, 1, 2)
   * SELECT * FROM all_columns;
   * }}}
   */
  private def resolveGetViewColumnByNameAndOrdinal(
      getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal): Expression = {
    val candidates = scopes.current.findAttributesByName(getViewColumnByNameAndOrdinal.colName)
    if (candidates.length != getViewColumnByNameAndOrdinal.expectedNumCandidates) {
      throw QueryCompilationErrors.incompatibleViewSchemaChangeError(
        getViewColumnByNameAndOrdinal.viewName,
        getViewColumnByNameAndOrdinal.colName,
        getViewColumnByNameAndOrdinal.expectedNumCandidates,
        candidates,
        getViewColumnByNameAndOrdinal.viewDDL
      )
    }

    candidates(getViewColumnByNameAndOrdinal.ordinal)
  }

  /**
   * Resolves [[UpCast]] by reusing [[UpCastResolution.resolve]].
   */
  private def resolveUpCast(unresolvedUpCast: UpCast): Expression = {
    val upCastWithResolvedChildren = unresolvedUpCast.copy(child = resolve(unresolvedUpCast.child))

    UpCastResolution.resolve(upCastWithResolvedChildren) match {
      case timezoneAwareExpression: TimeZoneAwareExpression =>
        timezoneAwareExpressionResolver.resolve(timezoneAwareExpression)
      case other =>
        other
    }
  }

  /**
   * Collation resolution requires resolving its collation name using [[CollationFactory]].
   */
  private def resolveCollation(unresolvedCollation: UnresolvedCollation): Expression = {
    ResolvedCollation(
      CollationFactory.resolveFullyQualifiedName(unresolvedCollation.collationName.toArray)
    )
  }

  /**
   * Resolves [[UnresolvedNamedLambdaVariable]] by looking up the variable name in the current
   * scope's lambda variable map. If the variable is found, it is resolved to the corresponding
   * [[NamedExpression]]. If nested fields are present, they are extracted using [[ExtractValue]].
   * If the variable is not found in the lambda variable map, it falls back to resolving it as an
   * [[UnresolvedAttribute]].
   *
   * Note: The parser creates [[UnresolvedNamedLambdaVariable]] for every name inside the lambda
   * body instead of creating [[UnresolvedAttribute]] in the first place. This special node type
   * allows the resolver to distinguish between lambda parameters and external references, enabling
   * the analyzer to resolve [[UnresolvedNamedLambdaVariable]] as lambda arguments first before
   * falling back to attribute resolution.
   *
   * In case of the following query:
   *
   * {{{
   *   SELECT filter(array('a'), x -> x = y) FROM VALUES('a', 'b') t(x, y);
   * }}}
   *
   * The `x` in the function body is resolved from the lambda variable map. The `y` is resolved
   * from the underlying table `t` since it is not present in the argument list of the above
   * [[LabmdaFunction]]. Thus, the following plan would look like:
   *
   * {{{
   *   Project [filter(array(a), lambdafunction((lambda x#2 = y#1), lambda x#2, false)) AS ...]
   *   +- SubqueryAlias t
   *    +- LocalRelation [x#0, y#1]
   * }}}
   */
  private def resolveUnresolvedNamedLambdaVariable(
      unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable): Expression = {
    val name = unresolvedNamedLambdaVariable.nameParts.head
    val nestedFields = unresolvedNamedLambdaVariable.nameParts.tail

    expressionResolutionContextStack
      .peek()
      .lambdaVariableMap
      .flatMap(_.get(name)) match {
      case Some(lambda) if !nestedFields.isEmpty =>
        nestedFields.foldLeft(lambda: Expression) { (expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), conf.resolver)
        }
      case Some(lambda) => lambda
      case None =>
        resolveAttribute(UnresolvedAttribute(unresolvedNamedLambdaVariable.nameParts))
    }
  }

  /**
   * [[NamedLambdaVariable]] doesn't need any specific resolution. We just need to map it using
   * [[ExpressionIdAssigner]].
   */
  private def resolveNamedLambdaVariable(namedLambdaVariable: NamedLambdaVariable): Expression = {
    expressionIdAssigner.mapExpression(namedLambdaVariable)
  }

  /**
   * Resolve [[NamedParameter]] by looking up its name in the `parameterNamesToValues` map from
   * the current operator resolution context. If the name is found in the map, replace the
   * [[NamedParameter]] with the corresponding expression. Otherwise, throw `UNBOUND_SQL_PARAMETER`.
   */
  private def resolveNamedParameter(namedParameter: NamedParameter): Expression = {
    operatorResolutionContextStack.current.parameterNamesToValues match {
      case Some(map) =>
        map.get(namedParameter.name) match {
          case expression: Expression => expression
          case null => throwUnboundSqlParameter(namedParameter)
        }
      case None => throwUnboundSqlParameter(namedParameter)
    }
  }

  private def pushResolutionContext(): Unit = {
    val parentContext = expressionResolutionContextStack.peek()
    expressionResolutionContextStack.push(ExpressionResolutionContext.createChild(parentContext))
  }

  private def popResolutionContext(): Unit = {
    val childContext = expressionResolutionContextStack.pop()
    expressionResolutionContextStack.peek().mergeChild(childContext)
  }

  private def tryAddReferencedAttribute(expression: Expression) = expression match {
    case attribute: Attribute =>
      traversals.current.referencedAttributes.put(attribute.exprId, attribute)
    case extractValue: ExtractValue =>
      extractValue.foreach {
        case attribute: Attribute =>
          traversals.current.referencedAttributes.put(attribute.exprId, attribute)
        case _ =>
      }
    case _ =>
  }

  /**
   * Throws an error if we are resolving [[Pivot.aggregates]] and the `expression` is an
   * [[Attribute]] that is not under an [[AggregateExpression]] (Pandas UDF are also not supported,
   * see [[AggregateExpressionResolver.validateResolvedAggregateExpression]]).
   */
  private def checkAggregateExpressionRequiredForPivotError(
      expression: Expression,
      expressionResolutionContext: ExpressionResolutionContext): Unit = {
    expression match {
      case _: Attribute
          if expressionResolutionContext.resolvingPivotAggregates &&
          !expressionResolutionContext.resolvingTreeUnderAggregateExpression =>
        throw QueryCompilationErrors.aggregateExpressionRequiredForPivotError(expression.sql)
      case _ =>
    }
  }

  /**
   * Returns true if LateralColumnAlias resolution is enabled, current operator is not created
   * because of a generated column and current expression is a grouping one (grouping expressions)
   * can't reference an LCA.
   */
  private def canLaterallyReferenceColumn: Boolean = {
    traversals.current.lcaEnabled &&
    // BEGIN-EDGE
    traversals.current.parentOperator
      .getTagValue(GeneratedColumn.GENERATED_COLUMN_VALIDATION)
      .isEmpty &&
    // END-EDGE
    !expressionResolutionContextStack.peek().resolvingGroupingExpressions
  }

  /**
   * Copy the tags from the `unresolvedExpression` to the resolved one using the
   * [[ExpressionResolver.resolve]]. If the result of the resolution is an [[AggregateExpression]]
   * than apply tags to the [[AggregateExpression.aggregateFunction]] as well as we don't resolve
   * it besides applying type coercion rules to it.
   */
  private def preserveTags(
      unresolvedExpression: Expression,
      resolvedExpression: Expression): Unit = {
    resolvedExpression match {
      case aggregateExpression: AggregateExpression =>
        aggregateExpression.aggregateFunction.copyTagsFrom(unresolvedExpression)
      case other =>
    }
    resolvedExpression.copyTagsFrom(unresolvedExpression)
  }

  /**
   * Resolves [[Expression]] only by resolving its children. This resolution method is used for
   * nodes that don't require any special resolution other than resolving its children.
   */
  private def resolveExpressionGenerically(expression: Expression): Expression =
    withResolvedChildren(expression, resolve _)

  /**
   * Resolves [[Expression]] by resolving its children and applying generic type coercion
   * transformations to the resulting expression. This resolution method is used for nodes that
   * require type coercion on top of [[resolveExpressionGenerically]].
   */
  private def resolveExpressionGenericallyWithTypeCoercion(expression: Expression): Expression = {
    val expressionWithResolvedChildren = withResolvedChildren(expression, resolve _)
    coerceExpressionTypes(
      expression = expressionWithResolvedChildren,
      expressionTreeTraversal = traversals.current
    )
  }

  /**
   * Resolves [[Expression]] by calling [[TimezoneAwareExpressionResolver]] to resolve expression's
   * children and apply timezone if needed. [[TimezoneAwareExpressionResolver]] will type coerce
   * the result if needed.
   */
  private def resolveExpressionGenericallyWithTimezoneWithTypeCoercion(
      timezoneAwareExpression: TimeZoneAwareExpression): Expression =
    timezoneAwareExpressionResolver.resolve(timezoneAwareExpression)

  private def validateResolvedExpressionGenerically(resolvedExpression: Expression): Unit = {
    if (resolvedExpression.checkInputDataTypes().isFailure) {
      TypeCoercionValidation.failOnTypeCheckResult(resolvedExpression)
    }

    resolvedExpression match {
      case runtimeReplaceable: RuntimeReplaceable if !runtimeReplaceable.replacement.resolved =>
        throwFailedToResolveRuntimeReplaceableExpression(runtimeReplaceable)
      case expression if !expression.resolved =>
        throwSinglePassFailedToResolveExpression(resolvedExpression)
      case _ =>
    }

    validateExpressionUnderSupportedOperator(resolvedExpression)
  }

  /**
   * Transform list to a non-lazy one. This is needed in order to enforce determinism in
   * single-pass resolver. Example:
   *
   *   val groupByColumns = LazyList(col("key"))
   *   val df = Seq((1, 2)).toDF("key", "value")
   *   df.groupBy(groupByCols: _*)
   *
   * For this case, it is necessary to transform `LazyList` (lazy object) to `List` (non-lazy).
   * Other object which has to be transformed is `Stream`.
   */
  private def tryDrainLazySequences(list: Seq[Expression]): Seq[Expression] = {
    @nowarn("cat=deprecation")
    val result = list match {
      case lazyObject @ (_: LazyList[_] | _: Stream[_]) =>
        lazyObject.toList
      case other => other
    }
    result
  }
  // BEGIN-EDGE

  private def logSC128052RelyingOnDerivedColumnAlias(nameTarget: NameTarget): Unit = {
    nameTarget.aliasMetadata match {
      case None =>
      case Some(metadata) =>
        if (metadata.contains(org.apache.spark.sql.catalyst.util.AUTO_GENERATED_ALIAS)) {
          recordBehavioralChange(SC128052_RELYING_ON_DERIVED_COLUMN_ALIAS.key, false)
        }
    }
  }

  private def logSC116075NameCollisionOnOuterReferenceAndLca(candidate: Expression) = {
    candidate match {
      case namedExpression: NamedExpression
          if scopes.current.hasAvailableAliasWithName(namedExpression.name) =>
        recordBehavioralChange(SC116075_NAME_COLLISION_ON_OUTER_REFERENCE_AND_LCA.key, false)
      case _ =>
    }
  }
  // END-EDGE

  private def throwUnsupportedSinglePassAnalyzerFeature(unresolvedExpression: Expression): Nothing =
    throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
      s"${unresolvedExpression.getClass} expression resolution"
    )

  private def throwSinglePassFailedToResolveExpression(expression: Expression): Nothing =
    throw SparkException.internalError(
      msg = s"Failed to resolve expression in single-pass: ${toSQLExpr(expression)}",
      context = expression.origin.getQueryContext,
      summary = expression.origin.context.summary()
    )

  private def throwFailedToResolveRuntimeReplaceableExpression(
      runtimeReplaceable: RuntimeReplaceable) = {
    throw SparkException.internalError(
      s"Cannot resolve the runtime replaceable expression ${toSQLExpr(runtimeReplaceable)}. " +
      s"The replacement is unresolved: ${toSQLExpr(runtimeReplaceable.replacement)}."
    )
  }

  private def throwUnboundSqlParameter(parameter: Parameter) = {
    parameter.failAnalysis(
      errorClass = "UNBOUND_SQL_PARAMETER",
      messageParameters = Map("name" -> parameter.name)
    )
  }

  /**
   * Allow [[Star]] in [[CreateNamedStruct]] only when it's name is [[NamePlaceholder]]. For now,
   * we prevent other [[Star]] cases in order to not accidentally expose an unsupported case when
   * experimental feature flag for [[NamePlaceholder]] is enabled.
   */
  private def allowStarInCreateNamedStruct(createNamedStruct: CreateNamedStruct) = {
    createNamedStruct.children.grouped(2).forall {
      case Seq(NamePlaceholder, _: Star) => true
      case Seq(_, _: Star) => false
      case _ => true
    }
  }

  private def containsStar(expressions: Seq[Expression]): Boolean = {
    expressions.exists {
      case _: Star => true
      case _ => false
    }
  }
}
