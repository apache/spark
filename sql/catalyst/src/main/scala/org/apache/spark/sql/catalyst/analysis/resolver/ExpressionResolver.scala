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
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  FunctionResolution,
  GetViewColumnByNameAndOrdinal,
  TypeCoercionValidation,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedStar,
  UpCastResolution
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

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
    with ResolvesExpressionChildren {

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
   * This field contains the aliases of [[AggregateExpression]]s that were extracted during the
   * most recent expression tree resolution. It is populated in
   * [[resolveExpressionTreeInOperatorImpl]] when [[ExpressionTreeTraversal]] is popped from the
   * stack.
   */
  private var lastExtractedAggregateExpressionAliases: Option[Seq[Alias]] = None

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
  private val traversals = new ExpressionTreeTraversalStack
  private val expressionResolutionContextStack = new ArrayDeque[ExpressionResolutionContext]
  private val scopes = resolver.getNameScopes
  private val subqueryRegistry = resolver.getSubqueryRegistry

  private val aliasResolver = new AliasResolver(this)
  private val timezoneAwareExpressionResolver = new TimezoneAwareExpressionResolver(this)
  private val conditionalExpressionResolver =
    new ConditionalExpressionResolver(this, timezoneAwareExpressionResolver)
  private val predicateResolver =
    new PredicateResolver(this, timezoneAwareExpressionResolver)
  private val binaryArithmeticResolver = new BinaryArithmeticResolver(
    this,
    timezoneAwareExpressionResolver
  )
  private val limitLikeExpressionValidator = new LimitLikeExpressionValidator
  private val typeCoercionResolver = new TypeCoercionResolver(timezoneAwareExpressionResolver)
  private val aggregateExpressionResolver =
    new AggregateExpressionResolver(resolver, this, timezoneAwareExpressionResolver)
  private val functionResolver = new FunctionResolver(
    this,
    timezoneAwareExpressionResolver,
    functionResolution,
    aggregateExpressionResolver,
    binaryArithmeticResolver
  )
  private val timeAddResolver = new TimeAddResolver(this, timezoneAwareExpressionResolver)
  private val unaryMinusResolver = new UnaryMinusResolver(this, timezoneAwareExpressionResolver)
  private val subqueryExpressionResolver = new SubqueryExpressionResolver(this, resolver)

  /**
   * Get the expression tree traversal stack.
   */
  def getExpressionTreeTraversals: ExpressionTreeTraversalStack = traversals

  /**
   * Get the expression resolution context stack.
   */
  def getExpressionResolutionContextStack: ArrayDeque[ExpressionResolutionContext] =
    expressionResolutionContextStack

  def getExpressionIdAssigner: ExpressionIdAssigner = expressionIdAssigner

  /**
   * Get [[NameScopeStack]] bound to the used [[Resolver]].
   */
  def getNameScopes: NameScopeStack = scopes

  /**
   * Get the [[TypeCoercionResolver]] which contains all the transformations for generic coercion.
   */
  def getGenericTypeCoercionResolver: TypeCoercionResolver = typeCoercionResolver

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
   * Returns all aliases of [[AggregateExpression]]s that were extracted during the most recent
   * expression tree resolution.
   */
  def getLastExtractedAggregateExpressionAliases: Seq[Alias] =
    lastExtractedAggregateExpressionAliases.getOrElse(Seq.empty)

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
   */
  override def resolve(unresolvedExpression: Expression): Expression =
    withOrigin(unresolvedExpression.origin) {
      planLogger.logExpressionTreeResolutionEvent(
        unresolvedExpression,
        "Unresolved expression tree"
      )

      if (tryPopSinglePassSubtreeBoundary(unresolvedExpression)) {
        unresolvedExpression
      } else {
        pushResolutionContext()

        val resolvedExpression = unresolvedExpression match {
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
          case unresolvedLiteral: Literal =>
            resolveLiteral(unresolvedLiteral)
          case unresolvedPredicate: Predicate =>
            resolvePredicate(unresolvedPredicate)
          case unresolvedScalarSubquery: ScalarSubquery =>
            subqueryExpressionResolver.resolveScalarSubquery(unresolvedScalarSubquery)
          case unresolvedListQuery: ListQuery =>
            subqueryExpressionResolver.resolveListQuery(unresolvedListQuery)
          case unresolvedTimeAdd: TimeAdd =>
            timeAddResolver.resolve(unresolvedTimeAdd)
          case unresolvedUnaryMinus: UnaryMinus =>
            unaryMinusResolver.resolve(unresolvedUnaryMinus)
          case createNamedStruct: CreateNamedStruct =>
            resolveExpressionGenerically(createNamedStruct)
          case unresolvedConditionalExpression: ConditionalExpression =>
            conditionalExpressionResolver.resolve(unresolvedConditionalExpression)
          case getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal =>
            resolveGetViewColumnByNameAndOrdinal(getViewColumnByNameAndOrdinal)
          case getTimeField: GetTimeField =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(getTimeField)
          case makeTimestamp: MakeTimestamp =>
            resolveExpressionGenericallyWithTimezoneWithTypeCoercion(makeTimestamp)
          case unresolvedRuntimeReplaceable: RuntimeReplaceable =>
            resolveExpressionGenericallyWithTypeCoercion(unresolvedRuntimeReplaceable)
          case unresolvedTimezoneExpression: TimeZoneAwareExpression =>
            timezoneAwareExpressionResolver.resolve(unresolvedTimezoneExpression)
          case unresolvedUpCast: UpCast =>
            resolveUpCast(unresolvedUpCast)
          case unresolvedCollation: UnresolvedCollation =>
            resolveCollation(unresolvedCollation)
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
   * will be resolved using generic [[resolve]]. However, [[UnresolvedStar]] is a special case,
   * because it is expanded into a sequence of [[NamedExpression]]s. Because of that this method
   * returns a sequence and doesn't conform to generic [[resolve]] interface - it's called directly
   * from the [[Resolver]] during [[Project]] resolution.
   *
   * The output sequence can be larger than the input sequence due to [[UnresolvedStar]] expansion.
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
   *   hasAggregateExpressions = true, // because it contains `count(col1)` in the project list
   * )
   */
  def resolveProjectList(
      sourceUnresolvedProjectList: Seq[NamedExpression],
      operator: LogicalPlan): ResolvedProjectList = {
    val unresolvedProjectList = tryDrainLazySequences(sourceUnresolvedProjectList)

    var hasAggregateExpressions = false
    var hasLateralColumnAlias = false

    val unresolvedProjectListWithStarsExpanded = unresolvedProjectList.flatMap {
      case unresolvedStar: UnresolvedStar =>
        resolveStar(unresolvedStar)
      case other => Seq(other)
    }

    val resolvedProjectList = unresolvedProjectListWithStarsExpanded.flatMap { expression =>
      val (resolvedElement, resolvedElementContext) = {
        resolveExpressionTreeInOperatorImpl(
          expression,
          operator,
          inProjectList = true
        )
      }

      hasAggregateExpressions |= resolvedElementContext.hasAggregateExpressions
      hasLateralColumnAlias |= resolvedElementContext.hasLateralColumnAlias

      Seq(resolvedElement.asInstanceOf[NamedExpression])
    }

    ResolvedProjectList(
      expressions = resolvedProjectList,
      hasAggregateExpressions = hasAggregateExpressions,
      hasLateralColumnAlias = hasLateralColumnAlias
    )
  }

  /**
   * Resolve aggregate expressions in [[Aggregate]] operator.
   *
   * The [[Aggregate]] list can contain different unresolved expressions before the resolution,
   * which will be resolved using generic [[resolve]]. However, [[UnresolvedStar]] is a special
   * case, because it is expanded into a sequence of [[NamedExpression]]s. Because of that this
   * method returns a sequence and doesn't conform to generic [[resolve]] interface - it's called
   * directly from the [[AggregateResolver]] during [[Aggregate]] resolution.
   *
   * Besides resolution, we do the following:
   *   - If there is a [[UnresolvedStar]] in the list we set `hasStar` to true in order to throw
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

    val unresolvedAggregateExpressionsWithStarsExpanded = unresolvedAggregateExpressions.flatMap {
      case unresolvedStar: UnresolvedStar =>
        hasStar = true
        resolveStar(unresolvedStar)
      case other => Seq(other)
    }

    val resolvedAggregateExpressions =
      unresolvedAggregateExpressionsWithStarsExpanded.zipWithIndex.flatMap {
        case (expression, index) =>
          val (resolvedElement, resolvedElementContext) = resolveExpressionTreeInOperatorImpl(
            expression,
            unresolvedAggregate,
            inProjectList = true
          )

          resolvedElement match {
            case alias: Alias =>
              scopes.current.addTopAggregateExpression(alias)
            case other =>
          }

          if (resolvedElementContext.hasAggregateExpressions) {
            expressionIndexesWithAggregateFunctions.add(index)
            hasAttributeOutsideOfAggregateExpressions |=
            resolvedElementContext.hasAttributeOutsideOfAggregateExpressions
          } else {
            expressionsWithoutAggregates += resolvedElement.asInstanceOf[NamedExpression]
          }

          Seq(resolvedElement.asInstanceOf[NamedExpression])
      }

    val isLcaEnabled = conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)
    if (isLcaEnabled && scopes.current.lcaRegistry.getAliasDependencyLevels().size() > 1) {
      throw new ExplicitlyUnsupportedResolverFeature("LateralColumnAlias in aggregate expressions")
    }

    ResolvedAggregateExpressions(
      expressions = resolvedAggregateExpressions,
      resolvedExpressionsWithoutAggregates = expressionsWithoutAggregates.toSeq,
      hasAttributeOutsideOfAggregateExpressions = hasAttributeOutsideOfAggregateExpressions,
      hasStar = hasStar,
      expressionIndexesWithAggregateFunctions = expressionIndexesWithAggregateFunctions
    )
  }

  /**
   * Resolve grouping expressions in [[Aggregate]] operator.
   *
   * It's done for every expression using the `resolveExpressionTreeInOperatorImpl`. For cases where
   * grouping is done based on aliases the resolution is following:
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
   */
  def resolveGroupingExpressions(
      sourceUnresolvedGroupingExpressions: Seq[Expression],
      unresolvedAggregate: Aggregate): Seq[Expression] = {
    val unresolvedGroupingExpressions = tryDrainLazySequences(sourceUnresolvedGroupingExpressions)

    unresolvedGroupingExpressions.map { expression =>
      val (resolvedExpression, _) = resolveExpressionTreeInOperatorImpl(
        expression,
        unresolvedAggregate,
        resolvingGroupingExpressions = true
      )

      resolvedExpression
    }
  }

  /**
   * Validate if `expression` is under supported operator or not. In case it's not, add `expression`
   * to the [[ExpressionTreeTraversal.invalidExpressionsInTheContextOfOperator]] list to throw
   * error later, when [[getLastInvalidExpressionsInTheContextOfOperator]] is called by the
   * [[Resolver]].
   */
  def validateExpressionUnderSupportedOperator(expression: Expression): Unit = {
    if (UnsupportedExpressionInOperatorValidation.isExpressionInUnsupportedOperator(
        expression,
        traversals.current.parentOperator
      )) {
      traversals.current.invalidExpressionsInTheContextOfOperator.add(expression)
    }
  }

  private def resolveExpressionTreeInOperatorImpl(
      unresolvedExpression: Expression,
      parentOperator: LogicalPlan,
      inProjectList: Boolean = false,
      resolvingGroupingExpressions: Boolean = false
  ): (Expression, ExpressionResolutionContext) = {
    traversals.withNewTraversal(parentOperator) {
      expressionResolutionContextStack.push(
        new ExpressionResolutionContext(
          isRoot = true,
          isTopOfProjectList = inProjectList,
          resolvingGroupingExpressions = resolvingGroupingExpressions
        )
      )

      try {
        val resolvedExpression = resolve(unresolvedExpression)

        lastReferencedAttributes = Some(traversals.current.referencedAttributes)
        lastInvalidExpressionsInTheContextOfOperator =
          Some(traversals.current.invalidExpressionsInTheContextOfOperator.asScala.toSeq)
        lastExtractedAggregateExpressionAliases =
          Some(traversals.current.extractedAggregateExpressionAliases.asScala.toSeq)

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
      case unresolvedStar: UnresolvedStar =>
        // We don't support edge cases of star usage, e.g. `WHERE col1 IN (*)`
        throw new ExplicitlyUnsupportedResolverFeature("Star outside of Project list")
      case attributeReference: AttributeReference =>
        handleResolvedAttributeReference(attributeReference)
      case outerReference: OuterReference =>
        handleResolvedOuterReference(outerReference)
      case _: UnresolvedNamedLambdaVariable =>
        throw new ExplicitlyUnsupportedResolverFeature("Lambda variables")
      case _ =>
        withPosition(unresolvedNamedExpression) {
          throwUnsupportedSinglePassAnalyzerFeature(unresolvedNamedExpression)
        }
    }

  /**
   * [[UnresolvedStar]] resolution relies on the [[NameScope]]'s ability to get the attributes by a
   * multipart name ([[UnresolvedStar]]'s `target` field):
   *
   * - Star target is defined:
   *
   * {{{
   * SELECT t.* FROM VALUES (1) AS t;
   * ->
   * Project [col1#19]
   * }}}
   *
   *
   * - Star target is not defined:
   *
   * {{{
   * SELECT * FROM (SELECT 1 as col1), (SELECT 2 as col2);
   * ->
   * Project [col1#19, col2#20]
   * }}}
   */
  private def resolveStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] =
    withPosition(unresolvedStar) {
      scopes.current.expandStar(unresolvedStar)
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
   * attribute as referenced and tag the LCA attribute for further [[Alias]] resolution.
   *
   * In case that attribute is resolved as a literal function (i.e. result is [[CurrentDate]]),
   * perform additional resolution on it.
   *
   * If the attribute is at the top of the project list (which is indicated by
   * [[ExpressionResolutionContext.isTopOfProjectList]]), we preserve the [[Alias]] or remove it
   * otherwise.
   *
   * Finally, we remap the expression ID of a top [[Alias]]. It's not necessary to remap the
   * expression ID of lower expressions, because they already got the appropriate ID from the
   * current scope output in [[resolveMultipartName]].
   */
  private def resolveAttribute(unresolvedAttribute: UnresolvedAttribute): Expression =
    withPosition(unresolvedAttribute) {
      val isLcaEnabled = conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)
      val expressionResolutionContext = expressionResolutionContextStack.peek()

      val nameTarget: NameTarget = scopes.resolveMultipartName(
        multipartName = unresolvedAttribute.nameParts,
        canLaterallyReferenceColumn = canLaterallyReferenceColumn(isLcaEnabled),
        canReferenceAggregateExpressionAliases = (
            expressionResolutionContextStack
              .peek()
              .resolvingGroupingExpressions && conf.groupByAliases
        ),
        canResolveNameByHiddenOutput = canResolveNameByHiddenOutput,
        canReferenceAggregatedAccessOnlyAttributes = (
            expressionResolutionContextStack
              .peek()
              .resolvingTreeUnderAggregateExpression
        )
      )

      val candidate = nameTarget.pickCandidate(unresolvedAttribute)

      expressionResolutionContext.hasAttributeOutsideOfAggregateExpressions = true
      if (nameTarget.isOuterReference) {
        expressionResolutionContext.hasOuterReferences = true
      } else {
        expressionResolutionContext.hasLocalReferences = true
      }

      if (isLcaEnabled) {
        nameTarget.lateralAttributeReference match {
          case Some(lateralAttributeReference) =>
            scopes.current.lcaRegistry
              .markAttributeLaterallyReferenced(lateralAttributeReference)
            candidate.setTagValue(ExpressionResolver.SINGLE_PASS_IS_LCA, ())
            expressionResolutionContext.hasLateralColumnAlias = true
          case None =>
        }
      }

      tryAddReferencedAttribute(candidate)

      val candidateOrLiteralFunction = candidate match {
        case currentDate: CurrentDate =>
          timezoneAwareExpressionResolver.resolve(currentDate)
        case other => other
      }

      val properlyAliasedExpressionTree =
        if (expressionResolutionContext.isTopOfProjectList && nameTarget.aliasName.isDefined) {
          Alias(candidateOrLiteralFunction, nameTarget.aliasName.get)()
        } else {
          candidateOrLiteralFunction
        }

      properlyAliasedExpressionTree match {
        case alias: Alias =>
          expressionIdAssigner.mapExpression(alias)
        case _ =>
          properlyAliasedExpressionTree
      }
    }

  private def canResolveNameByHiddenOutput = traversals.current.parentOperator match {
    case operator @ (_: Filter | _: Sort) => true
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

    val strippedAttributeReference = tryStripAmbiguousSelfJoinMetadata(attributeReference)

    val resultAttribute = if (!inOuterReferenceSubtree) {
      expressionResolutionContext.hasLocalReferences = true

      expressionIdAssigner.mapExpression(strippedAttributeReference)
    } else {
      expressionResolutionContext.hasOuterReferences = true

      expressionIdAssigner.mapOuterReference(strippedAttributeReference)
    }

    val existingAttributeWithId = scopes.current.getAttributeById(resultAttribute.exprId)
    val resultAttributeWithNullability = if (existingAttributeWithId.isEmpty) {
      resultAttribute
    } else {
      val nullability = existingAttributeWithId.get.nullable || resultAttribute.nullable
      resultAttribute.withNullability(nullability)
    }

    tryAddReferencedAttribute(resultAttributeWithNullability)

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
   * [[Literal]] resolution doesn't require any specific resolution logic at this point.
   */
  private def resolveLiteral(literal: Literal): Expression = literal

  /**
   * Resolve [[Predicate]] expression using [[PredicateResolver]]. Subquery expressions are a
   * special case and require special resolution logic.
   */
  private def resolvePredicate(unresolvedPredicate: Predicate): Expression = {
    unresolvedPredicate match {
      case unresolvedInSubquery: InSubquery =>
        subqueryExpressionResolver.resolveInSubquery(unresolvedInSubquery)
      case unresolvedExists: Exists =>
        subqueryExpressionResolver.resolveExists(unresolvedExists)
      case _ =>
        predicateResolver.resolve(unresolvedPredicate)
    }
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
   * Returns true if LateralColumnAlias resolution is enabled, current operator is not created
   * because of a generated column and current expression is a grouping one (grouping expressions)
   * can't reference an LCA.
   */
  private def canLaterallyReferenceColumn(isLcaEnabled: Boolean): Boolean = {
    isLcaEnabled &&
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
   * [[DetectAmbiguousSelfJoin]] rule in the fixed-point Analyzer detects ambiguous references in
   * self-joins based on special metadata added by [[Dataset]] code (see SPARK-27547). Just strip
   * this for now since we don't support joins yet.
   */
  private def tryStripAmbiguousSelfJoinMetadata(attributeReference: AttributeReference) = {
    val metadata = attributeReference.metadata
    if (ExpressionResolver.AMBIGUOUS_SELF_JOIN_METADATA.exists(metadata.contains(_))) {
      val metadataBuilder = new MetadataBuilder().withMetadata(metadata)
      for (metadataKey <- ExpressionResolver.AMBIGUOUS_SELF_JOIN_METADATA) {
        metadataBuilder.remove(metadataKey)
      }
      attributeReference.withMetadata(metadataBuilder.build())
    } else {
      attributeReference
    }
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
    typeCoercionResolver.resolve(expressionWithResolvedChildren)
  }

  /**
   * Resolves [[Expression]] by calling [[timezoneAwareExpressionResolver]] to resolve
   * expression's children and apply timezone if needed. Applies generic type coercion
   * rules to the result.
   */
  private def resolveExpressionGenericallyWithTimezoneWithTypeCoercion(
      timezoneAwareExpression: TimeZoneAwareExpression): Expression = {
    val expressionWithTimezone = timezoneAwareExpressionResolver.resolve(timezoneAwareExpression)
    typeCoercionResolver.resolve(expressionWithTimezone)
  }

  private def validateResolvedExpressionGenerically(resolvedExpression: Expression): Unit = {
    if (resolvedExpression.checkInputDataTypes().isFailure) {
      TypeCoercionValidation.failOnTypeCheckResult(resolvedExpression)
    }

    if (!resolvedExpression.resolved) {
      throwSinglePassFailedToResolveExpression(resolvedExpression)
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
}

object ExpressionResolver {
  private val AMBIGUOUS_SELF_JOIN_METADATA = Seq("__dataset_id", "__col_position")
  val SINGLE_PASS_SUBTREE_BOUNDARY = TreeNodeTag[Unit]("single_pass_subtree_boundary")
  val SINGLE_PASS_IS_LCA = TreeNodeTag[Unit]("single_pass_is_lca")
}
