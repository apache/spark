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

import java.util.ArrayDeque

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  FunctionResolution,
  GetViewColumnByNameAndOrdinal,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedStar,
  UpCastResolution
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  BinaryArithmetic,
  ConditionalExpression,
  CreateNamedStruct,
  DateAddYMInterval,
  Expression,
  ExtractIntervalPart,
  GetTimeField,
  Literal,
  MakeTimestamp,
  NamedExpression,
  Predicate,
  RuntimeReplaceable,
  TimeAdd,
  TimeZoneAwareExpression,
  UnaryMinus,
  UnresolvedNamedLambdaVariable,
  UpCast
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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
  private val isLcaEnabled = conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)

  /**
   * This is a flag indicating that we are resolving top of [[Project]] list. Otherwise extra
   * [[Alias]]es have to be stripped away.
   */
  private var isTopOfProjectList: Boolean = false

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
  private val parentOperators = new ArrayDeque[LogicalPlan]
  private val expressionIdAssigner = new ExpressionIdAssigner
  private val expressionResolutionContextStack = new ArrayDeque[ExpressionResolutionContext]
  private val scopes = resolver.getNameScopes

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
  private val limitExpressionResolver = new LimitExpressionResolver
  private val typeCoercionResolver = new TypeCoercionResolver(timezoneAwareExpressionResolver)
  private val aggregateExpressionResolver =
    new AggregateExpressionResolver(this, timezoneAwareExpressionResolver)
  private val functionResolver = new FunctionResolver(
    this,
    timezoneAwareExpressionResolver,
    functionResolution,
    aggregateExpressionResolver,
    binaryArithmeticResolver
  )
  private val timeAddResolver = new TimeAddResolver(this, timezoneAwareExpressionResolver)
  private val unaryMinusResolver = new UnaryMinusResolver(this, timezoneAwareExpressionResolver)

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
            predicateResolver.resolve(unresolvedPredicate)
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
          case expression: Expression =>
            resolveExpressionGenericallyWithTypeCoercion(expression)
        }

        preserveTags(unresolvedExpression, resolvedExpression)
        popResolutionContext()

        if (!resolvedExpression.resolved) {
          throwSinglePassFailedToResolveExpression(resolvedExpression)
        }

        planLogger.logExpressionTreeResolution(unresolvedExpression, resolvedExpression)
        resolvedExpression
      }
    }

  /**
   * Get the expression resolution context stack.
   */
  def getExpressionResolutionContextStack: ArrayDeque[ExpressionResolutionContext] = {
    expressionResolutionContextStack
  }

  def getExpressionIdAssigner: ExpressionIdAssigner = expressionIdAssigner

  /**
   * Get the most recent operator (bottommost) from the `parentOperators` stack.
   */
  def getParentOperator: Option[LogicalPlan] = {
    if (parentOperators.size() > 0) {
      Some(parentOperators.peek())
    } else {
      None
    }
  }

  /**
   * Get [[NameScopeStack]] bound to the used [[Resolver]].
   */
  def getNameScopes: NameScopeStack = scopes

  /**
   * Resolve the limit expression from either a [[LocalLimit]] or a [[GlobalLimit]] operator.
   */
  def resolveLimitExpression(
      unresolvedLimitExpr: Expression,
      unresolvedLimit: LogicalPlan): Expression = {
    val resolvedLimitExpr = resolveExpressionTreeInOperator(
      unresolvedLimitExpr,
      unresolvedLimit
    )
    limitExpressionResolver.resolve(resolvedLimitExpr)
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
   *   hasAttributes = false // because it doesn't contain any [[AttributeReference]]s in the
   *                         // project list (only under the aggregate expression, please check
   *                         // [[AggregateExpressionResolver]] for more details).
   */
  def resolveProjectList(
      unresolvedProjectList: Seq[NamedExpression],
      operator: LogicalPlan): ResolvedProjectList = {
    val projectListResolutionContext = new ExpressionResolutionContext
    val resolvedProjectList = unresolvedProjectList.flatMap {
      case unresolvedStar: UnresolvedStar =>
        resolveStar(unresolvedStar)
      case other =>
        val (resolvedElement, resolvedElementContext) =
          resolveExpressionTreeInOperatorImpl(other, operator)
        projectListResolutionContext.merge(resolvedElementContext)
        Seq(resolvedElement.asInstanceOf[NamedExpression])
    }
    ResolvedProjectList(
      expressions = resolvedProjectList,
      hasAggregateExpressions = projectListResolutionContext.hasAggregateExpressionsInASubtree,
      hasAttributes = projectListResolutionContext.hasAttributeInASubtree,
      hasLateralColumnAlias = projectListResolutionContext.hasLateralColumnAlias
    )
  }

  /**
   * Resolves [[Expression]] by resolving its children and applying generic type coercion
   * transformations to the resulting expression. This resolution method is used for nodes that
   * require type coercion on top of [[resolveExpressionGenerically]].
   */
  def resolveExpressionGenericallyWithTypeCoercion(expression: Expression): Expression = {
    val expressionWithResolvedChildren = withResolvedChildren(expression, resolve)
    typeCoercionResolver.resolve(expressionWithResolvedChildren)
  }

  private def resolveExpressionTreeInOperatorImpl(
      unresolvedExpression: Expression,
      parentOperator: LogicalPlan): (Expression, ExpressionResolutionContext) = {
    this.parentOperators.push(parentOperator)
    expressionResolutionContextStack.push(new ExpressionResolutionContext)
    try {
      val resolvedExpression = resolve(unresolvedExpression)
      (resolvedExpression, expressionResolutionContextStack.peek())
    } finally {
      expressionResolutionContextStack.pop()
      this.parentOperators.pop()
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
      case _: UnresolvedNamedLambdaVariable =>
        throw new ExplicitlyUnsupportedResolverFeature("Lambda variables")
      case _ =>
        withPosition(unresolvedNamedExpression) {
          throwUnsupportedSinglePassAnalyzerFeature(unresolvedNamedExpression)
        }
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
   * If the attribute is at the top of the project list (which is indicated by
   * [[isTopOfProjectList]]), we preserve the [[Alias]] or remove it otherwise.
   *
   * Finally, we remap the expression ID of a top [[NamedExpression]]. It's not necessary to remap
   * the expression ID of lower expressions, because they already got the appropriate ID from the
   * current scope output in [[resolveMultipartName]].
   */
  private def resolveAttribute(unresolvedAttribute: UnresolvedAttribute): Expression =
    withPosition(unresolvedAttribute) {
      expressionResolutionContextStack.peek().hasAttributeInASubtree = true

      val nameTarget: NameTarget =
        scopes.top.resolveMultipartName(unresolvedAttribute.nameParts, isLcaEnabled)

      val candidate = nameTarget.pickCandidate(unresolvedAttribute)

      if (isLcaEnabled) {
        nameTarget.lateralAttributeReference match {
          case Some(lateralAttributeReference) =>
            scopes.top.lcaRegistry
              .markAttributeLaterallyReferenced(lateralAttributeReference)
            candidate.setTagValue(ExpressionResolver.SINGLE_PASS_IS_LCA, ())
            expressionResolutionContextStack.peek().hasLateralColumnAlias = true
          case None =>
        }
      }

      val properlyAliasedExpressionTree =
        if (isTopOfProjectList && nameTarget.aliasName.isDefined) {
          Alias(candidate, nameTarget.aliasName.get)()
        } else {
          candidate
        }

      properlyAliasedExpressionTree match {
        case namedExpression: NamedExpression =>
          expressionIdAssigner.mapExpression(namedExpression)
        case _ =>
          properlyAliasedExpressionTree
      }
    }

  /**
   * [[AttributeReference]] is already resolved if it's passed to us from DataFrame `col(...)`
   * function, for example.
   */
  private def handleResolvedAttributeReference(attributeReference: AttributeReference) = {
    val strippedAttributeReference = tryStripAmbiguousSelfJoinMetadata(attributeReference)
    val resultAttribute = expressionIdAssigner.mapExpression(strippedAttributeReference)

    if (!scopes.top.hasAttributeWithId(resultAttribute.exprId)) {
      throw new ExplicitlyUnsupportedResolverFeature("DataFrame missing attribute propagation")
    }

    resultAttribute
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
  def resolveStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] =
    withPosition(unresolvedStar) {
      scopes.top.expandStar(unresolvedStar)
    }

  /**
   * [[Literal]] resolution doesn't require any specific resolution logic at this point.
   */
  private def resolveLiteral(literal: Literal): Expression = literal

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
    val candidates = scopes.top.findAttributesByName(getViewColumnByNameAndOrdinal.colName)
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
   * Resolves [[Expression]] by calling [[timezoneAwareExpressionResolver]] to resolve
   * expression's children and apply timezone if needed. Applies generic type coercion
   * rules to the result.
   */
  private def resolveExpressionGenericallyWithTimezoneWithTypeCoercion(
      timezoneAwareExpression: TimeZoneAwareExpression): Expression = {
    val expressionWithTimezone = timezoneAwareExpressionResolver.resolve(timezoneAwareExpression)
    typeCoercionResolver.resolve(expressionWithTimezone)
  }

  /**
   * Resolves [[Expression]] only by resolving its children. This resolution method is used for
   * nodes that don't require any special resolution other than resolving its children.
   */
  private def resolveExpressionGenerically(expression: Expression): Expression =
    withResolvedChildren(expression, resolve)

  private def popResolutionContext(): Unit = {
    val currentExpressionResolutionContext = expressionResolutionContextStack.pop()
    expressionResolutionContextStack.peek().merge(currentExpressionResolutionContext)
  }

  private def pushResolutionContext(): Unit = {
    isTopOfProjectList = expressionResolutionContextStack
        .size() == 1 && parentOperators.peek().isInstanceOf[Project]

    expressionResolutionContextStack.push(new ExpressionResolutionContext)
  }

  private def tryPopSinglePassSubtreeBoundary(unresolvedExpression: Expression): Boolean = {
    if (unresolvedExpression
        .getTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
        .isDefined) {
      unresolvedExpression.unsetTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
      true
    } else {
      false
    }
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
