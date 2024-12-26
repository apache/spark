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
  withPosition,
  FunctionResolution,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  BinaryArithmetic,
  ConditionalExpression,
  CreateNamedStruct,
  Expression,
  ExtractANSIIntervalDays,
  InheritAnalysisRules,
  Literal,
  NamedExpression,
  Predicate,
  RuntimeReplaceable,
  TimeAdd,
  TimeZoneAwareExpression,
  UnaryMinus
}
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
    scopes: NameScopeStack,
    functionResolution: FunctionResolution,
    planLogger: PlanLogger)
    extends TreeNodeResolver[Expression, Expression]
    with ProducesUnresolvedSubtree
    with ResolvesExpressionChildren
    with TracksResolvedNodes[Expression] {
  private val shouldTrackResolvedNodes =
    conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_TRACK_RESOLVED_NODES_ENABLED)
  private val aliasResolver = new AliasResolver(this, scopes)
  private val createNamedStructResolver = new CreateNamedStructResolver(this)
  private val timezoneAwareExpressionResolver = new TimezoneAwareExpressionResolver(this)
  private val conditionalExpressionResolver =
    new ConditionalExpressionResolver(this, timezoneAwareExpressionResolver)
  private val predicateResolver =
    new PredicateResolver(this, timezoneAwareExpressionResolver)
  private val binaryArithmeticResolver = {
    new BinaryArithmeticResolver(
      this,
      timezoneAwareExpressionResolver
    )
  }
  private val functionResolver = new FunctionResolver(
    this,
    timezoneAwareExpressionResolver,
    functionResolution
  )
  private val timeAddResolver = new TimeAddResolver(this, timezoneAwareExpressionResolver)
  private val unaryMinusResolver = new UnaryMinusResolver(this, timezoneAwareExpressionResolver)

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
  override def resolve(unresolvedExpression: Expression): Expression = {
    planLogger.logExpressionTreeResolutionEvent(unresolvedExpression, "Unresolved expression tree")

    if (unresolvedExpression
        .getTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
        .nonEmpty) {
      unresolvedExpression
    } else {
      throwIfNodeWasResolvedEarlier(unresolvedExpression)

      val resolvedExpression = unresolvedExpression match {
        case unresolvedBinaryArithmetic: BinaryArithmetic =>
          binaryArithmeticResolver.resolve(unresolvedBinaryArithmetic)
        case unresolvedExtractANSIIntervalDays: ExtractANSIIntervalDays =>
          resolveExtractANSIIntervalDays(unresolvedExtractANSIIntervalDays)
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
          createNamedStructResolver.resolve(createNamedStruct)
        case unresolvedConditionalExpression: ConditionalExpression =>
          conditionalExpressionResolver.resolve(unresolvedConditionalExpression)
        case unresolvedRuntimeReplaceable: RuntimeReplaceable =>
          resolveRuntimeReplaceable(unresolvedRuntimeReplaceable)
        case unresolvedTimezoneExpression: TimeZoneAwareExpression =>
          timezoneAwareExpressionResolver.resolve(unresolvedTimezoneExpression)
        case _ =>
          withPosition(unresolvedExpression) {
            throwUnsupportedSinglePassAnalyzerFeature(unresolvedExpression)
          }
      }

      markNodeAsResolved(resolvedExpression)

      planLogger.logExpressionTreeResolution(unresolvedExpression, resolvedExpression)

      resolvedExpression
    }
  }

  private def resolveNamedExpression(
      unresolvedNamedExpression: Expression,
      isTopOfProjectList: Boolean = false): Expression =
    unresolvedNamedExpression match {
      case alias: Alias =>
        aliasResolver.handleResolvedAlias(alias)
      case unresolvedAlias: UnresolvedAlias =>
        aliasResolver.resolve(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        resolveAttribute(unresolvedAttribute, isTopOfProjectList)
      case unresolvedStar: UnresolvedStar =>
        withPosition(unresolvedStar) {
          throwInvalidStarUsageError(unresolvedStar)
        }
      case attributeReference: AttributeReference =>
        handleResolvedAttributeReference(attributeReference)
      case _ =>
        withPosition(unresolvedNamedExpression) {
          throwUnsupportedSinglePassAnalyzerFeature(unresolvedNamedExpression)
        }
    }

  /**
   * The [[Project]] list can contain different unresolved expressions before the resolution, which
   * will be resolved using generic [[resolve]]. However, [[UnresolvedStar]] is a special case,
   * because it is expanded into a sequence of [[NamedExpression]]s. Because of that this method
   * returns a sequence and doesn't conform to generic [[resolve]] interface - it's called directly
   * from the [[Resolver]] during [[Project]] resolution.
   *
   * The output sequence can be larger than the input sequence due to [[UnresolvedStar]] expansion.
   */
  def resolveProjectList(unresolvedProjectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    unresolvedProjectList.flatMap {
      case unresolvedStar: UnresolvedStar =>
        resolveStar(unresolvedStar)
      case other =>
        Seq(resolveNamedExpression(other, isTopOfProjectList = true).asInstanceOf[NamedExpression])
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
   * If the attribute is at the top of the project list (which is indicated by
   * [[isTopOfProjectList]]), we preserve the [[Alias]] or remove it otherwise.
   */
  private def resolveAttribute(
      unresolvedAttribute: UnresolvedAttribute,
      isTopOfProjectList: Boolean): Expression =
    withPosition(unresolvedAttribute) {
      if (scopes.top.isExistingAlias(unresolvedAttribute.nameParts.head)) {
        // Temporarily disable referencing aliases until we support LCA resolution.
        throw new ExplicitlyUnsupportedResolverFeature("unsupported expression: LateralColumnAlias")
      }

      val nameTarget: NameTarget = scopes.top.matchMultipartName(unresolvedAttribute.nameParts)

      val candidate = nameTarget.pickCandidate(unresolvedAttribute)
      if (isTopOfProjectList && nameTarget.aliasName.isDefined) {
        Alias(candidate, nameTarget.aliasName.get)()
      } else {
        candidate
      }
    }

  /**
   * [[AttributeReference]] is already resolved if it's passed to us from DataFrame `col(...)`
   * function, for example.
   */
  private def handleResolvedAttributeReference(attributeReference: AttributeReference) =
    tryStripAmbiguousSelfJoinMetadata(attributeReference)

  /**
   * [[ExtractANSIIntervalDays]] resolution doesn't require any specific resolution logic apart
   * from resolving its children.
   */
  private def resolveExtractANSIIntervalDays(
      unresolvedExtractANSIIntervalDays: ExtractANSIIntervalDays) =
    withResolvedChildren(unresolvedExtractANSIIntervalDays, resolve)

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
   *
   * Since [[TracksResolvedNodes]] requires all the expressions in the tree to be unique objects,
   * we reallocate the literal in [[ANALYZER_SINGLE_PASS_TRACK_RESOLVED_NODES_ENABLED]] mode,
   * otherwise we preserve the old object to avoid unnecessary memory allocations.
   */
  private def resolveLiteral(literal: Literal): Expression = {
    if (shouldTrackResolvedNodes) {
      literal.copy()
    } else {
      literal
    }
  }

  /**
   * When [[RuntimeReplaceable]] is mixed in with [[InheritAnalysisRules]], child expression will
   * be runtime replacement. In that case we need to resolve the children of the expression.
   * otherwise, no resolution is necessary because replacement is already resolved.
   */
  private def resolveRuntimeReplaceable(unresolvedRuntimeReplaceable: RuntimeReplaceable) =
    unresolvedRuntimeReplaceable match {
      case inheritAnalysisRules: InheritAnalysisRules =>
        withResolvedChildren(inheritAnalysisRules, resolve)
      case other => other
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

  private def throwInvalidStarUsageError(unresolvedStar: UnresolvedStar): Nothing =
    // TODO(vladimirg-db): Use parent operator name instead of "query"
    throw QueryCompilationErrors.invalidStarUsageError("query", Seq(unresolvedStar))
}

object ExpressionResolver {
  private val AMBIGUOUS_SELF_JOIN_METADATA = Seq("__dataset_id", "__col_position")
  val SINGLE_PASS_SUBTREE_BOUNDARY = TreeNodeTag[Unit]("single_pass_subtree_boundary")
}
