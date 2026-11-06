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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{CoalesceHintUtils, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{
  HintErrorHandler,
  HintInfo,
  JoinStrategyHint,
  LogicalPlan,
  RebalancePartitions,
  ResolvedHint,
  SubqueryAlias,
  UnresolvedHint
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedHint]] operators by dispatching to hint-specific resolution logic.
 */
class HintResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[UnresolvedHint, LogicalPlan]
    with SQLConfHelper {

  private val scopes = resolver.getNameScopes

  /**
   * Resolves an [[UnresolvedHint]] by dispatching to the appropriate method based on hint name:
   * - If hints are disabled via [[SQLConf.DISABLE_HINTS]], the hint is stripped and its resolved
   *   child is returned directly.
   * - For coalesce-type hints (COALESCE, REPARTITION, REPARTITION_BY_RANGE, REBALANCE,
   *   REBALANCE_BY_SIZE), the [[UnresolvedHint]] is transformed into the corresponding plan node
   *   ([[Repartition]], [[RepartitionByExpression]], or [[RebalancePartitions]]) and then resolved
   *   through the main [[Resolver]].
   * - For join strategy hints (BROADCAST, BROADCASTJOIN, MAPJOIN, SHUFFLE_MERGE, MERGE, MERGEJOIN,
   *   SHUFFLE_HASH, SHUFFLE_REPLICATE_NL), the child is resolved first, then the hint is applied
   *   by either wrapping the entire child with [[ResolvedHint]] (when no table names are specified)
   *   or traversing the resolved child to find matching relation aliases.
   * - For truly unrecognized hints, the child is resolved and the [[UnresolvedHint]] wrapper is
   *   stripped, notifying the active [[HintErrorHandler]]. Hints whose name is not one of the
   *   known built-in hint names (see [[ResolverGuard.BUILTIN_HINT_NAMES]]) never reach this
   *   resolver: [[ResolverGuard]] rejects such queries up front so they fall back to the
   *   fixed-point analyzer, where custom hint resolution rules registered via
   *   [[SparkSessionExtensions.injectHintResolutionRule]] can run.
   */
  override def resolve(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hintName = unresolvedHint.name.toUpperCase(Locale.ROOT)
    hintName match {
      case _ if conf.getConf(SQLConf.DISABLE_HINTS) =>
        resolveChildAndStripHint(unresolvedHint)
      case "REPARTITION" =>
        resolveCoalesceHint(shuffle = true, unresolvedHint)
      case "COALESCE" =>
        resolveCoalesceHint(shuffle = false, unresolvedHint)
      case "REPARTITION_BY_RANGE" =>
        resolveRepartitionByRangeHint(unresolvedHint)
      case "REBALANCE" if conf.adaptiveExecutionEnabled =>
        resolveRebalanceHint(unresolvedHint)
      case "REBALANCE_BY_SIZE" if conf.adaptiveExecutionEnabled =>
        resolveRebalanceBySizeHint(unresolvedHint)
      case _ if isJoinStrategyHintName(hintName) =>
        resolveJoinStrategyHint(unresolvedHint)
      case _ =>
        resolveChildAndStripHint(unresolvedHint)
    }
  }

  /**
   * Resolves the child and strips the hint, notifying the error handler that the hint was not
   * recognized. Called by [[resolve]] when [[SQLConf.DISABLE_HINTS]] is set or when the hint
   * name does not match any known hint.
   */
  private def resolveChildAndStripHint(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val resolvedChild = resolver.resolve(unresolvedHint.child)
    activeHintErrorHandler.hintNotRecognized(unresolvedHint.name, unresolvedHint.parameters)
    resolvedChild
  }

  /**
   * Transforms a COALESCE or REPARTITION hint into a [[Repartition]] or
   * [[RepartitionByExpression]] node, then re-enters the resolver to resolve the resulting plan.
   * Called by [[resolve]] for the "COALESCE" and "REPARTITION" hint names. The transformation
   * is one of:
   *   - Only a partition count specified (such as `REPARTITION(3)`): a simple [[Repartition]].
   *   - `COALESCE` with anything other than just a partition count is rejected, since `COALESCE`
   *     only supports a partition count.
   *   - `REPARTITION` with partition expressions and an optional count (like `REPARTITION(3, c1)`
   *     or `REPARTITION(c1)`) becomes a [[RepartitionByExpression]].
   */
  private def resolveCoalesceHint(
      shuffle: Boolean,
      unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hint = transformStringToAttribute(unresolvedHint)
    resolver.resolve(CoalesceHintUtils.createRepartition(shuffle, hint))
  }

  /**
   * Transforms a REPARTITION_BY_RANGE hint into a [[RepartitionByExpression]] node with sort
   * orders, then re-enters the resolver. Called by [[resolve]] for the "REPARTITION_BY_RANGE"
   * hint name.
   */
  private def resolveRepartitionByRangeHint(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hint = transformStringToAttribute(unresolvedHint)
    resolver.resolve(CoalesceHintUtils.createRepartitionByRange(hint))
  }

  /**
   * Transforms a REBALANCE hint into a [[RebalancePartitions]] node, resolving the child and
   * partition expressions. Called by [[resolve]] for the "REBALANCE" hint name (only when AQE
   * is enabled).
   */
  private def resolveRebalanceHint(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hint = transformStringToAttribute(unresolvedHint)

    val (numPartitionsOption, partitionExpressions) = CoalesceHintUtils.getNumOfPartitions(hint)
    CoalesceHintUtils.validateParameters(hint.name, partitionExpressions)

    resolveRebalanceExpressions(
      hint = hint,
      partitionExpressions = partitionExpressions,
      optNumPartitions = numPartitionsOption,
      optAdvisoryPartitionSize = None)
  }

  /**
   * Transforms a `REBALANCE_BY_SIZE` hint into a [[RebalancePartitions]] node, resolving the child
   * and partition expressions. The first parameter is preserved as the advisory partition size;
   * subsequent string parameters are treated as partition column names.
   * Called by [[resolve]] for the "REBALANCE_BY_SIZE" hint name (only when AQE is enabled).
   */
  private def resolveRebalanceBySizeHint(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hint = transformStringToAttribute(unresolvedHint, skipFirstParameter = true)

    val (advisoryPartitionSize, partitionExpressions) =
      CoalesceHintUtils.getAdvisorySizeOfPartitions(hint)
    CoalesceHintUtils.validateParameters(hint.name, partitionExpressions)

    resolveRebalanceExpressions(
      hint = hint,
      partitionExpressions = partitionExpressions,
      optNumPartitions = None,
      optAdvisoryPartitionSize = Some(advisoryPartitionSize))
  }

  private def resolveRebalanceExpressions(
      hint: UnresolvedHint,
      partitionExpressions: Seq[Expression],
      optNumPartitions: Option[Int],
      optAdvisoryPartitionSize: Option[Long]): RebalancePartitions = {

    val resolvedChild = resolver.resolve(hint.child)
    val hintWithResolvedChild = hint.copy(child = resolvedChild)

    val resolvedExpressions = partitionExpressions.map { expression =>
      expressionResolver.resolveExpressionTreeInOperator(
        unresolvedExpression = expression,
        parentOperator = hintWithResolvedChild
      )
    }

    RebalancePartitions(
      partitionExpressions = resolvedExpressions,
      child = resolvedChild,
      optNumPartitions = optNumPartitions,
      optAdvisoryPartitionSize = optAdvisoryPartitionSize)
  }

  /**
   * Resolves a join strategy hint by wrapping matching relations with [[ResolvedHint]]. If the
   * hint specifies no table names, the entire resolved child is wrapped. If table names are
   * specified, the resolved child is traversed top-down by [[addHintOnRelation]] to find
   * [[SubqueryAlias]] nodes whose identifiers match.
   * Called by [[resolve]] for join strategy hint names (BROADCAST, SHUFFLE_MERGE, etc.).
   */
  private def resolveJoinStrategyHint(unresolvedHint: UnresolvedHint): LogicalPlan = {
    val hintName = unresolvedHint.name.toUpperCase(Locale.ROOT)
    val strategy = JoinStrategyHint.strategies.find { candidateStrategy =>
      candidateStrategy.hintAliases
        .map(_.toUpperCase(Locale.ROOT))
        .contains(hintName)
    }
    val hintInfo = HintInfo(strategy = strategy)

    applyHintToResolvedChild(
      unresolvedHint, extractJoinStrategyRelationNames(unresolvedHint), _ => hintInfo)
  }

  /**
   * Parses the relation identifiers from a join strategy hint's parameters. Each parameter must
   * be either a string literal or an [[UnresolvedAttribute]]; any other expression is rejected.
   */
  private def extractJoinStrategyRelationNames(
      unresolvedHint: UnresolvedHint): Set[Seq[String]] = {
    unresolvedHint.parameters.map {
      case StringLiteral(tableName) =>
        UnresolvedAttribute.parseAttributeName(tableName)
      case tableId: UnresolvedAttribute =>
        tableId.nameParts
      case unsupported =>
        throw QueryCompilationErrors.joinStrategyHintParameterNotSupportedError(unsupported)
    }.toSet
  }


  /**
   * Returns the [[HintErrorHandler]] used to report hint resolution issues. In dual-run mode this
   * returns a silent no-op handler to avoid duplicate notifications from both analyzers.
   */
  private def activeHintErrorHandler: HintErrorHandler = {
    if (HybridAnalyzer.isSinglePassResolverInDualRun) {
      HintResolver.SILENT_HINT_ERROR_HANDLER
    } else {
      conf.hintErrorHandler
    }
  }

  /**
   * Resolves the hint's child and applies the hint. When `relationNames` is empty, wraps the
   * entire resolved child with [[ResolvedHint]] using the resolved child's output (read from
   * the current name scope). Otherwise, installs a [[HintApplicationContext]] on the
   * [[OperatorResolutionContext]] that corresponds to this [[UnresolvedHint]] before resolving
   * the child; [[Resolver.resolveSubqueryAlias]] discovers it via [[maybeApplyHint]] by walking
   * up the [[OperatorResolutionContextStack]], stopping at the first ancestor [[SubqueryAlias]]
   * or [[View]], and wraps any matching alias with [[ResolvedHint]] inline as part of the
   * bottom-up resolution. After resolution, any relation identifiers that did not match are
   * reported through the active [[HintErrorHandler]].
   */
  private def applyHintToResolvedChild(
      unresolvedHint: UnresolvedHint,
      relationNames: Set[Seq[String]],
      hintInfoFactory: Seq[Attribute] => HintInfo): LogicalPlan = {
    if (relationNames.isEmpty) {
      val resolvedChild = resolver.resolve(unresolvedHint.child)
      val childOutput = scopes.current.output
      ResolvedHint(resolvedChild, hintInfoFactory(childOutput))
    } else {
      val hintApplicationContext = new HintApplicationContext(relationNames, hintInfoFactory)
      val operatorContext = resolver.getOperatorResolutionContextStack.current
      operatorContext.hintApplicationContext = Some(hintApplicationContext)
      val resolvedChild =
        try {
          resolver.resolve(unresolvedHint.child)
        } finally {
          operatorContext.hintApplicationContext = None
        }
      val unmatchedIdentifiers = relationNames -- hintApplicationContext.matchedRelations
      if (unmatchedIdentifiers.nonEmpty) {
        activeHintErrorHandler.hintRelationsNotFound(
          unresolvedHint.name, unresolvedHint.parameters, unmatchedIdentifiers
        )
      }

      resolvedChild
    }
  }

  private def isJoinStrategyHintName(hintName: String): Boolean = {
    JoinStrategyHint.isJoinStrategyHintName(hintName)
  }

  private def transformStringToAttribute(
      hint: UnresolvedHint,
      skipFirstParameter: Boolean = false): UnresolvedHint =
    CoalesceHintUtils.transformStringToAttribute(hint, skipFirstParameter)

  /**
   * Called by [[Resolver.resolveSubqueryAlias]] after a [[SubqueryAlias]] has been fully
   * resolved. Reads the [[HintApplicationContext]]s installed by enclosing [[UnresolvedHint]]
   * operators that are visible to this alias from
   * [[OperatorResolutionContext.enclosingHintApplicationContexts]] (precomputed when the context
   * is pushed and already scoped to not cross ancestor [[SubqueryAlias]]/[[View]] boundaries).
   * For each hint application whose relation identifiers match the alias's identifier, the alias
   * is wrapped with [[ResolvedHint]] carrying the hint-specific [[HintInfo]]. Multiple matching
   * hints are combined by merging their [[HintInfo]]s via [[HintInfo.merge]] in the order they
   * were encountered (innermost first), reusing the active [[HintErrorHandler]] for conflicts.
   *
   * `identifier` is the relation identifier to match the hint against. It's the identifier as
   * written in the query for relations resolved from an [[UnresolvedRelation]] (see
   * [[Resolver.resolveRelation]]), and the [[SubqueryAlias]] identifier otherwise.
   */
  def maybeApplyHint(resolvedSubqueryAlias: SubqueryAlias, identifier: Seq[String]): LogicalPlan = {
    val enclosingHintApplications =
      resolver.getOperatorResolutionContextStack.current.enclosingHintApplicationContexts
    val matchingHintApplications = enclosingHintApplications.filter { hintApplication =>
      hintApplication.relationNames
        .find(HintResolver.matchedIdentifier(_, identifier, conf.resolver))
        .map { matchedName =>
          hintApplication.matchedRelations.add(matchedName)
          true
        }
        .getOrElse(false)
    }

    if (matchingHintApplications.isEmpty) {
      resolvedSubqueryAlias
    } else {
      val aliasOutput = scopes.current.output
      val combinedHintInfo = matchingHintApplications
        .map(_.hintInfoFactory(aliasOutput))
        .reduce((inner, outer) => outer.merge(inner, activeHintErrorHandler))
      ResolvedHint(resolvedSubqueryAlias, combinedHintInfo)
    }
  }

  /**
   * Resolves a [[ResolvedHint]] by recursing into its child. This handles the case where a
   * [[ResolvedHint]] appears in an already-partially-resolved plan (e.g. DataFrame programs).
   * Called by [[Resolver.resolve]] when it encounters a [[ResolvedHint]] operator.
   */
  def handleResolvedHint(resolvedHint: ResolvedHint): LogicalPlan = {
    resolvedHint.copy(child = resolver.resolve(resolvedHint.child))
  }
}

object HintResolver {

  /**
   * A no-op [[HintErrorHandler]] that silently discards all hint notifications. Used by
   * [[HintResolver.activeHintErrorHandler]] in dual-run mode, where the fixed-point Analyzer
   * handles hint error reporting.
   */
  val SILENT_HINT_ERROR_HANDLER: HintErrorHandler = new HintErrorHandler {
    override def hintNotRecognized(name: String, parameters: Seq[Expression]): Unit = {}
    override def hintRelationsNotFound(
        name: String, parameters: Seq[Any], invalidRelations: Set[Seq[String]]): Unit = {}
    override def joinNotFoundForJoinHint(hint: HintInfo): Unit = {}
    override def joinHintNotSupported(hint: HintInfo, reason: String): Unit = {}
    override def hintOverridden(hint: HintInfo): Unit = {}
  }
  /**
   * Checks whether a multi-part identifier specified in a hint matches a relation identifier from
   * the query. They match if the identifier in the hint is a (right-aligned) suffix of the
   * identifier in the query, compared element-wise using the provided `resolver` for case
   * sensitivity. The comparison is purely literal and does not consult the session catalog (e.g.
   * the current database), so:
   *  - a `BROADCAST(t)` hint on `SELECT * FROM db1.t JOIN t` matches both `db1.t` and `t`, even
   *    when the current database is `db2`;
   *  - a `BROADCAST(default.t)` hint on `SELECT * FROM default.t JOIN t` matches only the
   *    left-side `default.t`.
   *
   * For example, hint identifier `Seq("t1")` matches query identifier
   * `Seq("catalog", "db", "t1")`, while `Seq("db", "t1")` does not match `Seq("t1")` because the
   * hint identifier is longer than the query identifier.
   */
  def matchedIdentifier(
      identInHint: Seq[String],
      identInQuery: Seq[String],
      resolver: (String, String) => Boolean): Boolean = {
    if (identInHint.length <= identInQuery.length) {
      identInHint.zip(identInQuery.takeRight(identInHint.length))
        .forall { case (i1, i2) => resolver(i1, i2) }
    } else {
      false
    }
  }
}
