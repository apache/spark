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

import java.util.{HashMap, HashSet, IdentityHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AnalysisContext,
  AnalysisErrorAt,
  CleanupAliases,
  FunctionResolution,
  IdentifierResolution,
  MultiInstanceRelation,
  NameParameterizedQuery,
  PosParameterizedQuery,
  PullOutNondeterministic,
  RelationCache,
  RelationResolution,
  ResolvedInlineTable,
  UnresolvedHaving,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedSubqueryColumnAliases,
  UnresolvedTableValuedFunction,
  UnresolvedTVFAliases,
  ValidateSubqueryExpression
}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSet,
  Expression,
  ExprId,
  PipeOperator
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{PLAN_EXPRESSION, UNRESOLVED_RELATION}
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}

/**
 * The Resolver implements a single-pass bottom-up analysis algorithm in the Catalyst.
 *
 * The functions here generally traverse the [[LogicalPlan]] nodes recursively,
 * constructing and returning the resolved [[LogicalPlan]] nodes bottom-up.
 * This is the primary entry point for implementing SQL and DataFrame plan analysis,
 * wherein the [[resolve]] method accepts a fully unresolved [[LogicalPlan]] and returns
 * a fully resolved [[LogicalPlan]] in response with all data types and attribute
 * reference ID assigned for valid requests. This resolver also takes responsibility
 * to detect any errors in the initial SQL query or DataFrame and return appropriate
 * error messages including precise parse locations wherever possible.
 *
 * The Resolver is a one-shot object per each SQL/DataFrame logical plan, the calling code must
 * re-create it for every new analysis run.
 *
 * @param catalogManager [[CatalogManager]] for relation and identifier resolution.
 * @param extensions A list of [[ResolverExtension]]s that will be used to resolve external
 *   operators.
 * @param metadataResolverExtensions A list of [[ResolverExtension]]s that will be used to resolve
 *   relation operators in [[MetadataResolver]].
 * @param externalRelationResolution An optional [[RelationResolution]] used to override the default
 *   one. The default is constructed using [[Resolver.createRelationResolution]].
 * @param extendedRewriteRules Additional post-resolution rules applied by the [[PlanRewriter]].
 */
class Resolver(
    catalogManager: CatalogManager,
    sharedRelationCache: RelationCache = RelationCache.empty,
    override val extensions: Seq[ResolverExtension] = Seq.empty,
    metadataResolverExtensions: Seq[ResolverExtension] = Seq.empty,
    externalRelationResolution: Option[RelationResolution] = None,
    extendedRewriteRules: Seq[Rule[LogicalPlan]] = Seq.empty)
    extends LogicalPlanResolver
    with ResolverMetricTracker
    with DelegatesResolutionToExtensions
    with InsertsMetadataColumns
    with QueryErrorsBase {
  private val planLogger = new PlanLogger
  private val subqueryRegistry = new SubqueryRegistry
  private val queryBlockScopes = new QueryBlockScopeStack
  private val nestedObjectRegistry = new NestedObjectRegistry
  private val scopes = new NameScopeStack(
    tempVariableManager = catalogManager.tempVariableManager,
    catalogManager = catalogManager,
    subqueryRegistry = subqueryRegistry,
    queryBlockScopes = queryBlockScopes,
    planLogger = planLogger
  )
  private val cteRegistry = new CteRegistry
  private val planPreprocessor =
    new PlanPreprocessor(this, resolveExpressionIdentifiersInRootScope = false)
  private val operatorResolutionContextStack = new OperatorResolutionContextStack
  private val relationResolution = externalRelationResolution.getOrElse {
    Resolver.createRelationResolution(catalogManager, sharedRelationCache)
  }
  private val functionResolution = new FunctionResolution(catalogManager, relationResolution)
  private val expressionResolver = new ExpressionResolver(this, functionResolution, planLogger)
  private val groupingAnalyticsResolver = new GroupingAnalyticsResolver(this, expressionResolver)
  private val aggregateResolver =
    new AggregateResolver(this, expressionResolver, groupingAnalyticsResolver)
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val autoGeneratedAliasProvider = expressionResolver.getAutoGeneratedAliasProvider
  private val cteDefinitionResolver = new CteDefinitionResolver(this, expressionResolver)
  private val projectResolver = new ProjectResolver(this, expressionResolver)
  private val viewResolver = new ViewResolver(resolver = this, catalogManager = catalogManager)
  private val setOperationLikeResolver =
    new SetOperationLikeResolver(this, expressionResolver)
  private val filterResolver = new FilterResolver(this, expressionResolver)
  private val sortResolver = new SortResolver(this, expressionResolver, groupingAnalyticsResolver)
  private val joinResolver = new JoinResolver(this, expressionResolver)
  private val asOfJoinResolver = new AsOfJoinResolver(this, expressionResolver)
  private val havingResolver =
    new HavingResolver(this, expressionResolver, groupingAnalyticsResolver)
  private val tableValuedFunctionResolver =
    new TableValuedFunctionResolver(this, expressionResolver, functionResolution)
  private val nameParameterizedQueryResolver =
    new NameParameterizedQueryResolver(this, expressionResolver)
  private val repartitionByExpressionResolver =
    new RepartitionByExpressionResolver(this, expressionResolver)
  private val pivotResolver = new PivotResolver(this, expressionResolver)
  private val unpivotResolver = new UnpivotResolver(this, expressionResolver)
  private val hintResolver = new HintResolver(this, expressionResolver)

  /**
   * Relation identifiers as written in the query, keyed by the [[SubqueryAlias]] that the
   * [[UnresolvedRelation]] was resolved to. Join strategy hints are matched against these
   * identifiers, like in [[ResolveHints.ResolveJoinStrategyHints]], which runs before the relations
   * are resolved. For example, `BROADCAST(db.t)` doesn't apply to `SELECT * FROM t JOIN t2`.
   */
  private val relationIdentifiersForHints = new IdentityHashMap[SubqueryAlias, Seq[String]]
  private val lateralJoinResolver = new LateralJoinResolver(this, expressionResolver)
  private val pipeOperatorResolver = new PipeOperatorResolver(this)
  private val posParameterizedQueryResolver = new PosParameterizedQueryResolver(this)
  private val binByResolver = new BinByResolver(this, expressionResolver)
  private val setVariableResolver = new SetVariableResolver(
    resolver = this,
    scopes = scopes,
    autoGeneratedAliasProvider = autoGeneratedAliasProvider,
    catalogManager = catalogManager)

  /**
   * Sequence of post-resolution rules that should be applied on the result of single-pass
   * resolution.
   */
  private val planRewriteRules: Seq[Rule[LogicalPlan]] = Seq(
    PruneMetadataColumns,
    CleanupAliases,
    PullOutNondeterministic
  )

  /**
   * `planRewriter` is used to rewrite the plan and the subqueries inside by applying
   * `planRewriteRules`.
   */
  private val planRewriter = new PlanRewriter(planRewriteRules, extendedRewriteRules)

  /**
   * [[relationMetadataProvider]] is used to resolve metadata for relations. It's initialized with
   * the default implementation [[MetadataResolver]] here and is called in
   * [[lookupMetadataAndResolve]] on the unresolved logical plan to visit it (both operators and
   * expressions) to resolve the metadata and populate its internal state. It's later queried by
   * [[resolveRelation]] to get the plan with resolved metadata (for example, a [[View]] or an
   * [[UnresolvedCatalogRelation]]) based on the [[UnresolvedRelation]].
   *
   * If the [[AnalyzerBridgeState]] is provided, we reset this provider to the
   * [[BridgedRelationMetadataProvider]] and later stick to it forever without resorting to the
   * actual blocking metadata resolution.
   */
  private var relationMetadataProvider: RelationMetadataProvider = new MetadataResolver(
    catalogManager,
    relationResolution,
    metadataResolverExtensions
  )

  /**
   * Get [[NameScopeStack]] bound to the used [[Resolver]].
   */
  def getNameScopes: NameScopeStack = scopes

  /**
   * Get [[QueryBlockScopeStack]] bound to the used [[Resolver]].
   */
  def getQueryBlockScopes: QueryBlockScopeStack = queryBlockScopes

  /**
   * Get [[OperatorResolutionContextStack]].
   */
  override def getOperatorResolutionContextStack: OperatorResolutionContextStack =
    operatorResolutionContextStack

  /**
   * Get the [[CteRegistry]] which is a single instance per query resolution.
   */
  def getCteRegistry: CteRegistry = cteRegistry

  /**
   * Get the [[SubqueryRegistry]] which is a single instance per query resolution.
   */
  def getSubqueryRegistry: SubqueryRegistry = subqueryRegistry

  /**
   * Get the [[NestedObjectRegistry]] which is a single instance per query resolution.
   */
  def getNestedObjectRegistry: NestedObjectRegistry = nestedObjectRegistry

  /**
   * Get the [[ViewResolver]] bound to the used [[Resolver]].
   */
  def getViewResolver: ViewResolver = viewResolver

  /**
   * Get the [[CatalogManager]] bound to the used [[Resolver]].
   */
  def getCatalogManager: CatalogManager = catalogManager

  /** Runs `body` with the query's named parameters available to expression resolution. */
  def withNameParameterizedQueryParameters[T](
      nameParameterizedQuery: NameParameterizedQuery)(body: => T): T = {
    operatorResolutionContextStack.push(nameParameterizedQuery)
    try {
      nameParameterizedQueryResolver.withResolvedParameters(nameParameterizedQuery)(body)
    } finally {
      operatorResolutionContextStack.pop()
    }
  }

  /** Resolves a root IDENTIFIER expression using this resolver's expression state. */
  def resolveIdentifierExpressionRoot(
      identifierExpression: Expression,
      parentOperator: LogicalPlan): Expression = {
    checkIdentifierExpressionSubquery(identifierExpression)
    expressionIdAssigner.withMappingForStandaloneExpression {
      expressionResolver.resolveIdentifierExpressionRoot(
        identifierExpression = identifierExpression,
        parentOperator = parentOperator
      )
    }
  }

  /**
   * Rejects relation-backed subqueries in a standalone IDENTIFIER expression. `PLAN_EXPRESSION`
   * identifies a nested logical plan, while `UNRESOLVED_RELATION` identifies a subquery requiring
   * relation metadata. Evaluating the unresolved identifier here preserves
   * `NOT_A_CONSTANT_STRING.NOT_CONSTANT` instead of surfacing `TABLE_OR_VIEW_NOT_FOUND` from
   * relation resolution.
   */
  private def checkIdentifierExpressionSubquery(identifierExpression: Expression): Unit = {
    if (identifierExpression.containsPattern(PLAN_EXPRESSION) &&
      identifierExpression.containsPattern(UNRESOLVED_RELATION)) {
      IdentifierResolution.evalIdentifierExpr(identifierExpression)
    }
  }

  /**
   * This method is a top-level analysis entry point:
   * 1. Substitute IDENTIFIERs and CTEs in the `unresolvedPlan` using
   *    [[PlanPreprocessor]];
   * 2. Resolve the metadata for the plan using [[MetadataResolver]]. When
   *    [[ANALYZER_SINGLE_PASS_RESOLVER_RELATION_BRIDGING_ENABLED]] is enabled, we need to
   *    re-instantiate the [[RelationMetadataProvider]] as [[View]] resolution context might have
   *    changed in the meantime;
   * 3. Resolve the plan using [[resolve]].
   * 4. Rewrites the plan using rules configured in the [[planRewriter]]. The plan rewrite is
   *    necessary in order to either fully resolve the plan or stay compatible with the fixed-point
   *    analyzer.
   *    Rewriting is done in `lookupMetadataAndResolve` so rules are applied after the main
   *    resolution both on the main plan and on the views. This is important so we apply these
   *    rules using proper configs that were stored in the [[View]] during its creation. Otherwise,
   *    rewrite rules that depend on config values may transform the plan incorrectly if we use
   *    the wrong config value (i.e. the current session config instead of the one stored in the
   *    view descriptor).
   *
   * This method is called for the top-level query and each unresolved [[View]].
   */
  def lookupMetadataAndResolve(
      unresolvedPlan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
    recordProfile("lookupMetadataAndResolve") {
      val planAfterSubstitution = substituteAndResolveMetadata(
        plan = unresolvedPlan,
        analyzerBridgeState = analyzerBridgeState
      )

      planLogger.logPlanResolutionEvent(planAfterSubstitution, "Main resolution")

      planAfterSubstitution.setTagValue(ResolverTag.TOP_LEVEL_OPERATOR, ())

      val resolvedPlan =
        recordProfile("resolve") {
          resolve(planAfterSubstitution)
        }

      recordProfile("rewrite") {
        planRewriter.rewriteWithSubqueries(resolvedPlan)
      }
    }
  }

  /**
   * Pre-resolution kernel: substitute IDENTIFIERs and CTEs via [[PlanPreprocessor]], then resolve
   * metadata via the [[RelationMetadataProvider]].
   *
   * When an [[AnalyzerBridgeState]] is provided, the provider is reset to a
   * [[BridgedRelationMetadataProvider]]; otherwise the already-installed provider is kept (the
   * self-assignment in the None branch documents that intent).
   */
  def substituteAndResolveMetadata(
      plan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
    planLogger.logPlanResolutionEvent(plan, "Preprocessing the unresolved plan")

    val planAfterSubstitution = planPreprocessor.substitutePlan(plan)

    planLogger.logPlanResolutionEvent(planAfterSubstitution, "Metadata lookup")

    relationMetadataProvider = analyzerBridgeState match {
      case Some(analyzerBridgeState) =>
        new BridgedRelationMetadataProvider(
          catalogManager,
          relationResolution,
          analyzerBridgeState,
          viewResolver
        )
      case None =>
        relationMetadataProvider
    }

    relationMetadataProvider.resolve(planAfterSubstitution)

    planAfterSubstitution
  }

  /**
   * This method takes an unresolved [[LogicalPlan]] and chooses the right `resolve*` method using
   * pattern matching on the `unresolvedPlan` type. This pattern matching enumerates all the
   * operator node types that are supported by the single-pass analysis.
   *
   * When developers introduce a new unresolved node type to the Catalyst, they should implement
   * a corresponding `resolve*` method in the [[Resolver]] and add it to this pattern match
   * list.
   *
   * [[resolve]] will be called recursively during the unresolved plan traversal eventually
   * producing a fully resolved plan or a descriptive error message.
   */
  override def resolve(unresolvedPlan: LogicalPlan): LogicalPlan = {
    val previousOrigin = CurrentOrigin.get
    CurrentOrigin.set(unresolvedPlan.origin)

    operatorResolutionContextStack.push(unresolvedPlan)

    try {
      planLogger.logPlanResolutionEvent(unresolvedPlan, "Unresolved plan")

      val resolvedPlan =
        unresolvedPlan match {
          case unresolvedJoin: Join =>
            joinResolver.resolve(unresolvedJoin)
          case unresolvedAsOfJoin: AsOfJoin =>
            asOfJoinResolver.resolve(unresolvedAsOfJoin)
          case unresolvedWith: UnresolvedWith =>
            resolveWith(unresolvedWith)
          case withCte: WithCTE =>
            handleResolvedWithCte(withCte)
          case unresolvedProject: Project =>
            projectResolver.resolve(unresolvedProject)
          case unresolvedAggregate: Aggregate =>
            aggregateResolver.resolve(unresolvedAggregate)
          case unresolvedFilter: Filter =>
            filterResolver.resolve(unresolvedFilter)
          case unresolvedHaving: UnresolvedHaving =>
            havingResolver.resolve(unresolvedHaving)
          case unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases =>
            resolveSubqueryColumnAliases(unresolvedSubqueryColumnAliases)
          case unresolvedSubqueryAlias: SubqueryAlias =>
            resolveSubqueryAlias(unresolvedSubqueryAlias)
          case unresolvedView: View =>
            viewResolver.resolve(unresolvedView)
          case unresolvedGlobalLimit: GlobalLimit =>
            resolveGlobalLimit(unresolvedGlobalLimit)
          case unresolvedLocalLimit: LocalLimit =>
            resolveLocalLimit(unresolvedLocalLimit)
          case unresolvedLimitAll: LimitAll =>
            resolveLimitAll(unresolvedLimitAll)
          case unresolvedOffset: Offset =>
            resolveOffset(unresolvedOffset)
          case unresolvedTail: Tail =>
            resolveTail(unresolvedTail)
          case unresolvedDistinct: Distinct =>
            resolveDistinct(unresolvedDistinct)
          case unresolvedRelation: UnresolvedRelation =>
            resolveRelation(unresolvedRelation)
          case unresolvedCteRelationRef: UnresolvedCteRelationRef =>
            resolveCteRelationRef(unresolvedCteRelationRef)
          case cteRelationDef: CTERelationDef =>
            handleResolvedCteRelationDef(cteRelationDef)
          case cteRelationRef: CTERelationRef =>
            handleLeafOperator(cteRelationRef)
          case unionLoopRef: UnionLoopRef =>
            handleLeafOperator(unionLoopRef)
          case unresolvedInlineTable: UnresolvedInlineTable =>
            resolveInlineTable(unresolvedInlineTable)
          case unresolvedSetOperationLike @ (_: UnionBase | _: SetOperation) =>
            setOperationLikeResolver.resolve(unresolvedSetOperationLike)
          case unresolvedSort: Sort =>
            sortResolver.resolve(unresolvedSort)
          // See the reason why we have to match both [[LocalRelation]] and [[ResolvedInlineTable]]
          // in the [[resolveInlineTable]] scaladoc
          case resolvedInlineTable: ResolvedInlineTable =>
            handleLeafOperator(resolvedInlineTable)
          case localRelation: LocalRelation =>
            handleLeafOperator(localRelation)
          case unresolvedOneRowRelation: OneRowRelation =>
            handleLeafOperator(unresolvedOneRowRelation)
          case unresolvedRange: Range =>
            handleLeafOperator(unresolvedRange)
          case hiveTableRelation: HiveTableRelation =>
            handleLeafOperator(hiveTableRelation)
          case supervisingCommand: SupervisingCommand =>
            resolveSupervisingCommand(supervisingCommand)
          case repartition: Repartition =>
            resolveRepartition(repartition)
          case repartitionByExpression: RepartitionByExpression =>
            repartitionByExpressionResolver.resolve(repartitionByExpression)
          case sample: Sample =>
            resolveSample(sample)
          case unresolvedTVF: UnresolvedTableValuedFunction =>
            tableValuedFunctionResolver.resolve(unresolvedTVF)
          case unresolvedTVFAliases: UnresolvedTVFAliases =>
            resolveUnresolvedTVFAliases(unresolvedTVFAliases)
          case setVariable: SetVariable =>
            setVariableResolver.resolve(setVariable)
          case posParameterizedQuery: PosParameterizedQuery =>
            posParameterizedQueryResolver.resolve(posParameterizedQuery)
          case unresolvedBinBy: UnresolvedBinBy =>
            binByResolver.resolve(unresolvedBinBy)
          case nameParameterizedQuery: NameParameterizedQuery =>
            nameParameterizedQueryResolver.resolve(nameParameterizedQuery)
          case pivot: Pivot =>
            pivotResolver.resolve(pivot)
          case unpivot: Unpivot =>
            unpivotResolver.resolve(unpivot)
          case lateralJoin: LateralJoin =>
            lateralJoinResolver.resolve(lateralJoin)
          case pipe: PipeOperator =>
            pipeOperatorResolver.resolve(pipe)
          case unresolvedHint: UnresolvedHint =>
            hintResolver.resolve(unresolvedHint)
          case resolvedHint: ResolvedHint =>
            hintResolver.handleResolvedHint(resolvedHint)
          case _ =>
            tryDelegateResolutionToExtension(unresolvedPlan).getOrElse {
              handleUnmatchedOperator(unresolvedPlan)
            }
        }

      val resolvedPlanWithMetadataColumns = insertReferencedMetadataColumnsForOperator(
        resolvedPlan = resolvedPlan,
        referencedMetadataColumns =
          operatorResolutionContextStack.current.referencedMetadataColumns,
        currentOutput = scopes.current.output,
        isTopLevel = unresolvedPlan.containsTag(ResolverTag.TOP_LEVEL_OPERATOR)
      )

      withPosition(unresolvedPlan) {
        validateResolvedOperatorGenerically(resolvedPlanWithMetadataColumns)
      }

      planLogger.logPlanResolution(unresolvedPlan, resolvedPlanWithMetadataColumns)

      resolvedPlanWithMetadataColumns.copyTagsFrom(unresolvedPlan)
      scopes.updatePlanIdTargetsForCurrentOperator(unresolvedPlan)

      resolvedPlanWithMetadataColumns
    } finally {
      operatorResolutionContextStack.pop()
      CurrentOrigin.set(previousOrigin)
    }
  }

  /**
   * [[UnresolvedWith]] contains a list of unresolved CTE definitions, which are represented by
   * (name, subquery) pairs, and an actual child query. First we resolve the CTE definitions
   * strictly in their declaration order, so they become available for other lower definitions
   * (lower both in this WITH clause list and in the plan tree) and for the [[UnresolvedWith]] child
   * query.
   *
   * After resolving all CTE definitions and the child query, if this is a root [[CteScope]] or if
   * `isInsideRecursiveCteScope` is set (for recursive CTEs), we return a [[WithCTE]] operator with
   * all the resolved [[CTERelationDef]]s merged together from this scope and child scopes.
   * Otherwise, we return the resolved child query so that the resolved [[CTERelationDef]]s
   * propagate up and will be merged together later.
   *
   * See [[CteScope]] scaladoc for all the details on how CTEs are resolved.
   */
  private def resolveWith(unresolvedWith: UnresolvedWith): LogicalPlan = {
    cteDefinitionResolver.resolve(unresolvedWith)

    cteRegistry.pushScope(
      isInsideRecursiveCteScope = cteRegistry.currentScope.isInsideRecursiveCteScope
    )

    val resolvedChild = try {
      resolve(unresolvedWith.child)
    } finally {
      cteRegistry.popScope()
    }

    cteRegistry.currentScope.tryPutWithCTE(
      unresolvedOperator = unresolvedWith,
      resolvedOperator = resolvedChild
    )
  }

  /**
   * We may meet resolved [[WithCTE]] while traversing partially resolved trees in DataFrame
   * programs. In that case we simply recurse into the CTE definitions and the main plan under
   * new scopes and mappings.
   */
  private def handleResolvedWithCte(withCte: WithCTE): LogicalPlan = {
    val resolvedCteDefs = withCte.cteDefs.map { cteDef =>
      expressionIdAssigner.pushMapping()
      queryBlockScopes.push()
      scopes.pushScope()
      cteRegistry.pushScope()

      try {
        resolve(cteDef).asInstanceOf[CTERelationDef]
      } finally {
        cteRegistry.popScope()
        scopes.popScope()
        queryBlockScopes.pop()
        expressionIdAssigner.popMapping()
      }
    }

    val resolvedPlan = resolve(withCte.plan)

    WithCTE(plan = resolvedPlan, cteDefs = resolvedCteDefs)
  }

  /**
   * [[UnresolvedSubqueryColumnAliases]] creates a [[Project]] on top of a [[SubqueryAlias]] with
   * the specified attribute aliases. Example:
   *
   * {{{
   * -- The output schema is [a: INT, b: INT]
   * SELECT t.a, t.b FROM VALUES (1, 2) t (a, b);
   * }}}
   *
   * For recursive CTEs with column aliases (e.g., `WITH RECURSIVE t(n) AS (...)`), the column
   * names must be registered in the CTE scope before resolving the child. This allows
   * self-references within the recursive branch to resolve columns using the provided aliases
   * rather than the anchor branch's output column names. For non-recursive CTEs, this registration
   * is a no-op.
   *
   * The [[Project]] outputs only the aliased columns, so this clears
   * [[NameScope.qualifiedAccessOnlyOutputIds]].
   */
  private def resolveSubqueryColumnAliases(
      unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases): LogicalPlan = {

    cteRegistry.currentScope.tryRegisterColumnNames(
      unresolvedSubqueryColumnAliases.outputColumnNames
    )

    val resolvedChild = resolve(unresolvedSubqueryColumnAliases.child)

    if (unresolvedSubqueryColumnAliases.outputColumnNames.size != scopes.current.output.size) {
      throw QueryCompilationErrors.aliasNumberNotMatchColumnNumberError(
        unresolvedSubqueryColumnAliases.outputColumnNames.size,
        scopes.current.output.size,
        unresolvedSubqueryColumnAliases
      )
    }

    val projectList =
      scopes.current.output.zip(unresolvedSubqueryColumnAliases.outputColumnNames).map {
        case (attr, columnName) =>
          autoGeneratedAliasProvider.newAlias(child = attr, name = Some(columnName))
      }

    scopes.overwriteCurrent(
      output = Some(projectList.map(_.toAttribute)),
      qualifiedAccessOnlyOutputIds = Some(new HashSet[ExprId])
    )

    Project(projectList = projectList, child = resolvedChild)
  }

  /**
   * [[SubqueryAlias]] has a single child and an identifier. We need to resolve the child and update
   * the scope with the output, since upper expressions can reference [[SubqueryAlias]]es output by
   * its identifier.
   *
   * Hidden output is reset when [[SubqueryAlias]] is reached during tree traversal. This has to be
   * done because upper SQL projection (or DataFrame) cannot look down into the previous
   * [[SubqueryAlias]]'s hidden output.
   * Examples (both will throw `UNRESOLVED_COLUMN` exception):
   *
   *  1. SQL
   *  {{{
   *  -- `hiddenOutput` and `availableAliases` will be reset at [[SubqueryAlias]]. Therefore both
   *  -- `output` and `hiddenOutput` will be [`col2`]. Because of that, `UNRESOLVED_COLUMN` is
   *  -- thrown when resolving `col1`
   *  SELECT col1 FROM (SELECT col2 FROM VALUES (1, 2));
   *  }}}
   *
   *  2. DataFrame
   *  {{{ spark.sql("SELECT * FROM VALUES (1, 2)").select("col1").as("q1").select("col2"); }}}
   */
  private def resolveSubqueryAlias(unresolvedSubqueryAlias: SubqueryAlias): LogicalPlan = {
    queryBlockScopes.push()
    val resolvedSubqueryAlias = try {
      unresolvedSubqueryAlias.copy(child = resolve(unresolvedSubqueryAlias.child))
    } finally {
      queryBlockScopes.pop()
    }

    val qualifier = resolvedSubqueryAlias.identifier.qualifier :+ resolvedSubqueryAlias.alias
    val output = scopes.current.output.map(attribute => attribute.withQualifier(qualifier))
    val metadataForHiddenOutput = computeMetadataHiddenOutputForSubqueryAlias(
      resolvedChild = resolvedSubqueryAlias.child,
      qualifier = qualifier,
      hiddenOutput = scopes.current.hiddenOutput
    )
    scopes.overwriteCurrent(
      output = Some(output),
      hiddenOutput = Some(output ++ metadataForHiddenOutput),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )

    val identifierForHints = Option(relationIdentifiersForHints.get(unresolvedSubqueryAlias))
      .getOrElse(
        resolvedSubqueryAlias.identifier.qualifier :+ resolvedSubqueryAlias.identifier.name
      )
    hintResolver.maybeApplyHint(resolvedSubqueryAlias, identifierForHints)
  }

  /**
   * Resolve [[GlobalLimit]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveGlobalLimit(unresolvedGlobalLimit: GlobalLimit): LogicalPlan = {
    val resolvedChild = resolve(unresolvedGlobalLimit.child)

    val resolvedLimitExpr = expressionResolver.resolveLimitLikeExpression(
      unresolvedGlobalLimit.limitExpr,
      unresolvedGlobalLimit.copy(child = resolvedChild)
    )

    GlobalLimit(resolvedLimitExpr, resolvedChild)
  }

  /**
   * Resolve [[LocalLimit]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveLocalLimit(unresolvedLocalLimit: LocalLimit): LogicalPlan = {
    val resolvedChild = resolve(unresolvedLocalLimit.child)

    val resolvedLimitExpr = expressionResolver.resolveLimitLikeExpression(
      unresolvedLocalLimit.limitExpr,
      unresolvedLocalLimit.copy(child = resolvedChild)
    )

    LocalLimit(resolvedLimitExpr, resolvedChild)
  }

  /**
   * Resolves [[LimitAll]] operator. [[LimitAll]] is typically a no-op in Spark, but for recursive
   * CTEs it signals unlimited recursion. The `resolvingLimitAll` flag is set in the
   * [[OperatorResolutionContext]] when pushing a context and propagated through allow-listed
   * operators to mark [[CTERelationRef]]s as unlimited. The [[LimitAll]] node itself is removed
   * from the plan.
   */
  private def resolveLimitAll(limitAll: LimitAll): LogicalPlan = {
    resolve(limitAll.child)
  }

  /**
   * Resolve [[Offset]]. We have to resolve its child and resolve and validate its offset
   * expression.
   */
  private def resolveOffset(unresolvedOffset: Offset): LogicalPlan = {
    val resolvedChild = resolve(unresolvedOffset.child)

    val resolvedOffsetExpr = expressionResolver.resolveLimitLikeExpression(
      unresolvedOffset.offsetExpr,
      unresolvedOffset.copy(child = resolvedChild)
    )

    Offset(resolvedOffsetExpr, resolvedChild)
  }

  /**
   * Resolve [[Tail]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveTail(unresolvedTail: Tail): LogicalPlan = {
    val resolvedChild = resolve(unresolvedTail.child)

    val resolvedTailExpr = expressionResolver.resolveLimitLikeExpression(
      unresolvedTail.limitExpr,
      unresolvedTail.copy(child = resolvedChild)
    )

    Tail(resolvedTailExpr, resolvedChild)
  }

  /**
   * [[Distinct]] operator doesn't require any special resolution.
   * We validate results of the resolution using the [[OperatorWithUncomparableTypeValidator]]
   * ([[MapType]], [[VariantType]], [[GeometryType]] and [[GeographyType]] are not supported
   * under [[Distinct]] operator).
   *
   * `hiddenOutput` and `availableAliases` are reset when [[Distinct]] is reached during tree
   * traversal. The [[QueryBlockScope]]'s [[Aggregate]] context is also cleared so
   * that upper sibling operators (ORDER BY in particular) do not pick up state from below.
   */
  private def resolveDistinct(unresolvedDistinct: Distinct): LogicalPlan = {
    val resolvedDistinct = unresolvedDistinct.copy(child = resolve(unresolvedDistinct.child))

    OperatorWithUncomparableTypeValidator.validate(resolvedDistinct, scopes.current.output)

    scopes.overwriteCurrent(
      hiddenOutput = Some(scopes.current.output),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )
    queryBlockScopes.current.clearAggregateState()
    resolvedDistinct
  }

  /**
   * [[UnresolvedRelation]] was previously looked up by the [[MetadataResolver]] and now we need to:
   * - Get the specific relation with metadata from `relationsWithResolvedMetadata`, like
   *   [[UnresolvedCatalogRelation]], or throw an error if it wasn't found
   * - Resolve it further, usually using extensions, like [[DataSourceResolver]]
   */
  private def resolveRelation(unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    viewResolver.withSourceUnresolvedRelation(unresolvedRelation) {
      val maybeResolvedRelation =
        relationMetadataProvider.getRelationWithResolvedMetadata(unresolvedRelation)

      val resolvedRelation = maybeResolvedRelation match {
        case Some(relationsWithResolvedMetadata) =>
          planLogger.logPlanResolutionEvent(
            relationsWithResolvedMetadata,
            "Relation metadata retrieved"
          )

          relationsWithResolvedMetadata
        case None =>
          val multipartId = unresolvedRelation.multipartIdentifier
          val catalogPath = {
            val ctx = AnalysisContext.get.catalogAndNamespace
            if (ctx.nonEmpty) ctx
            else (catalogManager.currentCatalog.name() +: catalogManager.currentNamespace).toSeq
          }
          val searchPath = catalogManager
            .sqlResolutionPathEntries(catalogPath.head, catalogPath.tail.toSeq)
            .map(toSQLId)
          unresolvedRelation.tableNotFound(multipartId, searchPath)
      }

      resolvedRelation match {
        case subqueryAlias: SubqueryAlias =>
          relationIdentifiersForHints.put(subqueryAlias, unresolvedRelation.multipartIdentifier)
          try {
            resolve(subqueryAlias)
          } finally {
            relationIdentifiersForHints.remove(subqueryAlias)
          }
        case other =>
          resolve(other)
      }
    }
  }

  /**
   * Resolve the [[UnresolvedCteRelationRef]] which was previously introduced by the
   * [[PlanPreprocessor]].
   */
  private def resolveCteRelationRef(
      unresolvedCteRelationRef: UnresolvedCteRelationRef): LogicalPlan = {
    val cteRelationRef =
      cteRegistry.resolveCteName(unresolvedCteRelationRef.name) match {
        case Some(cteRelationDef: CTERelationDef) =>
          planLogger.logPlanResolutionEvent(cteRelationDef, "CTE definition resolved")

          createCteRelationRef(
            name = unresolvedCteRelationRef.name,
            cteRelationDef = cteRelationDef
          )
        case Some(unionLoopRef: UnionLoopRef) =>
          SubqueryAlias(identifier = unresolvedCteRelationRef.name, child = unionLoopRef)
        case Some(
            unresolvedSubqueryColumnAliases @ UnresolvedSubqueryColumnAliases(_, _: UnionLoopRef)
            ) =>
          SubqueryAlias(
            identifier = unresolvedCteRelationRef.name,
            child = unresolvedSubqueryColumnAliases
          )
        case None =>
          unresolvedCteRelationRef.tableNotFound(Seq(unresolvedCteRelationRef.name))
        case Some(other) =>
          throw SparkException.internalError(
            s"Unexpected CTE definition for ${unresolvedCteRelationRef.name}: ${other.nodeName}"
          )
      }

    resolve(cteRelationRef)
  }

  /**
   * We may meet resolved [[CTERelationDef]] while traversing partially resolved trees in DataFrame
   * programs. In that case we simply recurse into the child plan.
   */
  private def handleResolvedCteRelationDef(cteRelationDef: CTERelationDef): LogicalPlan = {
    cteRelationDef.copy(child = resolve(cteRelationDef.child))
  }

  /**
   * [[UnresolvedInlineTable]] resolution requires all the rows to be resolved first. After that we
   * use [[EvaluateUnresolvedInlineTable]] and try to evaluate the row expressions if possible to
   * get [[LocalRelation]] right away. Sometimes it's not possible because of expressions like
   * `current_date()` which are evaluated in the optimizer (SPARK-46380).
   *
   * Note: By default if all the inline table expressions can be evaluated eagerly, the parser
   * would produce a [[LocalRelation]] and the analysis would just skip this step and go straight
   * to `resolveLocalRelation` (SPARK-48967, SPARK-49269).
   *
   * Row expressions may contain [[Alias]] wrappers from the SQL parser when users write
   * `VALUES (1 AS id, 2 AS name)`. These aliases are artifacts of the [[CreateNamedStruct]]
   * parsing. We resolve only the alias child and preserve the [[Alias]] wrapper, which
   * [[CleanupAliases]] strips during post-resolution. Unlike other leaf nodes, the resolved
   * relation is handled inline rather than via [[handleLeafOperator]], since the
   * [[ExpressionIdAssigner]] mapping must be created on the resolved result after row
   * expression resolution and evaluation.
   */
  private def resolveInlineTable(unresolvedInlineTable: UnresolvedInlineTable): LogicalPlan = {
    val withResolvedExpressions = UnresolvedInlineTable(
      unresolvedInlineTable.names,
      unresolvedInlineTable.rows.map(row => {
        row.map(unresolvedElement => {
          expressionResolver
            .resolveExpressionTreeInOperator(unresolvedElement, unresolvedInlineTable)
        })
      })
    )

    val resolvedRelation = EvaluateUnresolvedInlineTable
      .evaluateUnresolvedInlineTable(withResolvedExpressions)

    resolvedRelation match {
      case _: Expand =>
        throw new ExplicitlyUnsupportedResolverFeature(ResolutionGap.NonFoldableInlineTable)
      case leafOperator: LeafNode =>
        handleLeafOperator(leafOperator)
      case other =>
        resolve(other)
    }
  }

  /**
   * Delegate resolution of `unresolvedOperator` to a [[ResolverExtension]].
   */
  private def tryDelegateResolutionToExtension(
      unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    val resolutionResult = runExtensions(unresolvedOperator)
    resolutionResult.map { operator =>
      operator match {
        case leafOperator: LeafNode =>
          handleLeafOperator(leafOperator)
        case other => other
      }
    }
  }

  override def resolveExpressionTreeInOperator(
      unresolvedExpression: Expression,
      parentOperator: LogicalPlan): Expression =
    expressionResolver.resolveExpressionTreeInOperator(unresolvedExpression, parentOperator)

  private def runExtensions(unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    recordProfile("runExtensions") {
      super.tryDelegateResolutionToExtension(unresolvedOperator, this)
    }
  }

  /**
   * Leaf operators introduce original attributes to this operator subtree and need to be handled in
   * a special way:
   *  - Initialize [[ExpressionIdAssigner]] mapping for this operator branch and reassign
   *    `leafOperator`'s output attribute IDs. We don't reassign expression IDs in the leftmost
   *    branch, see [[ExpressionIdAssigner]] class doc for more details.
   *    [[CTERelationRef]]'s output can always be reassigned.
   *  - Overwrite the current [[NameScope]] with remapped output attributes (both
   *    [[NameScope.output]] and [[NameScope.hiddenOutput]] are updated). It's OK to call
   *    `output` on a [[LeafNode]], because it's not recursive (this call fits the single-pass
   *    framework).
   */
  override def handleLeafOperator(leafOperator: LeafNode): LogicalPlan = {
    val leafOperatorWithAssignedExpressionIds = leafOperator match {
      case leafOperator if expressionIdAssigner.shouldPreserveLeafOperatorIds(leafOperator) =>
        expressionIdAssigner.createMappingForLeafOperator(newOperator = leafOperator)

        leafOperator

      case originalLeafOperator =>
        val newLeafOperator = originalLeafOperator match {

          /**
           * [[InMemoryRelation.statsOfPlanToCache]] is mutable and does not get copied during
           * [[transformExpressionsUp]]. The easiest way to correctly copy it is via
           * [[newInstance]] call.
           *
           * We match [[MultiInstanceRelation]] to avoid a cyclic import between [[catalyst]] and
           * [[execution]].
           */
          case originalRelation: MultiInstanceRelation =>
            originalRelation.newInstance().asInstanceOf[LeafNode]

          case _ =>
            AnalysisHelper
              .allowInvokingTransformsInAnalyzer {
                leafOperator.transformExpressions {
                  case attribute: Attribute => attribute.newInstance()
                }
              }
              .asInstanceOf[LeafNode]
        }

        expressionIdAssigner.createMappingForLeafOperator(
          newOperator = newLeafOperator,
          oldOperator = Some(originalLeafOperator)
        )

        newLeafOperator
    }

    val output = leafOperatorWithAssignedExpressionIds.output
    scopes.overwriteCurrent(
      output = Some(output),
      hiddenOutput = Some(output ++ leafOperatorWithAssignedExpressionIds.metadataOutput)
    )

    leafOperatorWithAssignedExpressionIds
  }

  /**
   * [[SupervisingCommand]] is an [[ExplainCommand]] or [[DescribeQueryCommand]] that doesn't
   * require any resolution.
   */
  private def resolveSupervisingCommand(supervisingCommand: SupervisingCommand): LogicalPlan = {
    supervisingCommand
  }

  /**
   * Resolve [[Repartition]] operator. Its resolution doesn't require any specific logic (besides
   * child resolution).
   */
  private def resolveRepartition(repartition: Repartition): LogicalPlan = {
    repartition.copy(child = resolve(repartition.child))
  }

  /**
   * Resolve [[Sample]] operator. Its resolution doesn't require any specific logic (besides
   * child resolution).
   */
  private def resolveSample(sample: Sample): LogicalPlan = {
    sample.copy(child = resolve(sample.child))
  }

  /**
   * Resolve [[UnresolvedTVFAliases]] by resolving the child plan and applying output column
   * aliases. A [[Project]] is created that renames the output columns. It outputs only the renamed
   * columns, so this clears [[NameScope.qualifiedAccessOnlyOutputIds]].
   */
  private def resolveUnresolvedTVFAliases(
      unresolvedTVFAliases: UnresolvedTVFAliases): LogicalPlan = {
    val resolvedChild = resolve(unresolvedTVFAliases.child)

    validateTVFAliasesCount(unresolvedTVFAliases)

    val outputAttributes = scopes.current.output
    val projectList =
      outputAttributes.zip(unresolvedTVFAliases.outputNames).map {
        case (attribute, name) =>
          autoGeneratedAliasProvider.newAlias(child = attribute, name = Some(name))
      }

    scopes.overwriteCurrent(
      output = Some(projectList.map(_.toAttribute)),
      qualifiedAccessOnlyOutputIds = Some(new HashSet[ExprId])
    )

    Project(projectList = projectList, child = resolvedChild)
  }

  private def validateTVFAliasesCount(unresolvedTVFAliases: UnresolvedTVFAliases): Unit = {
    val outputAttributes = scopes.current.output
    if (unresolvedTVFAliases.outputNames.size != outputAttributes.size) {
      unresolvedTVFAliases.failAnalysis(
        errorClass = "NUM_TABLE_VALUE_ALIASES_MISMATCH",
        messageParameters = Map(
          "funcName" -> QueryCompilationErrors.toSQLId(unresolvedTVFAliases.name),
          "aliasesNum" -> unresolvedTVFAliases.outputNames.size.toString,
          "outColsNum" -> outputAttributes.size.toString
        )
      )
    }
  }

  private def createCteRelationRef(name: String, cteRelationDef: CTERelationDef): LogicalPlan = {
    SubqueryAlias(
      identifier = name,
      child = CTERelationRef(
        cteId = cteRelationDef.id,
        _resolved = true,
        isStreaming = cteRelationDef.isStreaming,
        output = cteRelationDef.output,
        recursive = false,
        maxRows = cteRelationDef.maxRows,
        isUnlimitedRecursion = operatorResolutionContextStack.current.resolvingLimitAll
      )
    )
  }

  /**
   * Check if the unresolved operator is explicitly unsupported and throw
   * [[ExplicitlyUnsupportedResolverFeature]] in that case. Otherwise, throw
   * [[QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature]].
   */
  private def handleUnmatchedOperator(unresolvedOperator: LogicalPlan): Nothing = {
    if (ExplicitlyUnsupportedResolverFeature.OPERATORS.contains(
        unresolvedOperator.getClass.getName
      )) {
      throw new ExplicitlyUnsupportedResolverFeature(
        ResolutionGap.UnsupportedOperator(unresolvedOperator.getClass)
      )
    }
    throw QueryCompilationErrors
      .unsupportedSinglePassAnalyzerFeature(
        s"${unresolvedOperator.getClass} operator resolution"
      )
      .withPosition(unresolvedOperator.origin)
  }

  private def validateResolvedOperatorGenerically(resolvedOperator: LogicalPlan): Unit = {
    validateOperatorResolution(resolvedOperator)

    operatorResolutionContextStack.current.baseOperator match {
      case Some(baseOperator) =>
        val baseOperatorToValidate = baseOperatorInResolvedPlan(resolvedOperator, baseOperator)
        validateOperatorResolution(baseOperatorToValidate)
        validateInvalidExpressions(baseOperatorToValidate)
      case None =>
        validateInvalidExpressions(resolvedOperator)
    }
  }

  /**
   * The [[OperatorResolutionContext.baseOperator]] recorded by an operator resolver is the operator
   * as it stood before [[insertReferencedMetadataColumnsForOperator]] ran. A [[LateralJoin]] is
   * resolved without wrapping itself in a [[Project]], so when a metadata column is referenced in
   * its condition, insertion rebuilds it (adding the column to the left leaf) and wraps it in a
   * [[Project]] - leaving the recorded base operator stale (its left lacks the column). Return the
   * rebuilt [[LateralJoin]] under that wrapping [[Project]] so validation (missing-input and
   * [[ValidateSubqueryExpression]], which requires the host to be a [[LateralJoin]]) sees it.
   *
   * Only [[LateralJoin]] needs this: other base operators either wrap themselves in a [[Project]]
   * during resolution (e.g. [[JoinResolver]]), so insertion leaves that [[Project]] in place, or
   * host subqueries that [[ValidateSubqueryExpression]] permits under a [[Project]].
   */
  private def baseOperatorInResolvedPlan(
      resolvedOperator: LogicalPlan,
      baseOperator: LogicalPlan): LogicalPlan = {
    (baseOperator, resolvedOperator) match {
      case (_: LateralJoin, Project(_, lateralJoin: LateralJoin)) => lateralJoin
      case _ => baseOperator
    }
  }

  private def validateOperatorResolution(operator: LogicalPlan): Unit = {
    if (!operator.resolved) {
      throwSinglePassFailedToResolveOperator(operator)
    }

    if (operator.children.nonEmpty) {
      val missingInput = operator.missingInput
      if (missingInput.nonEmpty) {
        Resolver.throwMissingAttributesError(operator, missingInput)
      }
    }
  }

  private def validateInvalidExpressions(operator: LogicalPlan): Unit = {
    val invalidExpressions = operatorResolutionContextStack.current.invalidExpressions
    if (invalidExpressions.nonEmpty) {
      throwUnsupportedExprForOperator(operator, invalidExpressions)
    }

    val subqueryExpressions =
      operatorResolutionContextStack.current.subqueryExpressionsToValidate.asScala.toSeq
    for (subqueryExpression <- subqueryExpressions) {
      val validationTarget = Option(
        operatorResolutionContextStack.current.subqueryValidationTargetOverrides.get(
          subqueryExpression.exprId
        )
      ).getOrElse(operator)
      ValidateSubqueryExpression(
        plan = validationTarget,
        expr = subqueryExpression
      )
    }
  }

  private def throwSinglePassFailedToResolveOperator(operator: LogicalPlan): Nothing =
    throw SparkException.internalError(
      msg = s"Failed to resolve operator in single-pass: $operator",
      context = operator.origin.getQueryContext,
      summary = operator.origin.context.summary()
    )

  private[resolver] def throwUnsupportedExprForOperator(
      operator: LogicalPlan,
      invalidExpressions: Seq[Expression]): Nothing = {
    throw new AnalysisException(
      errorClass = "UNSUPPORTED_EXPR_FOR_OPERATOR",
      messageParameters = Map(
        "operator" -> operator.nodeName,
        "invalidExprSqls" -> makeCommaSeparatedExpressionString(invalidExpressions)
      )
    )
  }

  private def makeCommaSeparatedExpressionString(expressions: Seq[Expression]): String = {
    expressions.map(toSQLExpr).mkString(", ")
  }
}

object Resolver {

  /**
   * Fails with `MISSING_ATTRIBUTES` because `operator` references attributes its child does not
   * produce.
   */
  def throwMissingAttributesError(
      operator: LogicalPlan,
      missingInput: AttributeSet): Nothing = {
    val inputAttributesByName = new IdentifierMap[Attribute]
    for (attribute <- operator.inputSet) {
      inputAttributesByName.put(attribute.name, attribute)
    }

    val attributesWithSameName = missingInput.filter { missingAttribute =>
      inputAttributesByName.contains(missingAttribute.name)
    }.toSeq

    throw QueryCompilationErrors.missingAttributesError(
      operator = operator,
      missingInput = missingInput.toSeq,
      input = operator.inputSet.toSeq,
      attributesWithSameName = attributesWithSameName
    )
  }

  /**
   * Create a new instance of the [[RelationResolution]].
   */
  def createRelationResolution(
      catalogManager: CatalogManager,
      sharedRelationCache: RelationCache = RelationCache.empty): RelationResolution = {
    new RelationResolution(catalogManager, sharedRelationCache)
  }
}
