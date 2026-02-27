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

import java.util.HashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AnalysisErrorAt,
  CleanupAliases,
  FunctionResolution,
  MultiInstanceRelation,
  NameParameterizedQuery,
  PullOutNondeterministic,
  RelationCache,
  RelationResolution,
  ResolvedInlineTable,
  TypeCheckResult,
  UnresolvedHaving,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedSubqueryColumnAliases,
  UnresolvedTableValuedFunction,
  ValidateSubqueryExpression
}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSet,
  Expression,
  ExprId
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors

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
 * @param externalRelationResolution An optional [[RelationResolution]] with to override the default
 *   one. The default is constructed using [[Resolver.createRelationResolution]].
 */
class Resolver(
    catalogManager: CatalogManager,
    sharedRelationCache: RelationCache = RelationCache.empty,
    override val extensions: Seq[ResolverExtension] = Seq.empty,
    metadataResolverExtensions: Seq[ResolverExtension] = Seq.empty,
    externalRelationResolution: Option[RelationResolution] = None,
    extendedRewriteRules: Seq[Rule[LogicalPlan]] = Seq.empty,
    tracker: Option[QueryPlanningTracker] = None)
    extends LogicalPlanResolver
    with ResolverMetricTracker // EDGE
    with DelegatesResolutionToExtensions {
  private val planLogger = new PlanLogger
  private val subqueryRegistry = new SubqueryRegistry
  private val scopes = new NameScopeStack(
    tempVariableManager = catalogManager.tempVariableManager,
    subqueryRegistry = subqueryRegistry,
    planLogger = planLogger
  )
  private val cteRegistry = new CteRegistry
  private val identifierAndCteSubstitutor = new IdentifierAndCteSubstitutor
  private val operatorResolutionContextStack = new OperatorResolutionContextStack
  private val relationResolution = externalRelationResolution.getOrElse {
    Resolver.createRelationResolution(catalogManager, sharedRelationCache)
  }
  private val functionResolution = new FunctionResolution(catalogManager, relationResolution)
  private val expressionResolver = new ExpressionResolver(this, functionResolution, planLogger)
  private val aggregateResolver = new AggregateResolver(this, expressionResolver)
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val autoGeneratedAliasProvider = expressionResolver.getAutoGeneratedAliasProvider
  private val projectResolver = new ProjectResolver(this, expressionResolver)
  private val viewResolver = new ViewResolver(resolver = this, catalogManager = catalogManager)
  private val setOperationLikeResolver =
    new SetOperationLikeResolver(this, expressionResolver)
  private val filterResolver = new FilterResolver(this, expressionResolver)
  private val sortResolver = new SortResolver(this, expressionResolver)
  private val joinResolver = new JoinResolver(this, expressionResolver)
  private val havingResolver = new HavingResolver(this, expressionResolver)
  private val tableValuedFunctionResolver =
    new TableValuedFunctionResolver(this, expressionResolver, functionResolution)
  private val nameParameterizedQueryResolver =
    new NameParameterizedQueryResolver(this, expressionResolver)
  private val repartitionByExpressionResolver =
    new RepartitionByExpressionResolver(this, expressionResolver)
  private val pivotResolver = new PivotResolver(this, expressionResolver)
  private val unpivotResolver = new UnpivotResolver(this, expressionResolver)
  private val generateResolver = new GenerateResolver(this, expressionResolver)

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
   * If the [[AnalyzerBridgeState]] is provided, we reset the this provider to the
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
   * Get [[OperatorResolutionContextStack]].
   */
  def getOperatorResolutionContextStack: OperatorResolutionContextStack =
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
   * Get the [[ViewResolver]] bound to the used [[Resolver]].
   */
  def getViewResolver: ViewResolver = viewResolver


  /**
   * This method is a top-level analysis entry point:
   * 1. Substitute IDENTIFIERs and CTEs in the `unresolvedPlan` using
   *    [[IdentifierAndCteSubstitutor]];
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
   *    we fail to resolve the following case properly:
   *
   *    {{{
   *      SET spark.databricks.sql.expression.aiFunctions.repartition = 0;
   *      CREATE VIEW testView AS SELECT ai_gen(col2) AS alias FROM t2;
   *      SET spark.databricks.sql.expression.aiFunctions.repartition = 8;
   *      SELECT * FROM testView;
   *    }}}
   *
   *    This is because [[AIFunctionRepartition]] is one of the rewrite rules that's triggered in
   *    this case, it depends on the `spark.databricks.sql.expression.aiFunctions.repartition`
   *    config value and thus it transforms the plan incorrectly if we use the wrong config value.
   *
   * This method is called for the top-level query and each unresolved [[View]].
   */
  def lookupMetadataAndResolve(
      unresolvedPlan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      planLogger.logPlanResolutionEvent(unresolvedPlan, "IDENTIFIER and CTE substitution")

      val planAfterSubstitution = identifierAndCteSubstitutor.substitutePlan(unresolvedPlan)

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

      planLogger.logPlanResolutionEvent(planAfterSubstitution, "Main resolution")

      planAfterSubstitution.setTagValue(ResolverTag.TOP_LEVEL_OPERATOR, ())

      val resolvedPlan = resolve(planAfterSubstitution)

      planRewriter.rewriteWithSubqueries(resolvedPlan)
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
          case nameParameterizedQuery: NameParameterizedQuery =>
            nameParameterizedQueryResolver.resolve(nameParameterizedQuery)
          case pivot: Pivot =>
            pivotResolver.resolve(pivot)
          case unpivot: Unpivot =>
            unpivotResolver.resolve(unpivot)
          case generate: Generate =>
            generateResolver.resolve(generate)
          case _ =>
            tryDelegateResolutionToExtension(unresolvedPlan).getOrElse {
              handleUnmatchedOperator(unresolvedPlan)
            }
        }

      withPosition(unresolvedPlan) {
        validateResolvedOperatorGenerically(resolvedPlan)
      }

      planLogger.logPlanResolution(unresolvedPlan, resolvedPlan)

      resolvedPlan.copyTagsFrom(unresolvedPlan)

      resolvedPlan
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
   * For each CTE definition, a new [[CteScope]] is pushed with:
   *  - `cteName` and `cteId` to identify this specific CTE
   *  - `allowRecursion` flag indicating if this is a recursive CTE
   *  - `maxDepth` specifying iteration limit for recursive CTEs
   *  - `isInsideRecursiveCteScope` set to true for recursive CTEs to force immediate inlining of
   *  CTE definitions and prevent unnecessary stack traversals.
   *
   * The order in which CTE ids are allocated (via [[CTERelationDef.newId]]) can matter to
   * downstream consumers that key off of [[CTERelationDef.id]] (for example, lineage serializes
   * CTEs using the numeric id as the "table name").
   *
   * For non-recursive CTEs, we preserve the historical depth-first allocation behavior by
   * allocating ids *after* resolving the CTE plan. This ensures nested WITH clauses inside the
   * definition consume ids first (inner CTEs get smaller ids than their enclosing CTEs).
   *
   * For recursive CTEs, we must allocate the id up-front so that self-references can be
   * represented during resolution (via [[UnionLoopRef]] / [[UnionLoop]]).
   *
   * During CTE resolution, the scope tracks whether the CTE references itself. If self-referenced,
   * the CTE is validated as a properly structured recursive CTE.
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
    for (cteRelation <- unresolvedWith.cteRelations) {
      val (cteName, ctePlan, maxDepth) = cteRelation
      val preallocatedCteIdForRecursive: Option[Long] =
        if (unresolvedWith.allowRecursion) Some(CTERelationDef.newId) else None

      expressionIdAssigner.pushMapping()
      scopes.pushScope()

      val recursiveCteParams = if (unresolvedWith.allowRecursion) {
        Some(
          RecursiveCteParameters(
            cteId = preallocatedCteIdForRecursive.get,
            cteName = cteName,
            maxDepth = maxDepth
          )
        )
      } else {
        None
      }

      cteRegistry.pushScope(
        recursiveCteParameters = recursiveCteParams,
        isInsideRecursiveCteScope = unresolvedWith.allowRecursion
      )

      var referencedRecursively = false

      val resolvedCtePlan = try {
        val plan = resolve(ctePlan)
        referencedRecursively = cteRegistry.currentScope.referencedRecursively
        plan
      } finally {
        cteRegistry.popScope()
        scopes.popScope()
        expressionIdAssigner.popMapping()
      }

      if (referencedRecursively) {
        checkValidRecursiveCTE(resolvedCtePlan)
        cteRegistry.currentScope
          .registerCte(
            cteName,
            CTERelationDef(
              resolvedCtePlan,
              id = preallocatedCteIdForRecursive.get,
              maxDepth = maxDepth
            )
          )
      } else {
        cteRegistry.currentScope
          .registerCte(cteName, CTERelationDef(resolvedCtePlan))
      }
    }

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
   * Validates that a recursive CTE has the correct structure. Valid structures are:
   * - [[SubqueryAlias]] -> [[UnionLoop]] (optionally wrapped with [[Project]] or [[WithCTE]])
   *
   * Invalid structures:
   * - [[SubqueryAlias]] -> [[Distinct]] -> [[UnionLoop]]: UNION without ALL is not supported
   *   (rejects UNION, requires UNION ALL)
   * - Any other structure: The CTE must have a proper anchor and recursion branch
   *
   * @param plan The resolved CTE plan to validate
   */
  private def checkValidRecursiveCTE(plan: LogicalPlan): Unit = {
    plan match {
      case SubqueryAlias(_, UnionLoop(_, _, _, _, _, _)) =>
      case SubqueryAlias(_, WithCTE(UnionLoop(_, _, _, _, _, _), _)) =>
      case SubqueryAlias(_, Project(_, UnionLoop(_, _, _, _, _, _))) =>
      case SubqueryAlias(_, Project(_, WithCTE(UnionLoop(_, _, _, _, _, _), _))) =>
      case SubqueryAlias(_, Distinct(UnionLoop(_, _, _, _, _, _))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, WithCTE(Distinct(UnionLoop(_, _, _, _, _, _)), _)) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, Project(_, Distinct(UnionLoop(_, _, _, _, _, _)))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, Project(_, WithCTE(Distinct(UnionLoop(_, _, _, _, _, _)), _))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case _ =>
        throw new AnalysisException(
          errorClass = "INVALID_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
    }
  }

  /**
   * We may meet resolved [[WithCTE]] while traversing partially resolved trees in DataFrame
   * programs. In that case we simply recurse into the CTE definitions and the main plan under
   * new scopes and mappings.
   */
  private def handleResolvedWithCte(withCte: WithCTE): LogicalPlan = {
    val resolvedCteDefs = withCte.cteDefs.map { cteDef =>
      expressionIdAssigner.pushMapping()
      scopes.pushScope()
      cteRegistry.pushScope()

      try {
        resolve(cteDef).asInstanceOf[CTERelationDef]
      } finally {
        cteRegistry.popScope()
        scopes.popScope()
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

    scopes.overwriteCurrent(output = Some(projectList.map(_.toAttribute)))

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
    val resolvedSubqueryAlias =
      unresolvedSubqueryAlias.copy(child = resolve(unresolvedSubqueryAlias.child))

    val qualifier = resolvedSubqueryAlias.identifier.qualifier :+ resolvedSubqueryAlias.alias
    val output = scopes.current.output.map(attribute => attribute.withQualifier(qualifier))
    scopes.overwriteCurrent(
      output = Some(output),
      hiddenOutput = Some(output),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )

    resolvedSubqueryAlias
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
   * traversal.
   */
  private def resolveDistinct(unresolvedDistinct: Distinct): LogicalPlan = {
    val resolvedDistinct = unresolvedDistinct.copy(child = resolve(unresolvedDistinct.child))

    OperatorWithUncomparableTypeValidator.validate(resolvedDistinct, scopes.current.output)

    scopes.overwriteCurrent(
      hiddenOutput = Some(scopes.current.output),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )
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
          RelationResolution.throwTableNotFound(unresolvedRelation)
      }

      resolve(resolvedRelation)
    }
  }

  /**
   * Resolve the [[UnresolvedCteRelationRef]] which was previously introduced by the
   * [[IdentifierAndCteSubstitutor]].
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
      }

    resolve(cteRelationRef)
  }

  /**
   * We may meet resolved [[CTERelationRef]] while traversing partially resolved trees in DataFrame
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

    resolve(resolvedRelation)
  }

  private def tryDelegateResolutionToExtension(
      unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    val resolutionResult = runExtensions(unresolvedOperator)
    resolutionResult.map { operator =>
      RestrictRowLevelSecurityFeature(operator)
      operator match {
        case leafOperator: LeafNode => handleLeafOperator(leafOperator)
        case other => other
      }
    }
  }

  private def runExtensions(unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    super.tryDelegateResolutionToExtension(unresolvedOperator, this)
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
  private def handleLeafOperator(leafOperator: LeafNode): LogicalPlan = {
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
    scopes.overwriteCurrent(output = Some(output), hiddenOutput = Some(output))

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
        s"unsupported operator: ${unresolvedOperator.getClass.getName}"
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
        validateOperatorResolution(baseOperator)
        validateInvalidExpressions(baseOperator)
      case None =>
        validateInvalidExpressions(resolvedOperator)
    }
  }

  private def validateOperatorResolution(operator: LogicalPlan): Unit = {
    if (!operator.resolved) {
      throwSinglePassFailedToResolveOperator(operator)
    }

    if (operator.children.nonEmpty) {
      val missingInput = operator.missingInput
      if (missingInput.nonEmpty) {
        throwMissingAttributesError(operator, missingInput)
      }
    }
  }

  private def validateInvalidExpressions(operator: LogicalPlan): Unit = {
    val invalidExpressions = expressionResolver.getLastInvalidExpressionsInTheContextOfOperator
    if (invalidExpressions.nonEmpty) {
      throwUnsupportedExprForOperator(invalidExpressions)
    }

    val subqueryExpressions =
      operatorResolutionContextStack.current.subqueryExpressionsToValidate.asScala.toSeq
    for (subqueryExpression <- subqueryExpressions) {
      ValidateSubqueryExpression(
        plan = operator,
        expr = subqueryExpression
      )
    }
  }

  private def throwMissingAttributesError(
      operator: LogicalPlan,
      missingInput: AttributeSet): Nothing = {
    val inputSet = operator.inputSet

    val inputAttributesByName = new IdentifierMap[Attribute]
    for (attribute <- inputSet) {
      inputAttributesByName.put(attribute.name, attribute)
    }

    val attributesWithSameName = missingInput.filter { missingAttribute =>
      inputAttributesByName.contains(missingAttribute.name)
    }

    if (attributesWithSameName.nonEmpty) {
      operator.failAnalysis(
        errorClass = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION",
        messageParameters = Map(
          "missingAttributes" -> makeCommaSeparatedExpressionString(missingInput.toSeq),
          "input" -> makeCommaSeparatedExpressionString(inputSet.toSeq),
          "operator" -> operator.simpleString(conf.maxToStringFields),
          "operation" -> makeCommaSeparatedExpressionString(attributesWithSameName.toSeq)
        )
      )
    } else {
      operator.failAnalysis(
        errorClass = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
        messageParameters = Map(
          "missingAttributes" -> makeCommaSeparatedExpressionString(missingInput.toSeq),
          "input" -> makeCommaSeparatedExpressionString(inputSet.toSeq),
          "operator" -> operator.simpleString(conf.maxToStringFields)
        )
      )
    }
  }

  private def throwSinglePassFailedToResolveOperator(operator: LogicalPlan): Nothing =
    throw SparkException.internalError(
      msg = s"Failed to resolve operator in single-pass: $operator",
      context = operator.origin.getQueryContext,
      summary = operator.origin.context.summary()
    )

  private def throwUnsupportedExprForOperator(invalidExpressions: Seq[Expression]): Nothing = {
    throw new AnalysisException(
      errorClass = "UNSUPPORTED_EXPR_FOR_OPERATOR",
      messageParameters = Map(
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
   * Create a new instance of the [[RelationResolution]].
   */
  def createRelationResolution(
      catalogManager: CatalogManager,
      sharedRelationCache: RelationCache = RelationCache.empty): RelationResolution = {
    new RelationResolution(catalogManager, sharedRelationCache)
  }
}
