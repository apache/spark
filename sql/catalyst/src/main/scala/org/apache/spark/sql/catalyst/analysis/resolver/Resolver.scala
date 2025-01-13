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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  FunctionResolution,
  NamedRelation,
  RelationResolution,
  ResolvedInlineTable,
  UnresolvedInlineTable,
  UnresolvedRelation
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  GlobalLimit,
  LocalLimit,
  LocalRelation,
  LogicalPlan,
  OneRowRelation,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.types.BooleanType

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
 * @param extensions A list of [[ResolverExtension]] that can resolve external operators.
 */
class Resolver(
    catalogManager: CatalogManager,
    override val extensions: Seq[ResolverExtension] = Seq.empty,
    metadataResolverExtensions: Seq[ResolverExtension] = Seq.empty)
    extends TreeNodeResolver[LogicalPlan, LogicalPlan]
    with QueryErrorsBase
    with ResolvesOperatorChildren
    with TracksResolvedNodes[LogicalPlan]
    with DelegatesResolutionToExtensions {
  private val scopes = new NameScopeStack
  private val planLogger = new PlanLogger
  private val relationResolution = Resolver.createRelationResolution(catalogManager)
  private val functionResolution = new FunctionResolution(catalogManager, relationResolution)
  private val expressionResolver =
    new ExpressionResolver(this, scopes, functionResolution, planLogger)
  private val limitExpressionResolver = new LimitExpressionResolver(expressionResolver)

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
   * This method is an analysis entry point. It resolves the metadata and invokes [[resolve]],
   * which does most of the analysis work.
   */
  def lookupMetadataAndResolve(
      unresolvedPlan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
    planLogger.logPlanResolutionEvent(unresolvedPlan, "Lookup metadata and resolve")

    relationMetadataProvider = analyzerBridgeState match {
      case Some(analyzerBridgeState) =>
        new BridgedRelationMetadataProvider(
          catalogManager,
          relationResolution,
          analyzerBridgeState
        )
      case None =>
        relationMetadataProvider
    }

    relationMetadataProvider match {
      case metadataResolver: MetadataResolver =>
        metadataResolver.resolve(unresolvedPlan)
      case _ =>
    }

    resolve(unresolvedPlan)
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
    planLogger.logPlanResolutionEvent(unresolvedPlan, "Unresolved plan")

    throwIfNodeWasResolvedEarlier(unresolvedPlan)

    val resolvedPlan =
      unresolvedPlan match {
        case unresolvedProject: Project =>
          resolveProject(unresolvedProject)
        case unresolvedFilter: Filter =>
          resolveFilter(unresolvedFilter)
        case unresolvedSubqueryAlias: SubqueryAlias =>
          resolveSubqueryAlias(unresolvedSubqueryAlias)
        case unresolvedGlobalLimit: GlobalLimit =>
          resolveGlobalLimit(unresolvedGlobalLimit)
        case unresolvedLocalLimit: LocalLimit =>
          resolveLocalLimit(unresolvedLocalLimit)
        case unresolvedRelation: UnresolvedRelation =>
          resolveRelation(unresolvedRelation)
        case unresolvedInlineTable: UnresolvedInlineTable =>
          resolveInlineTable(unresolvedInlineTable)
        // See the reason why we have to match both [[LocalRelation]] and [[ResolvedInlineTable]]
        // in the [[resolveInlineTable]] scaladoc
        case resolvedInlineTable: ResolvedInlineTable =>
          updateNameScopeWithPlanOutput(resolvedInlineTable)
        case localRelation: LocalRelation =>
          updateNameScopeWithPlanOutput(localRelation)
        case unresolvedOneRowRelation: OneRowRelation =>
          updateNameScopeWithPlanOutput(unresolvedOneRowRelation)
        case _ =>
          tryDelegateResolutionToExtension(unresolvedPlan).getOrElse {
            handleUnmatchedOperator(unresolvedPlan)
          }
      }

    markNodeAsResolved(resolvedPlan)

    planLogger.logPlanResolution(unresolvedPlan, resolvedPlan)

    resolvedPlan
  }

  /**
   * [[Project]] introduces a new scope to resolve its subtree and project list expressions. After
   * those are resolved in the child scope we overwrite current scope with resolved [[Project]]'s
   * output to expose new names to the parent operators.
   */
  private def resolveProject(unresolvedProject: Project): LogicalPlan = {
    val resolvedProject = scopes.withNewScope {
      val resolvedChild = resolve(unresolvedProject.child)
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList)
      Project(resolvedProjectList, resolvedChild)
    }

    withPosition(unresolvedProject) {
      scopes.overwriteTop(resolvedProject.output)
    }

    resolvedProject
  }

  /**
   * [[Filter]] has a single child and a single condition and we resolve them in this respective
   * order.
   */
  private def resolveFilter(unresolvedFilter: Filter): LogicalPlan = {
    val resolvedChild = resolve(unresolvedFilter.child)
    val resolvedCondition = expressionResolver.resolve(unresolvedFilter.condition)

    val resolvedFilter = Filter(resolvedCondition, resolvedChild)
    if (resolvedFilter.condition.dataType != BooleanType) {
      withPosition(unresolvedFilter) {
        throwDatatypeMismatchFilterNotBoolean(resolvedFilter)
      }
    }

    resolvedFilter
  }

  /**
   * [[SubqueryAlias]] has a single child and an identifier. We need to resolve the child and update
   * the scope with the output, since upper expressions can reference [[SubqueryAlias]]es output by
   * its identifier.
   */
  private def resolveSubqueryAlias(unresolvedSubqueryAlias: SubqueryAlias): LogicalPlan = {
    val resolvedSubqueryAlias =
      SubqueryAlias(unresolvedSubqueryAlias.identifier, resolve(unresolvedSubqueryAlias.child))
    withPosition(unresolvedSubqueryAlias) {
      scopes.overwriteTop(unresolvedSubqueryAlias.alias, resolvedSubqueryAlias.output)
    }
    resolvedSubqueryAlias
  }

  /**
   * Resolve [[GlobalLimit]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveGlobalLimit(unresolvedGlobalLimit: GlobalLimit): LogicalPlan = {
    val resolvedChild = resolve(unresolvedGlobalLimit.child)

    val resolvedLimitExpr = withPosition(unresolvedGlobalLimit) {
      limitExpressionResolver.resolve(unresolvedGlobalLimit.limitExpr)
    }

    GlobalLimit(resolvedLimitExpr, resolvedChild)
  }

  /**
   * Resolve [[LocalLimit]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveLocalLimit(unresolvedLocalLimit: LocalLimit): LogicalPlan = {
    val resolvedChild = resolve(unresolvedLocalLimit.child)

    val resolvedLimitExpr = withPosition(unresolvedLocalLimit) {
      limitExpressionResolver.resolve(unresolvedLocalLimit.limitExpr)
    }

    LocalLimit(resolvedLimitExpr, resolvedChild)
  }

  /**
   * [[UnresolvedRelation]] was previously looked up by the [[MetadataResolver]] and now we need to:
   * - Get the specific relation with metadata from `relationsWithResolvedMetadata`, like
   *   [[UnresolvedCatalogRelation]], or throw an error if it wasn't found
   * - Resolve it further, usually using extensions, like [[DataSourceResolver]]
   */
  private def resolveRelation(unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    relationMetadataProvider.getRelationWithResolvedMetadata(unresolvedRelation) match {
      case Some(relationWithResolvedMetadata) =>
        planLogger.logPlanResolutionEvent(
          relationWithResolvedMetadata,
          "Relation metadata retrieved"
        )

        withPosition(unresolvedRelation) {
          resolve(relationWithResolvedMetadata)
        }
      case None =>
        withPosition(unresolvedRelation) {
          unresolvedRelation.tableNotFound(unresolvedRelation.multipartIdentifier)
        }
    }
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
        row.map(expressionResolver.resolve(_))
      })
    )

    val resolvedRelation = EvaluateUnresolvedInlineTable
      .evaluateUnresolvedInlineTable(withResolvedExpressions)

    withPosition(unresolvedInlineTable) {
      resolve(resolvedRelation)
    }
  }

  /**
   * To finish the operator resolution we add its output to the current scope. This is usually
   * done for relations. [[NamedRelation]]'s output should be added to the scope under its name.
   */
  private def updateNameScopeWithPlanOutput(relation: LogicalPlan): LogicalPlan = {
    withPosition(relation) {
      relation match {
        case namedRelation: NamedRelation =>
          scopes.top.update(namedRelation.name, namedRelation.output)
        case _ =>
          scopes.top += relation.output
      }
    }
    relation
  }

  override def tryDelegateResolutionToExtension(
      unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    val resolutionResult = super.tryDelegateResolutionToExtension(unresolvedOperator)
    resolutionResult.map { resolvedOperator =>
      updateNameScopeWithPlanOutput(resolvedOperator)
    }
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

  private def throwDatatypeMismatchFilterNotBoolean(filter: Filter): Nothing =
    throw new AnalysisException(
      errorClass = "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN",
      messageParameters = Map(
        "sqlExpr" -> filter.expressions.map(toSQLExpr).mkString(","),
        "filter" -> toSQLExpr(filter.condition),
        "type" -> toSQLType(filter.condition.dataType)
      )
    )
}

object Resolver {

  /**
   * Create a new instance of the [[RelationResolution]].
   */
  def createRelationResolution(catalogManager: CatalogManager): RelationResolution = {
    new RelationResolution(catalogManager)
  }
}
