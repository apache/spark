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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AnalysisErrorAt,
  FunctionResolution,
  MultiInstanceRelation,
  RelationResolution,
  ResolvedInlineTable,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedSubqueryColumnAliases
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSet,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  AnalysisHelper,
  CTERelationDef,
  CTERelationRef,
  Distinct,
  Filter,
  GlobalLimit,
  LeafNode,
  LocalLimit,
  LocalRelation,
  LogicalPlan,
  OneRowRelation,
  Project,
  SubqueryAlias,
  Union,
  UnresolvedWith,
  View,
  WithCTE
}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
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
 * @param extensions A list of [[ResolverExtension]]s that will be used to resolve external
 *   operators.
 * @param metadataResolverExtensions A list of [[ResolverExtension]]s that will be used to resolve
 *   relation operators in [[MetadataResolver]].
 */
class Resolver(
    catalogManager: CatalogManager,
    override val extensions: Seq[ResolverExtension] = Seq.empty,
    metadataResolverExtensions: Seq[ResolverExtension] = Seq.empty)
    extends LogicalPlanResolver
    with ResolvesOperatorChildren
    with DelegatesResolutionToExtensions {
  private val scopes = new NameScopeStack
  private val cteRegistry = new CteRegistry
  private val planLogger = new PlanLogger
  private val relationResolution = Resolver.createRelationResolution(catalogManager)
  private val functionResolution = new FunctionResolution(catalogManager, relationResolution)
  private val expressionResolver = new ExpressionResolver(this, functionResolution, planLogger)
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val projectResolver = new ProjectResolver(this, expressionResolver)
  private val viewResolver = new ViewResolver(resolver = this, catalogManager = catalogManager)
  private val unionResolver = new UnionResolver(this, expressionResolver)

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
   * Get the [[CteRegistry]] which is a single instance per query resolution.
   */
  def getCteRegistry: CteRegistry = {
    cteRegistry
  }

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

    relationMetadataProvider.resolve(unresolvedPlan)

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
  override def resolve(unresolvedPlan: LogicalPlan): LogicalPlan =
    withOrigin(unresolvedPlan.origin) {
      planLogger.logPlanResolutionEvent(unresolvedPlan, "Unresolved plan")

      val resolvedPlan =
        unresolvedPlan match {
          case unresolvedWith: UnresolvedWith =>
            resolveWith(unresolvedWith)
          case unresolvedProject: Project =>
            projectResolver.resolve(unresolvedProject)
          case unresolvedFilter: Filter =>
            resolveFilter(unresolvedFilter)
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
          case unresolvedDistinct: Distinct =>
            resolveDistinct(unresolvedDistinct)
          case unresolvedRelation: UnresolvedRelation =>
            resolveRelation(unresolvedRelation)
          case unresolvedCteRelationDef: CTERelationDef =>
            resolveCteRelationDef(unresolvedCteRelationDef)
          case unresolvedInlineTable: UnresolvedInlineTable =>
            resolveInlineTable(unresolvedInlineTable)
          case unresolvedUnion: Union =>
            unionResolver.resolve(unresolvedUnion)
          // See the reason why we have to match both [[LocalRelation]] and [[ResolvedInlineTable]]
          // in the [[resolveInlineTable]] scaladoc
          case resolvedInlineTable: ResolvedInlineTable =>
            handleLeafOperator(resolvedInlineTable)
          case localRelation: LocalRelation =>
            handleLeafOperator(localRelation)
          case unresolvedOneRowRelation: OneRowRelation =>
            handleLeafOperator(unresolvedOneRowRelation)
          case _ =>
            tryDelegateResolutionToExtension(unresolvedPlan).getOrElse {
              handleUnmatchedOperator(unresolvedPlan)
            }
        }

      if (resolvedPlan.children.nonEmpty) {
        val missingInput = resolvedPlan.missingInput
        if (missingInput.nonEmpty) {
          withPosition(unresolvedPlan) {
            throwMissingAttributesError(resolvedPlan, missingInput)
          }
        }
      }

      if (!resolvedPlan.resolved) {
        throwSinglePassFailedToResolveOperator(resolvedPlan)
      }

      planLogger.logPlanResolution(unresolvedPlan, resolvedPlan)

      preservePlanIdTag(unresolvedPlan, resolvedPlan)
    }

  /**
   * Get [[NameScopeStack]] bound to the used [[Resolver]].
   */
  def getNameScopes: NameScopeStack = scopes

  /**
   * [[UnresolvedWith]] contains a list of unresolved CTE definitions, which are represented by
   * (name, subquery) pairs, and an actual child query. First we resolve the CTE definitions
   * strictly in their declaration order, so they become available for other lower definitions
   * (lower both in this WITH clause list and in the plan tree) and for the [[UnresolvedWith]] child
   * query. After that, we resolve the child query. Optionally, if this is a root [[CteScope]],
   * we return a [[WithCTE]] operator with all the resolved [[CTERelationDef]]s merged together
   * from this scope and child scopes. Otherwise, we return the resolved child query so that
   * the resolved [[CTERelationDefs]] propagate up and will be merged together later.
   *
   * See [[CteScope]] scaladoc for all the details on how CTEs are resolved.
   */
  private def resolveWith(unresolvedWith: UnresolvedWith): LogicalPlan = {
    val childOutputs = new ArrayBuffer[Seq[Attribute]]

    for (cteRelation <- unresolvedWith.cteRelations) {
      val (cteName, ctePlan) = cteRelation

      val resolvedCtePlan = scopes.withNewScope {
        expressionIdAssigner.withNewMapping() {
          cteRegistry.withNewScope() {
            val resolvedCtePlan = resolve(ctePlan)

            childOutputs.append(scopes.top.output)

            resolvedCtePlan
          }
        }
      }

      cteRegistry.currentScope.registerCte(cteName, CTERelationDef(resolvedCtePlan))
    }

    val resolvedChild = cteRegistry.withNewScope() {
      resolve(unresolvedWith.child)
    }

    childOutputs.append(scopes.top.output)

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(childOutputs.toSeq)

    if (cteRegistry.currentScope.isRoot) {
      WithCTE(resolvedChild, cteRegistry.currentScope.getKnownCtes)
    } else {
      resolvedChild
    }
  }

  /**
   * [[Filter]] has a single child and a single condition and we resolve them in this respective
   * order.
   */
  private def resolveFilter(unresolvedFilter: Filter): LogicalPlan = {
    val resolvedChild = resolve(unresolvedFilter.child)
    val resolvedCondition =
      expressionResolver
        .resolveExpressionTreeInOperator(unresolvedFilter.condition, unresolvedFilter)

    val resolvedFilter = Filter(resolvedCondition, resolvedChild)
    if (resolvedFilter.condition.dataType != BooleanType) {
      withPosition(unresolvedFilter) {
        throwDatatypeMismatchFilterNotBoolean(resolvedFilter)
      }
    }

    resolvedFilter
  }

  /**
   * [[UnresolvedSubqueryColumnAliases]] Creates a [[Project]] on top of a [[SubqueryAlias]] with
   * the specified attribute aliases. Example:
   *
   * {{{
   * -- The output schema is [a: INT, b: INT]
   * SELECT t.a, t.b FROM VALUES (1, 2) t (a, b);
   * }}}
   */
  private def resolveSubqueryColumnAliases(
      unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases): LogicalPlan = {
    val resolvedChild = resolve(unresolvedSubqueryColumnAliases.child)

    if (unresolvedSubqueryColumnAliases.outputColumnNames.size != scopes.top.output.size) {
      withPosition(unresolvedSubqueryColumnAliases) {
        throw QueryCompilationErrors.aliasNumberNotMatchColumnNumberError(
          unresolvedSubqueryColumnAliases.outputColumnNames.size,
          scopes.top.output.size,
          unresolvedSubqueryColumnAliases
        )
      }
    }

    val projectList = scopes.top.output.zip(unresolvedSubqueryColumnAliases.outputColumnNames).map {
      case (attr, columnName) => expressionIdAssigner.mapExpression(Alias(attr, columnName)())
    }

    overwriteTopScope(unresolvedSubqueryColumnAliases, projectList.map(_.toAttribute))

    Project(projectList = projectList, child = resolvedChild)
  }

  /**
   * [[SubqueryAlias]] has a single child and an identifier. We need to resolve the child and update
   * the scope with the output, since upper expressions can reference [[SubqueryAlias]]es output by
   * its identifier.
   */
  private def resolveSubqueryAlias(unresolvedSubqueryAlias: SubqueryAlias): LogicalPlan = {
    val resolvedSubqueryAlias =
      unresolvedSubqueryAlias.copy(child = resolve(unresolvedSubqueryAlias.child))

    val qualifier = resolvedSubqueryAlias.identifier.qualifier :+ resolvedSubqueryAlias.alias
    overwriteTopScope(
      unresolvedSubqueryAlias,
      scopes.top.output.map(attribute => attribute.withQualifier(qualifier))
    )

    resolvedSubqueryAlias
  }

  /**
   * Resolve [[GlobalLimit]]. We have to resolve its child and resolve and validate its limit
   * expression.
   */
  private def resolveGlobalLimit(unresolvedGlobalLimit: GlobalLimit): LogicalPlan = {
    val resolvedChild = resolve(unresolvedGlobalLimit.child)

    val resolvedLimitExpr = withPosition(unresolvedGlobalLimit) {
      expressionResolver.resolveLimitExpression(
        unresolvedGlobalLimit.limitExpr,
        unresolvedGlobalLimit
      )
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
      expressionResolver.resolveLimitExpression(
        unresolvedLocalLimit.limitExpr,
        unresolvedLocalLimit
      )
    }

    LocalLimit(resolvedLimitExpr, resolvedChild)
  }

  /**
   * [[Distinct]] operator doesn't require any special resolution.
   */
  private def resolveDistinct(unresolvedDistinct: Distinct): LogicalPlan = {
    withResolvedChildren(unresolvedDistinct, resolve)
  }

  /**
   * [[UnresolvedRelation]] was previously looked up by the [[MetadataResolver]] and now we need to:
   * - Get the specific relation with metadata from `relationsWithResolvedMetadata`, like
   *   [[UnresolvedCatalogRelation]], or throw an error if it wasn't found
   * - Resolve it further, usually using extensions, like [[DataSourceResolver]]
   */
  private def resolveRelation(unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    withPosition(unresolvedRelation) {
      viewResolver.withSourceUnresolvedRelation(unresolvedRelation) {
        val maybeResolvedRelation = cteRegistry.resolveCteName(unresolvedRelation.name).orElse {
          relationMetadataProvider.getRelationWithResolvedMetadata(unresolvedRelation)
        }

        val resolvedRelation = maybeResolvedRelation match {
          case Some(cteRelationDef: CTERelationDef) =>
            planLogger.logPlanResolutionEvent(cteRelationDef, "CTE definition resolved")

            SubqueryAlias(identifier = unresolvedRelation.name, child = cteRelationDef)
          case Some(relationsWithResolvedMetadata) =>
            planLogger.logPlanResolutionEvent(
              relationsWithResolvedMetadata,
              "Relation metadata retrieved"
            )

            relationsWithResolvedMetadata
          case None =>
            unresolvedRelation.tableNotFound(unresolvedRelation.multipartIdentifier)
        }

        resolve(resolvedRelation)
      }
    }
  }

  /**
   * Resolve [[CTERelationDef]] by replacing it with [[CTERelationRef]] with the same ID so that
   * the Optimizer can make a decision whether to inline the definition or not.
   *
   * [[CTERelationDef.statsOpt]] is filled by the Optimizer.
   */
  private def resolveCteRelationDef(unresolvedCteRelationDef: CTERelationDef): LogicalPlan = {
    val cteRelationRef = CTERelationRef(
      cteId = unresolvedCteRelationDef.id,
      _resolved = true,
      isStreaming = unresolvedCteRelationDef.isStreaming,
      output = unresolvedCteRelationDef.output,
      recursive = false,
      maxRows = unresolvedCteRelationDef.maxRows
    )

    handleLeafOperator(cteRelationRef)
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

    withPosition(unresolvedInlineTable) {
      resolve(resolvedRelation)
    }
  }

  /**
   * Preserve `PLAN_ID_TAG` which is used for DataFrame column resolution in Spark Connect.
   */
  private def preservePlanIdTag(
      unresolvedOperator: LogicalPlan,
      resolvedOperator: LogicalPlan): LogicalPlan = {
    unresolvedOperator.getTagValue(LogicalPlan.PLAN_ID_TAG) match {
      case Some(planIdTag) =>
        resolvedOperator.setTagValue(LogicalPlan.PLAN_ID_TAG, planIdTag)
      case None =>
    }
    resolvedOperator
  }

  private def tryDelegateResolutionToExtension(
      unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    val resolutionResult = super.tryDelegateResolutionToExtension(unresolvedOperator, this)
    resolutionResult match {
      case Some(leafOperator: LeafNode) =>
        Some(handleLeafOperator(leafOperator))
      case other =>
        other
    }
  }

  /**
   * Leaf operators introduce original attributes to this operator subtree and need to be handled in
   * a special way:
   *  - Initialize [[ExpressionIdAssigner]] mapping for this operator branch and reassign
   *    `leafOperator`'s output attribute IDs. We don't reassign expression IDs in the leftmost
   *    branch, see [[ExpressionIdAssigner]] class doc for more details.
   *    [[CTERelationRef]]'s output can always be reassigned.
   *  - Overwrite the current [[NameScope]] with remapped output attributes. It's OK to call
   *    `output` on a [[LeafNode]], because it's not recursive (this call fits the single-pass
   *    framework).
   */
  private def handleLeafOperator(leafOperator: LeafNode): LogicalPlan = {
    val leafOperatorWithAssignedExpressionIds = leafOperator match {
      case leafOperator
          if expressionIdAssigner.isLeftmostBranch && !leafOperator.isInstanceOf[CTERelationRef] =>
        expressionIdAssigner.createMapping(newOutput = leafOperator.output)
        leafOperator

      /**
       * [[InMemoryRelation.statsOfPlanToCache]] is mutable and does not get copied during normal
       * [[transformExpressionsUp]]. The easiest way to correctly copy it is via [[newInstance]]
       * call.
       *
       * We match [[MultiInstanceRelation]] to avoid a cyclic import between [[catalyst]] and
       * [[execution]].
       */
      case originalRelation: MultiInstanceRelation =>
        val newRelation = originalRelation.newInstance()

        expressionIdAssigner.createMapping(
          newOutput = newRelation.output,
          oldOutput = Some(originalRelation.output)
        )

        newRelation
      case _ =>
        expressionIdAssigner.createMapping()

        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          leafOperator.transformExpressionsUp {
            case expression: NamedExpression =>
              val newExpression = expressionIdAssigner.mapExpression(expression)
              if (newExpression.eq(expression)) {
                throw SparkException.internalError(
                  s"Leaf operator expression ID was not reassigned. Expression: $expression, " +
                  s"leaf operator: $leafOperator"
                )
              }
              newExpression
          }
        }
    }

    overwriteTopScope(leafOperator, leafOperatorWithAssignedExpressionIds.output)

    leafOperatorWithAssignedExpressionIds
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
        "sqlExpr" -> makeCommaSeparatedExpressionString(filter.expressions),
        "filter" -> toSQLExpr(filter.condition),
        "type" -> toSQLType(filter.condition.dataType)
      )
    )

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

  private def makeCommaSeparatedExpressionString(expressions: Seq[Expression]): String = {
    expressions.map(toSQLExpr).mkString(", ")
  }

  private def overwriteTopScope(
      sourceUnresolvedOperator: LogicalPlan,
      output: Seq[Attribute]): Unit = {
    withPosition(sourceUnresolvedOperator) {
      scopes.overwriteTop(output)
    }
  }
}

object Resolver {

  /**
   * Create a new instance of the [[RelationResolution]].
   */
  def createRelationResolution(catalogManager: CatalogManager): RelationResolution = {
    new RelationResolution(catalogManager)
  }
}
