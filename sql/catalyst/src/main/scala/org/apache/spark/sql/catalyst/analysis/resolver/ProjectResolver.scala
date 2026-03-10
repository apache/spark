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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves initially unresolved [[Project]] operator to either a resolved [[Project]] or
 * [[Aggregate]] node, based on whether there are aggregate expressions in the project list. When
 * LateralColumnAlias resolution is enabled, replaces the output operator with an appropriate
 * operator structure using information from the scope. Detailed explanation can be found in
 * [[buildProjectWithResolvedLCAs]] method.
 */
class ProjectResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Project, LogicalPlan]
    with RetainsOriginalJoinOutput {
  private val scopes = operatorResolver.getNameScopes
  private val cteRegistry = operatorResolver.getCteRegistry
  private val lcaResolver = expressionResolver.getLcaResolver
  private val generatorResolver = expressionResolver.getGeneratorResolver

  /**
   * [[Project]] introduces a new scope to resolve its subtree and project list expressions.
   * It also introduces a new [[WindowResolutionContext]] for potential window resolution.
   * During the resolution we determine whether the output operator will be [[Aggregate]],
   * [[Window]], [[Generate]] or [[Project]] (based on the `hasAggregateExpressionsOutsideWindow`,
   * `hasWindowExpressions` and `hasGeneratorExpressions` flags) and delegate the operator building
   * process to an appropriate method in the following way:
   *  - If the operator contains aggregate expressions that are not part of the window functions,
   *  delegate resolution to [[buildAggregate]];
   *  - Else if the operator contains lateral column aliases and window functions, delegate
   *  resolution to [[buildProjectWithLca]];
   *  - Else if the operator contains window functions, but not lateral column aliases, delegate
   *  resolution to [[WindowBuilder.buildWindow]];
   *  - Else if the operator contains generator expressions, delegate resolution to
   *  [[buildGenerate]];
   *  - Else if the operator contains just the lateral alias references, delegate resolution to
   *  [[buildProjectWithLca]];
   *
   * When resolving inside a recursive CTE definition, we pre-scan the unresolved project list to
   * detect aggregate and window functions before resolving the child. This is necessary because we
   * must disallow recursive CTE references before resolving the child, as the child resolution (or
   * subquery resolution within the project list) may encounter recursive CTE references that should
   * be disallowed in aggregates (always per SQL standard) or window functions (if the config flag
   * is set). The pre-scan is skipped for non-recursive CTE contexts to avoid unnecessary overhead
   * on the common path.
   *
   * After the subtree and project-list expressions are resolved in the child scope we overwrite
   * current scope with resolved operators output to expose new names to the parent operators.
   *
   * We need to clear [[NameScope.availableAliases]]. Those are only relevant for the immediate
   * project list for output prioritization to work correctly in
   * [[NameScope.tryResolveMultipartNameByOutput]].
   */
  override def resolve(unresolvedProject: Project): LogicalPlan = {
    scopes.pushScope()

    val (resolvedOperator, resolvedProjectList) = try {
      val resolvedChild = operatorResolver.resolve(unresolvedProject.child)

      scopes.current.clearAvailableAliases()

      val childReferencedAttributes = expressionResolver.getLastReferencedAttributes
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList, unresolvedProject)

      GeneratorResolver.checkCanResolveGeneratorExpressions(
        hasGeneratorExpressions = resolvedProjectList.hasGeneratorExpressions,
        hasLateralColumnAlias = resolvedProjectList.hasLateralColumnAlias,
        hasWindowExpressions = resolvedProjectList.hasWindowExpressions
      )

      val outputExpressionsWithFlattenedGenerators =
        expressionsWithFlattenedGenerators(resolvedProjectList.expressions)

      val resolvedChildWithMetadataColumns = retainOriginalJoinOutput(
        plan = resolvedChild,
        outputExpressions = outputExpressionsWithFlattenedGenerators,
        scopes = scopes,
        childReferencedAttributes = childReferencedAttributes
      )

      if (resolvedProjectList.hasAggregateExpressionsOutsideWindow) {
        buildAggregate(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns
        )
      } else if (resolvedProjectList.hasGeneratorExpressions) {
        checkNoMultipleGeneratorsInProject(resolvedProjectList)

        buildGenerate(
          resolvedProjectList = resolvedProjectList,
          resolvedChild = resolvedChildWithMetadataColumns
        )
      } else if (resolvedProjectList.hasLateralColumnAlias) {
        buildProjectWithLca(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns
        )
      } else {
        val resolvedProject =
          Project(resolvedProjectList.expressions, resolvedChildWithMetadataColumns)

        (resolvedProject, resolvedProjectList)
      }
    } finally {
      scopes.popScope()
    }

    scopes.overwriteOutputAndExtendHiddenOutput(
      output = resolvedProjectList.expressions.map(namedExpression => namedExpression.toAttribute),
      aggregateListAliases = resolvedProjectList.aggregateListAliases,
      baseAggregate = resolvedProjectList.baseAggregate
    )

    resolvedOperator
  }

  /**
   * Resolve the original [[Project]] node with aggregate expressions to an appropriate node
   * ([[Project]], [[Aggregate]] or [[Window]]).
   *
   *  - If the [[Aggregate]] contains generator expressions, delegate to
   *    [[buildAggregateWithGenerator]] which extracts generator children into the aggregate
   *    and builds a [[Generate]] node with [[Project]] on top;
   *  - If the [[Aggregate]] contains lateral column references, delegate the resolution to
   *    [[LateralColumnAliasResolver.handleLcaInAggregate]];
   *  - If the [[Aggregate]] has window expressions, delegate the resolution to [[buildWindow]].
   *    Currently, window expressions together with lateral column aliases are not supported;
   *  - Otherwise, validate the result [[Aggregate]] using [[ExprUtils.assertValidAggregation]];
   *
   *  Note: Recursive CTE self references are disallowed before entering this method by the
   *  pre-scan in [[resolve]].
   */
  private def buildAggregate(
      resolvedProjectList: ResolvedProjectList,
      resolvedChildWithMetadataColumns: LogicalPlan
  ): (LogicalPlan, ResolvedProjectList) = {
    val aggregate = Aggregate(
      groupingExpressions = Seq.empty[Expression],
      aggregateExpressions = resolvedProjectList.expressions,
      child = resolvedChildWithMetadataColumns,
      hint = None
    )

    cteRegistry.currentScope.failIfScopeHasSelfReference()

    if (resolvedProjectList.hasGeneratorExpressions) {
      generatorResolver.validateNoMoreThenOneGenerator(resolvedProjectList.expressions)
      buildAggregateWithGenerator(aggregate)
    } else if (resolvedProjectList.hasLateralColumnAlias) {
      val aggregateWithLcaResolutionResult = lcaResolver.handleLcaInAggregate(aggregate)
      val projectList = ResolvedProjectList(
        expressions = aggregateWithLcaResolutionResult.outputList,
        hasAggregateExpressionsOutsideWindow = false,
        hasLateralColumnAlias = false,
        hasWindowExpressions = false,
        hasGeneratorExpressions = false,
        hasCorrelatedScalarSubqueryExpressions = false,
        aggregateListAliases = aggregateWithLcaResolutionResult.aggregateListAliases,
        baseAggregate = Some(aggregateWithLcaResolutionResult.baseAggregate)
      )
      (aggregateWithLcaResolutionResult.resolvedOperator, projectList)
    } else {
      // TODO: This validation function does a post-traversal. This is discouraged in
      // single-pass Analyzer.
      ExprUtils.assertValidAggregation(aggregate)

      val resolvedAggregateList = resolvedProjectList.copy(
        aggregateListAliases = scopes.current.aggregateListAliases,
        baseAggregate = Some(aggregate)
      )

      (aggregate, resolvedAggregateList)
    }
  }

  /**
   * If lateral column alias resolution is enabled and the resolved project list contains LCAs, we
   * construct a multi-level [[Project]], created from all lateral column aliases and their
   * dependencies. Finally, we place the original resolved project on top of this multi-level one.
   */
  private def buildProjectWithLca(
      resolvedProjectList: ResolvedProjectList,
      resolvedChildWithMetadataColumns: LogicalPlan): (LogicalPlan, ResolvedProjectList) = {
    val projectWithLca = if (conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      lcaResolver.buildOperatorWithResolvedLca(
        resolvedChild = resolvedChildWithMetadataColumns,
        scope = scopes.current,
        originalProjectList = resolvedProjectList.expressions,
        firstIterationProjectList = scopes.current.output
      )
    } else {
      Project(resolvedProjectList.expressions, resolvedChildWithMetadataColumns)
    }
    (projectWithLca, resolvedProjectList)
  }

  /**
   * Flattens [[AliasedGenerator]] expressions into their output attributes.
   *
   * For regular expressions, returns them as-is. For [[AliasedGenerator]], returns the
   * generator's output attributes instead. This is needed to properly compute the output
   * expressions for metadata column handling before the [[Generator]] expression is transformed
   * into a [[Generate]] operator by [[GeneratorResolver]].
   */
  private def expressionsWithFlattenedGenerators(
      expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.flatMap {
      case aliasedGenerator: AliasedGenerator =>
        aliasedGenerator.nullableGeneratorOutput
      case other => Seq(other)
    }
  }

  /**
   * Block multiple generators in [[Project]] until ordering differences with the old analyzer are
   * resolved.
   */
  private def checkNoMultipleGeneratorsInProject(
      resolvedProjectList: ResolvedProjectList): Unit = {
    val generatorCount = resolvedProjectList.expressions.count {
      case _: AliasedGenerator => true
      case _ => false
    }
    if (generatorCount > 1) {
      throw new ExplicitlyUnsupportedResolverFeature(
        "Multiple generator expressions in a single SELECT are not supported"
      )
    }
  }

  /**
   * Builds [[Generate]] operator per [[Generator]] expression on `resolvedProjectList`.
   * See [[GeneratorResolver.resolveProjectListWithGenerators]] for more details
   */
  private def buildGenerate(
      resolvedProjectList: ResolvedProjectList,
      resolvedChild: LogicalPlan): (LogicalPlan, ResolvedProjectList) = {
    val project = generatorResolver.resolveProjectListWithGenerators(
      expressions = resolvedProjectList.expressions,
      childOperator = resolvedChild
    )

    val newResolvedProjectList = ResolvedProjectList(
      expressions = project.projectList,
      hasAggregateExpressionsOutsideWindow = false,
      hasLateralColumnAlias = false,
      hasWindowExpressions = false,
      hasGeneratorExpressions = false,
      hasCorrelatedScalarSubqueryExpressions = false,
      aggregateListAliases = Seq.empty
    )

    (project, newResolvedProjectList)
  }

  /**
   * Extracts [[Generator]] expression from aggregate and builds corresponding [[Generate]] operator
   * with [[Project]] on top.
   */
  private def buildAggregateWithGenerator(
      baseAggregate: Aggregate): (LogicalPlan, ResolvedProjectList) = {
    val (projectExpressions, aggregate) =
      extractProjectListWithGeneratorFromAggregate(baseAggregate)

    val project = generatorResolver.resolveProjectListWithGenerators(
      expressions = projectExpressions,
      childOperator = aggregate
    )

    // TODO: This validation function does a post-traversal. This is discouraged in
    // single-pass Analyzer.
    ExprUtils.assertValidAggregation(aggregate)

    val newResolvedProjectList = ResolvedProjectList(
      expressions = project.projectList,
      hasAggregateExpressionsOutsideWindow = false,
      hasLateralColumnAlias = false,
      hasWindowExpressions = false,
      hasGeneratorExpressions = false,
      hasCorrelatedScalarSubqueryExpressions = false,
      aggregateListAliases = scopes.current.aggregateListAliases,
      baseAggregate = Some(aggregate)
    )

    (project, newResolvedProjectList)
  }

  /**
   * Transforms an [[Aggregate]] with generator expressions by extracting the generator into new
   * project list and subsequent transformation in the [[Generate]] operator by
   * [[GeneratorResolver.resolveProjectListWithGenerators]].
   *
   * This is simplified version of generator's extraction logic in
   * [[ExpressionResolver.resolveAggregateExpressions]]. Because we know that the aggregate from
   * project always have empty grouping expressions, we can safely postpone extraction of generator
   * children until the aggregate is built.
   */
  private def extractProjectListWithGeneratorFromAggregate(
      baseAggregate: Aggregate): (Seq[NamedExpression], Aggregate) = {
    val (projectExpressions, aggregateExpressions) = baseAggregate.aggregateExpressions.map {
      case aliasedGenerator: AliasedGenerator =>
        GeneratorResolver.extract(aliasedGenerator)
      case other =>
        (other.toAttribute, Seq(other))
    }.unzip

    val aggregate = baseAggregate.copy(aggregateExpressions = aggregateExpressions.flatten)

    (projectExpressions, aggregate)
  }
}
