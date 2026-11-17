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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves initially unresolved [[Project]] operator to either a resolved [[Project]] or
 * [[Aggregate]] node, based on whether there are aggregate expressions in the project list. When
 * LateralColumnAlias resolution is enabled, replaces the output operator with an appropriate
 * operator structure using information from the scope. Detailed explanation can be found in
 * [[buildProjectWithLca]] method.
 */
class ProjectResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Project, LogicalPlan]
    with RetainsOriginalJoinOutput
    with ResolvesNameByHiddenOutput {
  protected val scopes: NameScopeStack = operatorResolver.getNameScopes
  private val queryBlockScopes = operatorResolver.getQueryBlockScopes
  private val cteRegistry = operatorResolver.getCteRegistry
  private val lcaResolver = expressionResolver.getLcaResolver

  /**
   * [[Project]] introduces a new scope to resolve its subtree and project list expressions.
   * During the resolution we determine whether the output operator will be [[Aggregate]] or
   * [[Project]] (based on the `hasAggregateExpressionsOutsideWindow` flag) and delegate the
   * operator building process to an appropriate method in the following way:
   *  - If the operator contains aggregate expressions that are not part of the window functions,
   *  delegate resolution to [[buildAggregate]];
   *  - Else if the operator contains just the lateral alias references, delegate resolution to
   *  [[buildProjectWithLca]];
   *
   * Project-list expressions can reference hidden output attributes, for example qualified star
   * expansion over a `USING` join. The resolver inserts those missing attributes into the child
   * before building the final operator, while keeping the final output defined by the resolved
   * project list.
   *
   * For projects that become aggregates, recursive references are forbidden in the child but may
   * occur in expression subqueries.
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
      val childHasRecursiveSelfReference = cteRegistry.currentScope.hasRecursiveSelfReference

      scopes.current.clearAvailableAliases()

      val childReferencedAttributes = expressionResolver.getLastReferencedAttributes
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList, unresolvedProject)
      val projectReferencedAttributes = expressionResolver.getLastReferencedAttributes

      val missingAttributes: Seq[Attribute] =
        scopes.current
          .resolveMissingAttributesByHiddenOutputForProjectList(projectReferencedAttributes)

      val resolvedChildWithMissingAttributes =
        insertMissingExpressions(
          operator = resolvedChild,
          missingExpressions = missingAttributes
        )
      val resolvedChildWithMetadataColumns = retainOriginalJoinOutput(
        plan = resolvedChildWithMissingAttributes,
        outputExpressions = resolvedProjectList.expressions,
        scopes = scopes,
        childReferencedAttributes = childReferencedAttributes
      )

      if (resolvedProjectList.hasAggregateExpressionsOutsideWindow) {
        if (childHasRecursiveSelfReference) {
          cteRegistry.currentScope.failIfScopeHasSelfReference()
        }
        buildAggregate(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns
        )
      } else if (resolvedProjectList.hasLateralColumnAlias) {
        buildProjectWithLca(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns,
          firstIterationProjectList = outputWithMissingAttributes(
            output = scopes.current.output,
            missingAttributes = missingAttributes
          )
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
      output = resolvedProjectList.expressions.map(namedExpression => namedExpression.toAttribute)
    )

    resolvedOperator
  }

  /**
   * Resolve the original [[Project]] node with aggregate expressions to an appropriate node
   * ([[Project]] or [[Aggregate]]).
   *
   *  - If the [[Aggregate]] contains lateral column references, delegate the resolution to
   *    [[LateralColumnAliasResolver.handleLcaInAggregate]];
   *  - Otherwise, validate the result [[Aggregate]] using [[AggregationValidator]];
   *
   *  Side effect: publishes the resolved [[Aggregate]] (and its alias list) to the current
   *  [[QueryBlockScope]] via [[QueryBlockScope.registerBaseAggregate]] so that HAVING / ORDER BY
   *  siblings can locate it.
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

    queryBlockScopes.current.registerBaseAggregate(aggregate)

    if (resolvedProjectList.hasLateralColumnAlias) {
      val aggregateWithLcaResolutionResult = lcaResolver.handleLcaInAggregate(aggregate)
      val projectList = ResolvedProjectList(
        expressions = aggregateWithLcaResolutionResult.outputList,
        hasAggregateExpressionsOutsideWindow = false,
        hasLateralColumnAlias = false,
        hasWindowExpressions = false
      )
      (aggregateWithLcaResolutionResult.resolvedOperator, projectList)
    } else {
      AggregationValidator(aggregate)

      (aggregate, resolvedProjectList)
    }
  }

  /**
   * If lateral column alias resolution is enabled and the resolved project list contains LCAs, we
   * construct a multi-level [[Project]], created from all lateral column aliases and their
   * dependencies. Finally, we place the original resolved project on top of this multi-level one.
   */
  private def buildProjectWithLca(
      resolvedProjectList: ResolvedProjectList,
      resolvedChildWithMetadataColumns: LogicalPlan,
      firstIterationProjectList: Seq[NamedExpression]): (LogicalPlan, ResolvedProjectList) = {
    val projectWithLca = if (conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      lcaResolver.buildOperatorWithResolvedLca(
        resolvedChild = resolvedChildWithMetadataColumns,
        scope = scopes.current,
        originalProjectList = resolvedProjectList.expressions,
        firstIterationProjectList = firstIterationProjectList
      )
    } else {
      Project(resolvedProjectList.expressions, resolvedChildWithMetadataColumns)
    }
    (projectWithLca, resolvedProjectList)
  }
}
