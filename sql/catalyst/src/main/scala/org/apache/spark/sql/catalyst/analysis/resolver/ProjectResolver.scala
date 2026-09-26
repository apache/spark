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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  ExprId,
  NamedExpression,
  PipeSetInput
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._
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

  /**
   * [[Project]] introduces a new scope to resolve its subtree and project list expressions.
   * It also introduces a new [[WindowResolutionContext]] for potential window resolution.
   * During the resolution we determine whether the output operator will be [[Aggregate]] or
   * [[Project]] (based on the `hasAggregateExpressionsOutsideWindow` flag) and delegate the
   * operator building process to an appropriate method in the following way:
   *  - If the operator contains aggregate expressions that are not part of the window functions,
   *  delegate resolution to [[buildAggregate]];
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

      val retainedPipeSetOutput = resolvedChild match {
        case pipeSetInput: PipeSetInput =>
          pipeSetInput.metadataOutput.filter(_.qualifiedAccessOnly)
        case _ =>
          Seq.empty
      }

      val resolvedChildWithMetadataColumns = retainOriginalJoinOutput(
        plan = resolvedChild,
        outputExpressions = resolvedProjectList.expressions,
        scopes = scopes,
        childReferencedAttributes = childReferencedAttributes
      )

      if (resolvedProjectList.hasAggregateExpressionsOutsideWindow) {
        buildAggregate(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns
        )
      } else if (resolvedProjectList.hasLateralColumnAlias) {
        buildProjectWithLca(
          resolvedProjectList = resolvedProjectList,
          resolvedChildWithMetadataColumns = resolvedChildWithMetadataColumns
        )
      } else {
        val retainedProjectList =
          if (retainedPipeSetOutput.isEmpty ||
              unresolvedProject.containsTag(ResolverTag.TOP_LEVEL_OPERATOR)) {
            Seq.empty
          } else {
            missingRetainedOutput(retainedPipeSetOutput, resolvedProjectList.expressions)
          }
        val resolvedProject =
          Project(
            resolvedProjectList.expressions ++ retainedProjectList,
            resolvedChildWithMetadataColumns)

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
   * Returns retained attributes that are not already produced by the visible project list.
   * Matching is occurrence-based because a projection can repeat one expression ID.
   */
  private def missingRetainedOutput(
      retainedOutput: Seq[Attribute],
      visibleOutput: Seq[NamedExpression]): Seq[Attribute] = {
    val visibleOccurrencesByExprId = mutable.HashMap.empty[ExprId, Int]
    visibleOutput.foreach { visibleExpression =>
      val exprId = visibleExpression.exprId
      visibleOccurrencesByExprId.updateWith(exprId) {
        case Some(occurrences) => Some(occurrences + 1)
        case None => Some(1)
      }
    }
    retainedOutput.filter { retainedAttribute =>
      visibleOccurrencesByExprId.get(retainedAttribute.exprId) match {
        case Some(occurrences) if occurrences > 0 =>
          visibleOccurrencesByExprId.update(retainedAttribute.exprId, occurrences - 1)
          false
        case _ =>
          true
      }
    }
  }

  /**
   * Resolve the original [[Project]] node with aggregate expressions to an appropriate node
   * ([[Project]], [[Aggregate]] or [[Window]]).
   *
   *  - If the [[Aggregate]] contains lateral column references, delegate the resolution to
   *    [[LateralColumnAliasResolver.handleLcaInAggregate]];
   *  - Otherwise, validate the result [[Aggregate]] using [[AggregationValidator]];
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

    if (resolvedProjectList.hasLateralColumnAlias) {
      val aggregateWithLcaResolutionResult = lcaResolver.handleLcaInAggregate(aggregate)
      val projectList = ResolvedProjectList(
            expressions = aggregateWithLcaResolutionResult.outputList,
            hasAggregateExpressionsOutsideWindow = false,
            hasLateralColumnAlias = false,
            aggregateListAliases = aggregateWithLcaResolutionResult.aggregateListAliases,
            baseAggregate = Some(aggregateWithLcaResolutionResult.baseAggregate)
          )
      (aggregateWithLcaResolutionResult.resolvedOperator, projectList)
    } else {
      AggregationValidator(aggregate)

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

}
