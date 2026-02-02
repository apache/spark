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

import java.util.{HashMap, HashSet}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  ExprId,
  ExprUtils,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
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
    extends TreeNodeResolver[Project, LogicalPlan] {
  private val scopes = operatorResolver.getNameScopes
  private val lcaResolver = expressionResolver.getLcaResolver

  /**
   * [[Project]] introduces a new scope to resolve its subtree and project list expressions.
   * During the resolution we determine whether the output operator will be [[Aggregate]] or
   * [[Project]] (based on the `hasAggregateExpressions` flag). If the result is an [[Aggregate]]
   * validate it using the [[ExprUtils.assertValidAggregation]].
   *
   * If the output operator is [[Project]] and if lateral column alias resolution is enabled, we
   * construct a multi-level [[Project]], created from all lateral column aliases and their
   * dependencies. Finally, we place the original resolved project on top of this multi-level one.
   *
   * If the output operator is [[Aggregate]] and if the [[Aggregate]] contains lateral column
   * references, delegate the resolution of these column to
   * [[LateralColumnAliasResolver.handleLcaInAggregate]].
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

      scopes.current.availableAliases.clear()

      val childReferencedAttributes = expressionResolver.getLastReferencedAttributes
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList, unresolvedProject)

      val resolvedChildWithMetadataColumns =
        retainOriginalJoinOutput(resolvedChild, resolvedProjectList, childReferencedAttributes)

      if (resolvedProjectList.hasAggregateExpressions) {
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
            hasAggregateExpressions = false,
            hasLateralColumnAlias = false,
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
      } else {
        val projectWithLca = if (conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
          lcaResolver.buildProjectWithResolvedLca(
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
   * This method adds a [[Project]] node on top of a [[Join]], if [[Join]]'s output has been
   * changed when metadata columns are added to [[Project]] nodes below the [[Join]]. This is
   * necessary in order to stay compatible with fixed-point analyzer. Instead of doing this in
   * [[JoinResolver]] we must do this here, because while resolving [[Join]] we still don't know if
   * we should add a [[Project]] or not. For example consider the following query:
   *
   * {{{
   * -- tables: nt1(k, v1), nt2(k, v2), nt3(k, v3)
   * SELECT * FROM nt1 NATURAL JOIN nt2 JOIN nt3 ON nt2.k = nt3.k;
   * }}}
   *
   * Unresolved plan will be:
   *
   * 'Project [*]
   * +- 'Join Inner, ('nt2.k = 'nt3.k)
   * :- 'Join NaturalJoin(Inner)
   * :  :- 'UnresolvedRelation [nt1], [], false
   * :  +- 'UnresolvedRelation [nt2], [], false
   * +- 'UnresolvedRelation [nt3], [], false
   *
   * After resolving the inner natural join, the plan becomes:
   *
   * 'Project [*]
   * +- 'Join Inner, ('nt2.k = 'nt3.k)
   *    :- Project [k#15, v1#16, v2#28, k#27]
   *    :  +- Join Inner, (k#15 = k#27)
   *    :  :  +- SubqueryAlias nt1
   *    :  :     +- LocalRelation [k#15, v1#16]
   *    :  +- SubqueryAlias nt2
   *    :     +- LocalRelation [k#27, v2#28]
   *    +- 'UnresolvedRelation [nt3], [], false
   *
   * Because we are resolving a natural join, we have placed a [[Project]] node on top of it with
   * the inner join's output. Additionally, in single-pass, we add all metadata columns as we
   * resolve up and then prune away unnecessary columns later (more in [[PruneMetadataColumns]]).
   * This is necessary in order to stay compatible with fixed-point's [[AddMetadataColumns]] rule,
   * because [[AddMetadata]] columns will recognize k#27 as missing attribute needed for [[Join]]
   * condition and will therefore add it in the below [[Project]] node. Because of this we are also
   * adding k#27 as a metadata column to this [[Project]]. This addition of a metadata column
   * changes the original output of the outer join (because one of the inputs has changed) and in
   * order to stay compatible with fixed-point, we need to place another [[Project]] on top of the
   * outer join with its original output. Now, the final plan looks like this:
   *
   * Project [k#15, v1#16, v2#28, k#31, v3#32]
   * +- Project [k#15, v1#16, v2#28, k#31, v3#32]
   *    +- Join Inner, (k#27 = k#31)
   *    :- Project [k#15, v1#16, v2#28, k#27]
   *    :  +- Join Inner, (k#15 = k#27)
   *    :     :- SubqueryAlias nt1
   *    :     :  +- LocalRelation [k#15, v1#16]
   *    :     +- SubqueryAlias nt2
   *    :        +- LocalRelation [k#27, v2#28]
   *    +- SubqueryAlias nt3
   *        +- LocalRelation [k#31, v3#32]
   *
   * As can be seen, the [[Project]] node immediately on top of [[Join]] doesn't contain the
   * metadata column k#27 that we have added. Because of this, k#27 will be pruned away later.
   *
   * Now consider the following query for the same input:
   *
   * {{{ SELECT *, nt2.k FROM nt1 NATURAL JOIN nt2 JOIN nt3 ON nt2.k = nt3.k; }}}
   *
   * The plan will be:
   *
   * Project [k#15, v1#16, v2#28, k#31, v3#32, k#27]
   * +- Join Inner, (k#27 = k#31)
   * :- Project [k#15, v1#16, v2#28, k#27]
   * :  +- Join Inner, (k#15 = k#27)
   * :     :- SubqueryAlias nt1
   * :     :  +- LocalRelation [k#15, v1#16]
   * :     +- SubqueryAlias nt2
   * :        +- LocalRelation [k#27, v2#28]
   * +- SubqueryAlias nt3
   *    +- LocalRelation [k#31, v3#32]
   *
   * In fixed-point, because we are referencing k#27 from [[Project]] node, [[AddMetadataColumns]]
   * (which is transforming the tree top-down) will see that [[Project]] has a missing metadata
   * column and will therefore place k#27 in the [[Project]] node below outer [[Join]]. This is
   * important, because by [[AddMetadataColumns]] logic, we don't check whether the output of the
   * outer [[Join]] has changed, and we only check the output change for top-most [[Project]].
   * Because we need to stay fully compatible with fixed-point, in this case w don't place a
   * [[Project]] on top of the outer [[Join]] even though its output has changed.
   */
  private def retainOriginalJoinOutput(
      plan: LogicalPlan,
      resolvedProjectList: ResolvedProjectList,
      childReferencedAttributes: HashMap[ExprId, Attribute]): LogicalPlan = {
    plan match {
      case join: Join
          if childHasMissingAttributesNotInProjectList(
            resolvedProjectList.expressions,
            childReferencedAttributes
          ) =>
        Project(scopes.current.output, join)
      case other => other
    }
  }

  /**
   * Returns true if a child node of [[Project]] has missing attributes that can be resolved from
   * [[NameScope.hiddenOutput]] and those attributes are not present in the project list.
   */
  private def childHasMissingAttributesNotInProjectList(
      projectList: Seq[NamedExpression],
      referencedAttributes: HashMap[ExprId, Attribute]): Boolean = {
    val expressionIdsFromProjectList = new HashSet[ExprId](projectList.map(_.exprId).asJava)
    val missingAttributes = new HashMap[ExprId, Attribute]
    referencedAttributes.asScala
      .foreach {
        case (exprId, attribute) =>
          if (!expressionIdsFromProjectList.contains(exprId) && attribute.isMetadataCol) {
            missingAttributes.put(exprId, attribute)
          }
      }
    val missingAttributeResolvedByHiddenOutput =
      scopes.current.resolveMissingAttributesByHiddenOutput(missingAttributes)

    missingAttributeResolvedByHiddenOutput.nonEmpty
  }
}
