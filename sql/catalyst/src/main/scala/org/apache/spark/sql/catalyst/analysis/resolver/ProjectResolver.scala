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
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{withPosition, AnalysisErrorAt}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
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
    extends TreeNodeResolver[Project, LogicalPlan] {

  private val isLcaEnabled = conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)
  private val scopes = operatorResolver.getNameScopes

  /**
   * * [[Project]] introduces a new scope to resolve its subtree and project list expressions.
   * * During the resolution we determine whether the output operator will be [[Aggregate]] or
   * * [[Project]] (based on the `hasAggregateExpressions` flag).
   *
   * If the output operator is [[Project]] and if lateral column alias resolution is enabled, we
   * construct a multi-level [[Project]], created from all lateral column aliases and their
   * dependencies. Finally, we place the original resolved project on top of this multi-level one.
   *
   * After the subtree and project-list expressions are resolved in the child scope we overwrite
   * current scope with resolved operators output to expose new names to the parent operators.
   */
  override def resolve(unresolvedProject: Project): LogicalPlan = {
    val (resolvedOperator, resolvedProjectList) = scopes.withNewScope {
      val resolvedChild = operatorResolver.resolve(unresolvedProject.child)
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList, unresolvedProject)
      if (resolvedProjectList.hasAggregateExpressions) {
        if (resolvedProjectList.hasLateralColumnAlias) {
          // Disable LCA in Aggregates until fully supported.
          throw new ExplicitlyUnsupportedResolverFeature("LateralColumnAlias in Aggregate")
        }
        val aggregate = Aggregate(
          groupingExpressions = Seq.empty[Expression],
          aggregateExpressions = resolvedProjectList.expressions,
          child = resolvedChild,
          hint = None
        )
        if (resolvedProjectList.hasAttributes) {
          aggregate.failAnalysis(errorClass = "MISSING_GROUP_BY", messageParameters = Map.empty)
        }
        (aggregate, resolvedProjectList)
      } else {
        val projectWithLca = if (isLcaEnabled) {
          buildProjectWithResolvedLCAs(resolvedChild, resolvedProjectList.expressions)
        } else {
          Project(resolvedProjectList.expressions, resolvedChild)
        }
        (projectWithLca, resolvedProjectList)
      }
    }

    withPosition(unresolvedProject) {
      scopes.overwriteTop(
        resolvedProjectList.expressions.map(namedExpression => namedExpression.toAttribute)
      )
    }

    resolvedOperator
  }

  /**
   * Builds a multi-level [[Project]] with all lateral column aliases and their dependencies. First,
   * from top scope, we acquire dependency levels of all aliases. Dependency level is defined as a
   * number of attributes that an attribute depends on in the lateral alias reference chain. For
   * example, in a query like:
   *
   * {{{ SELECT 0 AS a, 1 AS b, 2 AS c, b AS d, a AS e, d AS f, a AS g, g AS h, h AS i }}}
   *
   * Dependency levels will be as follows:
   *
   * level 0: a, b, c
   * level 1: d, e, g
   * level 2: f, h
   * level 3: i
   *
   * Once we have dependency levels, we construct a multi-level [[Project]] in a following way:
   *  - There is exactly one [[Project]] node per level.
   *  - Project lists are compounded such that project lists on upper levels must contain all
   *  attributes from the below levels.
   *  - Project list on level 0 includes all attributes from the output of the operator below the
   *  original [[Project]].
   *  - Original [[Project]] is placed on top of the multi-level [[Project]]. Any aliases that have
   *  been laterally referenced need to be replaced with only their names. This is because their
   *  full definitions ( `attr` as `name` ) have already been defined on lower levels.
   *  - If an attribute is never referenced, it does not show up in multi-level project lists, but
   *  instead only in the top-most [[Project]].
   *
   *  For previously given query, following above rules, resolved [[Project]] would look like:
   *
   * Project [a, b, 2 AS c, d, a AS e, d AS f, g, h, h AS i]
   * +- Project [b, a, d, g, g AS h]
   *    +- Project [b, a, b AS d, a AS g]
   *       +- Project [1 AS b, 0 AS a]
   *          +- OneRowRelation
   */
  private def buildProjectWithResolvedLCAs(
      resolvedChild: LogicalPlan,
      originalProjectList: Seq[NamedExpression]) = {
    val aliasDependencyMap = scopes.top.lcaRegistry.getAliasDependencyLevels()
    val (finalChildPlan, _) = aliasDependencyMap.asScala.foldLeft(
      (resolvedChild, scopes.top.output.map(_.asInstanceOf[NamedExpression]))
    ) {
      case ((currentPlan, currentProjectList), availableAliases) =>
        val referencedAliases = new ArrayBuffer[Alias]
        availableAliases.forEach(
          alias =>
            if (scopes.top.lcaRegistry.isAttributeLaterallyReferenced(alias.toAttribute)) {
              referencedAliases.append(alias)
            }
        )

        if (referencedAliases.nonEmpty) {
          val newProjectList = currentProjectList.map(_.toAttribute) ++ referencedAliases
          (Project(newProjectList, currentPlan), newProjectList)
        } else {
          (currentPlan, currentProjectList)
        }
    }

    val finalProjectList = originalProjectList.map(
      alias =>
        if (scopes.top.lcaRegistry.isAttributeLaterallyReferenced(alias.toAttribute)) {
          alias.toAttribute
        } else {
          alias
        }
    )

    Project(finalProjectList, finalChildPlan)
  }
}
