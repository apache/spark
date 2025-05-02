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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
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
   * After the subtree and project-list expressions are resolved in the child scope we overwrite
   * current scope with resolved operators output to expose new names to the parent operators.
   */
  override def resolve(unresolvedProject: Project): LogicalPlan = {
    val (resolvedOperator, resolvedProjectList) = scopes.withNewScope() {
      val resolvedChild = operatorResolver.resolve(unresolvedProject.child)
      val childReferencedAttributes = expressionResolver.getLastReferencedAttributes
      val resolvedProjectList =
        expressionResolver.resolveProjectList(unresolvedProject.projectList, unresolvedProject)

      val resolvedChildWithMetadataColumns =
        retainOriginalJoinOutput(resolvedChild, resolvedProjectList, childReferencedAttributes)

      if (resolvedProjectList.hasAggregateExpressions) {
        if (resolvedProjectList.hasLateralColumnAlias) {
          // Disable LCA in Aggregates until fully supported.
          throw new ExplicitlyUnsupportedResolverFeature("LateralColumnAlias in Aggregate")
        }
        val aggregate = Aggregate(
          groupingExpressions = Seq.empty[Expression],
          aggregateExpressions = resolvedProjectList.expressions,
          child = resolvedChildWithMetadataColumns,
          hint = None
        )

        // TODO: This validation function does a post-traversal. This is discouraged in
        // single-pass Analyzer.
        ExprUtils.assertValidAggregation(aggregate)

        (aggregate, resolvedProjectList)
      } else {
        val projectWithLca = if (conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
          buildProjectWithResolvedLCAs(
            resolvedChildWithMetadataColumns,
            resolvedProjectList.expressions
          )
        } else {
          Project(resolvedProjectList.expressions, resolvedChildWithMetadataColumns)
        }
        (projectWithLca, resolvedProjectList)
      }
    }

    scopes.overwriteOutputAndExtendHiddenOutput(
      output =
        resolvedProjectList.expressions.map(namedExpression => namedExpression.toAttribute)
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
    val aliasDependencyMap = scopes.current.lcaRegistry.getAliasDependencyLevels()
    val (finalChildPlan, _) = aliasDependencyMap.asScala.foldLeft(
      (resolvedChild, scopes.current.output.map(_.asInstanceOf[NamedExpression]))
    ) {
      case ((currentPlan, currentProjectList), availableAliases) =>
        val referencedAliases = new ArrayBuffer[Alias]
        availableAliases.forEach(
          alias =>
            if (scopes.current.lcaRegistry.isAttributeLaterallyReferenced(alias.toAttribute)) {
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
        if (scopes.current.lcaRegistry.isAttributeLaterallyReferenced(alias.toAttribute)) {
          alias.toAttribute
        } else {
          alias
        }
    )

    Project(finalProjectList, finalChildPlan)
  }
}
