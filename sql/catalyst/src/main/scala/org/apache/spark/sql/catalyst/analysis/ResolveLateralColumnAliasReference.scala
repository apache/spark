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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, Expression, LateralColumnAliasReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LATERAL_COLUMN_ALIAS_REFERENCE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule is the second phase to resolve lateral column alias.
 *
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list.
 * Plan-wise, it handles two types of operators: Project and Aggregate.
 * - in Project, pushing down the referenced lateral alias into a newly created Project, resolve
 *   the attributes referencing these aliases
 * - in Aggregate TODO inserting the Project node above and fall back to the resolution of Project.
 *
 * The whole process is generally divided into two phases:
 * 1) recognize resolved lateral alias, wrap the attributes referencing them with
 *    [[LateralColumnAliasReference]]
 * 2) when the whole operator is resolved, unwrap [[LateralColumnAliasReference]].
 *    For Project, it further resolves the attributes and push down the referenced lateral aliases.
 *    For Aggregate, TODO
 *
 * Example for Project:
 * Before rewrite:
 * Project [age AS a, 'a + 1]
 * +- Child
 *
 * After phase 1:
 * Project [age AS a, lateralalias(a) + 1]
 * +- Child
 *
 * After phase 2:
 * Project [a, a + 1]
 * +- Project [child output, age AS a]
 *    +- Child
 *
 * Example for Aggregate TODO
 *
 * For Aggregate, it first wraps the attribute resolved by lateral alias with
 * [[LateralColumnAliasReference]].
 * Before wrap (omit some cast or alias):
 * Aggregate [dept#14] [dept#14 AS a#12, 'a + 1, avg(salary#16) AS b#13, 'b + avg(bonus#17)]
 * +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 * After wrap:
 * Aggregate [dept#14] [dept#14 AS a#12, lca(a) + 1, avg(salary#16) AS b#13, lca(b) + avg(bonus#17)]
 * +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 * When the whole Aggregate is resolved, it inserts a [[Project]] above with the aggregation
 * expression list, but extracts the [[AggregateExpression]] and grouping expressions in the
 * list to the current Aggregate. It restores all the [[LateralColumnAliasReference]] back to
 * [[UnresolvedAttribute]]. The problem falls back to the lateral alias resolution in Project.
 *
 * After restore:
 * Project [dept#14 AS a#12, 'a + 1, avg(salary)#26 AS b#13, 'b + avg(bonus)#27]
 * +- Aggregate [dept#14] [avg(salary#16) AS avg(salary)#26, avg(bonus#17) AS avg(bonus)#27,dept#14]
 *    +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 *
 * The name resolution priority:
 * local table column > local lateral column alias > outer reference
 *
 * Because lateral column alias has higher resolution priority than outer reference, it will try
 * to resolve an [[OuterReference]] using lateral column alias in phase 1, similar as an
 * [[UnresolvedAttribute]]. If success, it strips [[OuterReference]] and also wraps it with
 * [[LateralColumnAliasReference]].
 */
object ResolveLateralColumnAliasReference extends Rule[LogicalPlan] {
  case class AliasEntry(alias: Alias, index: Int)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      plan
    } else {
      // phase 2: unwrap
      plan.resolveOperatorsUpWithPruning(
        _.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE), ruleId) {
        case p @ Project(projectList, child) if p.resolved
          && projectList.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
          var aliasMap = AttributeMap.empty[AliasEntry]
          val referencedAliases = collection.mutable.Set.empty[AliasEntry]
          def unwrapLCAReference(e: NamedExpression): NamedExpression = {
            e.transformWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
              case lcaRef: LateralColumnAliasReference if aliasMap.contains(lcaRef.a) =>
                val aliasEntry = aliasMap.get(lcaRef.a).get
                // If there is no chaining of lateral column alias reference, push down the alias
                // and unwrap the LateralColumnAliasReference to the NamedExpression inside
                // If there is chaining, don't resolve and save to future rounds
                if (!aliasEntry.alias.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                  referencedAliases += aliasEntry
                  lcaRef.ne
                } else {
                  lcaRef
                }
              case lcaRef: LateralColumnAliasReference if !aliasMap.contains(lcaRef.a) =>
                // It shouldn't happen, but restore to unresolved attribute to be safe.
                UnresolvedAttribute(lcaRef.nameParts)
            }.asInstanceOf[NamedExpression]
          }
          val newProjectList = projectList.zipWithIndex.map {
            case (a: Alias, idx) =>
              val lcaResolved = unwrapLCAReference(a)
              // Insert the original alias instead of rewritten one to detect chained LCA
              aliasMap += (a.toAttribute -> AliasEntry(a, idx))
              lcaResolved
            case (e, _) =>
              unwrapLCAReference(e)
          }

          if (referencedAliases.isEmpty) {
            p
          } else {
            val outerProjectList = collection.mutable.Seq(newProjectList: _*)
            val innerProjectList =
              collection.mutable.ArrayBuffer(child.output.map(_.asInstanceOf[NamedExpression]): _*)
            referencedAliases.foreach { case AliasEntry(alias: Alias, idx) =>
              outerProjectList.update(idx, alias.toAttribute)
              innerProjectList += alias
            }
            p.copy(
              projectList = outerProjectList.toSeq,
              child = Project(innerProjectList.toSeq, child)
            )
          }

        case agg @ Aggregate(groupingExpressions, aggregateExpressions, _) if agg.resolved
            && aggregateExpressions.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>

          val newAggExprs = collection.mutable.Set.empty[NamedExpression]
          val expressionMap = collection.mutable.LinkedHashMap.empty[Expression, NamedExpression]
          val projectExprs = aggregateExpressions.map { exp =>
            exp.transformDown {
              case aggExpr: AggregateExpression =>
                // Doesn't support referencing a lateral alias in aggregate function
                if (aggExpr.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                  aggExpr.collectFirst {
                    case lcaRef: LateralColumnAliasReference =>
                      throw QueryCompilationErrors.LateralColumnAliasInAggFuncUnsupportedError(
                        lcaRef.nameParts, aggExpr)
                  }
                }
                val ne = expressionMap.getOrElseUpdate(
                  aggExpr.canonicalized,
                  ResolveAliases.assignAliases(Seq(UnresolvedAlias(aggExpr))).map {
                    // TODO temporarily clear the metadata for an issue found in test
                    case a: Alias => a.copy(a.child, a.name)(
                      a.exprId, a.qualifier, None, a.nonInheritableMetadataKeys)
                    case other => other
                  }.head)
                newAggExprs += ne
                ne.toAttribute
              case e if groupingExpressions.exists(_.semanticEquals(e)) =>
                // TODO one concern here, is condition here be able to match all grouping
                //  expressions? For example, Agg [age + 10] [a + age + 10], when transforming down,
                //  is it possible that (a + age) + 10, so that it won't be able to match (age + 10)
                //  add a test.
                val ne = expressionMap.getOrElseUpdate(
                  e.canonicalized,
                  ResolveAliases.assignAliases(Seq(UnresolvedAlias(e))).head)
                newAggExprs += ne
                ne.toAttribute
            }.asInstanceOf[NamedExpression]
          }
          if (newAggExprs.isEmpty) {
            agg
          } else {
            Project(
              projectList = projectExprs,
              child = agg.copy(aggregateExpressions = newAggExprs.toSeq)
            )
          }
        // TODO withOrigin?
      }
    }
  }
}
