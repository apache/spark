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

import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression, OuterReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{OUTER_REFERENCE, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list,
 * - in Project inserting a new Project node with the referenced alias so that it can be
 *   resolved by other rules
 * - in Aggregate TODO.
 *
 * The name resolution priority:
 * local table column > local lateral column alias > outer reference
 *
 * Because lateral column alias has higher resolution priority than outer reference, it will try
 * to resolve an [[OuterReference]] using lateral column alias, similar as an
 * [[UnresolvedAttribute]]. If success, it strips [[OuterReference]] and restores it back to
 * [[UnresolvedAttribute]]
 *
 * For Project, it rewrites by inserting a newly created Project plan between the original Project
 * and its child, pushing the referenced lateral column aliases to this new Project, and updating
 * the project list of the original Project.
 *
 * Before rewrite:
 * Project [age AS a, 'a + 1]
 * +- Child
 *
 * After rewrite:
 * Project [a, 'a + 1]
 * +- Project [child output, age AS a]
 *    +- Child
 *
 * For Aggregate TODO.
 */
object ResolveLateralColumnAlias extends Rule[LogicalPlan] {
  private case class AliasEntry(alias: Alias, index: Int)
  def resolver: Resolver = conf.resolver

  private def rewriteLateralColumnAlias(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUpWithPruning(
      _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE), ruleId) {
      case p @ Project(projectList, child) if p.childrenResolved
        && !Analyzer.containsStar(projectList)
        && projectList.exists(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) =>

        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        def insertIntoAliasMap(a: Alias, idx: Int): Unit = {
          val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
          aliasMap += (a.name -> (prevAliases :+ AliasEntry(a, idx)))
        }

        val referencedAliases = collection.mutable.Set.empty[AliasEntry]
        def resolveLCA(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) {
            case o: OuterReference
              if aliasMap.contains(o.nameParts.map(_.head).getOrElse(o.name)) =>
              val name = o.nameParts.map(_.head).getOrElse(o.name)
              val aliases = aliasMap.get(name).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(o.name, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  // Only resolved alias can be the lateral column alias
                  referencedAliases += aliases.head
                  o.nameParts.map(UnresolvedAttribute(_)).getOrElse(UnresolvedAttribute(o.name))
                case _ =>
                  o
              }
            case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
              Analyzer.resolveExpressionByPlanChildren(u, p, resolver)
                .isInstanceOf[UnresolvedAttribute] =>
              val aliases = aliasMap.get(u.nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(u.name, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  // Only resolved alias can be the lateral column alias
                  referencedAliases += aliases.head
                case _ =>
              }
              u
          }.asInstanceOf[NamedExpression]
        }
        val newProjectList = projectList.zipWithIndex.map {
          case (a: Alias, idx) =>
            // Add all alias to the aliasMap. But note only resolved alias can be LCA and pushed
            // down. Unresolved alias is added to the map to perform the ambiguous name check.
            // If there is a chain of LCA, for example, SELECT 1 AS a, 'a + 1 AS b, 'b + 1 AS c,
            // because only resolved alias can be LCA, in the first round the rule application,
            // only 1 AS a is pushed down, even though 1 AS a, 'a + 1 AS b and 'b + 1 AS c are
            // all added to the aliasMap. On the second round, when 'a + 1 AS b is resolved,
            // it is pushed down.
            val lcaResolved = resolveLCA(a).asInstanceOf[Alias]
            insertIntoAliasMap(lcaResolved, idx)
            lcaResolved
          case (e, _) =>
            resolveLCA(e)
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
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      plan
    } else {
      rewriteLateralColumnAlias(plan)
    }
  }
}
