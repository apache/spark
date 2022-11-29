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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list,
 * - in Project inserting a new Project node with the referenced alias so that it can be
 *   resolved by other rules
 * - in Aggregate TODO.
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
    plan.resolveOperatorsUpWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE), ruleId) {
      case p @ Project(projectList, child) if p.childrenResolved
        && !Analyzer.containsStar(projectList)
        && projectList.exists(_.containsPattern(UNRESOLVED_ATTRIBUTE)) =>

        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        def insertIntoAliasMap(a: Alias, idx: Int): Unit = {
          val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
          aliasMap += (a.name -> (prevAliases :+ AliasEntry(a, idx)))
        }
        def lookUpLCA(e: Expression): Option[AliasEntry] = {
          var matchedLCA: Option[AliasEntry] = None
          e.transformWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE)) {
            case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
              Analyzer.resolveExpressionByPlanChildren(u, p, resolver)
                .isInstanceOf[UnresolvedAttribute] =>
              val aliases = aliasMap.get(u.nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(u.name, n)
                case _ =>
                  val referencedAlias = aliases.head
                  // Only resolved alias can be the lateral column alias
                  if (referencedAlias.alias.resolved) {
                    matchedLCA = Some(referencedAlias)
                  }
              }
              u
          }
          matchedLCA
        }

        val referencedAliases = projectList.zipWithIndex.flatMap {
          case (a: Alias, idx) =>
            // Add all alias to the aliasMap. But note only resolved alias can be LCA and pushed
            // down. Unresolved alias is added to the map to perform the ambiguous name check.
            // If there is a chain of LCA, for example, SELECT 1 AS a, 'a + 1 AS b, 'b + 1 AS c,
            // because only resolved alias can be LCA, in the first round the rule application,
            // only 1 AS a is pushed down, even though 1 AS a, 'a + 1 AS b and 'b + 1 AS c are
            // all added to the aliasMap. On the second round, when 'a + 1 AS b is resolved,
            // it is pushed down.
            val matchedLCA = lookUpLCA(a)
            insertIntoAliasMap(a, idx)
            matchedLCA
          case (e, _) =>
            lookUpLCA(e)
        }.toSet

        if (referencedAliases.isEmpty) {
          p
        } else {
          val outerProjectList = collection.mutable.Seq(projectList: _*)
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
