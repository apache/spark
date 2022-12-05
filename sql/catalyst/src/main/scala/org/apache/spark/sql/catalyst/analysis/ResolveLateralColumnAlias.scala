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

import org.apache.spark.sql.catalyst.expressions.{Alias, LateralColumnAliasReference, NamedExpression, OuterReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, OUTER_REFERENCE, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list.
 * Plan-wise it handles two types of operators: Project and Aggregate.
 * - in Project, pushing down the referenced lateral alias into a newly created Project, resolve
 *   the attributes referencing these aliases
 * - in Aggregate TODO.
 *
 * The whole process is generally divided into two phases:
 * 1) recognize lateral alias, wrap the attributes referencing them with
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
 *
 * The name resolution priority:
 * local table column > local lateral column alias > outer reference
 *
 * Because lateral column alias has higher resolution priority than outer reference, it will try
 * to resolve an [[OuterReference]] using lateral column alias in phase 1, similar as an
 * [[UnresolvedAttribute]]. If success, it strips [[OuterReference]] and also wraps it with
 * [[LateralColumnAliasReference]].
 */
object ResolveLateralColumnAlias extends Rule[LogicalPlan] {
  private case class AliasEntry(alias: Alias, index: Int)
  private def insertIntoAliasMap(
      a: Alias,
      idx: Int,
      aliasMap: CaseInsensitiveMap[Seq[AliasEntry]]): CaseInsensitiveMap[Seq[AliasEntry]] = {
    val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
    aliasMap + (a.name -> (prevAliases :+ AliasEntry(a, idx)))
  }

  def resolver: Resolver = conf.resolver

  private def rewriteLateralColumnAlias(plan: LogicalPlan): LogicalPlan = {
    // phase 1: wrap
    val rewrittenPlan = plan.resolveOperatorsUpWithPruning(
      _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE), ruleId) {
      case p @ Project(projectList, child) if p.childrenResolved
        && !Analyzer.containsStar(projectList)
        && projectList.exists(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) =>

        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        def wrapLCAReference(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) {
            case o: OuterReference
              if aliasMap.contains(o.nameParts.map(_.head).getOrElse(o.name)) =>
              val nameParts = o.nameParts.getOrElse(Seq(o.name))
              val aliases = aliasMap.get(nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(nameParts, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  // Only resolved alias can be the lateral column alias
                  // TODO We need to resolve to the nested field type, e.g. for query
                  //  SELECT named_struct() AS foo, foo.a, we can't say this foo.a is the
                  //  LateralColumnAliasReference(foo, foo.a). Otherwise, the type can be mismatched
                  LateralColumnAliasReference(aliases.head.alias, nameParts)
                case _ => o
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
                  // TODO similar problem
                  LateralColumnAliasReference(aliases.head.alias, u.nameParts)
                case _ => u
              }
          }.asInstanceOf[NamedExpression]
        }
        val newProjectList = projectList.zipWithIndex.map {
          case (a: Alias, idx) =>
            val lcaWrapped = wrapLCAReference(a).asInstanceOf[Alias]
            // Insert the LCA-resolved alias instead of the unresolved one into map. If it is
            // resolved, it can be referenced as LCA by later expressions (chaining).
            // Unresolved Alias is also added to the map to perform ambiguous name check, but only
            // resolved alias can be LCA
            aliasMap = insertIntoAliasMap(lcaWrapped, idx, aliasMap)
            lcaWrapped
          case (e, _) =>
            wrapLCAReference(e)
        }
        p.copy(projectList = newProjectList)
    }

    // phase 2: unwrap
    rewrittenPlan.resolveOperatorsUpWithPruning(
      _.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE), UnknownRuleId) {
      case p @ Project(projectList, child) if p.resolved
        && projectList.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        // build the map again in case the project list changes and index goes off
        // TODO one risk: is there any rule that strips off the Alias? that the LCA is resolved
        //  in the beginning, but when it comes to push down, it really can't find the matching one?
        //  Restore back to UnresolvedAttribute
        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        val referencedAliases = collection.mutable.Set.empty[AliasEntry]
        def unwrapLCAReference(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsAnyPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
            case lcaRef: LateralColumnAliasReference if aliasMap.contains(lcaRef.nameParts.head) =>
              val aliasEntry = aliasMap.get(lcaRef.nameParts.head).get.head
              if (!aliasEntry.alias.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                // If there is no chaining, push down the alias and resolve the attribute by
                // constructing a dummy plan
                referencedAliases += aliasEntry
                // Implementation notes (to-delete):
                // this is a design decision whether to restore the UnresolvedAttribute, or
                // directly resolve by constructing a plan and using resolveExpressionByPlanChildren
                Analyzer.resolveExpressionByPlanOutput(
                  expr = UnresolvedAttribute(lcaRef.nameParts),
                  plan = Project(Seq(aliasEntry.alias), OneRowRelation()),
                  resolver = resolver,
                  throws = false
                )
              } else {
                // If there is chaining, don't resolve and save to future rounds
                lcaRef
              }
            case lcaRef: LateralColumnAliasReference if !aliasMap.contains(lcaRef.nameParts.head) =>
              // It shouldn't happen. Restore to unresolved attribute to be safe.
              UnresolvedAttribute(lcaRef.name)
          }.asInstanceOf[NamedExpression]
        }

        val newProjectList = projectList.zipWithIndex.map {
          case (a: Alias, idx) =>
            val lcaResolved = unwrapLCAReference(a)
            // Insert the original alias instead of rewritten one to detect chained LCA
            aliasMap = insertIntoAliasMap(a, idx, aliasMap)
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
