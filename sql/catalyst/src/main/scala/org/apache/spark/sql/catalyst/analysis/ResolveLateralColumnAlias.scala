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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, LateralColumnAliasReference, NamedExpression, OuterReference}
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
  def resolver: Resolver = conf.resolver

  private case class AliasEntry(alias: Alias, index: Int)
  private def insertIntoAliasMap(
      a: Alias,
      idx: Int,
      aliasMap: CaseInsensitiveMap[Seq[AliasEntry]]): CaseInsensitiveMap[Seq[AliasEntry]] = {
    val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
    aliasMap + (a.name -> (prevAliases :+ AliasEntry(a, idx)))
  }

  /**
   * Use the given lateral alias to resolve the unresolved attribute with the name parts.
   *
   * Construct a dummy plan with the given lateral alias as project list, use the output of the
   * plan to resolve.
   * @return The resolved [[LateralColumnAliasReference]] if succeeds. None if fails to resolve.
   */
  private def resolveByLateralAlias(
      nameParts: Seq[String], lateralAlias: Alias): Option[LateralColumnAliasReference] = {
    // TODO question: everytime it resolves the extract field it generates a new exprId.
    //  Does it matter?
    val resolvedAttr = Analyzer.resolveExpressionByPlanOutput(
      expr = UnresolvedAttribute(nameParts),
      plan = Project(Seq(lateralAlias), OneRowRelation()),
      resolver = resolver,
      throws = false
    ).asInstanceOf[NamedExpression]
    if (resolvedAttr.resolved) {
      Some(LateralColumnAliasReference(resolvedAttr, nameParts, lateralAlias.toAttribute))
    } else {
      None
    }
  }

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
            case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
              Analyzer.resolveExpressionByPlanChildren(u, p, resolver)
                .isInstanceOf[UnresolvedAttribute] =>
              val aliases = aliasMap.get(u.nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(u.name, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  // Only resolved alias can be the lateral column alias
                  // The lateral alias can be a struct and have nested field, need to construct
                  // a dummy plan to resolve the expression
                  resolveByLateralAlias(u.nameParts, aliases.head.alias).getOrElse(u)
                case _ => u
              }
            case o: OuterReference
              if aliasMap.contains(o.nameParts.map(_.head).getOrElse(o.name)) =>
              // handle OuterReference exactly same as UnresolvedAttribute
              val nameParts = o.nameParts.getOrElse(Seq(o.name))
              val aliases = aliasMap.get(nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAlias(nameParts, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  resolveByLateralAlias(nameParts, aliases.head.alias).getOrElse(o)
                case _ => o
              }
          }.asInstanceOf[NamedExpression]
        }
        val newProjectList = projectList.zipWithIndex.map {
          case (a: Alias, idx) =>
            val lcaWrapped = wrapLCAReference(a).asInstanceOf[Alias]
            // Insert the LCA-resolved alias instead of the unresolved one into map. If it is
            // resolved, it can be referenced as LCA by later expressions (chaining).
            // Unresolved Alias is also added to the map to perform ambiguous name check, but only
            // resolved alias can be LCA.
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
        var aliasMap = Map[Attribute, AliasEntry]()
        val referencedAliases = collection.mutable.Set.empty[AliasEntry]
        def unwrapLCAReference(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
            case lcaRef: LateralColumnAliasReference if aliasMap.contains(lcaRef.a) =>
              val aliasEntry = aliasMap(lcaRef.a)
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
