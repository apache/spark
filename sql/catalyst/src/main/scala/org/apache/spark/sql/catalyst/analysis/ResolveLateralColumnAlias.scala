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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, LateralColumnAliasReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list.
 * - in Project, inserting a new Project node below with the referenced alias so that it can be
 *   resolved by other rules
 * - in Aggregate, inserting the Project node above and fall back to the resolution of Project
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
 */
object ResolveLateralColumnAlias extends Rule[LogicalPlan] {
  private case class AliasEntry(alias: Alias, index: Int)
  def resolver: Resolver = conf.resolver

  def unwrapLCAReference(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    exprs.map { expr =>
      expr.transformWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
        case l: LateralColumnAliasReference =>
          UnresolvedAttribute(l.nameParts)
      }.asInstanceOf[NamedExpression]
    }
  }
  private def rewriteLateralColumnAlias(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUpWithPruning(
      _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, LATERAL_COLUMN_ALIAS_REFERENCE), ruleId) {
      case p @ Project(projectList, child) if p.childrenResolved
        && !Analyzer.containsStar(projectList)
        && projectList.exists(_.containsPattern(UNRESOLVED_ATTRIBUTE)) =>

        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        def insertIntoAliasMap(a: Alias, idx: Int): Unit = {
          val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
          aliasMap += (a.name -> (prevAliases :+ AliasEntry(a, idx)))
        }
        def lookUpLCA(e: Expression): Seq[AliasEntry] = {
          var matchedLCA: Seq[AliasEntry] = Seq.empty[AliasEntry]
          e.transformWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE)) {
            case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
              Analyzer.resolveExpressionByPlanChildren(u, p, resolver)
                .isInstanceOf[UnresolvedAttribute] =>
              val aliases = aliasMap.get(u.nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAliasError(u.name, n)
                case _ =>
                  val referencedAlias = aliases.head
                  // Only resolved alias can be the lateral column alias
                  if (referencedAlias.alias.resolved) {
                    matchedLCA :+= referencedAlias
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

      // wrap LCA
      // Implementation notes:
      // In Aggregate, introducing and wrapping this resolved leaf expression
      // LateralColumnAliasReference is especially needed because it needs an accurate condition to
      // trigger adding a Project above and extracting aggregate functions or grouping expressions.
      // Such operation can only be done once. With this LateralColumnAliasReference, the condition
      // can simply be when the whole Aggregate is resolved. Otherwise, it can't really tell if
      // all aggregate functions are created and resolved, because the lateral alias reference
      // itself is unresolved.
      case agg @ Aggregate(groupingExpressions, aggregateExpressions, child)
        if agg.childrenResolved
          && !Analyzer.containsStar(aggregateExpressions)
          && aggregateExpressions.exists(_.containsPattern(UNRESOLVED_ATTRIBUTE)) =>

        var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
        def insertIntoAliasMap(a: Alias, idx: Int): Unit = {
          val prevAliases = aliasMap.getOrElse(a.name, Seq.empty[AliasEntry])
          aliasMap += (a.name -> (prevAliases :+ AliasEntry(a, idx)))
        }
        def wrapLCAReference(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE)) {
            case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
              Analyzer.resolveExpressionByPlanChildren(u, agg, resolver)
                .isInstanceOf[UnresolvedAttribute] =>
              val aliases = aliasMap.get(u.nameParts.head).get
              aliases.size match {
                case n if n > 1 =>
                  throw QueryCompilationErrors.ambiguousLateralColumnAliasError(u.name, n)
                case n if n == 1 && aliases.head.alias.resolved =>
                  LateralColumnAliasReference(aliases.head.alias, u.nameParts)
                case _ =>
                  u
              }
          }.asInstanceOf[NamedExpression]
        }

        val newAggExprs = aggregateExpressions.zipWithIndex.map {
          case (a: Alias, idx) =>
            val LCAResolved = wrapLCAReference(a).asInstanceOf[Alias]
            // insert the LCA-resolved alias instead of the unresolved one into map. If it is
            // resolved, it can be referenced as LCA by later expressions
            insertIntoAliasMap(LCAResolved, idx)
            LCAResolved
          case (e, _) =>
            wrapLCAReference(e)
        }
        agg.copy(aggregateExpressions = newAggExprs)

      case agg @ Aggregate(groupingExpressions, aggregateExpressions, _)
        if agg.resolved
          && aggregateExpressions.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>

        val newAggExprs = collection.mutable.Set.empty[NamedExpression]
        val projectExprs = aggregateExpressions.map { exp =>
          exp.transformDown {
            case aggExpr: AggregateExpression =>
              // TODO (improvement) dedup
              // Doesn't support referencing a lateral alias in aggregate function
              if (aggExpr.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                aggExpr.collectFirst {
                  case lcaRef: LateralColumnAliasReference =>
                    throw QueryCompilationErrors.LateralColumnAliasInAggFuncUnsupportedError(
                      lcaRef.nameParts, aggExpr)
                }
              }
              val ne = ResolveAliases.assignAliases(Seq(UnresolvedAlias(aggExpr))).head
              newAggExprs += ne
              ne.toAttribute
            case e if groupingExpressions.exists(_.semanticEquals(e)) =>
              // TODO (improvement) dedup
              val alias = ResolveAliases.assignAliases(Seq(UnresolvedAlias(e))).head
              newAggExprs += alias
              alias.toAttribute
          }.asInstanceOf[NamedExpression]
        }
        val unwrappedAggExprs = unwrapLCAReference(newAggExprs.toSeq)
        val unwrappedProjectExprs = unwrapLCAReference(projectExprs)
        Project(
          projectList = unwrappedProjectExprs,
          child = agg.copy(aggregateExpressions = unwrappedAggExprs)
        )
        // TODO: think about a corner case, when the Alias passed to LateralColumnAliasReference
        //  contains a LateralColumnAliasReference. Is it safe to do a.toAttribute when resolving
        //  the LateralColumnAliasReference?
        // TODO withOrigin?
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
