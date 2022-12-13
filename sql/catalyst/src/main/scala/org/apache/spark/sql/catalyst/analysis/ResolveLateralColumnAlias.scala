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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, LateralColumnAliasReference, NamedExpression, OuterReference}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, OUTER_REFERENCE, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * The first phase to resolve lateral column alias.
 *
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list.
 * Plan-wise, it handles two types of operators: Project and Aggregate.
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
object WrapLateralColumnAliasReference extends Rule[LogicalPlan] {
  import ResolveLateralColumnAliasReference.AliasEntry

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
    val resolvedAttr = SimpleAnalyzer.resolveExpressionByPlanOutput(
      expr = UnresolvedAttribute(nameParts),
      plan = LocalRelation(Seq(lateralAlias.toAttribute)),
      throws = false
    ).asInstanceOf[NamedExpression]
    if (resolvedAttr.resolved) {
      Some(LateralColumnAliasReference(resolvedAttr, nameParts, lateralAlias.toAttribute))
    } else {
      None
    }
  }

  /**
   * Recognize all the attributes in the given expression that reference lateral column aliases
   * by looking up the alias map. Resolve these attributes and replace by wrapping with
   * [[LateralColumnAliasReference]].
   *
   * @param currentPlan Because lateral alias has lower resolution priority than table columns,
   *                    the current plan is needed to first try resolving the attribute by its
   *                    children
   */
  private def wrapLCARef(
      e: NamedExpression,
      currentPlan: LogicalPlan,
      aliasMap: CaseInsensitiveMap[Seq[AliasEntry]]): NamedExpression = {
    e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) {
      case u: UnresolvedAttribute if aliasMap.contains(u.nameParts.head) &&
        SimpleAnalyzer.resolveExpressionByPlanChildren(
          u, currentPlan).isInstanceOf[UnresolvedAttribute] =>
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
        if aliasMap.contains(
          o.getTagValue(ResolveLateralColumnAliasReference.NAME_PARTS_FROM_UNRESOLVED_ATTR)
            .map(_.head)
            .getOrElse(o.name)) =>
        // handle OuterReference exactly same as UnresolvedAttribute
        val nameParts = o
          .getTagValue(ResolveLateralColumnAliasReference.NAME_PARTS_FROM_UNRESOLVED_ATTR)
          .getOrElse(Seq(o.name))
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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      plan
    } else {
      plan.resolveOperatorsUpWithPruning(
        _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE), ruleId) {
        case p @ Project(projectList, _) if p.childrenResolved
          && !SimpleAnalyzer.ResolveReferences.containsStar(projectList)
          && projectList.exists(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, OUTER_REFERENCE)) =>
          var aliasMap = CaseInsensitiveMap(Map[String, Seq[AliasEntry]]())
          val newProjectList = projectList.zipWithIndex.map {
            case (a: Alias, idx) =>
              val lcaWrapped = wrapLCARef(a, p, aliasMap).asInstanceOf[Alias]
              // Insert the LCA-resolved alias instead of the unresolved one into map. If it is
              // resolved, it can be referenced as LCA by later expressions (chaining).
              // Unresolved Alias is also added to the map to perform ambiguous name check, but
              // only resolved alias can be LCA.
              aliasMap = insertIntoAliasMap(lcaWrapped, idx, aliasMap)
              lcaWrapped
            case (e, _) =>
              wrapLCARef(e, p, aliasMap)
          }
          p.copy(projectList = newProjectList)
      }
    }
  }
}

/**
 * This rule is the second phase to resolve lateral column alias.
 * See comments in [[WrapLateralColumnAliasReference]] for more detailed explanation.
 */
object ResolveLateralColumnAliasReference extends Rule[LogicalPlan] {
  case class AliasEntry(alias: Alias, index: Int)

  /**
   * A tag to store the nameParts from the original unresolved attribute.
   * It is set for [[OuterReference]], used in the current rule to convert [[OuterReference]] back
   * to [[LateralColumnAliasReference]].
   */
  val NAME_PARTS_FROM_UNRESOLVED_ATTR: TreeNodeTag[Seq[String]] =
    TreeNodeTag[Seq[String]]("name_parts_from_unresolved_attr")

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
      }
    }
  }
}
