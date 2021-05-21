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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, AttributeSet, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess

object DeduplicateRelations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    renewDuplicatedRelations(Nil, plan)._1.resolveOperatorsUpWithPruning(
      AlwaysProcess.fn, ruleId) {
      case p: LogicalPlan if !p.childrenResolved => p
      // To resolve duplicate expression IDs for Join.
      case j @ Join(left, right, _, _, _) if !j.duplicateResolved =>
        j.copy(right = dedupRight(left, right))
      // intersect/except will be rewritten to join at the beginning of optimizer. Here we need to
      // deduplicate the right side plan, so that we won't produce an invalid self-join later.
      case i @ Intersect(left, right, _) if !i.duplicateResolved =>
        i.copy(right = dedupRight(left, right))
      case e @ Except(left, right, _) if !e.duplicateResolved =>
        e.copy(right = dedupRight(left, right))
      // Only after we finish by-name resolution for Union
      case u: Union if !u.byName && !u.duplicateResolved =>
        // Use projection-based de-duplication for Union to avoid breaking the checkpoint sharing
        // feature in streaming.
        val newChildren = u.children.foldRight(Seq.empty[LogicalPlan]) { (head, tail) =>
          head +: tail.map {
            case child if head.outputSet.intersect(child.outputSet).isEmpty =>
              child
            case child =>
              val projectList = child.output.map { attr =>
                Alias(attr, attr.name)()
              }
              Project(projectList, child)
          }
        }
        u.copy(children = newChildren)
      case m @ MergeIntoTable(targetTable, sourceTable, _, _, _) if !m.duplicateResolved =>
        m.copy(sourceTable = dedupRight(targetTable, sourceTable))
    }
  }

  /**
   * Deduplicate any duplicated relations of a LogicalPlan
   * @param existingRelations the known unique relations for a LogicalPlan
   * @param plan the LogicalPlan that requires the deduplication
   * @return (the new LogicalPlan which already deduplicate all duplicated relations (if any),
   *         all relations of the new LogicalPlan )
   */
  private def renewDuplicatedRelations(
      existingRelations: Seq[MultiInstanceRelation],
      plan: LogicalPlan): (LogicalPlan, Seq[MultiInstanceRelation]) = plan match {
    case p: LogicalPlan if p.isStreaming => (plan, Nil)

    case m: MultiInstanceRelation =>
      if (isDuplicated(existingRelations, m)) {
        val newNode = m.newInstance()
        newNode.copyTagsFrom(m)
        (newNode, Nil)
      } else {
        (m, Seq(m))
      }

    case plan: LogicalPlan =>
      val relations = ArrayBuffer.empty[MultiInstanceRelation]
      val newPlan = if (plan.children.nonEmpty) {
        val newChildren = ArrayBuffer.empty[LogicalPlan]
        for (c <- plan.children) {
          val (renewed, collected) = renewDuplicatedRelations(existingRelations ++ relations, c)
          newChildren += renewed
          relations ++= collected
        }

        if (plan.childrenResolved) {
          val attrMap = AttributeMap(
            plan
              .children
              .flatMap(_.output).zip(newChildren.flatMap(_.output))
              .filter { case (a1, a2) => a1.exprId != a2.exprId }
          )
          plan.withNewChildren(newChildren.toSeq).rewriteAttrs(attrMap)
        } else {
          plan.withNewChildren(newChildren.toSeq)
        }
      } else {
        plan
      }

      val planWithNewSubquery = newPlan.transformExpressions {
        case subquery: SubqueryExpression =>
          val (renewed, collected) = renewDuplicatedRelations(
            existingRelations ++ relations, subquery.plan)
          relations ++= collected
          subquery.withNewPlan(renewed)
      }
      (planWithNewSubquery, relations.toSeq)
  }

  private def isDuplicated(
      existingRelations: Seq[MultiInstanceRelation],
      relation: MultiInstanceRelation): Boolean = {
    existingRelations.exists { er =>
      er.asInstanceOf[LogicalPlan].outputSet
        .intersect(relation.asInstanceOf[LogicalPlan].outputSet).nonEmpty
    }
  }

  /**
   * Generate a new logical plan for the right child with different expression IDs
   * for all conflicting attributes.
   */
  private def dedupRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet.intersect(right.outputSet)
    logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} " +
      s"between $left and $right")

    /**
     * For LogicalPlan likes MultiInstanceRelation, Project, Aggregate, etc, whose output doesn't
     * inherit directly from its children, we could just stop collect on it. Because we could
     * always replace all the lower conflict attributes with the new attributes from the new
     * plan. Theoretically, we should do recursively collect for Generate and Window but we leave
     * it to the next batch to reduce possible overhead because this should be a corner case.
     */
    def collectConflictPlans(plan: LogicalPlan): Seq[(LogicalPlan, LogicalPlan)] = plan match {
      // Handle base relations that might appear more than once.
      case oldVersion: MultiInstanceRelation
          if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        val newVersion = oldVersion.newInstance()
        newVersion.copyTagsFrom(oldVersion)
        Seq((oldVersion, newVersion))

      case oldVersion: SerializeFromObject
          if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(
          serializer = oldVersion.serializer.map(_.newInstance()))))

      // Handle projects that create conflicting aliases.
      case oldVersion @ Project(projectList, _)
          if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(projectList = newAliases(projectList))))

      // We don't need to search child plan recursively if the projectList of a Project
      // is only composed of Alias and doesn't contain any conflicting attributes.
      // Because, even if the child plan has some conflicting attributes, the attributes
      // will be aliased to non-conflicting attributes by the Project at the end.
      case _ @ Project(projectList, _)
        if findAliases(projectList).size == projectList.size =>
        Nil

      case oldVersion @ Aggregate(_, aggregateExpressions, _)
          if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(
          aggregateExpressions = newAliases(aggregateExpressions))))

      // We don't search the child plan recursively for the same reason as the above Project.
      case _ @ Aggregate(_, aggregateExpressions, _)
        if findAliases(aggregateExpressions).size == aggregateExpressions.size =>
        Nil

      case oldVersion @ FlatMapGroupsInPandas(_, _, output, _)
          if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(output = output.map(_.newInstance()))))

      case oldVersion @ FlatMapCoGroupsInPandas(_, _, _, output, _, _)
        if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(output = output.map(_.newInstance()))))

      case oldVersion @ MapInPandas(_, output, _)
        if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(output = output.map(_.newInstance()))))

      case oldVersion: Generate
          if oldVersion.producedAttributes.intersect(conflictingAttributes).nonEmpty =>
        val newOutput = oldVersion.generatorOutput.map(_.newInstance())
        Seq((oldVersion, oldVersion.copy(generatorOutput = newOutput)))

      case oldVersion: Expand
          if oldVersion.producedAttributes.intersect(conflictingAttributes).nonEmpty =>
        val producedAttributes = oldVersion.producedAttributes
        val newOutput = oldVersion.output.map { attr =>
          if (producedAttributes.contains(attr)) {
            attr.newInstance()
          } else {
            attr
          }
        }
        Seq((oldVersion, oldVersion.copy(output = newOutput)))

      case oldVersion @ Window(windowExpressions, _, _, child)
          if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
          .nonEmpty =>
        Seq((oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions))))

      case oldVersion @ ScriptTransformation(_, output, _, _)
          if AttributeSet(output).intersect(conflictingAttributes).nonEmpty =>
        Seq((oldVersion, oldVersion.copy(output = output.map(_.newInstance()))))

      case _ => plan.children.flatMap(collectConflictPlans)
    }

    val conflictPlans = collectConflictPlans(right)

    /*
     * Note that it's possible `conflictPlans` can be empty which implies that there
     * is a logical plan node that produces new references that this rule cannot handle.
     * When that is the case, there must be another rule that resolves these conflicts.
     * Otherwise, the analysis will fail.
     */
    if (conflictPlans.isEmpty) {
      right
    } else {
      val planMapping = conflictPlans.toMap
      right.transformUpWithNewOutput {
        case oldPlan =>
          val newPlanOpt = planMapping.get(oldPlan)
          newPlanOpt.map { newPlan =>
            newPlan -> oldPlan.output.zip(newPlan.output)
          }.getOrElse(oldPlan -> Nil)
      }
    }
  }

  private def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.map {
      case a: Alias => Alias(a.child, a.name)()
      case other => other
    }
  }

  private def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
    AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
  }
}
