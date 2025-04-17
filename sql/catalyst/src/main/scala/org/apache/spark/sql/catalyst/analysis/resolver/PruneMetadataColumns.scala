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

import java.util.HashSet

import org.apache.spark.sql.catalyst.expressions.{ExprId, NamedExpression, OuterReference}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util._

/**
 * This is a special rule for single-pass resolver that performs a single downwards traversal in
 * order to prune away unnecessary metadata columns. This is necessary because fixed-point is
 * looking into the operator tree in order to determine whether it is necessary to add metadata
 * columns. In single-pass, we always add metadata columns during the main traversal and this rule
 * performs the cleanup of those columns that are unnecessary. Important thing to note here is that
 * by "unnecessary" columns we are not referring to the ones that are not needed in upper operators
 * for correct result, but to the columns that have been added by single-pass resolver but are not
 * present in the fixed-point plan.
 */
object PruneMetadataColumns extends Rule[LogicalPlan] {

  /**
   * Entry point for [[PruneMetadataColumns]] rule.
   */
  override def apply(plan: LogicalPlan): LogicalPlan =
    pruneMetadataColumns(plan = plan, neededAttributes = new HashSet[ExprId])

  /**
   * This method recursively prunes away unnecessary metadata columns, going from top to bottom.
   *
   * @param plan Operator for which we are pruning columns.
   * @param neededAttributes A set of [[ExprId]]s that is required by operators above [[plan]].
   *
   * We distinguish three separate cases here, based on the type of operator node:
   *  - For [[Aggregate]] nodes we only need to propagate aggregate expressions as needed
   *  attributes to the lower operators. This is because in single-pass, we are not adding metadata
   *   columns to [[Aggregate]] operators.
   *  - For [[Project]] nodes we prune away all metadata columns that are either not required by
   *  operators above or they are duplicated in the project list.
   *  - For all other operators we collect references, add them to [[neededAttributes]] and
   *  recursively call [[pruneMetadataColumns]] for children.
   */
  private def pruneMetadataColumns(
      plan: LogicalPlan,
      neededAttributes: HashSet[ExprId]): LogicalPlan = plan match {
    case aggregate: Aggregate =>
      withNewChildrenPrunedByNeededAttributes(
        aggregate,
        aggregate.aggregateExpressions
      )
    case project: Project =>
      pruneMetadataColumnsInProject(project, neededAttributes)
    case other =>
      pruneMetadataColumnsGenerically(other, neededAttributes)
  }

  /**
   * Prune unnecessary columns for a [[Project]] node and recursively do it for its children. While
   * pruning we preserve all non-qualified-access-only columns as well as any columns that are
   * needed in the operators above, but without duplicating them in the project list. This behavior
   * is consistent with fixed-point's behavior when
   * [[SQLConf.ONLY_NECESSARY_AND_UNIQUE_METADATA_COLUMNS]] is true. We don't support legacy
   * behavior in single-pass.
   *
   * IMPORTANT NOTE: In this case we only prune away only the qualified access only columns instead
   * of all metadata columns. This is because we can have metadata columns from sources other than
   * hidden output and additionally, when a column is resolved from any source, its qualified only
   * flag is removed (see more in [[AttributeSeq]]). Therefore, qualified access only columns that
   * appear in the project list must have come from artificial appending, and we should potentially
   * prune them.
   */
  private def pruneMetadataColumnsInProject(project: Project, neededAttributes: HashSet[ExprId]) = {
    val existingExprIds = new HashSet[ExprId]
    val newProjectList = if (!neededAttributes.isEmpty) {
      project.projectList.collect {
        case namedExpression: NamedExpression if !namedExpression.toAttribute.qualifiedAccessOnly =>
          existingExprIds.add(namedExpression.exprId)
          namedExpression
        case namedExpression: NamedExpression
            if namedExpression.toAttribute.qualifiedAccessOnly && neededAttributes.contains(
              namedExpression.exprId
            ) && !existingExprIds.contains(namedExpression.exprId) =>
          existingExprIds.add(namedExpression.exprId)
          namedExpression
      }
    } else {
      project.projectList
    }
    val projectWithNewChildren =
      withNewChildrenPrunedByNeededAttributes(project, newProjectList).asInstanceOf[Project]
    val newProject = CurrentOrigin.withOrigin(projectWithNewChildren.origin) {
      projectWithNewChildren.copy(projectList = newProjectList)
    }
    newProject.copyTagsFrom(project)
    newProject
  }

  /**
   * Prune unnecessary metadata column in operators that are not [[Project]] or [[Aggregate]].
   */
  private def pruneMetadataColumnsGenerically(
      operator: LogicalPlan,
      neededAttributes: HashSet[ExprId]) = {
    operator.references.foreach(attr => neededAttributes.add(attr.exprId))
    val newChildren = operator.children.map { child =>
      pruneMetadataColumns(child, new HashSet(neededAttributes))
    }
    operator.withNewChildren(newChildren)
  }

  private def withNewChildrenPrunedByNeededAttributes(
      plan: LogicalPlan,
      newNeededAttributes: Seq[NamedExpression]): LogicalPlan = {
    val neededAttributes = new HashSet[ExprId]
    newNeededAttributes.foreach {
      case _: OuterReference =>
      case other: NamedExpression =>
        other.foreach {
          case namedExpression: NamedExpression =>
            neededAttributes.add(namedExpression.exprId)
          case _ =>
        }
    }
    plan.withNewChildren(
      plan.children.map(
        child => pruneMetadataColumns(child, neededAttributes)
      )
    )
  }
}
