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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, ConditionalExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertOnlyMerge, Join, LogicalPlan, MergeRows, Project}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Insert, Keep}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND

/**
 * Replaces a single insert instruction with a projection and source filter after constant folding.
 */
object OptimizeInsertOnlyMerge extends Rule[LogicalPlan] with AliasHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case merge @ InsertOnlyMerge(_, rows @ MergeRows(TrueLiteral, FalseLiteral, Nil,
        Seq(instruction @ Keep(Insert, condition, values)), Nil, false, _, child), _, _)
        if !hasUnfoldedConstants(instruction) =>
      val filtered = condition match {
        case TrueLiteral => child
        case _ => filterSource(child, condition).getOrElse(Filter(condition, child))
      }
      val projectList = values.zip(rows.output).map { case (value, attr) =>
        Alias(value, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier,
          explicitMetadata = Some(attr.metadata))
      }
      merge.copy(query = Project(projectList, filtered))
  }

  private def hasUnfoldedConstants(expr: Expression): Boolean = expr match {
    case _: Literal => false
    // These failures stay protected by the expression after MergeRows is removed.
    case c: ConditionalExpression if !c.foldable => false
    case e => e.foldable || e.children.exists(hasUnfoldedConstants)
  }

  private def filterSource(plan: LogicalPlan, condition: Expression): Option[LogicalPlan] = {
    plan match {
      case join @ Join(_, _, LeftAnti, _, _) =>
        Some(join.copy(left = Filter(condition, join.left)))
      // Column pruning may put a projection between MergeRows and the anti join.
      case project: Project if project.projectList.forall(_.deterministic) =>
        val rewritten = replaceAlias(condition, getAliasMap(project))
        filterSource(project.child, rewritten).map(child => project.copy(child = child))
      case _ => None
    }
  }
}
