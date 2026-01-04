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

import org.apache.spark.sql.catalyst.analysis.ResolvedInlineTable
import org.apache.spark.sql.catalyst.expressions.{Alias, LateralSubquery, SubExprUtils}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rewrites correlated [[ResolvedInlineTable]] into [[Project]] + [[OneRowRelation]] + [[Union]]
 * to enable decorrelation by the existing subquery decorrelation framework.
 *
 * A correlated inline table contains [[OuterReference]] expressions that refer to columns from
 * an outer query. These cannot be evaluated at planning time and must be decorrelated.
 *
 * The transformation converts each row into a [[Project]] over [[OneRowRelation]], then unions
 * them together. This structure allows [[PullupCorrelatedPredicates]] to handle the correlation.
 *
 * Example:
 * {{{
 *   Input:
 *     ResolvedInlineTable(
 *       rows = [[OuterReference(t.c1)], [OuterReference(t.c2)]],
 *       output = [col1]
 *     )
 *
 *   Output:
 *     Union(
 *       Project([OuterReference(t.c1) AS col1], OneRowRelation),
 *       Project([OuterReference(t.c2) AS col1], OneRowRelation)
 *     )
 * }}}
 *
 * This rule runs before [[PullupCorrelatedPredicates]] in the optimizer.
 */
object RewriteCorrelatedInlineTable extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Transform both plan nodes and expressions containing plans (e.g., LateralSubquery)
    plan.transformUpWithPruning(
      _.containsPattern(org.apache.spark.sql.catalyst.trees.TreePattern.INLINE_TABLE_EVAL)) {
      case table: ResolvedInlineTable
          if table.rows.flatten.exists(SubExprUtils.containsOuter) =>
        rewriteTable(table)
    }.transformAllExpressionsWithPruning(
      _.containsPattern(org.apache.spark.sql.catalyst.trees.TreePattern.INLINE_TABLE_EVAL)) {
      case ls: LateralSubquery =>
        val newPlan = ls.plan.transformUp {
          case table: ResolvedInlineTable
              if table.rows.flatten.exists(SubExprUtils.containsOuter) =>
            rewriteTable(table)
        }
        ls.withNewPlan(newPlan)
    }
  }

  /**
   * Transforms a correlated [[ResolvedInlineTable]] into a [[Union]] of [[Project]] nodes.
   *
   * Each row becomes: Project([expr1 AS col1, expr2 AS col2, ...], OneRowRelation)
   * Multiple rows are combined with Union.
   */
  private def rewriteTable(table: ResolvedInlineTable): LogicalPlan = {
    val projects: Seq[LogicalPlan] = table.rows.map { row =>
      val aliases = row.zip(table.output).map { case (expr, attr) =>
        Alias(expr, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
      }
      Project(aliases, OneRowRelation())
    }

    if (projects.length == 1) {
      projects.head
    } else {
      projects.reduce((left: LogicalPlan, right: LogicalPlan) =>
        Union(Seq(left, right)))
    }
  }
}
