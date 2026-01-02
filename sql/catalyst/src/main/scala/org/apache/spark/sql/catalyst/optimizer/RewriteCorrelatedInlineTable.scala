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
 * Rewrites correlated [[ResolvedInlineTable]] (inline tables with outer references) into
 * [[Project]] + [[OneRowRelation]] + [[Union]] to enable decorrelation by the standard
 * subquery decorrelation framework.
 *
 * Note: This rule only processes correlated inline tables that made it past analysis.
 * If spark.sql.legacy.valuesGeneralizedExpressionsEnabled is false, such tables are
 * rejected during analysis, so this rule becomes a no-op.
 *
 * This transformation is necessary because:
 * 1. ResolvedInlineTable with OuterReference cannot be evaluated at planning time
 * 2. The decorrelation framework already handles Project with OuterReference
 * 3. This rewrite allows correlated LATERAL VALUES to work correctly
 *
 * Example transformation:
 * {{{
 *   Before:
 *     ResolvedInlineTable(
 *       rows = [[OuterReference(t.c1)], [OuterReference(t.c2)]],
 *       output = [col1]
 *     )
 *
 *   After:
 *     Union(
 *       Project([OuterReference(t.c1) AS col1], OneRowRelation),
 *       Project([OuterReference(t.c2) AS col1], OneRowRelation)
 *     )
 * }}}
 *
 * This rule should run before decorrelation (PullupCorrelatedPredicates).
 */
object RewriteCorrelatedInlineTable extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Note: No need to check config here. If config is disabled, we never reach here
    // with correlated tables (they're rejected during analysis).

    // Need to transform both the plan tree AND expressions containing plans (like LateralSubquery)
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

  private def rewriteTable(table: ResolvedInlineTable): LogicalPlan = {
    // Convert each row to a Project over OneRowRelation
    val projects: Seq[LogicalPlan] = table.rows.map { row =>
      val aliases = row.zip(table.output).map { case (expr, attr) =>
        // Create Alias with same name and exprId to preserve references
        Alias(expr, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
      }
      Project(aliases, OneRowRelation())
    }

    // Union all rows together
    // If only one row, return the Project directly
    if (projects.length == 1) {
      projects.head
    } else {
      projects.reduce((left: LogicalPlan, right: LogicalPlan) =>
        Union(Seq(left, right)))
    }
  }
}
