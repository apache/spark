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

import org.apache.spark.sql.catalyst.expressions.EvalHelper
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, OverwriteByExpression, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.INLINE_TABLE_EVAL
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable

/**
 * An analyzer rule that replaces [[UnresolvedInlineTable]] with [[ResolvedInlineTable]].
 */
object ResolveInlineTables extends Rule[LogicalPlan] with EvalHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(INLINE_TABLE_EVAL), ruleId) {
      // For INSERT, ignore collation differences in inline table columns since the INSERT
      // coercion will cast to the target column's collation.
      // Preserve the inline table's origin so error messages point to the VALUES clause,
      // not the INSERT statement.
      case i @ InsertIntoStatement(_, _, _, table: UnresolvedInlineTable, _, _, _, _, _)
          if table.expressionsResolved =>
        CurrentOrigin.withOrigin(table.origin) {
          i.copy(query = EvaluateUnresolvedInlineTable
            .evaluateUnresolvedInlineTable(table, ignoreCollation = true))
        }
      case i @ InsertIntoStatement(
          _, _, _, sa @ SubqueryAlias(_, table: UnresolvedInlineTable), _, _, _, _, _)
          if table.expressionsResolved =>
        CurrentOrigin.withOrigin(table.origin) {
          i.copy(query = sa.copy(child = EvaluateUnresolvedInlineTable
            .evaluateUnresolvedInlineTable(table, ignoreCollation = true)))
        }
      case w @ OverwriteByExpression(_, _, table: UnresolvedInlineTable, _, _, _, _, _)
          if table.expressionsResolved =>
        CurrentOrigin.withOrigin(table.origin) {
          w.copy(query = EvaluateUnresolvedInlineTable
            .evaluateUnresolvedInlineTable(table, ignoreCollation = true))
        }
      case w @ OverwriteByExpression(
          _, _, sa @ SubqueryAlias(_, table: UnresolvedInlineTable), _, _, _, _, _)
          if table.expressionsResolved =>
        CurrentOrigin.withOrigin(table.origin) {
          w.copy(query = sa.copy(child = EvaluateUnresolvedInlineTable
            .evaluateUnresolvedInlineTable(table, ignoreCollation = true)))
        }
      case table: UnresolvedInlineTable if table.expressionsResolved =>
        EvaluateUnresolvedInlineTable.evaluateUnresolvedInlineTable(table)
    }
  }
}
