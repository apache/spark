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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateTable}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.resolveColumnDefaultInAssignmentValue
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[UpdateTable]]. It's only used by the real
 * rule `ResolveReferences`. The column resolution order for [[UpdateTable]] is:
 * 1. Resolves the column to `AttributeReference` with the output of the child plan. This
 *    includes metadata columns as well.
 * 2. Resolves the column to a literal function which is allowed to be invoked without braces, e.g.
 *    `SELECT col, current_date FROM t`.
 * 3. Resolves the column to the default value expression, if the column is the assignment value
 *    and the corresponding assignment key is a top-level column.
 */
class ResolveReferencesInUpdate(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {

  def apply(u: UpdateTable): UpdateTable = {
    assert(u.table.resolved)
    if (u.resolved) return u

    val newAssignments = u.assignments.map { assign =>
      val resolvedKey = assign.key match {
        case c if !c.resolved =>
          resolveExprInAssignment(c, u)
        case o => o
      }
      val resolvedValue = assign.value match {
        case c if !c.resolved =>
          val resolved = resolveExprInAssignment(c, u)
          if (conf.enableDefaultColumns) {
            resolveColumnDefaultInAssignmentValue(
              resolvedKey,
              resolved,
              QueryCompilationErrors
                .defaultReferencesNotAllowedInComplexExpressionsInUpdateSetClause())
          } else {
            resolved
          }
        case o => o
      }
      val resolved = Assignment(resolvedKey, resolvedValue)
      resolved.copyTagsFrom(assign)
      resolved
    }

    val newUpdate = u.copy(
      assignments = newAssignments,
      condition = u.condition.map(resolveExpressionByPlanChildren(_, u)))
    newUpdate.copyTagsFrom(u)
    newUpdate
  }
}
