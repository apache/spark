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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}

/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[Filter]]. It's only used by the real
 * rule `ResolveReferences`. The column resolution order for [[Filter]] is:
 * 1. Resolves the column to [[AttributeReference]] with the output of the child plan. This
 *    includes metadata columns as well.
 * 2. Resolves the column to a literal function which is allowed to be invoked without braces, e.g.
 *    `SELECT col, current_date FROM t`.
 * 3. Resolves the column to [[AttributeReference]] with the output of a descendant plan node.
 *    Spark will propagate the missing attributes from the descendant plan node to the Filter node.
 *    This is to allow Filter to reference columns that are not in the child output. For example,
 *    `df.select($"a").filter($"b" > 0)`.
 * 4. If the child plan is Aggregate, resolves the column to [[TempResolvedColumn]] with the output
 *    of Aggregate's child plan. This is to allow Filter to host grouping expressions and aggregate
 *    functions, which can be pushed down to the Aggregate later. For example,
 *    `df.groupBy(...).agg(...).filter(sum($"a") > 0)`.
 * 6. Resolves the column to outer references with the outer plan if we are resolving subquery
 *    expressions.
 *
 * Note, 3 and 4 are actually orthogonal. If the child plan is Aggregate, 4 can only resolve columns
 * as the grouping columns, which is completely covered by 3. These 2 features were orginally added
 * for SQL HAVING, but later on HAVING has a dedicated logical plan `UnresolvedHaving`, and these
 * two features become DataFrame only.
 */
object ResolveReferencesInFilter extends SQLConfHelper with ColumnResolutionHelper {

  def apply(f: Filter): LogicalPlan = {
    val resolvedNoOuter = resolveExpressionByPlanChildren(f.condition, f)
    val (missingAttrResolved, newChild) = resolveExprsAndAddMissingAttrs(
      Seq(resolvedNoOuter), f.child)
    val resolvedWithAgg = resolveColWithAgg(missingAttrResolved.head, f.child)
    // Outer reference has lowermost priority. See the doc of `ResolveReferences`.
    val finalCond = resolveOuterRef(resolvedWithAgg)
    if (f.child.output == newChild.output) {
      rewriteAggregate(f.copy(condition = finalCond))
    } else {
      // Add missing attributes and then project them away.
      val newFilter = rewriteAggregate(Filter(finalCond, newChild))
      Project(f.child.output, newFilter)
    }
  }
}
