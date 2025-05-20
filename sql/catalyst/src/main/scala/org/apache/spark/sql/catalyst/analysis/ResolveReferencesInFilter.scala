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
import org.apache.spark.sql.connector.catalog.CatalogManager


/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[Filter]]. It's only used by the real
 * rules `ResolveReferences` and `ResolveTableConstraints`. Filters containing unresolved stars
 * should have been expanded before applying this rule.
 * Filter can host both grouping expressions/aggregate functions and missing attributes.
 * The grouping expressions/aggregate functions resolution takes precedence over missing
 * attributes. See the classdoc of `ResolveReferences` for details.
 */
 class ResolveReferencesInFilter(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {
  def apply(f: Filter): LogicalPlan = {
    if (f.condition.resolved && f.missingInput.isEmpty) {
      return f
    }
    val resolvedBasic = resolveExpressionByPlanChildren(f.condition, f)
    val resolvedWithAgg = resolveColWithAgg(resolvedBasic, f.child)
    val (newCond, newChild) = resolveExprsAndAddMissingAttrs(Seq(resolvedWithAgg), f.child)
    // Missing columns should be resolved right after basic column resolution.
    // See the doc of `ResolveReferences`.
    val resolvedFinal = resolveColsLastResort(newCond.head)
    if (f.child.output == newChild.output) {
      f.copy(condition = resolvedFinal)
    } else {
      // Add missing attributes and then project them away.
      val newFilter = Filter(resolvedFinal, newChild)
      Project(f.child.output, newFilter)
    }
  }

 }
