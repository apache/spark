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

import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DF_DROP_COLUMNS
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * A rule that rewrites DataFrameDropColumns to Project.
 * Note that DataFrameDropColumns allows and ignores non-existing columns.
 */
class ResolveDataFrameDropColumns(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with ColumnResolutionHelper  {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(DF_DROP_COLUMNS)) {
    case d: DataFrameDropColumns if d.childrenResolved =>
      // expressions in dropList can be unresolved, e.g.
      //   df.drop(col("non-existing-column"))
      val dropped = d.dropList.map {
        case u: UnresolvedAttribute =>
          resolveExpressionByPlanChildren(u, d)
        case e => e
      }
      val remaining = d.child.output.filterNot(attr => dropped.exists(_.semanticEquals(attr)))
      if (remaining.size == d.child.output.size) {
        d.child
      } else {
        Project(remaining, d.child)
      }
  }
}
