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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

object PushDownOperatorsToDataSource extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // PhysicalOperation guarantees that filters are deterministic; no need to check
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      assert(relation.filters.isEmpty, "data source v2 should do push down only once.")

      val projectAttrs = project.map(_.toAttribute)
      val projectSet = AttributeSet(project.flatMap(_.references))
      val filterSet = AttributeSet(filters.flatMap(_.references))

      val projection = if (filterSet.subsetOf(projectSet) &&
          AttributeSet(projectAttrs) == projectSet) {
        // When the required projection contains all of the filter columns and column pruning alone
        // can produce the required projection, push the required projection.
        // A final projection may still be needed if the data source produces a different column
        // order or if it cannot prune all of the nested columns.
        projectAttrs
      } else {
        // When there are filter columns not already in the required projection or when the required
        // projection is more complicated than column pruning, base column pruning on the set of
        // all columns needed by both.
        (projectSet ++ filterSet).toSeq
      }

      val newRelation = relation.copy(
        projection = projection.asInstanceOf[Seq[AttributeReference]],
        filters = Some(filters))

      // Add a Filter for any filters that need to be evaluated after scan.
      val postScanFilterCond = newRelation.postScanFilters.reduceLeftOption(And)
      val filtered = postScanFilterCond.map(Filter(_, newRelation)).getOrElse(newRelation)

      // Add a Project to ensure the output matches the required projection
      if (newRelation.output != projectAttrs) {
        Project(project, filtered)
      } else {
        filtered
      }

    case other => other.mapChildren(apply)
  }
}
