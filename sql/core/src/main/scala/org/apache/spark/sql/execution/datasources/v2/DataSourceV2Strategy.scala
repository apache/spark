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

import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}

object DataSourceV2Strategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val projectSet = AttributeSet(project.flatMap(_.references))
      val filterSet = AttributeSet(filters.flatMap(_.references))

      val projection = if (filterSet.subsetOf(projectSet) &&
          AttributeSet(relation.output) == projectSet) {
        // When the required projection contains all of the filter columns and column pruning alone
        // can produce the required projection, push the required projection.
        // A final projection may still be needed if the data source produces a different column
        // order or if it cannot prune all of the nested columns.
        relation.output
      } else {
        // When there are filter columns not already in the required projection or when the required
        // projection is more complicated than column pruning, base column pruning on the set of
        // all columns needed by both.
        (projectSet ++ filterSet).toSeq
      }

      val reader = relation.newReader

      val output = DataSourceV2Relation.pushRequiredColumns(relation, reader,
        projection.asInstanceOf[Seq[AttributeReference]].toStructType)

      val (postScanFilters, pushedFilters) = DataSourceV2Relation.pushFilters(reader, filters)

      logInfo(s"Post-Scan Filters: ${postScanFilters.mkString(",")}")
      logInfo(s"Pushed Filters: ${pushedFilters.mkString(", ")}")

      val scan = DataSourceV2ScanExec(
        output, relation.source, relation.options, pushedFilters, reader)

      val filter = postScanFilters.reduceLeftOption(And)
      val withFilter = filter.map(execution.FilterExec(_, scan)).getOrElse(scan)

      val withProjection = if (withFilter.output != project) {
        execution.ProjectExec(project, withFilter)
      } else {
        withFilter
      }

      withProjection :: Nil

    case r: StreamingDataSourceV2Relation =>
      DataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader) :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case _ => Nil
  }
}
