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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Prune the partitions of file source based table using partition filters. Currently, this rule
 * is applied to [[HadoopFsRelation]] with [[CatalogFileIndex]].
 *
 * For [[HadoopFsRelation]], the location will be replaced by pruned file index, and corresponding
 * statistics will be updated. And the partition filters will be kept in the filters of returned
 * logical plan.
 */
private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {

  private def rebuildPhysicalOperation(
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: LeafNode): Project = {
    val withFilter = if (filters.nonEmpty) {
      val filterExpression = filters.reduceLeft(And)
      Filter(filterExpression, relation)
    } else {
      relation
    }
    Project(projects, withFilter)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op @ PhysicalOperation(projects, filters,
        logicalRelation @
          LogicalRelation(fsRelation @
            HadoopFsRelation(
              catalogFileIndex: CatalogFileIndex,
              partitionSchema,
              _,
              _,
              _,
              _),
            _,
            _,
            _))
        if filters.nonEmpty && fsRelation.partitionSchema.nonEmpty =>
      val normalizedFilters = DataSourceStrategy.normalizeExprs(
        filters.filter(f =>
          !SubqueryExpression.hasSubquery(f) && DataSourceUtils.shouldPushFilter(f)),
        logicalRelation.output)
      val (partitionKeyFilters, _) = DataSourceUtils
        .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)

      if (partitionKeyFilters.nonEmpty) {
        val prunedFileIndex = catalogFileIndex.filterPartitions(partitionKeyFilters)
        val prunedFsRelation =
          fsRelation.copy(location = prunedFileIndex)(fsRelation.sparkSession)
        // Change table stats based on the sizeInBytes of pruned files
        val filteredStats =
          FilterEstimation(Filter(partitionKeyFilters.reduce(And), logicalRelation)).estimate
        val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
          (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
        })
        val withStats = logicalRelation.catalogTable.map(_.copy(
          stats = Some(CatalogStatistics(
            sizeInBytes = BigInt(prunedFileIndex.sizeInBytes),
            rowCount = filteredStats.flatMap(_.rowCount),
            colStats = colStats.getOrElse(Map.empty)))))
        val prunedLogicalRelation = logicalRelation.copy(
          relation = prunedFsRelation, catalogTable = withStats)
        // Keep partition-pruning predicates so that they are visible in physical planning
        rebuildPhysicalOperation(projects, filters, prunedLogicalRelation)
      } else {
        op
      }
  }
}
