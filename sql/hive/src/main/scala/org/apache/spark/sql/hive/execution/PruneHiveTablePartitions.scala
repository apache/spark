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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, ExpressionSet, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

/**
 * Prune hive table partitions using partition filters on [[HiveTableRelation]]. The pruned
 * partitions will be kept in [[HiveTableRelation.prunedPartitions]], and the statistics of
 * the hive table relation will be updated based on pruned partitions.
 *
 * This rule is executed in optimization phase, so the statistics can be updated before physical
 * planning, which is useful for some spark strategy, e.g.
 * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection]].
 *
 * TODO: merge this with PruneFileSourcePartitions after we completely make hive as a data source.
 */
private[sql] class PruneHiveTablePartitions(session: SparkSession)
  extends Rule[LogicalPlan] with CastSupport with PredicateHelper {

  /**
   * Extract the partition filters from the filters on the table.
   */
  private def getPartitionKeyFilters(
      filters: Seq[Expression],
      relation: HiveTableRelation): ExpressionSet = {
    val normalizedFilters = DataSourceStrategy.normalizeExprs(
      filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f)), relation.output)
    val partitionColumnSet = AttributeSet(relation.partitionCols)
    ExpressionSet(
      normalizedFilters.flatMap(extractPredicatesWithinOutputSet(_, partitionColumnSet)))
  }

  /**
   * Update the statistics of the table.
   */
  private def updateTableMeta(
      relation: HiveTableRelation,
      prunedPartitions: Seq[CatalogTablePartition],
      partitionKeyFilters: ExpressionSet): CatalogTable = {
    val sizeOfPartitions = prunedPartitions.map { partition =>
      val rawDataSize = partition.parameters.get(StatsSetupConst.RAW_DATA_SIZE).map(_.toLong)
      val totalSize = partition.parameters.get(StatsSetupConst.TOTAL_SIZE).map(_.toLong)
      if (rawDataSize.isDefined && rawDataSize.get > 0) {
        rawDataSize.get
      } else if (totalSize.isDefined && totalSize.get > 0L) {
        totalSize.get
      } else {
        0L
      }
    }
    if (sizeOfPartitions.forall(_ > 0)) {
      val filteredStats =
        FilterEstimation(Filter(partitionKeyFilters.reduce(And), relation)).estimate
      val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
        (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
      })
      relation.tableMeta.copy(
        stats = Some(CatalogStatistics(
          sizeInBytes = BigInt(sizeOfPartitions.sum),
          rowCount = filteredStats.flatMap(_.rowCount),
          colStats = colStats.getOrElse(Map.empty))))
    } else {
      relation.tableMeta
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case op @ PhysicalOperation(projections, filters, relation: HiveTableRelation)
      if filters.nonEmpty && relation.isPartitioned && relation.prunedPartitions.isEmpty =>
      val partitionKeyFilters = getPartitionKeyFilters(filters, relation)
      if (partitionKeyFilters.nonEmpty) {
        val newPartitions = ExternalCatalogUtils.listPartitionsByFilter(conf,
          session.sessionState.catalog, relation.tableMeta, partitionKeyFilters.toSeq)
        val newTableMeta = updateTableMeta(relation, newPartitions, partitionKeyFilters)
        val newRelation = relation.copy(
          tableMeta = newTableMeta, prunedPartitions = Some(newPartitions))
        // Keep partition filters so that they are visible in physical planning
        Project(projections, Filter(filters.reduceLeft(And), newRelation))
      } else {
        op
      }
  }
}
