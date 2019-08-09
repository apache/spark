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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.ReadDataSourceV2Relation
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.v2.reader.{ScanBuilder, SupportsPushDownFilters}

private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case v2Read @ ReadDataSourceV2Relation(
        _, builder: SupportsPushDownFilters, output, _, filters) =>
      val (withSubquery, withoutSubquery) = filters.partition(SubqueryExpression.hasSubquery)
      val normalizedFilters = DataSourceStrategy.normalizeFilters(
        withoutSubquery, output)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (_, postScanFiltersWithoutSubquery) =
        pushFilters(builder, normalizedFilters)
      val postScanFilters = postScanFiltersWithoutSubquery ++ withSubquery
      v2Read.copy(filters = postScanFilters)

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
        if filters.nonEmpty && fsRelation.partitionSchemaOption.isDefined =>
      val normalizedFilters = DataSourceStrategy.normalizeFilters(
        filters.filterNot(SubqueryExpression.hasSubquery), logicalRelation.output)

      val sparkSession = fsRelation.sparkSession
      val partitionColumns =
        logicalRelation.resolve(
          partitionSchema, sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters
          .filter(_.references.subsetOf(partitionSet)))

      if (partitionKeyFilters.nonEmpty) {
        val prunedFileIndex = catalogFileIndex.filterPartitions(partitionKeyFilters.toSeq)
        val prunedFsRelation =
          fsRelation.copy(location = prunedFileIndex)(sparkSession)
        // Change table stats based on the sizeInBytes of pruned files
        val withStats = logicalRelation.catalogTable.map(_.copy(
          stats = Some(CatalogStatistics(sizeInBytes = BigInt(prunedFileIndex.sizeInBytes)))))
        val prunedLogicalRelation = logicalRelation.copy(
          relation = prunedFsRelation, catalogTable = withStats)
        // Keep partition-pruning predicates so that they are visible in physical planning
        val filterExpression = filters.reduceLeft(And)
        val filter = Filter(filterExpression, prunedLogicalRelation)
        Project(projects, filter)
      } else {
        op
      }
  }

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  private def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    scanBuilder match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceStrategy.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr))
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }
}
