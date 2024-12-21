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

import java.util.Locale

import scala.collection.mutable

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
  // the expression value for lookup should be canonicalized
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

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Collect the set of partition filters for different `CatalogFileIndex`es.
    // We will need to `Or` these filters when we call list partitions to get the union of
    // partitions we need.
    val catalogFileIndexFilters = mutable.Map.empty[CatalogFileIndex, Option[ExpressionSet]]
    val attrMapForNormalization = mutable.Map[CatalogFileIndex, Map[String, Attribute]]()
    val singleOccurenceNoFilterCFI = mutable.Set[CatalogFileIndex]()
    val step1Plan = plan transformDown {
      case op @ PhysicalOperation(projects, filters,
      logicalRelation @ LogicalRelationWithTable(
      fsRelation @
        HadoopFsRelation(
        catalogFileIndex: CatalogFileIndex,
        partitionSchema,
        _,
        _,
        _,
        _),
      _)
      )
      if fsRelation.partitionSchema.nonEmpty =>
        val normalizedFilters = DataSourceStrategy.normalizeExprs(
          filters.filter { f =>
            f.deterministic &&
              !SubqueryExpression.hasSubquery(f) &&
              // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
              !f.exists(_.isInstanceOf[PythonUDF])
          },
          logicalRelation.output)
        val (partitionKeyFilters, _) = DataSourceUtils
          .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)
        val netPartitionFilter = partitionKeyFilters.reduceOption(And)
        val normalizedNetPartitionFilter = netPartitionFilter.map(expr => {
          attrMapForNormalization.get(catalogFileIndex).fold {
            val lrOutput = logicalRelation.output
            val attrMap = catalogFileIndex.table.partitionColumnNames.map(
              col =>
                col.toLowerCase(Locale.ROOT) ->
                  lrOutput.find(_.name.equalsIgnoreCase(col)).get).toMap
            attrMapForNormalization.put(catalogFileIndex, attrMap)
            expr
          }(mapping => {
            expr.transformUp {
              case attr: Attribute if mapping.contains(attr.name.toLowerCase(Locale.ROOT)) =>
                mapping(attr.name.toLowerCase(Locale.ROOT))
            }
          })
        })

        if (catalogFileIndexFilters.contains(catalogFileIndex) || partitionKeyFilters.nonEmpty) {
          singleOccurenceNoFilterCFI -= catalogFileIndex
        } else {
          singleOccurenceNoFilterCFI += catalogFileIndex
        }
        val prevFiltersOpt = catalogFileIndexFilters.getOrElseUpdate(catalogFileIndex,
          normalizedNetPartitionFilter.map(expr => ExpressionSet(Seq(expr))))
        // if the value returned above is None than nothing needs to be done as it means
        // fetch all partitions. This could be either the previous value present was None
        // or current partition filter is empty which would also result in None being returned
        prevFiltersOpt.foreach(prevExprSet => {
          val newValue = normalizedNetPartitionFilter.map(prevExprSet + _)
          catalogFileIndexFilters.put(catalogFileIndex, newValue)
        })
        val logicalRelationWrapper = PartitionedLogicalRelation(logicalRelation,
          netPartitionFilter, normalizedNetPartitionFilter)
        // Keep partition-pruning predicates so that they are visible in physical planning
        rebuildPhysicalOperation(projects, filters, logicalRelationWrapper)
    }
    applyPartitionPruning(catalogFileIndexFilters, step1Plan, singleOccurenceNoFilterCFI.toSet)
  }

  private def applyPartitionPruning(
      cfiToPartitionFilters: mutable.Map[CatalogFileIndex, Option[ExpressionSet]],
      step1Plan: LogicalPlan,
      nonRepeatedEmptyFilterCFI: Set[CatalogFileIndex]): LogicalPlan =
    if (cfiToPartitionFilters.isEmpty) {
      step1Plan
    } else {
      // Run partitions to get the union of partitions
      val tablePartitions = cfiToPartitionFilters.flatMap {
        case (catalogFileIndex, partitionFilters) =>
          if (nonRepeatedEmptyFilterCFI.contains(catalogFileIndex)) {
            Seq.empty
          } else {
            val unionedExprOpt = partitionFilters.map(exprSet => exprSet.reduce(Or))
            Seq(catalogFileIndex -> (catalogFileIndex.listPartitions(unionedExprOpt.map(Seq(_))
              .getOrElse(Seq.empty)), unionedExprOpt))
          }
      }

      step1Plan.transformUp {
        case PartitionedLogicalRelation(
          logicalRelation @ LogicalRelationWithTable(
          fsRelation @
            HadoopFsRelation(
            catalogFileIndex: CatalogFileIndex,
            _,
            _,
            _,
            _,
            _),
          _),
          partitionFilter,
          normalizedPartitionFilter) =>
          tablePartitions.get(catalogFileIndex) match {
            case Some(((catalogTablePartitions, baseTimeNs), unionedFilter)) =>
              val prunedFileIndex = catalogFileIndex.filterPartitions(catalogTablePartitions,
                baseTimeNs, partitionFilter.fold(Seq.empty[Expression])(pFilterExpr =>
                  if (unionedFilter == normalizedPartitionFilter) {
                    Seq.empty[Expression]
                  } else {
                    Seq(pFilterExpr)
                  }))
              // TODO: What shall we do with baseTimeNs?.
              //  Asif: I have added it to the diff
              val prunedFsRelation =
              fsRelation.copy(location = prunedFileIndex)(fsRelation.sparkSession)

              partitionFilter.fold(
                logicalRelation.copy(relation = prunedFsRelation))(partitionKeyFilter => {
                val filteredStats =
                  FilterEstimation(Filter(partitionKeyFilter, logicalRelation)).estimate
                val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
                  (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
                })
                val withStats = logicalRelation.catalogTable.map(_.copy(
                  stats = Option(CatalogStatistics(
                    sizeInBytes = BigInt(prunedFileIndex.sizeInBytes),
                    rowCount = filteredStats.flatMap(_.rowCount),
                    colStats = colStats.getOrElse(Map.empty)))))
                logicalRelation.copy(
                  relation = prunedFsRelation, catalogTable = withStats)
              })

            case _ => logicalRelation
          }
      }
    }

  private case class PartitionedLogicalRelation(
      logicalRelation: LogicalRelation,
      partitionFilter: Option[Expression],
      normalizedPartitionFilter: Option[Expression]) extends LeafNode {
    override def output: Seq[Attribute] = logicalRelation.output
  }
}
