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

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project, Statistics}
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
  type PrunedFileIndexLookUpKey = (CatalogFileIndex, Option[Expression])
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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val catalogFileIndexToCombinedPartitionFilter = mutable.Map[CatalogFileIndex, ExpressionSet]()
    val basisPartitionSchemaMapping = mutable.Map[CatalogFileIndex, Map[String, Attribute]]()
    val nonRepeatedEmptyFilterCFI = mutable.Set[CatalogFileIndex]()
    val step1Plan = plan transformDown {
      case op@PhysicalOperation(projects, filters,
      logicalRelation@
        LogicalRelation(fsRelation@
          HadoopFsRelation(
          cfi: CatalogFileIndex,
          partitionSchema,
          _,
          _,
          _,
          _),
        _,
        _,
        _))
        if fsRelation.partitionSchema.nonEmpty =>
        val normalizedFilters = DataSourceStrategy.normalizeExprs(
          filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f) &&
            DataSourceUtils.shouldPushFilter(f, fsRelation.fileFormat.supportsCollationPushDown)),
          logicalRelation.output)
        val (partitionKeyFilters, _) = DataSourceUtils
          .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)
        var cfiInserted = false
        val basisPartitionSchemaMap = basisPartitionSchemaMapping.getOrElseUpdate(cfi, {
          cfiInserted = true
          val lrOutput = logicalRelation.output
          cfi.table.partitionColumnNames.map(colName => colName.toLowerCase(Locale.ROOT) ->
            lrOutput.find(_.name.equalsIgnoreCase(colName)).get).toMap
        })

        val reducedPartitionFilters = partitionKeyFilters.reduceOption(And).
          map(expr => if (cfiInserted) {
            ExpressionSet(Seq(expr))
          } else {
            ExpressionSet(Seq(expr.transformUp {
              case attr: Attribute if basisPartitionSchemaMap.contains(
                attr.name.toLowerCase(Locale.ROOT)) =>
                basisPartitionSchemaMap(attr.name.toLowerCase(Locale.ROOT))
            }))
          }).getOrElse(ExpressionSet(Seq.empty))

        if (catalogFileIndexToCombinedPartitionFilter.contains(cfi) ||
          partitionKeyFilters.nonEmpty) {
          nonRepeatedEmptyFilterCFI -= cfi
        } else {
          nonRepeatedEmptyFilterCFI += cfi
        }

        if (partitionKeyFilters.nonEmpty) {
          // Keep partition-pruning predicates so that they are visible in physical planning
          val resultExprSetOpt = catalogFileIndexToCombinedPartitionFilter.get(cfi)
          // The currentExprSet may be empty which indicates that all the partitions are needed
          // so if a table is repeated and if either the currentExprSet is empty or
          // the reducedPartitionFilters of this node is empty, it would mean fetching all
          // partitions and we keep the empty ExpressionSet
          val newResultExprSet = resultExprSetOpt.map(currentExprSet =>
            if (currentExprSet.isEmpty) {
              currentExprSet
            } else {
              if (currentExprSet.head.canonicalized == reducedPartitionFilters.head.canonicalized) {
                currentExprSet
              } else {
                ExpressionSet(Seq(Or(currentExprSet.head, reducedPartitionFilters.head)))
              }
            }).getOrElse(reducedPartitionFilters)
          catalogFileIndexToCombinedPartitionFilter.put(cfi, newResultExprSet)

          val filteredStats =
            FilterEstimation(Filter(partitionKeyFilters.reduce(And), logicalRelation)).estimate
          val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
            (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
          })
          val logicalRelationWrapper = LogicalRelationWrapper(logicalRelation, cfi, fsRelation,
            reducedPartitionFilters, filteredStats, colStats)

          // Keep partition-pruning predicates so that they are visible in physical planning
          rebuildPhysicalOperation(projects, filters, logicalRelationWrapper)
        } else {
          val logicalRelationWrapper = LogicalRelationWrapper(logicalRelation, cfi, fsRelation,
            reducedPartitionFilters)
          catalogFileIndexToCombinedPartitionFilter.put(cfi, ExpressionSet(Seq.empty))
          op.transformUp {
            case _: LogicalRelation => logicalRelationWrapper
          }
        }
    }

    val finalPlan = if (catalogFileIndexToCombinedPartitionFilter.isEmpty) {
      step1Plan
    } else {
      val cfiToBasePFI: mutable.Map[PrunedFileIndexLookUpKey, FileIndex]
      = catalogFileIndexToCombinedPartitionFilter.flatMap {
        case (cfi, totalFilter) =>
          if (nonRepeatedEmptyFilterCFI.contains(cfi)) {
            Seq(((cfi, None), cfi))
          } else {
            val basePfi = cfi.filterPartitions(totalFilter.toSeq).
              asInstanceOf[InMemoryFileIndex]
            Seq(
              ((cfi, totalFilter.headOption.map(_.canonicalized)), basePfi),
              ((cfi, None), basePfi)
            )
          }
      }

      step1Plan.transformUp {
        case LogicalRelationWrapper(logicalRelation, cfi, fsRelation, specificFilterToApply,
        filteredStats, colStats) =>
          // first check from cachedPFI if we already have a suitable PrunedInMemoryFileIndex
          // if not we create one.
          val pfiLookUpKey: PrunedFileIndexLookUpKey =
            (cfi, specificFilterToApply.headOption.map(_.canonicalized))

          val prunedFileIndex = cfiToBasePFI.getOrElse(pfiLookUpKey, {
            val basePruneFileIndex = cfiToBasePFI(cfi -> None)
            val specificPFI = basePruneFileIndex.asInstanceOf[InMemoryFileIndex].
              applyFilters(specificFilterToApply.toSeq)
            cfiToBasePFI.put(pfiLookUpKey, specificPFI)
            specificPFI
          })

          val prunedFsRelation =
            fsRelation.copy(location = prunedFileIndex)(fsRelation.sparkSession)

          // Change table stats based on the sizeInBytes of pruned files
          if (specificFilterToApply.isEmpty) {
            logicalRelation.copy(relation = prunedFsRelation)
          } else {
            val withStats = logicalRelation.catalogTable.map(_.copy(
              stats = Some(CatalogStatistics(
                sizeInBytes = BigInt(prunedFileIndex.sizeInBytes),
                rowCount = filteredStats.flatMap(_.rowCount),
                colStats = colStats.getOrElse(Map.empty)))))
            val prunedLogicalRelation = logicalRelation.copy(
              relation = prunedFsRelation, catalogTable = withStats)
            prunedLogicalRelation
          }
      }
    }
    finalPlan
  }

  private case class LogicalRelationWrapper(
      lr: LogicalRelation,
      catalogFileIndex: CatalogFileIndex,
      fsRelation: HadoopFsRelation,
      partitionKeyFilter: ExpressionSet,
      filteredStats: Option[Statistics] = None,
      colStats: Option[Map[String, CatalogColumnStat]] = None) extends LeafNode {
    override def output: Seq[Attribute] = lr.output
  }
}
