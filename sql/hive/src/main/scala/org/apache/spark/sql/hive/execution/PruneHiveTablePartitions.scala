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

import java.io.IOException

import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CommandUtils

/**
 * TODO: merge this with PruneFileSourcePartitions after we completely make hive as a data source.
 */
private[sql] class PruneHiveTablePartitions(session: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Extract the partition filters from the filters on the table.
   */
  private def extractPartitionPruningFilters(
              filters: Seq[Expression],
              relation: HiveTableRelation): Seq[Expression] = {
    val normalizedFilters = filters.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(relation.output.find(_.semanticEquals(a)).get.name)
      }
    }
    val partitionSet = AttributeSet(relation.partitionCols)
    normalizedFilters.filter { predicate =>
      !predicate.references.isEmpty && predicate.references.subsetOf(partitionSet)
    }
  }

  /**
   * Prune the hive table using filters on the partitions of the table,
   * and also update the statistics of the table.
   */
  private def prunedHiveTableWithStats(
              relation: HiveTableRelation,
              partitionFilters: Seq[Expression]): HiveTableRelation = {
    val conf = session.sessionState.conf
    val prunedPartitions = session.sessionState.catalog.listPartitionsByFilter(
      relation.tableMeta.identifier,
      partitionFilters)
    val sizeInBytes = try {
      val partitionsWithSize = prunedPartitions.map { part =>
        val rawDataSize = part.parameters.get(StatsSetupConst.RAW_DATA_SIZE).map(_.toLong)
        val totalSize = part.parameters.get(StatsSetupConst.TOTAL_SIZE).map(_.toLong)
        if (rawDataSize.isDefined && rawDataSize.get > 0) {
          (part, rawDataSize.get)
        } else if (totalSize.isDefined && totalSize.get > 0L) {
          (part, totalSize.get)
        } else {
          (part, 0L)
        }
      }
      val sizeOfPartitions =
        if (partitionsWithSize.count(_._2==0) <= conf.fallBackToHdfsForStatsMaxPartitionNum) {
          partitionsWithSize.map{ pair =>
            val (part, size) = (pair._1, pair._2)
            if (size == 0) {
              CommandUtils.calculateLocationSize(
                session.sessionState, relation.tableMeta.identifier, part.storage.locationUri)
            } else {
              size
            }
          }.sum
        } else {
          partitionsWithSize.filter(_._2>0).map(_._2).sum
        }
      // If size of partitions is zero fall back to the default size.
      if (sizeOfPartitions == 0L) conf.defaultSizeInBytes else sizeOfPartitions
    } catch {
      case e: IOException =>
        logWarning("Failed to get table size from HDFS.", e)
        conf.defaultSizeInBytes
    }
    val newTableMeta =
      if (relation.tableMeta.stats.isDefined) {
        relation.tableMeta.copy(
          stats = Some(relation.tableMeta.stats.get.copy(sizeInBytes = BigInt(sizeInBytes))))
      } else {
        relation.tableMeta.copy(stats = Some(CatalogStatistics(sizeInBytes = BigInt(sizeInBytes))))
      }
    relation.copy(tableMeta = newTableMeta, prunedPartitions = Some(prunedPartitions))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case op @ PhysicalOperation(projections, filters, relation: HiveTableRelation)
      if filters.nonEmpty && relation.isPartitioned && relation.prunedPartitions.isEmpty =>
      val partitionPruningFilters = extractPartitionPruningFilters(filters, relation)
      // SPARK-24085: subquery should be skipped for partition pruning
      val hasSubquery = partitionPruningFilters.exists(SubqueryExpression.hasSubquery)
      val conf = session.sessionState.conf
      if (conf.metastorePartitionPruning && partitionPruningFilters.nonEmpty && !hasSubquery) {
        val prunedHiveTable = prunedHiveTableWithStats(relation, partitionPruningFilters)
        val filterExpression = filters.reduceLeft(And)
        val filter = Filter(filterExpression, prunedHiveTable)
        Project(projections, filter)
      } else {
        op
      }
  }
}
