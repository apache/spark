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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, BindReferences, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.types.BooleanType

/**
 * TODO: merge this with PruneFileSourcePartitions after we completely make hive as a data source.
 */
private[sql] class PruneHiveTablePartitions(session: SparkSession)
  extends Rule[LogicalPlan] with CastSupport {

  override val conf = session.sessionState.conf

  /**
   * Extract the partition filters from the filters on the table.
   */
  private def getPartitionKeyFilters(
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
   * Prune the hive table using filters on the partitions of the table.
   */
  private def prunePartitions(
      relation: HiveTableRelation,
      partitionFilters: Seq[Expression]): Seq[CatalogTablePartition] = {
    val partitions =
    if (conf.metastorePartitionPruning) {
      session.sessionState.catalog.listPartitionsByFilter(
        relation.tableMeta.identifier, partitionFilters)
    } else {
      session.sessionState.catalog.listPartitions(relation.tableMeta.identifier)
    }
    val shouldKeep = partitionFilters.reduceLeftOption(And).map { filter =>
      require(filter.dataType == BooleanType,
        s"Data type of predicate $filter must be ${BooleanType.catalogString} rather than " +
          s"${filter.dataType.catalogString}.")
      BindReferences.bindReference(filter, relation.partitionCols)
    }
    if (shouldKeep.nonEmpty) {
      partitions.filter{ partition =>
        val hivePartition =
          HiveClientImpl.toHivePartition(partition, HiveClientImpl.toHiveTable(relation.tableMeta))
        val dataTypes = relation.partitionCols.map(_.dataType)
        val castedValues = hivePartition.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => cast(Literal(value), dataType).eval(null) }
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.get.eval(row).asInstanceOf[Boolean]
      }
    } else {
      partitions
    }
  }

  /**
   * Update the statistics of the table.
   */
  private def updateTableMeta(
      tableMeta: CatalogTable,
      prunedPartitions: Seq[CatalogTablePartition]): CatalogTable = {
    val sizeOfPartitions = try {
      prunedPartitions.map { partition =>
        val rawDataSize = partition.parameters.get(StatsSetupConst.RAW_DATA_SIZE).map(_.toLong)
        val totalSize = partition.parameters.get(StatsSetupConst.TOTAL_SIZE).map(_.toLong)
        if (rawDataSize.isDefined && rawDataSize.get > 0) {
          rawDataSize.get
        } else if (totalSize.isDefined && totalSize.get > 0L) {
          totalSize.get
        } else {
          0L
        }
      }.sum
    } catch {
      case e: IOException =>
        logWarning("Failed to get table size from HDFS.", e)
        conf.defaultSizeInBytes
    }
    // If size of partitions is zero fall back to the default size.
    val sizeInBytes = if (sizeOfPartitions == 0L) conf.defaultSizeInBytes else sizeOfPartitions
    tableMeta.copy(stats = Some(CatalogStatistics(sizeInBytes = BigInt(sizeInBytes))))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case op @ PhysicalOperation(projections, filters, relation: HiveTableRelation)
      if filters.nonEmpty && relation.isPartitioned && relation.prunedPartitions.isEmpty =>
      val partitionPruningFilters = getPartitionKeyFilters(filters, relation)
      // SPARK-24085: subquery should be skipped for partition pruning
      val hasSubquery = partitionPruningFilters.exists(SubqueryExpression.hasSubquery)
      if (partitionPruningFilters.nonEmpty && !hasSubquery) {
        val newPartitions = prunePartitions(relation, partitionPruningFilters)
        val newTableMeta = updateTableMeta(relation.tableMeta, newPartitions)
        val newRelation = relation.copy(
          tableMeta = newTableMeta, prunedPartitions = Some(newPartitions))
        Project(projections, Filter(filters.reduceLeft(And), newRelation))
      } else {
        op
      }
  }
}
