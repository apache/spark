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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * In-memory table whose scan builder implements [[SupportsPushDownCatalystFilters]], so pushed
 * filters (including partition filters derived from generated partition columns) arrive as raw
 * Catalyst expressions. Filters that reference only partition columns are used to prune partitions;
 * the remaining data filters are returned for post-scan evaluation.
 */
class InMemoryTableWithCatalystFilter(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTable(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryCatalystFilterScanBuilder(schema())
  }

  class InMemoryCatalystFilterScanBuilder(tableSchema: StructType)
    extends ScanBuilder
    with SupportsPushDownCatalystFilters
    with SupportsPushDownRequiredColumns {

    private var readSchema: StructType = tableSchema
    private var pushedPartitionFilters: Seq[Expression] = Nil

    override def inferGeneratedColumnPartitionFilters: Boolean =
      properties.getOrDefault(
        InMemoryTableWithCatalystFilter.INFER_GENERATED_COLUMN_PARTITION_FILTERS, "true").toBoolean

    private val partitionColumnNames: Set[String] =
      partCols.map(_.mkString(".")).toSet

    private def referencesOnlyPartitionColumns(e: Expression): Boolean =
      e.references.nonEmpty && e.references.forall(a => partitionColumnNames.contains(a.name))

    override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
      val (partitionFilters, dataFilters) = filters.partition(referencesOnlyPartitionColumns)
      pushedPartitionFilters = partitionFilters
      // Data filters are evaluated after the scan; partition filters are consumed for pruning.
      dataFilters
    }

    override def pushedFilters: Array[Predicate] = Array.empty

    override def pruneColumns(requiredSchema: StructType): Unit = {
      readSchema = requiredSchema
    }

    override def build(): Scan = {
      val prunedPartitions = prunePartitions(pushedPartitionFilters)
      InMemoryCatalystFilterBatchScan(
        prunedPartitions.map(_.asInstanceOf[InputPartition]),
        readSchema,
        tableSchema,
        pushedPartitionFilters)
    }

    private def prunePartitions(filters: Seq[Expression]): Seq[BufferedRows] = {
      val allPartitions = data.toSeq
      if (filters.isEmpty) {
        return allPartitions
      }
      val nameToOrdinal = partCols.map(_.mkString(".")).zipWithIndex.toMap
      val predicates = filters.flatMap { filter =>
        val bound = filter.transformUp {
          case a: AttributeReference if nameToOrdinal.contains(a.name) =>
            BoundReference(nameToOrdinal(a.name), a.dataType, a.nullable)
        }
        // Only evaluate filters we could fully bind against the partition key.
        if (bound.references.isEmpty) Some(CatalystPredicate.createInterpreted(bound)) else None
      }
      allPartitions.filter(part => predicates.forall(_.eval(part.partitionKey())))
    }
  }

  /** Batch scan that records the Catalyst partition filters pushed to it. */
  case class InMemoryCatalystFilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      pushedPartitionFilters: Seq[Expression])
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
}

object InMemoryTableWithCatalystFilter {
  /**
   * Table property controlling whether the scan builder opts in to deriving generated column
   * partition filters via `inferGeneratedColumnPartitionFilters`. Defaults to `true`.
   */
  val INFER_GENERATED_COLUMN_PARTITION_FILTERS = "infer.generated.column.partition.filters"
}

class InMemoryTableWithCatalystFilterCatalog extends InMemoryTableCatalog {
  import CatalogV2Implicits._

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }
    InMemoryTableCatalog.maybeSimulateFailedTableCreation(tableInfo.properties)
    val tableName = s"$name.${ident.quoted}"
    val table = new InMemoryTableWithCatalystFilter(
      tableName, tableInfo.columns(), tableInfo.partitions(), tableInfo.properties)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}
