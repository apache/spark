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
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Catalog that hands out [[InMemoryScanMergingPartitionFilterTable]]s, an iterative-pushdown source
 * that additionally opts in to Spark-side scan merging. Used to exercise merging two DSv2 scans
 * whose (equal) filter is strict only via the iterative PartitionPredicate second pass.
 */
class InMemoryScanMergingPartitionFilterCatalog
  extends InMemoryTableEnhancedPartitionFilterCatalog {
  import CatalogV2Implicits._

  /**
   * The table this catalog hands out. Overridden by [[InMemoryScanMergingReportingCatalog]], which
   * shares the `createTable` body below and differs only in the table it creates.
   */
  protected def newScanMergingTable(
      tableName: String,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    new InMemoryScanMergingPartitionFilterTable(tableName, columns, partitions, properties)

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }
    InMemoryTableCatalog.maybeSimulateFailedTableCreation(properties)
    val tableName = s"$name.${ident.quoted}"
    val table = newScanMergingTable(tableName, columns, partitions, properties)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

/**
 * Like [[InMemoryScanMergingPartitionFilterCatalog]] but hands out tables that KEEP their reported
 * partitioning/ordering (no [[NonReportingScan]] wrapper), so a scan merge that must preserve the
 * reported key-grouped partitioning across the merge can be exercised.
 */
class InMemoryScanMergingReportingCatalog extends InMemoryScanMergingPartitionFilterCatalog {
  override protected def newScanMergingTable(
      tableName: String,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    new InMemoryScanMergingReportingTable(tableName, columns, partitions, properties)
}

/**
 * An [[InMemoryEnhancedPartitionFilterTable]] that opts into `TableCapability.SCAN_MERGING`, so
 * [[org.apache.spark.sql.execution.planmerging.PlanMerger]] may fuse two scans of it. It does NOT
 * strip the reported partitioning: a partitioned table's scan reports `KeyGroupedPartitioning` as
 * usual, so this is the fixture for checking that a merge preserves that report on the rebuilt
 * merged scan (re-derived by V2ScanPartitioningAndOrdering). It is also the base of
 * [[InMemoryScanMergingPartitionFilterTable]], which drops the report again.
 */
class InMemoryScanMergingReportingTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryEnhancedPartitionFilterTable(name, columns, partitioning, properties) {

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability](super.capabilities())
    caps.add(TableCapability.SCAN_MERGING)
    caps
  }
}

/**
 * An [[InMemoryScanMergingReportingTable]] whose scan is wrapped in a thin [[NonReportingScan]], so
 * a partitioned table does not set the scan relation's `keyGroupedPartitioning`, keeping this
 * fixture focused on the iterative-pushdown behavior under test. Preserving a reported partitioning
 * across a merge is exercised by the base [[InMemoryScanMergingReportingTable]] instead.
 */
class InMemoryScanMergingPartitionFilterTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryScanMergingReportingTable(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new InMemoryEnhancedPartitionFilterScanBuilder(schema()) {
      override def build(): Scan = NonReportingScan(super.build())
    }
}

/**
 * Thin scan decorator that exposes only `readSchema`, `toBatch` and `description`, dropping the
 * base scan's `SupportsReportPartitioning`/`SupportsReportStatistics`. So the scan relation carries
 * no reported partitioning/ordering/statistics -- for a partitioned table this keeps
 * `keyGroupedPartitioning` unset, so the fixture stays focused on pushdown; preserving reported
 * partitioning across a merge is exercised by [[InMemoryScanMergingReportingTable]].
 */
case class NonReportingScan(inner: Scan) extends Scan {
  override def readSchema(): StructType = inner.readSchema()
  override def toBatch: Batch = inner.toBatch
  override def description(): String = inner.description()
}
