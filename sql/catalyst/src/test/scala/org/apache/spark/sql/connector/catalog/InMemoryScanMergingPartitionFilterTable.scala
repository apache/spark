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
    val table =
      new InMemoryScanMergingPartitionFilterTable(tableName, columns, partitions, properties)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

/**
 * An [[InMemoryEnhancedPartitionFilterTable]] that returns the `TableCapability.SCAN_MERGING`
 * capability, so [[org.apache.spark.sql.execution.planmerging.PlanMerger]] may fuse two scans of
 * this table. Its scan is wrapped in a thin [[NonReportingScan]] so a partitioned table does not
 * set the scan relation's `keyGroupedPartitioning` (whose preservation across a merge is a separate
 * follow-up); this keeps the fixture focused on the iterative-pushdown behavior under test.
 */
class InMemoryScanMergingPartitionFilterTable(
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

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new InMemoryEnhancedPartitionFilterScanBuilder(schema()) {
      override def build(): Scan = NonReportingScan(super.build())
    }
}

/**
 * Thin scan decorator that exposes only `readSchema`, `toBatch` and `description`, dropping the
 * base scan's `SupportsReportPartitioning`/`SupportsReportStatistics`. So the scan relation carries
 * no reported partitioning/ordering/statistics -- for a partitioned table this keeps
 * `keyGroupedPartitioning` unset, which the scan merge requires (preserving reported partitioning
 * across a merge is a separate follow-up).
 */
case class NonReportingScan(inner: Scan) extends Scan {
  override def readSchema(): StructType = inner.readSchema()
  override def toBatch: Batch = inner.toBatch
  override def description(): String = inner.description()
}
