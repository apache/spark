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
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder, SupportsScanMerging}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Catalog that hands out [[InMemoryScanMergingPartitionFilterTable]]s, an iterative-pushdown source
 * that additionally opts its scans in to Spark-side scan merging. Used to exercise the merge of two
 * DSv2 scans whose (equal) filter is strict only via the iterative PartitionPredicate second pass.
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
 * An [[InMemoryEnhancedPartitionFilterTable]] whose built scans also implement
 * [[SupportsScanMerging]], so [[org.apache.spark.sql.execution.planmerging.PlanMerger]] may fuse
 * two scans of this table. The scan is wrapped in a thin [[ScanMergingWrapperScan]] rather than
 * subclassing the base batch scan: the wrapper adds the merge marker and deliberately does not
 * re-expose `SupportsReportPartitioning`, so a partitioned table does not set the scan relation's
 * `keyGroupedPartitioning` (whose preservation across a merge is a separate follow-up). This keeps
 * the fixture focused on the iterative-pushdown behavior under test.
 */
class InMemoryScanMergingPartitionFilterTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryEnhancedPartitionFilterTable(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new InMemoryEnhancedPartitionFilterScanBuilder(schema()) {
      override def build(): Scan = ScanMergingWrapperScan(super.build())
    }
}

/**
 * Thin scan decorator that adds the [[SupportsScanMerging]] marker and delegates reading to the
 * wrapped scan's batch. It exposes only `readSchema` and `toBatch`, so the scan relation carries no
 * reported partitioning/ordering/statistics -- exactly the shape [[SupportsScanMerging]] promises
 * is reproducible from a fresh scan builder.
 */
case class ScanMergingWrapperScan(inner: Scan) extends Scan with SupportsScanMerging {
  override def readSchema(): StructType = inner.readSchema()
  override def toBatch: Batch = inner.toBatch
  override def description(): String = inner.description()
}
