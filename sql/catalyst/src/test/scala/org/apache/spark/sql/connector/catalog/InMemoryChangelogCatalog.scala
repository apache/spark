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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An [[InMemoryTableCatalog]] that declares [[TableCatalogCapability.SUPPORT_CHANGELOG]]
 * and implements [[TableCatalog.loadChangelog()]].
 *
 * Change rows can be pre-populated via [[addChangeRows()]] before querying.
 */
class InMemoryChangelogCatalog extends InMemoryTableCatalog {

  // tableName -> list of change rows (each row: Array[Any] matching changelog schema)
  private val changeData: mutable.Map[String, mutable.ArrayBuffer[InternalRow]] =
    mutable.Map.empty

  override def capabilities: java.util.Set[TableCatalogCapability] = {
    val caps = new java.util.HashSet(super.capabilities)
    caps.add(TableCatalogCapability.SUPPORT_CHANGELOG)
    caps
  }

  override def loadChangelog(
      ident: Identifier,
      changelogInfo: ChangelogInfo): Changelog = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
    val table = loadTable(ident)
    val rows = changeData.getOrElse(
      ident.toString, mutable.ArrayBuffer.empty)
    new InMemoryChangelog(
      table.name + "_changelog", table.columns, rows.toSeq)
  }

  /**
   * Add change rows for a table. Each row should match the changelog schema:
   * (data columns..., _change_type STRING, _commit_version LONG, _commit_timestamp LONG).
   */
  def addChangeRows(ident: Identifier, rows: Seq[InternalRow]): Unit = {
    val buf = changeData.getOrElseUpdate(
      ident.toString, mutable.ArrayBuffer.empty)
    buf ++= rows
  }

  def clearChangeRows(ident: Identifier): Unit = {
    changeData.remove(ident.toString)
  }
}

/**
 * A test [[Changelog]] that returns pre-populated change rows.
 *
 * Reports `containsCarryoverRows = false` so Spark skips carry-over removal.
 */
class InMemoryChangelog(
    tableName: String,
    dataColumns: Array[Column],
    changeRows: Seq[InternalRow]) extends Changelog {

  private val cdcColumns: Array[Column] = dataColumns ++ Array(
    Column.create("_change_type", StringType),
    Column.create("_commit_version", LongType),
    Column.create("_commit_timestamp", TimestampType))

  override def name(): String = tableName

  override def columns(): Array[Column] = cdcColumns

  override def containsCarryoverRows(): Boolean = false

  override def containsIntermediateChanges(): Boolean = false

  override def representsUpdateAsDeleteAndInsert(): Boolean = false

  override def newScanBuilder(
      options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryChangelogScanBuilder(readSchema, changeRows)
  }

  def readSchema: StructType = {
    CatalogV2Util.v2ColumnsToStructType(cdcColumns)
  }
}

private class InMemoryChangelogScanBuilder(
    schema: StructType,
    rows: Seq[InternalRow]) extends ScanBuilder {
  override def build(): Scan =
    new InMemoryChangelogScan(schema, rows)
}

private class InMemoryChangelogScan(
    schema: StructType,
    rows: Seq[InternalRow]) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(InMemoryChangelogPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new InMemoryChangelogReaderFactory()
  }
}

private case class InMemoryChangelogPartition(
    rows: Seq[InternalRow]) extends InputPartition

private class InMemoryChangelogReaderFactory
    extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    new InMemoryChangelogReader(
      partition.asInstanceOf[InMemoryChangelogPartition])
  }
}

private class InMemoryChangelogReader(
    partition: InMemoryChangelogPartition)
    extends PartitionReader[InternalRow] {

  private var index = -1
  private val rows = partition.rows

  override def next(): Boolean = {
    index += 1
    index < rows.size
  }

  override def get(): InternalRow = rows(index)

  override def close(): Unit = {}
}
